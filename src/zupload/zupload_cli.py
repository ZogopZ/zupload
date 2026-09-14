# Standard library imports.
from pathlib import Path
from pprint import pprint
from typing import Any, Literal
import json
import ast
import re
from collections import defaultdict
from datetime import datetime as dt
from urllib.parse import urlparse, urlunparse
# Related third party imports.
import numpy as np
import pandas as pd
import pyproj
import xarray
from halo import Halo
from openpyxl import Workbook, load_workbook
from openpyxl.pivot.fields import Boolean
from pandas import Series
import typer
import requests
# Local application/library specific imports.
from zupload.utils import (
    calculate_hashsum,
    get_conf,
    get_cookie_jar,
    write_json,
    get_prev_by_name,
    get_dataset_type
)
from zupload.cli_shared import resolve_spreadsheet, select_rows
from zupload.constants.envri import DatasetType, EnvriConfig, ICOS_CONFIG
from zupload.constants.object_specs import ALL_OBJECT_SPECS
from zupload.constants.organizations import (
    ORG_DISPLAY_NAMES,
    ORG_LU_CITIES,
)
from zupload.constants.stations import CITIES_FOR_STATION
from zupload.constants.upload_descriptions import CITIES_UPLOAD_DESCRIPTIONS
from zupload.logs import RunLogger
from zupload.metadata_fetch import _object_id
from zupload.validation import (
    LANDING_URL_ISSUE,
    validate_columns,
    validate_dataframe,
)


app = typer.Typer(help='Upload data & metadata to the specific portal.')


def _portal_display_name(envri_name: str) -> str:
    return {
        'ICOSCities': 'cities',
        'ICOS': 'icos',
        'SITES': 'sites',
    }.get(envri_name, envri_name.lower())


def _is_blank(value: Any) -> bool:
    """Return True when a spreadsheet cell is missing, NaN, or whitespace only."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        return False
    return not str(value).strip()


def _ids_for_cell(value: Any) -> str | None:
    """Rewrite an isNextVersionOf cell as object ids, or return None to leave it alone.

    The portal rejects a landing page URL in isNextVersionOf with HTTP 400; it wants the
    object id at the end of that URL. The cell shape is preserved: a single URI becomes
    a single bare id, a JSON list of URIs stays a JSON list. A cell that already holds
    ids is returned as None so the caller skips it and the workbook is left untouched.
    """
    raw = str(value).strip()
    if raw.startswith('['):
        try:
            parsed = json.loads(raw)
        except (ValueError, TypeError):
            return None
        if not isinstance(parsed, list):
            return None
        items = [str(item).strip() for item in parsed]
        if not any(item.startswith(('http://', 'https://')) for item in items):
            return None
        return json.dumps([_object_id(item) for item in items])
    if not raw.startswith(('http://', 'https://')):
        return None
    return _object_id(raw)


def _fix_next_version_ids(df, spreadsheet: str | Path) -> tuple[int, int, Any]:
    """Rewrite landing page URLs in isNextVersionOf as object ids, in the spreadsheet.

    Mirrors the --data-dir path: the DataFrame is corrected so the rest of the run sees
    the fixed values, and the same cells are written back into the workbook. When there
    is nothing to convert the workbook is not opened at all.
    """
    if 'isNextVersionOf' not in df.columns:
        return 0, 0, df
    updates: list[tuple[Any, str]] = []
    for idx, value in df['isNextVersionOf'].items():
        if _is_blank(value):
            continue
        fixed = _ids_for_cell(value)
        if fixed is None:
            continue
        updates.append((idx, fixed))
    if not updates:
        return 0, 0, df
    # A row selection hands back a slice, so copy before writing into it.
    df = df.copy()
    df['isNextVersionOf'] = df['isNextVersionOf'].astype(object)
    for idx, fixed in updates:
        df.at[idx, 'isNextVersionOf'] = fixed
    wb = load_workbook(spreadsheet)
    ws = wb['upload_meta']
    headers = {cell.value: i for i, cell in enumerate(ws[1], start=1)}
    column = headers.get('isNextVersionOf')
    if column is None:
        raise ValueError('isNextVersionOf column is missing from the upload_meta sheet')
    for idx, fixed in updates:
        ws.cell(row=idx + 2, column=column).value = fixed
    wb.save(spreadsheet)
    return len(updates), len({idx for idx, _ in updates}), df


def _detect_dataset_type(df, envri_conf: EnvriConfig) -> DatasetType:
    """Detect the specificInfo shape once per run from the first object specification."""
    spec = None
    if 'objectSpecification' in df.columns:
        for value in df['objectSpecification']:
            if not _is_blank(value):
                spec = str(value).strip()
                break
    if spec is None:
        typer.echo('No objectSpecification found; assuming spatioTemporal metadata.')
        return 'spatioTemporal'
    try:
        dataset_type = get_dataset_type(
            object_spec=spec,
            portal=_portal_display_name(envri_conf.envri)
        )
    except Exception as e:
        typer.echo(f'Dataset type lookup failed ({e}); assuming spatioTemporal metadata.')
        return 'spatioTemporal'
    if dataset_type is None:
        typer.echo(
            'Dataset type unknown for the object specification; '
            'assuming spatioTemporal metadata.'
        )
        return 'spatioTemporal'
    typer.echo(f'Dataset type: {dataset_type}')
    return dataset_type


def _resolve_known_specs(df, envri_conf: EnvriConfig) -> set[str]:
    """Return the sheet's object specifications that the portal itself recognises."""
    known: set[str] = set()
    if 'objectSpecification' not in df.columns:
        return known
    portal = _portal_display_name(envri_conf.envri)
    seen: set[str] = set()
    for value in df['objectSpecification']:
        if _is_blank(value):
            continue
        spec = str(value).strip()
        if spec in seen:
            continue
        seen.add(spec)
        try:
            resolved = get_dataset_type(object_spec=spec, portal=portal)
        except Exception:
            # The portal is unreachable; fall back to the local spec list only.
            return known
        if resolved is not None:
            known.add(spec)
    return known


def _to_landing_uri(pid: str) -> str:
    pid = pid.strip()
    if pid.startswith('http://') or pid.startswith('https://'):
        return pid
    return f'https://meta.icos-cp.eu/objects/{pid}'


@app.command()
def fetch(
        pid: str = typer.Argument(
            ...,
            metavar='PID',
            help='Landing page URL, object id, or object hash.'
        )
):
    """Fetch and print dataset metadata from the ICOS portal by landing page URL, object id, or hash."""
    landing_uri = _to_landing_uri(pid)
    typer.echo(f'Fetching metadata for: {landing_uri}')
    resp = requests.get(
        'https://meta.icos-cp.eu/dtodownload',
        params={'uri': landing_uri},
    )
    if resp.status_code != 200:
        typer.echo(f'Fetch failed ({resp.status_code})')
        typer.echo(resp.text)
        raise typer.Exit(code=1)
    try:
        payload = resp.json()
    except ValueError:
        try:
            payload = json.loads(resp.text)
        except json.JSONDecodeError:
            payload = ast.literal_eval(resp.text)
    pprint(payload)

@app.command()
def validate(
        spreadsheet: str | None = typer.Option(
            None,
            '--spreadsheet',
            help='Spreadsheet to use (.xlsx). Auto-detected if the current directory has a single .xlsx.'
        ),
        rows: str | None = typer.Option(
            None,
            '--rows',
            help='Validate a single row or a contiguous range by upload_meta sheet row numbers, e.g. "5" or "5-12" (inclusive on both ends).'
        ),
        data_dir: str | None = typer.Option(
            None,
            '--data-dir',
            help='Locate each data file by name under this directory, then fill in fileLocation and hashSum where resolvable and update the spreadsheet in place. A backup is written under ./logs/.'
        ),
        fix_ids: bool = typer.Option(
            False,
            '--fix-ids',
            help='Convert landing page URLs in isNextVersionOf to the object ids the portal expects and update the spreadsheet in place. A backup is written under ./logs/.'
        )
):
    """Check upload_meta rows for metadata problems without uploading or changing the spreadsheet."""
    spreadsheet = resolve_spreadsheet(spreadsheet)
    df = pd.read_excel(spreadsheet, sheet_name='upload_meta')
    try:
        envri_conf = get_conf(file_path=spreadsheet)
    except typer.Exit:
        typer.echo('Using icos for the dataset type lookup.')
        envri_conf = ICOS_CONFIG
    known_specs = _resolve_known_specs(df, envri_conf)
    dataset_type = _detect_dataset_type(df, envri_conf)
    schema_issues = validate_columns(df, dataset_type=dataset_type)
    resolved = 0
    ambiguous = 0
    not_found = 0
    fixed_cells = 0
    fixed_rows = 0
    data_root: Path | None = None
    run_logger: RunLogger | None = None
    if data_dir is not None:
        data_root = Path(data_dir)
        if not data_root.is_dir():
            typer.echo(f'--data-dir not found or not a directory: {data_dir}')
            raise typer.Exit(code=1)
    # Both --data-dir and --fix-ids rewrite the workbook, so one logger covers the run:
    # its before copy predates every edit and its after copy follows all of them.
    if data_dir is not None or fix_ids:
        run_logger = RunLogger.start(spreadsheet=spreadsheet)
    if data_root is not None:
        try:
            name_to_paths: dict[str, list[Path]] = {}
            for candidate in data_root.rglob('*'):
                if candidate.is_file():
                    name_to_paths.setdefault(candidate.name, []).append(candidate)
            for col_name in ('fileLocation', 'hashSum'):
                if col_name in df.columns:
                    df[col_name] = df[col_name].astype(object)
            updates = []
            for idx, row in df.iterrows():
                file_name = row.get('fileName')
                if pd.isna(file_name) or not str(file_name).strip():
                    continue
                file_name = str(file_name).strip()
                target = None
                file_location = row.get('fileLocation')
                if not pd.isna(file_location) and str(file_location).strip():
                    candidate = Path(str(file_location).strip()) / file_name
                    if candidate.exists():
                        target = candidate
                if target is None:
                    matches = name_to_paths.get(file_name, [])
                    if len(matches) == 1:
                        target = matches[0]
                    elif len(matches) > 1:
                        ambiguous += 1
                        continue
                    else:
                        not_found += 1
                        continue
                new_loc = str(target.parent.resolve())
                df.at[idx, 'fileLocation'] = new_loc
                updates.append((idx + 2, 'fileLocation', new_loc))
                hash_sum = row.get('hashSum')
                if pd.isna(hash_sum) or not str(hash_sum).strip():
                    new_hash = calculate_hashsum(file_path=target)
                    df.at[idx, 'hashSum'] = new_hash
                    updates.append((idx + 2, 'hashSum', new_hash))
                resolved += 1
            if updates:
                wb = load_workbook(spreadsheet)
                ws = wb['upload_meta']
                headers = {cell.value: i for i, cell in enumerate(ws[1], start=1)}
                col_index = {}
                for col_name in ('fileLocation', 'hashSum'):
                    c = headers.get(col_name)
                    if c is None:
                        c = ws.max_column + 1
                        ws.cell(row=1, column=c).value = col_name
                    col_index[col_name] = c
                for sheet_row, col_name, value in updates:
                    ws.cell(row=sheet_row, column=col_index[col_name]).value = value
                wb.save(spreadsheet)
        except Exception as e:
            run_logger.finish(status='error', error=str(e))
            typer.echo(f'Failed to update spreadsheet: {e}')
            raise typer.Exit(code=1)
    if rows is not None:
        df = select_rows(df, rows)
    if fix_ids:
        # After the row selection, so --fix-ids touches only the rows asked for, and
        # before validate_dataframe, so the same run reports the corrected values.
        try:
            fixed_cells, fixed_rows, df = _fix_next_version_ids(df, spreadsheet)
        except Exception as e:
            run_logger.finish(status='error', error=str(e))
            typer.echo(f'Failed to update spreadsheet: {e}')
            raise typer.Exit(code=1)
    if run_logger is not None:
        run_logger.finish(status='ok')
    results = validate_dataframe(
        df,
        dataset_type=dataset_type,
        known_specs=known_specs,
    )
    total = len(results)
    rows_with_errors = 0
    rows_with_warnings = 0
    ok_rows = 0
    for result in results:
        typer.echo(f'Row {result["row"]}: {result["fileName"]}')
        issues = result['issues']
        if not issues:
            typer.echo('  ok')
            ok_rows += 1
            continue
        has_error = any(severity == 'error' for severity, _ in issues)
        has_warning = any(severity == 'warning' for severity, _ in issues)
        if has_error:
            rows_with_errors += 1
        if has_warning:
            rows_with_warnings += 1
        for severity, message in issues:
            typer.echo(f'  {severity}: {message}')
    typer.echo('')
    typer.echo('-- Summary --')
    if schema_issues:
        typer.echo('Schema check:')
        for severity, message in schema_issues:
            typer.echo(f'  {severity}: {message}')
    schema_errors = sum(1 for severity, _ in schema_issues if severity == 'error')
    schema_warnings = sum(1 for severity, _ in schema_issues if severity == 'warning')
    typer.echo(
        f'{total} rows checked - {rows_with_errors} with errors, '
        f'{rows_with_warnings} rows with warnings, {ok_rows} ok; '
        f'{schema_errors} schema errors, {schema_warnings} schema warnings'
    )
    warnings_by_message = defaultdict(list)
    for result in results:
        for severity, message in result['issues']:
            if severity == 'warning':
                warnings_by_message[message].append(result['row'])
    if warnings_by_message:
        typer.echo('Warnings by type:')
        ordered = sorted(
            warnings_by_message.items(),
            key=lambda item: -len(item[1]),
        )
        width = max(len(str(len(rows))) for _, rows in ordered)
        for message, rows in ordered:
            count = len(rows)
            if count == total:
                suffix = '(all rows)'
            elif count <= 10:
                suffix = 'rows: ' + ', '.join(str(r) for r in sorted(rows))
            else:
                suffix = f'({count} rows)'
            typer.echo(f'  {count:>{width}}  {message}  {suffix}')
    if data_dir is not None:
        considered = resolved + ambiguous + not_found
        if resolved > 0:
            detail = []
            if not_found:
                detail.append(f'{not_found} not found')
            if ambiguous:
                detail.append(f'{ambiguous} duplicate names')
            suffix = f' ({", ".join(detail)})' if detail else ''
            typer.echo(f'{resolved} of {considered} data files found{suffix}; spreadsheet updated.')
        else:
            typer.echo(
                f'0 of {considered} data files found under {data_dir}. '
                'The spreadsheet was left unchanged.'
            )
    else:
        found = 0
        missing = 0
        for _, row in df.iterrows():
            file_name = row.get('fileName')
            if pd.isna(file_name) or not str(file_name).strip():
                continue
            file_location = row.get('fileLocation')
            if pd.isna(file_location) or not str(file_location).strip():
                missing += 1
                continue
            if (Path(str(file_location)) / str(file_name)).exists():
                found += 1
            else:
                missing += 1
        typer.echo(
            f'Data files: {found} found, {missing} not found at their fileLocation'
        )
        if missing > 0:
            typer.echo(
                'To locate the files and fill in fileLocation and hashSum, run:'
            )
            typer.echo(
                f'  zupload validate --spreadsheet {spreadsheet} '
                '--data-dir <path to your data files>'
            )
    if fix_ids:
        if fixed_cells:
            typer.echo(
                f'isNextVersionOf: {fixed_cells} cell(s) in {fixed_rows} row(s) '
                'converted to object ids; spreadsheet updated.'
            )
        else:
            typer.echo(
                'isNextVersionOf: no landing page URLs to convert. '
                'The spreadsheet was left unchanged.'
            )
    else:
        url_rows = sum(
            1 for result in results
            if any(message == LANDING_URL_ISSUE for _, message in result['issues'])
        )
        if url_rows:
            typer.echo(
                f'isNextVersionOf: {url_rows} row(s) hold a landing page URL '
                'where the portal needs an object id'
            )
            typer.echo('To convert them in place, run:')
            typer.echo(
                f'  zupload validate --spreadsheet {spreadsheet} --fix-ids'
            )
    if run_logger is not None:
        typer.echo(f'Logs saved to {run_logger.run_dir}')


@app.callback(invoke_without_command=True)
def main(
        ctx: typer.Context,
        spreadsheet: str | None = typer.Option(
            None,
            '--spreadsheet',
            help='Spreadsheet to use (.xlsx). Auto-detected if the current directory has a single .xlsx.'
        ),
        extract_json: bool = typer.Option(
            False,
            help="Write each row's metadata JSON next to its data file and skip the upload."
        ),
        upload: bool = typer.Option(
            True,
            help='Upload to the portal (default). Use --no-upload for a dry run that builds metadata without uploading or writing files.'
        ),
        metadata_only: bool = typer.Option(
            False,
            '--metadata-only',
            help='Upload metadata only and skip data file upload.'
        ),
        rows: str | None = typer.Option(
            None,
            '--rows',
            help='Upload a single row or a contiguous range by upload_meta sheet row numbers, e.g. "5" or "5-12" (inclusive on both ends).'
        ),
        staging: bool = typer.Option(
            False,
            '--staging',
            help='Upload metadata to the portal staging environment instead of production.'
        ),
        yes: bool = typer.Option(
            False,
            '--yes', '-y',
            help='Skip the production upload confirmation prompt.'
        )
):
    run_logger: RunLogger | None = None
    if ctx.invoked_subcommand:
        return
    try:
        if extract_json:
            upload = False
        spreadsheet = resolve_spreadsheet(spreadsheet)
        run_logger = RunLogger.start(spreadsheet=spreadsheet)
        envri_conf = get_conf(file_path=spreadsheet)
        if upload:
            env_label = 'STAGING' if staging else 'PRODUCTION'
            typer.echo(
                f'Using portal: {_portal_display_name(envri_conf.envri)}  |  environment: {env_label}'
            )
            if not staging and not yes:
                typer.echo('You are about to upload to PRODUCTION.')
                if not typer.confirm('Continue?'):
                    raise typer.Abort()
        wb = load_workbook(spreadsheet)
        ws = wb['upload_meta']
        headers = {cell.value: i for i, cell in enumerate(ws[1], start=1)}
        data_url_col = headers.get('dataUploadUrl')
        if data_url_col is None:
            data_url_col = ws.max_column + 1
            ws.cell(row=1, column=data_url_col).value = 'dataUploadUrl'
        landing_col = headers.get('landingPageURI')
        if landing_col is None:
            landing_col = ws.max_column + 1
            ws.cell(row=1, column=landing_col).value = 'landingPageURI'
        hash_col = headers.get('hashSum')
        if hash_col is None:
            hash_col = ws.max_column + 1
            ws.cell(row=1, column=hash_col).value = 'hashSum'
        df = pd.read_excel(spreadsheet, sheet_name='upload_meta')
        if rows is not None:
            df = select_rows(df, rows)
        dataset_type = _detect_dataset_type(df, envri_conf)
        for idx, row in df.iterrows():
            typer.echo(f'Row {idx + 2}: {row["fileName"]}')
            # Read the sheet's own hashSum before make_json so a hash it computes on
            # demand is only written back into a cell the user left blank.
            hash_was_blank = _is_blank(row.get('hashSum'))
            meta_json = make_json(meta=row, dataset_type=dataset_type)
            if upload:
                data_url, landing_url = upload_meta(meta_json=meta_json, envri_conf=envri_conf, staging=staging)
                ws.cell(row=idx + 2, column=data_url_col).value = data_url
                ws.cell(row=idx + 2, column=landing_col).value = landing_url
                # make_json reports hashSum None when hashing was skipped, so a value
                # here means one was actually computed for this row.
                new_hash = meta_json.get('hashSum')
                hash_written = hash_was_blank and bool(new_hash)
                if hash_written:
                    ws.cell(row=idx + 2, column=hash_col).value = new_hash
                wb.save(spreadsheet)
                if hash_written:
                    typer.echo(f'Hash written to spreadsheet: {new_hash}')
                if not metadata_only:
                    upload_data(file_path=Path(row['fileLocation']) / row['fileName'], data_url=data_url)
            elif extract_json:
                p = Path(row['fileLocation']) / row['fileName']
                json_path = p.with_suffix('.json')
                write_json(file=json_path, content=meta_json)
                pprint(meta_json)
                typer.echo(f'JSON written to {json_path}')
            else:
                pprint(meta_json)
                typer.echo('Dry run: metadata built, nothing uploaded or written (use --extract-json to write JSON).')
        if upload:
            typer.echo(f'Updated upload URLs in {spreadsheet}')
    except Exception as e:
        if run_logger is not None:
            run_logger.finish(status='error', error=str(e))
        raise
    if run_logger is not None:
        run_logger.finish(status='ok')
        typer.echo(f'Logs saved to {run_logger.run_dir}')

@app.command()
def generate(
        directory: Path = typer.Argument(
            Path('.'),
            exists=True,
            file_okay=False,
            dir_okay=True,
            help='Directory of NetCDF data files to scan (defaults to the current directory).'
        ),
        output: Path = typer.Option(
            Path('upload_meta.xlsx'),
            '--output',
            '-o',
            help='Spreadsheet to create or update (default: upload_meta.xlsx).'
        ),
        portal: str = typer.Option(
            'icos',
            '--portal',
            help='Target portal: icos, sites, or cities.'
        ),
        spec_label: str = typer.Option(
            'Non-standard spatial product',
            '--spec-label',
            help='Object specification label; must match a known spec in constants/object_specs.py.'
        ),
        hash_mode: str = typer.Option(
            'auto',
            '--hash-mode',
            help=('Hash strategy: auto=reuse existing hashes, compute missing; '
                  'reuse=only reuse existing; recompute=compute all hashes again.')
        ),
        prev_mode: str = typer.Option(
            'auto',
            '--prev-mode',
            help=('Previous-version strategy: auto=reuse existing values, query missing; '
                  'reuse=only reuse existing; recompute=query all via SPARQL; skip=leave empty.')
        ),
        description_key: Literal['paris', 'munich', 'zurich'] = typer.Option(
            ...,
            '--description-key',
            help='Predefined description key from constants/upload_descriptions.py.'
        ),
        update_columns: str = typer.Option(
            '',
            '--update-columns',
            help=(
                'Comma-separated columns to update in-place from static metadata '
                '(e.g. comment,forStation,description).'
            )
        ),
):
    """Scaffold an upload_meta.xlsx for ICOS Cities footprint NetCDF files in a directory."""
    run_logger = RunLogger.start(spreadsheet=output)
    typer.echo(f'Scanning input directory: {directory}')
    try:
        if spec_label not in ALL_OBJECT_SPECS:
            typer.echo('Invalid spec label. Available labels:')
            for label in sorted(ALL_OBJECT_SPECS):
                typer.echo(f'- {label}')
            raise typer.Exit(code=1)
        if hash_mode not in {'auto', 'reuse', 'recompute'}:
            typer.echo('Invalid hash mode. Use one of: auto, reuse, recompute')
            raise typer.Exit(code=1)
        if prev_mode not in {'auto', 'reuse', 'recompute', 'skip'}:
            typer.echo(
                'Invalid prev mode. Use one of: auto, reuse, recompute, skip'
            )
            raise typer.Exit(code=1)
        portal_input = portal.strip().lower()
        allowed_portals = {'icos', 'sites', 'cities'}
        if portal_input not in allowed_portals:
            typer.echo('Invalid portal. Use one of: icos, sites, cities')
            raise typer.Exit(code=1)
        portal_aliases = {'cities': 'icoscities'}
        portal_norm = portal_aliases.get(portal_input, portal_input)
        description = CITIES_UPLOAD_DESCRIPTIONS[str(description_key)]
        for_station = CITIES_FOR_STATION[str(description_key)]
        static_meta = build_static_cities_meta(
            description=description,
            for_station=for_station
        )
        typer.echo(
            f'Options: portal={portal_input}, type="{spec_label}", '
            f'hash-mode={hash_mode}, prev-mode={prev_mode}, description-key={description_key}'
        )
        if update_columns:
            if not output.exists():
                typer.echo(f'Cannot update columns: output file does not exist: {output}')
                raise typer.Exit(code=1)
            alias_map = {'description': 'abstract/description '}
            selected_input = [c.strip() for c in update_columns.split(',') if c.strip()]
            selected = [alias_map.get(c, c) for c in selected_input]
            if not selected:
                typer.echo('No columns provided in --update-columns.')
                raise typer.Exit(code=1)
            wb = load_workbook(output)
            if 'upload_meta' not in wb.sheetnames:
                typer.echo('Cannot update columns: sheet "upload_meta" not found.')
                raise typer.Exit(code=1)
            ws = wb['upload_meta']
            headers = {cell.value: i for i, cell in enumerate(ws[1], start=1)}
            missing = [c for c in selected if c not in headers]
            if missing:
                typer.echo(f'Columns not found in upload_meta: {", ".join(missing)}')
                raise typer.Exit(code=1)
            unsupported = [c for c in selected if c not in static_meta]
            if unsupported:
                typer.echo(
                    f'Columns are not static-update columns: {", ".join(unsupported)}'
                )
                raise typer.Exit(code=1)
            for row in range(2, ws.max_row + 1):
                for col in selected:
                    ws.cell(row=row, column=headers[col]).value = static_meta[col]
            wb.save(output)
            typer.echo(f'Updated columns in {output}: {", ".join(selected)}')
            run_logger.finish(status='ok')
            typer.echo(f'Logs saved to {run_logger.run_dir}')
            return
        object_spec = ALL_OBJECT_SPECS[spec_label]
        files = sorted(p.name for p in directory.iterdir() if p.is_file())
        typer.echo(f'Found {len(files)} files')
        existing_hashes: dict[str, str] = {}
        existing_prev: dict[str, str] = {}
        should_reuse = hash_mode in {'auto', 'reuse'}
        should_reuse_prev = prev_mode in {'auto', 'reuse'}
        if output.exists() and (should_reuse or should_reuse_prev):
            typer.echo(f'Loading reusable values from existing output: {output}')
            wb_prev = load_workbook(output, data_only=True)
            if 'upload_meta' in wb_prev.sheetnames:
                ws_prev = wb_prev['upload_meta']
                headers = {cell.value: i for i, cell in enumerate(ws_prev[1], start=1)}
                loc_idx = headers.get('fileLocation')
                hash_idx = headers.get('hashSum')
                prev_idx = headers.get('isNextVersionOf')
                if loc_idx and hash_idx and should_reuse:
                    for row in range(2, ws_prev.max_row + 1):
                        file_location = ws_prev.cell(row=row, column=loc_idx).value
                        hash_sum = ws_prev.cell(row=row, column=hash_idx).value
                        if file_location and hash_sum:
                            existing_hashes[str(file_location)] = str(hash_sum)
                if loc_idx and prev_idx and should_reuse_prev:
                    for row in range(2, ws_prev.max_row + 1):
                        file_location = ws_prev.cell(row=row, column=loc_idx).value
                        prev_version = ws_prev.cell(row=row, column=prev_idx).value
                        if file_location and prev_version:
                            existing_prev[str(file_location)] = str(prev_version)

        rows = []
        hash_reused = 0
        hash_computed = 0
        prev_reused = 0
        prev_queried = 0
        prev_empty = 0
        spinner = Halo(text='Extracting metadata and building rows...', spinner='dots')
        spinner.start()
        try:
            for name in files:
                file_path = directory / name
                file_location = str(file_path.parent.resolve())
                hash_sum = None if hash_mode == 'recompute' else existing_hashes.get(file_location)
                if hash_sum:
                    hash_reused += 1
                else:
                    hash_sum = calculate_hashsum(file_path=file_path, transient=True)
                    hash_computed += 1
                if prev_mode == 'skip':
                    prev_version = None
                elif prev_mode == 'recompute':
                    prev_version = get_prev_by_name(
                        file_name=name,
                        object_spec=object_spec,
                        portal=portal_norm
                    )
                    prev_queried += 1
                else:
                    prev_version = existing_prev.get(file_location)
                    if prev_version:
                        prev_reused += 1
                    elif prev_mode == 'auto':
                        prev_version = get_prev_by_name(
                            file_name=name,
                            object_spec=object_spec,
                            portal=portal_norm
                        )
                        prev_queried += 1
                meta = extract_cities_upload_meta(
                    file_path=file_path,
                    file_name=name,
                    description=description,
                    for_station=for_station
                )
                if not prev_version:
                    prev_empty += 1
                rows.append([
                    name,
                    file_location,
                    hash_sum,
                    spec_label,
                    object_spec,
                    prev_version,
                    meta['Level'],
                    meta['title'],
                    meta['created'],
                    meta['startCov'],
                    meta['stopCov'],
                    meta['resolution'],
                    meta['spatialCoverage'],
                    meta['coverageURI'],
                    meta['samplingHeight'],
                    meta['forStation'],
                    meta['creator'],
                    meta['creatorURI'],
                    meta['contributors'],
                    meta['contributorURI'],
                    meta['hostOrganisation'],
                    meta['hostOrganizationURI'],
                    meta['keywords'],
                    meta['licenseName'],
                    meta['licenseUrl'],
                    meta['abstract/description '],
                    meta['comment'],
                    meta['submitterID'],
                    meta['landingPageURI'],
                    meta['doiURI'],
                    meta['variablesToIngest'],
                    meta['documentation'],
                    meta['documentationURI'],
                ])
        except Exception:
            spinner.fail('Metadata extraction failed')
            raise
        spinner.succeed('Metadata extraction completed')

        if hash_computed > 0:
            typer.echo()
        typer.echo('Writing workbook...')
        wb = Workbook()
        ws_envri = wb.active
        ws_envri.title = 'envri_info'
        ws_envri.append(['portal'])
        ws_envri.append([portal_input])
        ws_meta = wb.create_sheet('upload_meta')
        ws_meta.append([
            'fileName',
            'fileLocation',
            'hashSum',
            'type',
            'objectSpecification',
            'isNextVersionOf',
            'Level',
            'title',
            'created',
            'startCov',
            'stopCov',
            'resolution',
            'spatialCoverage',
            'coverageURI',
            'samplingHeight',
            'forStation',
            'creator',
            'creatorURI',
            'contributors',
            'contributorURI',
            'hostOrganisation',
            'hostOrganizationURI',
            'keywords',
            'licenseName',
            'licenseUrl',
            'abstract/description ',
            'comment',
            'submitterID',
            'landingPageURI',
            'doiURI',
            'variablesToIngest',
            'documentation',
            'documentationURI',
        ])
        for row in rows:
            ws_meta.append(row)
        wb.save(output)
        typer.echo(f'Created {output} with {len(files)} file names')
        typer.echo(
            f'Summary: hash reused={hash_reused}, computed={hash_computed}; '
            f'prev reused={prev_reused}, queried={prev_queried}, empty={prev_empty}'
        )
    except Exception as e:
        run_logger.finish(status='error', error=str(e))
        raise
    run_logger.finish(status='ok')
    typer.echo(f'Logs saved to {run_logger.run_dir}')


def make_spatial_box(lat_min: float, lat_max: float, lon_min: float, lon_max: float) -> dict[str, Any]:
    return {
        '_type': 'LatLonBox',
        'geo': {
            'coordinates': [[
                [lon_min, lat_min],
                [lon_min, lat_max],
                [lon_max, lat_max],
                [lon_max, lat_min],
                [lon_min, lat_min]
            ]],
            'type': 'Polygon'
        },
        'max': {'lat': lat_max, 'lon': lon_max},
        'min': {'lat': lat_min, 'lon': lon_min}
    }


def build_static_cities_meta(description: str, for_station: str) -> dict[str, str]:
    return {
        'Level': '3',
        'resolution': 'half-hourly',
        'spatialCoverage': '',
        'samplingHeight': '',
        'forStation': for_station,
        'creator': 'Betty Molinier',
        'creatorURI': 'https://citymeta.icos-cp.eu/resources/people/Betty_Molinier',
        'contributors': 'Betty Molinier, Natascha Kljun',
        'contributorURI': json.dumps([
            'https://citymeta.icos-cp.eu/resources/people/Betty_Molinier',
            'https://citymeta.icos-cp.eu/resources/people/Natascha_Kljun'
        ]),
        'hostOrganisation': ORG_DISPLAY_NAMES[ORG_LU_CITIES],
        'hostOrganizationURI': ORG_LU_CITIES,
        'keywords': json.dumps([
            'Flux footprints',
            'atmospheric modelling',
            'urban flux',
            'ICOS Cities'
        ]),
        'licenseName': 'ICOS CCBY4 Data Licence',
        'licenseUrl': 'http://meta.icos-cp.eu/ontologies/cpmeta/icosLicence',
        'abstract/description ': description,
        'comment': (
            'In this version, the axis definition follows the netCDF C API requirements. '
            'Please note that the netCDF C API (this file) interprets data as row major, '
            'and consequently MATLAB users must transpose it.'
        ),
        'submitterID': 'CP',
        'landingPageURI': '',
        'doiURI': '',
        'variablesToIngest': '',
        'documentation': '',
        'documentationURI': 'https://citymeta.icos-cp.eu/objects/ylBBo5HL8RztF6kb0bHclRQd',
    }


def to_landing_page_url(data_url: str) -> str:
    parsed = urlparse(data_url)
    host = parsed.netloc
    if 'data' in host:
        host = host.replace('data', 'meta', 1)
    return urlunparse(parsed._replace(netloc=host))


def extract_cities_upload_meta(
        file_path: Path,
        file_name: str,
        description: str,
        for_station: str
) -> dict[str, str]:
    meta = {
        'title': '',
        'created': '',
        'startCov': '',
        'stopCov': '',
        'coverageURI': '',
    }
    meta.update(build_static_cities_meta(description=description, for_station=for_station))
    ds = None
    try:
        ds = xarray.open_dataset(file_path)
        date = dt.strptime(
            re.split(r'[_\\.]', file_name)[2],
            '%y%m%d'
        ).strftime('%Y-%m-%d')
        meta['title'] = ds.Title.replace('Daily', 'Diurnal') + f' - {date}'
        d_date = ds.Date_Created if 'Date_Created' in ds.attrs else ds.Creation_Date
        meta['created'] = dt.strptime(d_date, '%d-%b-%Y').strftime('%Y-%m-%dT11:00:00Z')
        start_raw = '20' + str(ds.timestep[0].item())
        stop_raw = '20' + str(ds.timestep[-1].item())
        meta['startCov'] = dt.strptime(start_raw, '%Y%m%d%H%M').strftime('%Y-%m-%dT%H:%M:%SZ')
        meta['stopCov'] = dt.strptime(stop_raw, '%Y%m%d%H%M').strftime('%Y-%m-%dT%H:%M:%SZ')
        tm_proj = pyproj.CRS(ds.crs_projection4)
        wgs84_proj = pyproj.CRS('EPSG:4326')
        transformer = pyproj.Transformer.from_crs(
            tm_proj,
            wgs84_proj,
            always_xy=True
        )
        lon, lat = transformer.transform(
            np.array(ds.x, dtype=np.float32),
            np.array(ds.y, dtype=np.float32)
        )
        meta['coverageURI'] = json.dumps(
            make_spatial_box(
                float(lat.min()),
                float(lat.max()),
                float(lon.min()),
                float(lon.max())
            )
        )
    except Exception as e:
        typer.echo(f'Warning: could not extract cities metadata for {file_name}: {e}')
    finally:
        if ds is not None:
            ds.close()
    return meta


def _nan_to_none(obj):
    """Recursively replace float NaN values with None so the payload is JSON-serializable."""
    if isinstance(obj, dict):
        return {key: _nan_to_none(value) for key, value in obj.items()}
    if isinstance(obj, list):
        return [_nan_to_none(item) for item in obj]
    if isinstance(obj, float) and np.isnan(obj):
        return None
    return obj


def make_json(meta: Series, dataset_type: DatasetType = 'spatioTemporal'):
    description = (
        meta['abstract/description ']
        if 'abstract/description ' in meta
        else meta.get('abstract/description', '')
    )
    spatial_raw = meta.get('coverageURI')
    spatial = None
    if not pd.isna(spatial_raw):
        if isinstance(spatial_raw, str):
            stripped = spatial_raw.strip()
            if stripped.startswith(('http://', 'https://')):
                spatial = stripped
            else:
                spatial = json.loads(stripped)
        else:
            spatial = spatial_raw
    documentation = None
    documentation_uri = meta.get('documentationURI')
    if not pd.isna(documentation_uri):
        doc_raw = str(documentation_uri).strip()
        if doc_raw:
            documentation = doc_raw.rstrip('/').split('/')[-1]
    hash_sum = (
        None
        if pd.isna(meta.get('hashSum'))
        else str(meta.get('hashSum')).strip()
    )
    if not hash_sum:
        file_location = meta.get('fileLocation')
        file_name = meta.get('fileName')
        if _is_blank(file_location) or _is_blank(file_name):
            typer.echo('Hash skipped (fileLocation or fileName is blank).')
        else:
            # fileLocation is the directory holding the data file, so the path to the
            # file itself is always this join.
            data_path = Path(str(file_location).strip()) / str(file_name).strip()
            if data_path.exists():
                hash_sum = calculate_hashsum(file_path=data_path)
            else:
                typer.echo(f'Hash skipped (data file not found): {data_path}')
    prev_raw = meta.get('isNextVersionOf')
    is_next_version_of = None
    if not pd.isna(prev_raw):
        if isinstance(prev_raw, str):
            stripped = prev_raw.strip()
            if stripped.startswith('['):
                try:
                    parsed = json.loads(stripped)
                except json.JSONDecodeError:
                    parsed = None
                is_next_version_of = parsed if isinstance(parsed, list) else stripped
            else:
                is_next_version_of = stripped
        else:
            is_next_version_of = prev_raw
    contributors_raw = meta.get('contributorURI')
    # Every object the portal returns reports contributors as a list, empty rather than
    # null, so a blank or absent cell means "no contributors" instead of "unknown".
    if _is_blank(contributors_raw):
        contributors = []
    elif isinstance(contributors_raw, str):
        contributors = json.loads(contributors_raw)
    else:
        contributors = contributors_raw
    production = {
        'creator': meta.get('creatorURI'),
        'contributors': contributors,
        'hostOrganization': meta.get('hostOrganizationURI'),
        'comment': None if _is_blank(meta.get('comment')) else meta.get('comment'),
        'sources': [],
        'documentation': documentation,
        'creationDate': meta.get('created'),
    }
    if dataset_type == 'stationTimeSeries':
        station = meta.get('stationURI')
        if _is_blank(station):
            station = meta.get('forStation')
        station = None if _is_blank(station) else str(station).strip()
        instrument = None
        instrument_raw = meta.get('instrumentURI')
        if not _is_blank(instrument_raw):
            if isinstance(instrument_raw, str):
                stripped = instrument_raw.strip()
                if stripped.startswith('['):
                    try:
                        parsed = json.loads(stripped)
                    except json.JSONDecodeError:
                        parsed = None
                    instrument = parsed if isinstance(parsed, list) else stripped
                else:
                    instrument = stripped
            else:
                instrument = instrument_raw
        sampling_height = None
        sampling_height_raw = meta.get('samplingHeight')
        if not _is_blank(sampling_height_raw):
            try:
                sampling_height = float(str(sampling_height_raw).strip())
            except (TypeError, ValueError):
                sampling_height = None
        n_rows = None
        n_rows_raw = meta.get('numRows')
        if not _is_blank(n_rows_raw):
            try:
                n_rows = int(float(str(n_rows_raw).strip()))
            except (TypeError, ValueError):
                n_rows = None
        acquisition_interval = None
        if not _is_blank(meta.get('startCov')) and not _is_blank(meta.get('stopCov')):
            acquisition_interval = {
                'start': meta.get('startCov'),
                'stop': meta.get('stopCov'),
            }
        specific_info: dict[str, Any] = {
            'station': station,
            'instrument': instrument,
            'samplingHeight': sampling_height,
            'acquisitionInterval': acquisition_interval,
            'nRows': n_rows,
            'production': production,
            'spatial': spatial,
        }
    else:
        specific_info = {
            'title': meta.get('title'),
            'description': description,
            'spatial': spatial,
            'temporal': {
                'interval': {
                    'start': meta.get('startCov'),
                    'stop': meta.get('stopCov'),
                },
                'resolution': None if pd.isna(meta.get('resolution')) else meta.get('resolution'),
            },
            'forStation': (
                None
                if pd.isna(meta.get('forStation')) or not str(meta.get('forStation')).strip()
                else meta.get('forStation')
            ),
            'production': production,
            'variables': (
                None
                if pd.isna(meta.get('variablesToIngest'))
                or not meta.get('variablesToIngest')
                else json.loads(meta.get('variablesToIngest'))
            )
        }
    keywords_raw = meta.get('keywords')
    # Object-level keywords are optional. The portal's own payloads omit the key entirely
    # for objects submitted without any, and the keywords a landing page displays may
    # belong to the object specification rather than to the object.
    if _is_blank(keywords_raw):
        keywords = None
    elif isinstance(keywords_raw, str):
        keywords = json.loads(keywords_raw)
    else:
        keywords = keywords_raw
    json_meta = dict({
        'fileName': meta.get('fileName'),
        'hashSum': hash_sum,
        'isNextVersionOf': is_next_version_of,
        'preExistingDoi': None if _is_blank(meta.get('doiURI')) else meta.get('doiURI'),
        'objectSpecification': meta.get('objectSpecification'),
        'references': {
            'keywords': keywords,
            'licence': meta.get('licenseUrl'),
            'autodeprecateSameFilenameObjects': False,
            'duplicateFilenameAllowed': True,
        },
        'specificInfo': specific_info,
        'submitterId': meta.get('submitterID'),
    })
    return _nan_to_none(json_meta)


def upload_meta(meta_json: dict[str, Any], envri_conf: EnvriConfig, staging: bool = False) -> tuple[str, str]:
    """Upload metadata package to specified portal."""
    typer.echo(f'Uploading metadata for: {meta_json["fileName"]}', nl=False)
    resp = requests.post(
        url=envri_conf.upload_url(staging),
        json=meta_json,
        cookies=get_cookie_jar()
    )
    response_url = resp.text.strip()
    landing_url = to_landing_page_url(response_url)
    if resp.status_code == 200:
        typer.echo(
            f' -> {landing_url} ({resp.status_code}) OK'
        )
    else:
        typer.echo(f' ({resp.status_code}) FAILED')
        typer.echo(resp.text)
        raise typer.Exit(code=1)
    return response_url, landing_url


def upload_data(file_path: str | Path, data_url: str) -> None:
    """Upload data file to specified portal."""
    file_path = Path(file_path)
    typer.echo(f'Uploading data for: {file_path}', nl=False)
    resp = requests.put(
        url=data_url,
        data=open(file=file_path, mode='rb'),
        cookies=get_cookie_jar(),
        headers={'Content-Type': 'application/octet-stream'},
    )
    if resp.status_code == 200:
        typer.echo(f' -> {resp.text} ({resp.status_code}) OK')
    else:
        typer.echo(f' ({resp.status_code}) FAILED')
        typer.echo(resp.text)
        raise typer.Exit(code=1)
    return


if __name__ == "__main__":
    app()
