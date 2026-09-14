"""Fill an upload spreadsheet's metadata columns from the portal.

harvest has two modes, and neither one uploads anything.

Given an upload spreadsheet whose ``landingPageURI`` column is filled, it asks the portal
what it knows about each object and writes that back into the other columns. Given
``--landing-page`` instead, it builds a whole spreadsheet from scratch out of the URIs
you name, with every column zupload reads present so the sheet is ready to edit and
upload as a new version.
"""
# Standard library imports.
from pathlib import Path
from typing import Any
# Related third party imports.
import typer
from openpyxl import Workbook, load_workbook
# Local application/library specific imports.
from zupload.cli_shared import resolve_spreadsheet, select_rows
from zupload.logs import RunLogger
from zupload.metadata_fetch import (
    column_order,
    detect_dataset_type,
    dto_to_row,
    fetch_dto,
    fetch_object_json,
    is_blank,
    metadata_host,
    portal_for_host,
    values_equal,
)
from zupload.utils import ensure_header, get_conf, header_index, read_upload_meta

app = typer.Typer(help='Fill spreadsheet metadata columns from the portal.')

# fileLocation is a local directory path rather than portal metadata, so harvest never
# writes it. "zupload validate --data-dir" is what fills that column.
NEVER_WRITTEN = frozenset({'fileLocation', 'landingPageURI'})


@app.command()
def main(
        spreadsheet: str | None = typer.Argument(
            None,
            metavar='[SPREADSHEET]',
            help='Spreadsheet to fill (.xlsx). Auto-detected if the current directory has a single .xlsx.'
        ),
        landing_page: list[str] | None = typer.Option(
            None,
            '--landing-page',
            help='Build a new spreadsheet from scratch instead of filling an existing one. '
                 'Repeat the flag once per object; each URI becomes one row.'
        ),
        output: str = typer.Option(
            'harvest.xlsx',
            '--output',
            help='Where to write the spreadsheet built by --landing-page. Refuses to write over '
                 'an existing file. Ignored without --landing-page.'
        ),
        rows: str | None = typer.Option(
            None,
            '--rows',
            help='Harvest a single row or a contiguous range by upload_meta sheet row numbers, '
                 'e.g. "5" or "5-12" (inclusive on both ends). Existing spreadsheets only.'
        ),
        overwrite: bool = typer.Option(
            False,
            '--overwrite',
            help='Replace cells that already have a value and disagree with the portal. '
                 'By default those cells are left alone and the difference is reported.'
        ),
        dry_run: bool = typer.Option(
            False,
            '--dry-run',
            help='Report what would change and write nothing at all.'
        )
):
    """Fill an upload spreadsheet from the portal, or build one from landing page URIs."""
    if landing_page:
        _reject_generate_conflicts(spreadsheet=spreadsheet, rows=rows, overwrite=overwrite)
        output_path = Path(output)
        _run_logged(
            spreadsheet=output_path,
            work=lambda: _generate(
                landing_uris=list(landing_page),
                output=output_path,
                dry_run=dry_run,
            ),
        )
        return
    spreadsheet_path = resolve_spreadsheet(spreadsheet, command='harvest')
    _run_logged(
        spreadsheet=spreadsheet_path,
        work=lambda: _harvest(
            spreadsheet=spreadsheet_path,
            rows=rows,
            overwrite=overwrite,
            dry_run=dry_run,
        ),
    )


def _run_logged(spreadsheet: Path, work) -> None:
    """Run one harvest mode, recording the outcome under ./logs whichever way it ends.

    The CLI does not finalize the run log by itself, so every exit path has to close it.
    """
    run_logger = RunLogger.start(spreadsheet=spreadsheet, prefix='harvest')
    try:
        work()
    except typer.Exit as e:
        exit_code = getattr(e, 'exit_code', 1)
        if exit_code:
            run_logger.finish(status='error', error=f'exited with code {exit_code}')
        else:
            run_logger.finish(status='ok')
        raise
    except typer.Abort:
        run_logger.finish(status='error', error='aborted')
        raise
    except Exception as e:
        run_logger.finish(status='error', error=str(e))
        raise
    run_logger.finish(status='ok')
    typer.echo(f'Logs saved to {run_logger.run_dir}')


def _reject_generate_conflicts(spreadsheet: str | None, rows: str | None, overwrite: bool) -> None:
    """Turn away the flag combinations that cannot mean anything."""
    if spreadsheet is not None:
        typer.echo('--landing-page builds a new spreadsheet, so it cannot take a SPREADSHEET argument.')
        typer.echo(f'Drop "{spreadsheet}" to build a new sheet, or drop --landing-page to fill that one.')
        typer.echo('Use --output to choose where the new sheet is written.')
        raise typer.Exit(code=1)
    if rows is not None:
        typer.echo('--rows picks rows out of an existing spreadsheet, so it cannot be used with --landing-page.')
        typer.echo('--landing-page creates exactly one row per URI you pass.')
        raise typer.Exit(code=1)
    if overwrite:
        # --overwrite is about cell values, and reading it as "overwrite the output file"
        # would be a costly misunderstanding, so say plainly that it does nothing here.
        typer.echo('Note: --overwrite applies to cells in an existing sheet and does nothing here.')
        typer.echo('It is not a way to replace the --output file; move or remove that file yourself.')


def _harvest(spreadsheet: Path, rows: str | None, overwrite: bool, dry_run: bool) -> None:
    envri_conf = get_conf(file_path=spreadsheet)
    df = read_upload_meta(spreadsheet)
    if 'landingPageURI' not in df.columns:
        typer.echo('The upload_meta sheet has no landingPageURI column.')
        typer.echo('harvest reads that column to know which objects to fetch, so there is nothing to do.')
        typer.echo('Add a landingPageURI column and fill it with the landing page URL of each object, e.g.:')
        typer.echo('https://meta.icos-cp.eu/objects/6TNdmGyjojb8iLTQ3acDs3-F')
        typer.echo('Rows uploaded by zupload already have this column filled in.')
        raise typer.Exit(code=1)
    if rows is not None:
        df = select_rows(df, rows)
    expected_host = _expected_host(envri_conf)
    typer.echo(f'Portal from envri_info: {envri_conf.envri} ({expected_host})')

    updates: list[tuple[int, str, Any]] = []
    warned_hosts: set[str] = set()
    filled = 0
    overwritten = 0
    conflicts = 0
    skipped = 0
    failed = 0

    for idx, row in df.iterrows():
        sheet_row = int(idx) + 2
        landing_raw = row.get('landingPageURI')
        if is_blank(landing_raw):
            skipped += 1
            continue
        landing_uri = str(landing_raw).strip()
        try:
            host = metadata_host(landing_uri)
        except ValueError as e:
            failed += 1
            typer.echo(f'Row {sheet_row}: {e}')
            continue
        if expected_host and host != expected_host and host not in warned_hosts:
            warned_hosts.add(host)
            typer.echo(
                f'Warning: row {sheet_row} points at {host}, but the envri_info sheet names '
                f'{envri_conf.envri} ({expected_host}). Harvesting from {host} anyway.'
            )
        try:
            dto = fetch_dto(landing_uri)
        except Exception as e:
            failed += 1
            typer.echo(f'Row {sheet_row}: could not fetch {landing_uri} ({e}); skipping this row.')
            continue
        obj_json = fetch_object_json(landing_uri)
        dataset_type = detect_dataset_type(dto, obj_json)
        harvested = dto_to_row(dto=dto, obj_json=obj_json, landing_uri=landing_uri)
        row_filled, row_overwritten, row_conflicts = _plan_row(
            sheet_row=sheet_row,
            row=row,
            harvested=harvested,
            overwrite=overwrite,
            updates=updates,
        )
        filled += row_filled
        overwritten += row_overwritten
        conflicts += len(row_conflicts) if not overwrite else 0
        label = dto.get('fileName') or landing_uri
        typer.echo(
            f'Row {sheet_row}: {label} [{dataset_type or "unknown dataset type"}] '
            f'filled {row_filled}, overwrote {row_overwritten}, conflicts {len(row_conflicts)}'
        )
        if obj_json is None:
            typer.echo(
                '    the portal\'s rich record was unavailable, so submitterID, licenseName, '
                'variablesToIngest and the generated title were skipped'
            )
        for column, current, value in row_conflicts:
            verdict = 'overwritten' if overwrite else 'left alone'
            typer.echo(f'    {column}: sheet="{current}" portal="{value}" ({verdict})')

    _write_back(spreadsheet=spreadsheet, updates=updates, dry_run=dry_run)
    typer.echo(
        f'Summary: cells filled={filled}, cells overwritten={overwritten}, '
        f'conflicts left alone={conflicts}, rows skipped={skipped}, rows failed={failed}'
    )
    if conflicts and not overwrite:
        typer.echo('Rerun with --overwrite to replace the cells listed above with the portal values.')


def _generate(landing_uris: list[str], output: Path, dry_run: bool) -> None:
    """Build a whole spreadsheet from a list of landing page URIs."""
    if output.exists():
        typer.echo(f'{output} already exists, and harvest will not write over it.')
        typer.echo('Pass --output <other path> to write elsewhere, or move or remove that file first.')
        raise typer.Exit(code=1)
    portal = _portal_for_uris(landing_uris)
    records: list[dict[str, Any]] = []
    dataset_types: set[str] = set()
    filled = 0
    failed = 0

    for raw_uri in landing_uris:
        landing_uri = str(raw_uri).strip()
        record, dataset_type, ok = _harvest_one(landing_uri)
        records.append(record)
        if not ok:
            failed += 1
            continue
        if dataset_type:
            dataset_types.add(dataset_type)
        # landingPageURI is the input rather than something the portal told us, so it is
        # not counted among the cells harvest filled in.
        filled += len(record) - 1

    if failed == len(records):
        typer.echo('Every landing page URI failed, so there was nothing to build a spreadsheet from.')
        raise typer.Exit(code=1)

    if len(dataset_types) > 1:
        # zupload settles on one dataset type per sheet and applies it to every row, so a
        # mixed sheet builds the wrong specificInfo for whichever type is in the minority.
        typer.echo(
            f'Warning: these URIs mix dataset types ({", ".join(sorted(dataset_types))}). '
            'zupload uses a single dataset type per upload, so one sheet per type is safer.'
        )
    columns = column_order(dataset_types)
    for record in records:
        for column in record:
            if column not in columns:
                columns.append(column)

    if dry_run:
        typer.echo(
            f'Dry run: would create {output} with {len(records)} row(s) '
            f'and {len(columns)} column(s); nothing was written.'
        )
        typer.echo(f'    portal: {portal}')
        typer.echo(f'    columns: {", ".join(columns)}')
    else:
        _write_new_workbook(output=output, portal=portal, columns=columns, records=records)
        typer.echo(f'Created {output} with {len(records)} row(s) and {len(columns)} column(s)')
    typer.echo(
        f'Summary: rows created={len(records)}, cells filled={filled}, rows failed={failed}, '
        f'portal={portal}'
    )
    if failed:
        typer.echo(
            f'{failed} row(s) carry only landingPageURI. Rerun "harvest {output}" '
            'to retry just those rows once the portal can answer for them.'
        )
    if not dry_run:
        typer.echo('Fill in fileLocation and any blank cells the portal could not supply, then upload with zupload.')


def _harvest_one(landing_uri: str) -> tuple[dict[str, Any], str | None, bool]:
    """Fetch one object, returning its cells, its dataset type, and whether the fetch worked.

    A failure still yields a usable row: landingPageURI alone, so a later plain harvest
    over the generated sheet can retry that row.
    """
    try:
        metadata_host(landing_uri)
    except ValueError as e:
        typer.echo(f'{landing_uri}: {e}; the row will carry only landingPageURI.')
        return {'landingPageURI': landing_uri}, None, False
    try:
        dto = fetch_dto(landing_uri)
    except Exception as e:
        typer.echo(f'{landing_uri}: could not fetch ({e}); the row will carry only landingPageURI.')
        return {'landingPageURI': landing_uri}, None, False
    obj_json = fetch_object_json(landing_uri)
    dataset_type = detect_dataset_type(dto, obj_json)
    record = dto_to_row(dto=dto, obj_json=obj_json, landing_uri=landing_uri)
    record['landingPageURI'] = landing_uri
    typer.echo(
        f'{dto.get("fileName") or landing_uri} [{dataset_type or "unknown dataset type"}]: '
        f'{len(record) - 1} cell(s) from the portal'
    )
    if obj_json is None:
        typer.echo(
            '    the portal\'s rich record was unavailable, so submitterID, licenseName, '
            'variablesToIngest and the generated title were skipped'
        )
    return record, dataset_type, True


def _portal_for_uris(landing_uris: list[str]) -> str:
    """Return the single envri_info portal value the given URIs imply.

    The envri_info sheet holds one portal, so URIs spread across hosts cannot produce a
    coherent spreadsheet.
    """
    hosts: dict[str, int] = {}
    for raw_uri in landing_uris:
        try:
            host = metadata_host(str(raw_uri).strip())
        except ValueError:
            # Not a URI at all; it cannot vote on the portal, and it becomes a
            # landingPageURI-only row further on.
            continue
        hosts[host] = hosts.get(host, 0) + 1
    if not hosts:
        typer.echo('None of the landing page URIs is an absolute URL, so no portal could be identified.')
        typer.echo('Pass full landing page URLs, e.g. https://meta.icos-cp.eu/objects/<id>.')
        raise typer.Exit(code=1)
    if len(hosts) > 1:
        typer.echo('The landing page URIs point at more than one portal:')
        for host, count in hosts.items():
            typer.echo(f'- {host} ({count} URI(s))')
        typer.echo('A spreadsheet names a single portal in its envri_info sheet, so these cannot share one.')
        typer.echo('Run harvest once per portal, each with its own --output.')
        raise typer.Exit(code=1)
    host = next(iter(hosts))
    portal = portal_for_host(host)
    if portal is None:
        typer.echo(f'Could not work out which portal {host} is.')
        typer.echo('harvest can only generate spreadsheets for the portals zupload knows about.')
        raise typer.Exit(code=1)
    typer.echo(f'Portal for {host}: {portal}')
    return portal


def _write_new_workbook(
        output: Path,
        portal: str,
        columns: list[str],
        records: list[dict[str, Any]],
) -> None:
    wb = Workbook()
    ws_envri = wb.active
    ws_envri.title = 'envri_info'
    ws_envri.append(['portal'])
    ws_envri.append([portal])
    ws_meta = wb.create_sheet('upload_meta')
    ws_meta.append(columns)
    for record in records:
        ws_meta.append([record.get(column) for column in columns])
    wb.save(output)


def _plan_row(
        sheet_row: int,
        row,
        harvested: dict[str, Any],
        overwrite: bool,
        updates: list[tuple[int, str, Any]],
) -> tuple[int, int, list[tuple[str, Any, Any]]]:
    """Decide what to do with one row's harvested values, appending to updates in place."""
    filled = 0
    overwritten = 0
    conflicts: list[tuple[str, Any, Any]] = []
    for column, value in harvested.items():
        if column in NEVER_WRITTEN:
            continue
        current = row.get(column)
        if is_blank(current):
            updates.append((sheet_row, column, value))
            filled += 1
            continue
        if values_equal(current, value):
            continue
        conflicts.append((column, current, value))
        if overwrite:
            updates.append((sheet_row, column, value))
            overwritten += 1
    return filled, overwritten, conflicts


def _write_back(spreadsheet: Path, updates: list[tuple[int, str, Any]], dry_run: bool) -> None:
    if dry_run:
        typer.echo(f'Dry run: {len(updates)} cell(s) would change in {spreadsheet}; nothing was written.')
        return
    if not updates:
        typer.echo(f'No cells to update in {spreadsheet}.')
        return
    wb = load_workbook(spreadsheet)
    ws = wb['upload_meta']
    # Headers are matched with whitespace stripped, so a column the sheet already has
    # is filled in place rather than shadowed by a second one with the tidy name.
    headers = header_index(ws)
    col_index: dict[str, int] = {}
    added_columns: list[str] = []
    for _, column, _ in updates:
        if column in col_index:
            continue
        column_number, created = ensure_header(ws, headers, column)
        if created:
            added_columns.append(column)
        col_index[column] = column_number
    for sheet_row, column, value in updates:
        ws.cell(row=sheet_row, column=col_index[column]).value = value
    wb.save(spreadsheet)
    if added_columns:
        typer.echo(f'Added column(s): {", ".join(added_columns)}')
    typer.echo(f'Updated {len(updates)} cell(s) in {spreadsheet}')


def _expected_host(envri_conf) -> str:
    """Return the metadata host the envri_info sheet implies, for cross-checking."""
    try:
        return metadata_host(envri_conf.meta_url)
    except ValueError:
        return ''


if __name__ == '__main__':
    app()
