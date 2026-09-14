# Standard library imports.
from http.cookiejar import CookieJar
from pathlib import Path
from typing import cast, Any
import hashlib
import json
import sys
# Related third party imports.
from icoscp_core import icos, cities, auth
from requests.utils import cookiejar_from_dict
from requests.cookies import RequestsCookieJar, cookiejar_from_dict
from halo import Halo
import pandas as pd
import typer
# Local application/library specific imports.
from zupload.constants.envri import ENVRIES, DatasetType, EnvriConfig, Envri

GET_PREV_BY_NAME_QUERY = """
PREFIX cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
SELECT ?dobj
WHERE {
    VALUES ?spec {
        <#spec_anchor>
    }
    ?dobj cpmeta:hasObjectSpec ?spec .
    ?dobj cpmeta:hasName ?fileName .
    FILTER(STRSTARTS(str(?fileName), "#name_anchor"))
    FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?dobj}
    FILTER EXISTS {?dobj cpmeta:hasSizeInBytes []}
}
"""

GET_DATASET_TYPE_QUERY = """
PREFIX cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
SELECT ?dsType
WHERE {
    VALUES ?spec {
        <#http_anchor>
        <#https_anchor>
    }
    ?spec cpmeta:hasSpecificDatasetType ?dsType .
}
"""

DATASET_TYPE_BY_SEGMENT: dict[str, DatasetType] = {
    'stationTimeSeriesDataset': 'stationTimeSeries',
    'spatioTemporalDataset': 'spatioTemporal',
}

_DATASET_TYPE_CACHE: dict[tuple[str, str], DatasetType | None] = {}


def calculate_hashsum(file_path: str | Path, transient: bool = False) -> str:
    """Calculate and return hash-sum of given file."""
    file_path = Path(file_path)
    spinner = Halo(
        text=f'Calculating hashSum for {file_path.name}',
        spinner='dots'
    )
    spinner.start()
    try:
        sha256_hash = hashlib.sha256()
        with open(file=file_path, mode='rb') as f_hdl:
            for byte_block in iter(lambda: f_hdl.read(4096), b''):
                sha256_hash.update(byte_block)
        digest = sha256_hash.hexdigest()
    except Exception:
        spinner.fail(f'Failed to calculate hashSum for {file_path.name}')
        raise
    if transient and sys.stdout.isatty():
        spinner.stop()
        typer.echo(f'\r\033[K✔ Calculated hashSum for {file_path.name}', nl=False)
        return digest
    spinner.succeed(f'Calculated hashSum for {file_path.name}')
    return digest


def get_prev_by_name(
        file_name: str,
        object_spec: str,
        portal: str = 'icos'
) -> str | None:
    portal_norm = portal.strip().lower()
    client = cities if portal_norm in {'cities', 'icoscities'} else icos
    query = GET_PREV_BY_NAME_QUERY \
        .replace('#name_anchor', file_name) \
        .replace('#spec_anchor', object_spec)
    sparql_res = client.meta.sparql_select(query=query)
    if not sparql_res.bindings:
        return None
    prev_uri = sparql_res.bindings[0]['dobj'].uri
    return prev_uri.rsplit('/', 1)[-1]


def get_dataset_type(
        object_spec: str,
        portal: str = 'icos'
) -> DatasetType | None:
    """Ask the portal which specificInfo branch an object specification uses."""
    portal_norm = portal.strip().lower()
    spec_norm = object_spec.strip()
    cache_key = (spec_norm, portal_norm)
    if cache_key in _DATASET_TYPE_CACHE:
        return _DATASET_TYPE_CACHE[cache_key]
    # The metadata store holds ICOS spec URIs under http://, while spreadsheets
    # often write https://. SPARQL treats the two as different URIs, so query both.
    if spec_norm.startswith('https://'):
        https_spec = spec_norm
        http_spec = 'http://' + spec_norm[len('https://'):]
    elif spec_norm.startswith('http://'):
        http_spec = spec_norm
        https_spec = 'https://' + spec_norm[len('http://'):]
    else:
        http_spec = spec_norm
        https_spec = spec_norm
    client = cities if portal_norm in {'cities', 'icoscities'} else icos
    query = GET_DATASET_TYPE_QUERY \
        .replace('#http_anchor', http_spec) \
        .replace('#https_anchor', https_spec)
    sparql_res = client.meta.sparql_select(query=query)
    dataset_type: DatasetType | None = None
    if sparql_res.bindings:
        ds_type_uri = sparql_res.bindings[0]['dsType'].uri
        segment = ds_type_uri.rsplit('/', 1)[-1]
        dataset_type = DATASET_TYPE_BY_SEGMENT.get(segment)
    _DATASET_TYPE_CACHE[cache_key] = dataset_type
    return dataset_type


def get_conf(file_path: Path) -> EnvriConfig:
    """Read portal information from spreadsheet."""
    try:
        portal_raw = pd.read_excel(
            file_path,
            sheet_name='envri_info'
        )['portal'].iloc[0]
    except Exception as e:
        typer.echo(f'Could not read portal value from "envri_info" sheet: {e}')
        raise typer.Exit(code=1)
    if pd.isna(portal_raw):
        typer.echo('Invalid or missing portal value.')
        raise typer.Exit(code=1)
    portal_aliases = {'cities': 'icoscities'}
    portal_norm = portal_aliases.get(str(portal_raw).strip().lower(), str(portal_raw).strip().lower())
    by_lower = {k.lower(): k for k in ENVRIES.keys()}
    if portal_norm not in by_lower:
        typer.echo(
            f'Invalid portal value "{portal_raw}". Expected one of: icos, sites, cities'
        )
        raise typer.Exit(code=1)
    portal_key = cast(Envri, by_lower[portal_norm])
    return ENVRIES[portal_key]


def strip_column_names(df: pd.DataFrame) -> pd.DataFrame:
    """Return df with leading and trailing whitespace stripped from column names.

    A header typed with a stray space is the same column as far as a reader is
    concerned, so the rest of zupload should never have to spell one out. When two
    headers would collapse onto the same name the first one wins and the second keeps
    the name it was written with, because dropping it or letting pandas hold two
    identical labels would lose a column of the user's data.
    """
    taken: set[Any] = set()
    renamed: list[Any] = []
    for name in df.columns:
        candidate = name.strip() if isinstance(name, str) else name
        if candidate in taken:
            clash = candidate
            candidate = name
            suffix = 1
            while candidate in taken:
                candidate = f'{name}.{suffix}'
                suffix += 1
            typer.echo(
                f'Warning: upload_meta column "{name}" is the same as an earlier '
                f'column once whitespace is stripped ("{clash}"). Keeping the first '
                f'one, and reading this one as "{candidate}".'
            )
        taken.add(candidate)
        renamed.append(candidate)
    if renamed == list(df.columns):
        return df
    df = df.copy()
    df.columns = renamed
    return df


def read_upload_meta(file_path: str | Path) -> pd.DataFrame:
    """Read the upload_meta sheet, with whitespace stripped from every column name."""
    df = pd.read_excel(file_path, sheet_name='upload_meta')
    return strip_column_names(df)


def header_index(ws) -> dict[Any, int]:
    """Map each header cell of an openpyxl sheet to its column number, ignoring whitespace.

    Lookups go through the stripped name while writes land in the column the sheet
    already has, so a header carrying a stray space is filled in place instead of being
    shadowed by a freshly appended duplicate. Where two headers strip to the same name
    the first one wins, matching strip_column_names.
    """
    headers: dict[Any, int] = {}
    for number, cell in enumerate(ws[1], start=1):
        value = cell.value
        if value is None:
            continue
        key = value.strip() if isinstance(value, str) else value
        if key not in headers:
            headers[key] = number
    return headers


def ensure_header(ws, headers: dict[Any, int], column: str) -> tuple[int, bool]:
    """Return the column number for column, appending the header if the sheet lacks it.

    The second element of the return value says whether the column had to be created.
    A created column is written with the canonical name, free of stray whitespace.
    """
    key = column.strip()
    number = headers.get(key)
    if number is not None:
        return number, False
    number = ws.max_column + 1
    ws.cell(row=1, column=number).value = key
    headers[key] = number
    return number, True


def get_cookie_jar() -> RequestsCookieJar:
    cookie_string = icos.auth.get_token().cookie_value
    cookie_dict = {
        cookie.split('=')[0]: cookie.split('=')[1]
        for cookie in cookie_string.split('; ')
    }
    cookie_jar = cookiejar_from_dict(cookie_dict)
    return cookie_jar


def write_json(file: str | Path, content: dict[str, Any]) -> Path:
    """Write dictionary to JSON file."""
    file = Path(file)
    with open(file=file, mode='w+') as json_handle:
        json.dump(content, json_handle, indent=4)
    return file
