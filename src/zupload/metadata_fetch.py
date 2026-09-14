"""Fetch object metadata from a portal and map it back onto upload_meta columns.

This module is the inverse of ``zupload_cli.make_json``: where ``make_json`` turns a
spreadsheet row into a DataObjectDto, ``dto_to_row`` turns a DataObjectDto back into
spreadsheet cells. The two must stay in step, because zupload has to be able to read
back whatever harvest writes.

Like ``validation.py`` this module imports only the standard library, pandas/requests
and ``constants`` - no Typer - so the CLI stays a thin wrapper around it.
"""
# Standard library imports.
from datetime import datetime as dt
from collections.abc import Iterable
from typing import Any
from urllib.parse import urlparse, urlunparse
import base64
import binascii
import json
# Related third party imports.
import pandas as pd
import requests
# Local application/library specific imports.
from zupload.constants.envri import ENVRIES, DatasetType
from zupload.validation import (
    OPTIONAL_COLUMNS,
    REQUIRED_COLUMNS,
    SPATIOTEMPORAL_OPTIONAL_COLUMNS,
    STATION_OPTIONAL_COLUMNS,
)

REQUEST_TIMEOUT = 30

# The portal's own name for each specificInfo branch, used only as a fallback
# confirmation when the DataObjectDto's shape is inconclusive.
DATASET_TYPE_BY_SPECIFIC_TYPE: dict[str, DatasetType] = {
    'stationtimeseries': 'stationTimeSeries',
    'spatiotemporal': 'spatioTemporal',
}


# The column order a generated sheet uses, so it reads like the spreadsheet template
# recorded in map.md, narrowed to the columns zupload actually reads back.
BASE_COLUMNS = [
    'fileName',
    'fileLocation',
    'hashSum',
    'objectSpecification',
    'isNextVersionOf',
    'title',
    'created',
    'startCov',
    'stopCov',
    'coverageURI',
    'creatorURI',
    'contributorURI',
    'hostOrganizationURI',
    'keywords',
    'licenseName',
    'licenseUrl',
    'abstract/description',
    'comment',
    'submitterID',
    'landingPageURI',
    'doiURI',
    'documentationURI',
]

STATION_COLUMNS = [
    'stationURI',
    'instrumentURI',
    'samplingHeight',
    'numRows',
]

SPATIOTEMPORAL_COLUMNS = [
    'resolution',
    'forStation',
    'variablesToIngest',
]


def portal_for_host(host: str) -> str | None:
    """Return the envri_info "portal" value for a metadata host, or None if unrecognised.

    The mapping is derived from ENVRIES rather than hardcoded, so a new portal only has
    to be added there. Configs whose URLs are placeholders rather than real hosts drop
    out on their own, because they have no host to compare against.
    """
    for key, config in ENVRIES.items():
        try:
            candidate = metadata_host(config.meta_url)
        except ValueError:
            continue
        if candidate == host:
            return key.lower()
    return None


def column_order(dataset_types: Iterable[str]) -> list[str]:
    """Return the columns a generated upload_meta sheet should carry, in template order.

    Every column is created even when the portal has no value for it: make_json indexes
    several columns directly, so a missing column raises KeyError where a blank cell is
    merely an empty value.
    """
    dataset_types = set(dataset_types)
    columns = list(BASE_COLUMNS)
    if 'stationTimeSeries' in dataset_types:
        columns += STATION_COLUMNS
    if 'spatioTemporal' in dataset_types:
        columns += SPATIOTEMPORAL_COLUMNS
    # Safety net: whatever validation.py asks for must exist, so that adding a column
    # there can never silently start producing sheets that are missing it.
    expected = list(REQUIRED_COLUMNS) + list(OPTIONAL_COLUMNS)
    if 'stationTimeSeries' in dataset_types:
        expected += STATION_OPTIONAL_COLUMNS
    if 'spatioTemporal' in dataset_types:
        expected += SPATIOTEMPORAL_OPTIONAL_COLUMNS
    for column in expected:
        if column not in columns:
            columns.append(column)
    return columns


def is_blank(value: Any) -> bool:
    """Return True when a spreadsheet cell is missing, NaN, or whitespace only."""
    if value is None:
        return True
    try:
        if pd.isna(value):
            return True
    except (TypeError, ValueError):
        return False
    return not str(value).strip()


def metadata_host(uri: str) -> str:
    """Return just the scheme and host of a URI, e.g. "https://meta.icos-cp.eu".

    Deriving the host from the landing page URI itself is what lets both
    meta.icos-cp.eu and citymeta.icos-cp.eu work with no special casing.
    """
    parsed = urlparse(str(uri).strip())
    if not parsed.scheme or not parsed.netloc:
        raise ValueError(f'Not an absolute URI: {uri}')
    return urlunparse((parsed.scheme, parsed.netloc, '', '', '', ''))


def fetch_dto(landing_uri: str) -> dict:
    """Return the object's DataObjectDto, the round-trip payload zupload itself uploads."""
    landing_uri = str(landing_uri).strip()
    resp = requests.get(
        f'{metadata_host(landing_uri)}/dtodownload',
        params={'uri': landing_uri},
        timeout=REQUEST_TIMEOUT,
    )
    if resp.status_code != 200:
        raise RuntimeError(f'dtodownload returned {resp.status_code}')
    return resp.json()


def fetch_object_json(landing_uri: str) -> dict | None:
    """Return the portal's rich record for an object, or None when it cannot be fetched.

    This never raises: the four fields it supplies are best-effort extras, so a failure
    here must not cost the caller the DataObjectDto it already has.
    """
    try:
        resp = requests.get(
            str(landing_uri).strip(),
            headers={'Accept': 'application/json'},
            timeout=REQUEST_TIMEOUT,
        )
        if resp.status_code != 200:
            return None
        payload = resp.json()
    except (requests.RequestException, ValueError):
        return None
    return payload if isinstance(payload, dict) else None


def detect_dataset_type(dto: dict, obj_json: dict | None) -> DatasetType | None:
    """Work out which specificInfo branch an object uses.

    The DataObjectDto's own keys are preferred so detection still works when the rich
    record could not be fetched.
    """
    specific_info = dto.get('specificInfo') or {}
    if 'station' in specific_info or 'acquisitionInterval' in specific_info:
        return 'stationTimeSeries'
    if 'title' in specific_info or 'temporal' in specific_info:
        return 'spatioTemporal'
    if obj_json:
        specific_type = (obj_json.get('specification') or {}).get('specificDatasetType')
        if specific_type:
            return DATASET_TYPE_BY_SPECIFIC_TYPE.get(str(specific_type).strip().lower())
    return None


def dto_to_row(dto: dict, obj_json: dict | None, landing_uri: str) -> dict[str, Any]:
    """Map an object's portal metadata onto upload_meta column names.

    Only columns the portal actually has a value for are returned, so a caller can tell
    "the portal says nothing here" apart from "the portal says this is empty".
    """
    landing_uri = str(landing_uri).strip()
    host = metadata_host(landing_uri)
    specific_info = dto.get('specificInfo') or {}
    production = specific_info.get('production') or {}
    references = dto.get('references') or {}
    row: dict[str, Any] = {}

    def put(column: str, value: Any) -> None:
        if not is_blank(value):
            row[column] = value

    put('fileName', dto.get('fileName'))
    # The portal reports the hashSum base64url encoded; write it through untouched.
    put('hashSum', dto.get('hashSum'))
    put('objectSpecification', dto.get('objectSpecification'))
    put('doiURI', dto.get('preExistingDoi'))
    if references.get('keywords') is not None:
        # Keywords come from the DataObjectDto only. The rich record merges object-level
        # keywords with the object specification's, so harvesting those would silently
        # promote spec keywords into object keywords on the next upload.
        put('keywords', json.dumps(references['keywords']))
    put('licenseUrl', references.get('licence'))
    put('creatorURI', production.get('creator'))
    if production.get('contributors') is not None:
        put('contributorURI', json.dumps(production['contributors']))
    put('hostOrganizationURI', production.get('hostOrganization'))
    put('comment', production.get('comment'))
    put('created', production.get('creationDate'))
    documentation = production.get('documentation')
    if not is_blank(documentation):
        # The dto carries a bare object hash here, while the sheet holds a full URI.
        # This inverts make_json, which reduces the URI to its last path segment.
        put('documentationURI', f'{host}/objects/{str(documentation).strip()}')
    put('coverageURI', _spatial_to_cell(specific_info.get('spatial')))
    if 'variables' in specific_info:
        # The dto already holds the submittable list of names, so prefer it: the rich
        # record reports what was actually ingested rather than what the upload asked
        # for, and reaching it costs a second request that can fail.
        variables = _variable_names(specific_info['variables'])
        if variables:
            put('variablesToIngest', json.dumps(variables))
    # This is the portal's own value, mirrored back unchanged: harvest never invents one.
    # Re-uploading a harvested sheet updates the same object's metadata in place rather
    # than creating a new version, so the object keeps whatever predecessor it already
    # had. Pointing this at the harvested object would make it a next version of itself,
    # which the portal rejects, because an ICOS object id is the first 24 characters of
    # its own hashSum. Edit this cell only to correct a mistake in the original upload.
    put('isNextVersionOf', _previous_versions_cell(dto.get('isNextVersionOf')))

    dataset_type = detect_dataset_type(dto, obj_json)
    if dataset_type == 'stationTimeSeries':
        put('stationURI', specific_info.get('station'))
        instrument = specific_info.get('instrument')
        if instrument is not None:
            # make_json only json.loads this cell when it starts with "[", so a single
            # instrument must stay a bare string.
            put('instrumentURI', json.dumps(instrument) if isinstance(instrument, list) else instrument)
        put('samplingHeight', specific_info.get('samplingHeight'))
        put('numRows', specific_info.get('nRows'))
        interval = specific_info.get('acquisitionInterval') or {}
        put('startCov', interval.get('start'))
        put('stopCov', interval.get('stop'))
    elif dataset_type == 'spatioTemporal':
        put('title', specific_info.get('title'))
        put('abstract/description', specific_info.get('description'))
        temporal = specific_info.get('temporal') or {}
        interval = temporal.get('interval') or {}
        put('startCov', interval.get('start'))
        put('stopCov', interval.get('stop'))
        put('resolution', temporal.get('resolution'))
        put('forStation', specific_info.get('forStation'))

    _add_rich_record_fields(row, obj_json, dataset_type, put)
    return row


def values_equal(sheet_value: Any, portal_value: Any) -> bool:
    """Return True when a spreadsheet cell already says what the portal says.

    Cells and portal fields often encode the same fact differently - a hex hashSum
    against a base64url one, http:// against https://, 3744 against 3744.0 - and
    reporting those as disagreements would bury the differences that matter.
    """
    if is_blank(sheet_value) and is_blank(portal_value):
        return True
    if is_blank(sheet_value) or is_blank(portal_value):
        return False
    left = str(sheet_value).strip()
    right = str(portal_value).strip()
    if left == right:
        return True
    return (
        _numbers_equal(left, right)
        or _hashes_equal(left, right)
        or _instants_equal(left, right)
        or _uris_equal(left, right)
    )


def _add_rich_record_fields(
        row: dict[str, Any],
        obj_json: dict | None,
        dataset_type: DatasetType | None,
        put,
) -> None:
    """Fill the four columns the DataObjectDto cannot supply."""
    if not obj_json:
        return
    submission = obj_json.get('submission') or {}
    submitter = submission.get('submitter') or {}
    submitter_uri = (submitter.get('self') or {}).get('uri')
    if not is_blank(submitter_uri):
        # The dto's own submitterId is always an empty string, so it is never usable.
        put('submitterID', str(submitter_uri).rstrip('/').rsplit('/', 1)[-1])
    references = obj_json.get('references') or {}
    licence = references.get('licence')
    if isinstance(licence, dict):
        put('licenseName', licence.get('name'))
    if dataset_type == 'stationTimeSeries':
        # The portal generates these titles; they are not in the dto at all.
        put('title', references.get('title'))
    if 'variablesToIngest' not in row:
        # Fallback for objects whose dto omits the variables key entirely, such as the
        # Cities footprint objects. It must never override what the dto supplied.
        variables = _variable_names((obj_json.get('specificInfo') or {}).get('variables'))
        if variables:
            put('variablesToIngest', json.dumps(variables))


def _object_id(uri: str) -> str:
    """Return the object id at the end of a landing page URI."""
    return str(uri).strip().rstrip('/').rsplit('/', 1)[-1]


def _previous_versions_cell(previous: Any) -> Any:
    """Reduce the dto's isNextVersionOf to the value make_json reads back.

    A single predecessor stays a bare id, which reads well in a spreadsheet and matches
    the convention of utils.get_prev_by_name; make_json passes it through untouched
    because it only json.loads a cell that starts with "[". Several predecessors have to
    be JSON so that make_json parses them back into a list. No predecessor leaves the
    cell blank rather than writing an empty list.
    """
    if isinstance(previous, str):
        return _object_id(previous) or None
    if not isinstance(previous, list):
        return None
    ids = []
    for item in previous:
        if not isinstance(item, str):
            continue
        # Defensive: these arrive as bare ids, but reduce a full URI just in case.
        candidate = _object_id(item)
        if candidate:
            ids.append(candidate)
    if not ids:
        return None
    if len(ids) == 1:
        return ids[0]
    return json.dumps(ids)


def _variable_names(variables: Any) -> list[str]:
    """Reduce the portal's variables to the plain names that can be submitted.

    The dto is the primary source for this column and already uses this shape, so a dto
    list passes straight through. The rich record is the fallback for objects whose dto
    omits the key, and there each variable arrives in the read representation: alongside
    the name it carries minMax, model and valueType, all computed by the portal at
    ingestion and none of them submittable. Only the names go in, matching the plain
    strings in constants/excluded_vars.py.

    The plain-name shape is confirmed against the upload endpoint: a staging upload
    posting ["cement", "combustion"] succeeded.
    """
    if not isinstance(variables, list):
        return []
    names: list[str] = []
    for entry in variables:
        if isinstance(entry, str):
            name = entry.strip()
        elif isinstance(entry, dict):
            name = str(entry.get('label') or '').strip()
        else:
            continue
        if name:
            names.append(name)
    return names


def _spatial_to_cell(spatial: Any) -> Any:
    """Reduce specificInfo.spatial to the value make_json can read back.

    A ``spcov_...`` URI is the clean round-trip value; anything else has to go into the
    cell as JSON, which is what make_json falls back to parsing.
    """
    if spatial is None:
        return None
    if isinstance(spatial, str):
        return spatial.strip() or None
    if isinstance(spatial, dict):
        feature = spatial.get('feature')
        if isinstance(feature, dict) and not is_blank(feature.get('uri')):
            return feature['uri']
        if not is_blank(spatial.get('uri')):
            return spatial['uri']
    return json.dumps(spatial)


def _numbers_equal(left: str, right: str) -> bool:
    try:
        return float(left) == float(right)
    except (TypeError, ValueError):
        return False


def _hash_bytes(value: str) -> bytes | None:
    """Decode a sha256 digest written as 64 hex chars or 43 base64url chars."""
    if len(value) == 64:
        try:
            return bytes.fromhex(value)
        except ValueError:
            return None
    if len(value) == 43:
        try:
            return base64.urlsafe_b64decode(value + '=')
        except (binascii.Error, ValueError):
            return None
    return None


def _hashes_equal(left: str, right: str) -> bool:
    left_bytes = _hash_bytes(left)
    return left_bytes is not None and left_bytes == _hash_bytes(right)


def _parse_instant(value: str) -> dt | None:
    candidate = value[:-1] + '+00:00' if value.endswith('Z') else value
    try:
        return dt.fromisoformat(candidate)
    except ValueError:
        return None


def _instants_equal(left: str, right: str) -> bool:
    left_instant = _parse_instant(left)
    right_instant = _parse_instant(right)
    if left_instant is None or right_instant is None:
        return False
    try:
        return left_instant == right_instant
    except TypeError:
        # One is timezone aware and the other is not, so they are not comparable.
        return False


def _strip_scheme(value: str) -> str:
    for prefix in ('https://', 'http://'):
        if value.startswith(prefix):
            return value[len(prefix):].rstrip('/')
    return value.rstrip('/')


def _uris_equal(left: str, right: str) -> bool:
    """Treat http:// and https:// forms of the same URI as equal.

    The metadata store holds ICOS spec URIs under http:// while spreadsheets usually
    write https://, so a strict comparison flags nearly every row.
    """
    if not left.startswith(('http://', 'https://')) or not right.startswith(('http://', 'https://')):
        return False
    return _strip_scheme(left) == _strip_scheme(right)
