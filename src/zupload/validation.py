import json
from pathlib import Path
from typing import Any

import pandas as pd
from pandas import Series

from zupload.constants.object_specs import ALL_OBJECT_SPECS


def _normalise_scheme(value: str) -> str:
    """Strip the http/https distinction so spec URIs compare equal either way."""
    candidate = value.strip()
    if candidate.startswith('https://'):
        return candidate[len('https://'):]
    if candidate.startswith('http://'):
        return candidate[len('http://'):]
    return candidate


def _looks_like_hash(value: str) -> bool:
    """Return True if value is a plausible object hash (base64url or hex)."""
    candidate = value.strip()
    if len(candidate) < 16:
        return False
    return all(
        char.isalnum() or char in '-_=' for char in candidate
    )


LANDING_URL_ISSUE = (
    'isNextVersionOf is a landing page URL; the portal needs the object id'
)


_KNOWN_SPECS_NORM = {
    _normalise_scheme(value) for value in ALL_OBJECT_SPECS.values()
}


REQUIRED_COLUMNS = [
    'fileName',
    'fileLocation',
    'isNextVersionOf',
    'doiURI',
    'objectSpecification',
    'licenseUrl',
    'title',
    'startCov',
    'stopCov',
    'creatorURI',
    'contributorURI',
    'created',
    'submitterID',
]


OPTIONAL_COLUMNS = [
    'abstract/description',
    'coverageURI',
    'documentationURI',
    'hashSum',
    # Both are optional in the payload, so an absent column is not a schema problem:
    # make_json reads them with .get() and harvest omits columns the portal cannot fill.
    'keywords',
    'comment',
    # Object-level host organization is optional too: the portal's own dtodownload
    # payloads omit the hostOrganization key entirely for objects submitted without one.
    'hostOrganizationURI',
]


STATION_OPTIONAL_COLUMNS = [
    'numRows',
    'stationURI',
    'instrumentURI',
    'samplingHeight',
]


SPATIOTEMPORAL_OPTIONAL_COLUMNS = [
    'resolution',
    'variablesToIngest',
    'forStation',
]


def validate_columns(df, dataset_type: str | None = None) -> list[tuple[str, str]]:
    """Report upload_meta columns that make_json requires but are absent from the sheet."""
    issues: list[tuple[str, str]] = []
    for column in REQUIRED_COLUMNS:
        if column not in df.columns:
            issues.append(('error', f'{column} column is missing from the upload_meta sheet'))
    optional_columns = list(OPTIONAL_COLUMNS)
    if dataset_type in (None, 'stationTimeSeries'):
        optional_columns += STATION_OPTIONAL_COLUMNS
    if dataset_type in (None, 'spatioTemporal'):
        optional_columns += SPATIOTEMPORAL_OPTIONAL_COLUMNS
    for column in optional_columns:
        if column not in df.columns:
            issues.append((
                'warning',
                f'{column} column is optional and is missing from the upload_meta sheet',
            ))
    return issues


def validate_row(
        row: Series,
        dataset_type: str | None = None,
        known_specs: set[str] | None = None,
) -> list[tuple[str, str]]:
    """Check one upload_meta row and return issues without raising."""
    issues: list[tuple[str, str]] = []
    is_station = dataset_type == 'stationTimeSeries'

    def is_blank(value: Any) -> bool:
        if pd.isna(value):
            return True
        return not str(value).strip()

    # keywords is deliberately absent here. Object-level keywords are optional: the
    # portal's own objects routinely carry none, and the keywords a landing page shows
    # may come from the object specification rather than from the object itself.
    # contributorURI is deliberately absent too, but for a different reason: the column
    # stays required while its value does not. The portal's own payloads always carry a
    # contributors list, yet it is often empty, so a blank cell is normal and make_json
    # turns it into [].
    required_fields = [
        'fileName',
        'title',
        'objectSpecification',
        'submitterID',
        'created',
    ]
    if is_station:
        # StationTimeSeriesDto has no title field.
        required_fields.remove('title')
    for field in required_fields:
        if field in row and is_blank(row.get(field)):
            issues.append(('error', f'{field} is required and is blank'))

    if is_station:
        if is_blank(row.get('stationURI')) and is_blank(row.get('forStation')):
            issues.append((
                'error',
                'station is required; both stationURI and forStation are blank',
            ))

    # hostOrganizationURI is deliberately absent here, for the same reason as keywords:
    # object-level host organization is optional, and the portal's own dtodownload
    # payloads omit the hostOrganization key entirely for objects submitted without one.
    expected_fields = [
        'creatorURI',
        'startCov',
        'stopCov',
    ]
    if is_station:
        # acquisitionInterval is optional in the station branch.
        expected_fields.remove('startCov')
        expected_fields.remove('stopCov')
    for field in expected_fields:
        if field in row and is_blank(row.get(field)):
            issues.append(('warning', f'{field} is blank'))

    if 'licenseUrl' in row and is_blank(row.get('licenseUrl')):
        issues.append(('warning', 'licenseUrl is blank; the portal will assign its default licence'))

    if not is_blank(row.get('keywords')):
        try:
            parsed = json.loads(str(row.get('keywords')))
            if not isinstance(parsed, list):
                issues.append(('error', 'keywords is not a JSON list'))
        except (ValueError, TypeError):
            issues.append(('error', 'keywords is not valid JSON'))

    if not is_blank(row.get('contributorURI')):
        try:
            parsed = json.loads(str(row.get('contributorURI')))
            if not isinstance(parsed, list):
                issues.append(('error', 'contributorURI is not a JSON list'))
        except (ValueError, TypeError):
            issues.append(('error', 'contributorURI is not valid JSON'))

    if not is_blank(row.get('variablesToIngest')):
        try:
            json.loads(str(row.get('variablesToIngest')))
        except (ValueError, TypeError):
            issues.append(('error', 'variablesToIngest is not valid JSON'))

    if not is_blank(row.get('coverageURI')):
        coverage_raw = str(row.get('coverageURI')).strip()
        if not coverage_raw.startswith(('http://', 'https://')):
            try:
                json.loads(coverage_raw)
            except (ValueError, TypeError):
                issues.append((
                    'error',
                    'coverageURI is neither a URI nor valid JSON'
                ))

    if not is_blank(row.get('numRows')):
        num_rows_raw = str(row.get('numRows')).strip()
        try:
            num_rows = float(num_rows_raw)
        except (TypeError, ValueError):
            issues.append(('error', 'numRows is not a positive integer'))
        else:
            if num_rows <= 0 or not num_rows.is_integer():
                issues.append(('error', 'numRows is not a positive integer'))

    if not is_blank(row.get('objectSpecification')):
        spec = str(row.get('objectSpecification')).strip()
        resolved_specs = {
            _normalise_scheme(value) for value in (known_specs or set())
        }
        if _normalise_scheme(spec) not in _KNOWN_SPECS_NORM | resolved_specs:
            issues.append(('error', 'objectSpecification is not a known spec URI'))

    uri_fields = [
        'creatorURI',
        'hostOrganizationURI',
        'licenseUrl',
        'doiURI',
        'documentationURI',
    ]
    for field in uri_fields:
        if not is_blank(row.get(field)):
            value = str(row.get(field)).strip()
            if not value.startswith(('http://', 'https://')):
                issues.append(('warning', f'{field} does not look like a URI'))

    if not is_blank(row.get('isNextVersionOf')):
        prev = str(row.get('isNextVersionOf')).strip()
        prev_values = [prev]
        if prev.startswith('['):
            try:
                parsed = json.loads(prev)
            except (ValueError, TypeError):
                parsed = None
            if isinstance(parsed, list):
                prev_values = [str(item).strip() for item in parsed]
        for value in prev_values:
            # A landing page URL is the natural thing to paste, but the portal rejects
            # it with HTTP 400: isNextVersionOf takes the object id or a full hashSum.
            if value.startswith(('http://', 'https://')):
                issues.append(('error', LANDING_URL_ISSUE))
            elif not _looks_like_hash(value):
                issues.append((
                    'warning',
                    'isNextVersionOf does not look like a URI or hash',
                ))

    date_fields = ['created', 'startCov', 'stopCov']
    parsed_dates: dict[str, Any] = {}
    for field in date_fields:
        if not is_blank(row.get(field)):
            parsed = pd.to_datetime(row.get(field), errors='coerce')
            if pd.isna(parsed):
                issues.append(('warning', f'{field} is not a valid date'))
            else:
                parsed_dates[field] = parsed
    if 'startCov' in parsed_dates and 'stopCov' in parsed_dates:
        if parsed_dates['startCov'] > parsed_dates['stopCov']:
            issues.append(('warning', 'startCov is after stopCov'))

    if is_blank(row.get('hashSum')):
        if not is_blank(row.get('fileLocation')) and not is_blank(row.get('fileName')):
            data_path = Path(str(row.get('fileLocation'))) / str(row.get('fileName'))
            if not data_path.exists():
                issues.append((
                    'warning',
                    'hashSum is blank and no local data file to hash'
                ))

    return issues


def validate_dataframe(
        df,
        dataset_type: str | None = None,
        known_specs: set[str] | None = None,
) -> list[dict[str, Any]]:
    """Validate every row and return structured results without printing."""
    results: list[dict[str, Any]] = []
    for idx, row in df.iterrows():
        results.append({
            'row': idx + 2,
            'fileName': row['fileName'],
            'issues': validate_row(
                row=row,
                dataset_type=dataset_type,
                known_specs=known_specs,
            ),
        })
    return results
