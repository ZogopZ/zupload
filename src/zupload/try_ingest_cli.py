# Standard library imports.
from pathlib import Path
import json
from typing import Any
# Related third party imports.
import requests
import typer
import xarray
# Local application/library specific imports.
from zupload.constants.excluded_vars import EXCLUDED_VARIABLES
from zupload.constants.envri import ENVRIES, EnvriConfig, Envri
from zupload.utils import get_conf, read_upload_meta

app = typer.Typer(help='Try ingesting data.')

@app.command()
def main(spreadsheet: str | None = None):
    if spreadsheet is None:
        matches = list(Path.cwd().glob('*.xlsx'))
        if not matches:
            typer.echo('No .xlsx files found in current directory.')
            raise typer.Exit(code=1)
        spreadsheet = matches[0]
    else: spreadsheet = Path(spreadsheet)
    envri_conf = get_conf(file_path=spreadsheet)
    df = read_upload_meta(spreadsheet)
    for _, row in df.iterrows():
        components = build_try_ingest(file_path=Path(row['fileLocation']) / row['fileName'],
                                      obj_spec=row['objectSpecification'],
                                      envri_conf=envri_conf)
        try_ingestion(components=components)

def try_ingestion(components: dict[str, Any]) -> None:
    """Tests ingestion of provided file to the specified portal."""
    typer.echo(f'Trying ingestion of {components["file_path"]}', nl=False)
    resp = requests.put(url=components['url'],
                        data=open(file=components['file_path'], mode='rb'),
                        params=components['params'])
    if resp.status_code == 200:
        typer.echo(f' ({resp.status_code}) OK')
    else:
        typer.echo(f' ({resp.status_code}) FAILED')
        typer.echo(resp.text)
        raise typer.Exit(code=1)
    return


def build_try_ingest(file_path: str, obj_spec: str, envri_conf: EnvriConfig) -> dict[str, Any]:
    """Build the try-ingest command for each data file."""
    xr_vars = []
    try:
        xr_ds = xarray.open_dataset(file_path)
    except ValueError as e:
        print(e)
        xr_vars = None
    else:
        xr_vars = list(v for v in xr_ds.data_vars if v not in EXCLUDED_VARIABLES)
    # The variable list must be formatted like this and only this:
    # '["variable_1", "variable_2", ...]'
    meta_vars = f'{json.dumps(xr_vars)}'

    params = dict({'specUri': obj_spec,
                   'varnames': meta_vars})
    return {'url': envri_conf.try_ingest_url, 'params': params, 'file_path': file_path}


if __name__ == '__main__':
    app()