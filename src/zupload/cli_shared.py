"""Helpers shared by the package's Typer CLIs.

These functions parse and validate command line input, so unlike ``validation.py``
they are allowed to depend on Typer: they echo user facing guidance and raise
``typer.Exit``. Keep pure metadata logic out of here.
"""
# Standard library imports.
from pathlib import Path
# Related third party imports.
import typer


def resolve_spreadsheet(spreadsheet: str | None, command: str = 'zupload') -> Path:
    """Return the spreadsheet to work on, auto-detecting a single .xlsx in the current directory."""
    if spreadsheet is None:
        matches = list(Path.cwd().glob('*.xlsx'))
        if not matches:
            typer.echo('No .xlsx files found in current directory.')
            raise typer.Exit(code=1)
        if len(matches) > 1:
            typer.echo('More than one spreadsheet found in current directory:')
            for match in matches:
                typer.echo(f'- {match.name}')
            typer.echo('Please rerun by explicitly passing the spreadsheet path, for example:')
            typer.echo(f'{command} ./your_spreadsheet.xlsx [options]')
            raise typer.Exit(code=1)
        return matches[0]
    return Path(spreadsheet)


def select_rows(df, rows_value, flag='--rows'):
    """Slice df to the given upload_meta sheet row spec ("5" or "5-12", inclusive). Returns the sliced df."""
    value = rows_value.strip()
    if '-' in value:
        parts = value.split('-')
        if len(parts) != 2 or not parts[0] or not parts[1]:
            typer.echo(
                f'Invalid {flag} value "{rows_value}". '
                'Expected an integer like "5" or a range like "5-12".'
            )
            raise typer.Exit(code=1)
        try:
            start = int(parts[0])
            end = int(parts[1])
        except ValueError:
            typer.echo(
                f'Invalid {flag} value "{rows_value}". '
                'Expected an integer like "5" or a range like "5-12".'
            )
            raise typer.Exit(code=1)
        if start > end:
            typer.echo(
                f'Invalid {flag} range "{rows_value}": '
                'start must be <= end.'
            )
            raise typer.Exit(code=1)
    else:
        try:
            start = int(value)
        except ValueError:
            typer.echo(
                f'Invalid {flag} value "{rows_value}". '
                'Expected an integer like "5" or a range like "5-12".'
            )
            raise typer.Exit(code=1)
        end = start
    if start < 2 or end > (len(df) + 1):
        typer.echo(
            f'Invalid {flag} range "{rows_value}". '
            f'Expected 2..{len(df) + 1} for upload_meta.'
        )
        raise typer.Exit(code=1)
    return df.iloc[start - 2 : end - 1]
