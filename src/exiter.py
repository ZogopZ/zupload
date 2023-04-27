# Format read from https://peps.python.org/pep-0008/#imports.
# Standard library imports.
import re

# Related third party imports.

# Local application/library specific imports.


def exit_zupload(exit_type: str = None, info: dict = None):
    exit_message = str()
    if info and \
            all(key in info.keys() for key in ['general_date', 'file_name']):
        exit_message = (
            f'\tError! Incorrect 6-digit date values: {info["general_date"]}'
            f' where spotted in file: {info["file_name"]}.\n'
            f'\tNeed to have only one 6-digit date value specified in file\'s'
            f' name.\n'
            f'\tZupload will now exit.'
        )
    elif exit_type:
        if exit_type == 'upload_data':
            content = list(filter(None, re.split('\n|\r', info['text'])))
            content = '\n\t'.join(content)
            exit_message = (
                f'\tError while uploading data for file '
                f'{info["file_name"]}.\n'
                f'\tStatus code: {info["status_code"]}\n'
                f'\t***\n'
                f'\t{content}\n'
                f'\t***\n'
                f'\tZupload will now exit.'
            )
        elif exit_type == 'upload_meta_data':
            content = list(filter(None, re.split('\n|\r', info['text'])))
            content = '\n\t'.join(content)
            exit_message = (
                f'\tError while uploading meta-data for file '
                f'{info["file_name"]}.\n'
                f'\tStatus code: {info["status_code"]}\n'
                f'\t***\n'
                f'\t{content}\n'
                f'\t***\n'
                f'\tZupload will now exit.'
            )
        elif exit_type == 'try_ingest':
            content = list(filter(None, re.split('\n|\r', info['text'])))
            content = '\n\t'.join(content)
            exit_message = (
                f'\tError while trying-ingestion of file '
                f'{info["file_name"]}.\n'
                f'\tStatus code: {info["status_code"]}\n'
                f'\t***\n'
                f'\t{content}\n'
                f'\t***\n'
                f'\tZupload will now exit.'
            )
        elif exit_type == 'authentication':
            exit_message = (
                f'\tControlled exit during authentication.\n'
                f'\tZupload will now exit.'
            )
        elif exit_type == 'todo':
            exit_message = (
                f'\tControlled exit dues to todos.\n'
                f'\tZupload will now exit.'
            )
    else:
        exit_message = 'Zupload will now exit.'
    exit(exit_message)
    return
