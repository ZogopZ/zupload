# Standard library imports.
from typing import Optional, Any
import re


def exit_zupload(info: Optional[dict[Any, Any]] = None,
                 exit_type: Optional[str] = None) -> None:
    exit_message = str()
    if info:
        if all(key in info.keys() for key in ["general_date", "file_name"]):
            exit_message = (
                f"\tError! Incorrect 6-digit date values: "
                f"{info['general_date']} where spotted in file: "
                f"{info['file_name']}.\n"
                f"\tNeed to have only one 6-digit date value specified in "
                f"file\"s name.\n"
                f"\tZupload will now exit."
            )
        elif "message" in info.keys():
            exit_message = \
                f"\tZupload will now exit with message: {info['message']}."
    if exit_type:
        if exit_type == "upload_data":
            content = list(filter(None, re.split("\n|\r", info["text"])))
            content = "\n\t".join(content)
            exit_message = (
                f"\tError while uploading data for file "
                f"{info['file_name']}.\n"
                f"\tStatus code: {info['status_code']}\n"
                f"\t***\n"
                f"\t{content}\n"
                f"\t***\n"
                f"\tZupload will now exit."
            )
        elif exit_type == "upload_meta_data":
            content = list(filter(None, re.split("\n|\r", info["text"])))
            content = "\n\t".join(content)
            exit_message = (
                f"\tError while uploading meta-data for file "
                f"{info['file_name']}.\n"
                f"\tStatus code: {info['status_code']}\n"
                f"\t***\n"
                f"\t{content}\n"
                f"\t***\n"
                f"\tZupload will now exit."
            )
        elif exit_type == "try_ingest":
            content = list(filter(None, re.split("\n|\r", info["text"])))
            content = "\n\t".join(content)
            exit_message = (
                f"\tError while trying-ingestion of file "
                f"{info['file_name']}.\n"
                f"\tStatus code: {info['status_code']}\n"
                f"\t***\n"
                f"\t{content}\n"
                f"\t***\n"
                f"\tZupload will now exit."
            )
            print(exit_message)
            return
        elif exit_type == "authentication":
            exit_message = (
                f'\tControlled exit during authentication.\n'
                f'\tZupload will now exit.'
            )
        elif exit_type == 'empty_meta_data':
            exit_message = (
                f'\tError while constructing meta-data '
                f'\t{info}\n'
                f'\tZupload will now exit.'
            )
        elif exit_type == 'todo' or exit_type == 'todos':
            exit_message = (
                f'\tControlled exit due to todos.\n'
                f'\tZupload will now exit.'
            )
    else:
        exit_message = exit_message or "Zupload will now exit"
    exit(exit_message)
