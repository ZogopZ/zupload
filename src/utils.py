# Standard library imports.
from typing import Optional, Any
from http.cookiejar import CookieJar

# Related third party imports.
from icoscp_core import icos
import requests
from requests.utils import cookiejar_from_dict

# Local application/library specific imports.


def get_cookie_jar() -> Optional[CookieJar]:
    cookie_string = str()
    cookie_jar: Optional[CookieJar] = None
    cookie_dict = dict()
    try:
        cookie_string = icos.auth.get_token().cookie_value
    except Exception as e:
        if "Config file does not exist" in str(e):
            icos.auth.init_config_file()
            cookie_string = icos.auth.get_token().cookie_value
        elif "Incorrect user name or password" in str(e):
            icos.auth.init_config_file()
            cookie_string = icos.auth.get_token().cookie_value
        else:
            print("Got a weird exception...", e)
    else:
        # Convert string to dictionary.
        cookie_dict = {cookie.split("=")[0]: cookie.split("=")[1] for
                       cookie in cookie_string.split("; ")}
    finally:
        if cookie_string:
            # Convert dictionary to RequestsCookieJar.
            cookie_jar = cookiejar_from_dict(cookie_dict)
    return cookie_jar


def progress_bar(info: dict[str, str], operation: Optional[str] = None,
                 current: Optional[int] = None, total: Optional[int] = None,
                 bar_length: int = 20) -> None:
    """
    Outputs a loading-like bar for various operations.

    Credits to: https://stackoverflow.com/a/37630397
    """
    prepender = str()
    if operation == 'archive_system_info':
        prepender = (
            f'\tArchiving {info["file_name"]}'
        )
    elif operation == 'download_rest_countries':
        prepender = (
            f'\tDownloading rest countries from '
            f'{endpoints.REST_COUNTRIES}:'
        )
    elif operation == 'zip_files':
        prepender = (
            f'\tZipping  |{info["source_file"]}|  to  '
            f'|{info["target_zip"]}|'
        )
    elif operation == 'calculate_hash_sum':
        prepender = (
            f'\tCalculating hash sum of {info["file_name"]}'
        )
    elif operation == 'archive_meta_data':
        prepender = (
            f'\tArchiving meta-data for file {info["file_name"]}'
        )
    elif operation == 'try_ingest':
        prepender = (
            f'\tTried ingestion of file: {info["file_name"]}'
        )
    elif operation == 'upload_meta_data':
        prepender = (
            f'\tUploaded meta-data for file: {info["file_name"]}'
        )
    elif operation == 'upload_data':
        prepender = '\t'
        upload_info = (
            f'\tUploaded data for file: {info["file_name"]} | '
            f'status code: {info["response"].status_code} | '
            f'response text: {info["response"].text}'
        )
        print(upload_info)
    elif operation == 'chunk':
        prepender = (
            f'\tUploading data: '
        )
    fraction = current / total
    arrow = int(fraction * bar_length - 1) * '-' + '>'
    padding = int(bar_length - len(arrow)) * ' '
    if current == total:
        prepender = '\tCompletion'
        ending = f' {icons.ICON_CHECK}\n'
    else:
        ending = '\r'
    progress = str(
        f'{prepender} '
        f'{info["file_name"] if "file_name" in info.keys() else ""} '
        f'[{arrow}{padding}] {int(fraction*100)}%'
    )
    print(200*' ', end='\r', flush=True)
    print(progress, end=ending, flush=True)


def handle_request(request: str, args: dict[Any, Any]) -> requests.Response:
    """Send and handle a request."""
    response = requests.Response()
    try:
        if request == "get":
            response = requests.get(**args)
        elif request == "put":
            response = requests.put(**args)
        elif request == "post":
            response = requests.post(**args)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        pass
    else:
        if response.status_code == 200:
            pass
    return response
