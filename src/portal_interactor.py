# Standard library imports.
from datetime import datetime
from multiprocessing import Pool
from typing import Any
from pathlib import Path
import re


# Related third party imports.
from icoscp_core import icos
import requests

# Local application/library specific imports.
from settings import Settings
from constants.icons import ICON_CHECK
from constants.endpoints import METADATA_UPLOAD_URL, \
    METASTAGING_UPLOAD_URL
from rename_specs import YamlSettings
from json_manager import read_json, write_json
import exiter
import utils
from json_manager import make_monthly_cte_hr_collection


class PortalInteractor:

    def __init__(self, settings: YamlSettings) -> None:
        self.settings = settings

    def upload_metadata(self) -> None:
        print('- Uploading meta-data.')
        archive = read_json(self.settings.archive_path)
        for base_key, base_info in archive.items():
            data = open(file=base_info["json_file_path"], mode="rb")
            headers = {"Content-Type": "application/json"}
            url = METADATA_UPLOAD_URL if self.settings.upload_to_production \
                else METASTAGING_UPLOAD_URL
            upload_metadata_response = requests.post(
                url=url,
                data=data,
                headers=headers,
                cookies=utils.get_cookie_jar()
            )
            if upload_metadata_response.status_code == 200:
                base_info["file_data_url"] = upload_metadata_response.text
                base_info["file_metadata_url"] = \
                    upload_metadata_response.text.replace("data", "meta")
            else:
                exiter.exit_zupload(
                    exit_type='upload_meta_data',
                    info=dict({
                        'status_code':
                            upload_metadata_response.status_code,
                        'text': upload_metadata_response.text,
                        'file_name': base_info['file_name']
                    }))
            self.maybe_show_progress_upload_meta_data(
                file_name=base_info["file_name"],
                landing_page=base_info["file_metadata_url"]
            )
            # Todo: Probably remove this.
            # tools.progress_bar(operation='upload_meta_data',
            #                    current=index + 1,
            #                    total=total,
            #                    info=dict({
            #                        'file_name': base_info['file_name']
            #                    }))
        self.maybe_save_archive(archive=archive)
        return

    def upload_data(self) -> None:
        print('- Uploading data.')
        archive = read_json(self.settings.archive_path)
        for base_key, base_info in archive.items():
            if 'file_data_url' not in base_info.keys():
                print(f"file_data_url key does not exist for {base_key}")
                continue
            args = {
                'url': base_info['file_data_url'],
                'cookies': utils.get_cookie_jar(),
                'headers': {'Content-Type': 'application/octet-stream'},
                'data': open(file=base_info['file_path'], mode='rb')
            }
            response = utils.handle_request(request='put', args=args)
            if response.status_code != 200:
                exiter.exit_zupload(
                    exit_type='upload_data',
                    info=dict({
                        'status_code': response.status_code,
                        'text': response.text,
                        'file_name': base_info['file_name']
                    }))
            self.maybe_show_progress_upload_data(
                file_name=base_info["file_name"]
            )
            # tools.progress_bar(operation='upload_data',
            #                    current=index + 1,
            #                    total=total,
            #                    info=dict({
            #                        'file_name': base_info['file_name'],
            #                        'response': response
            #                    }))
        self.maybe_save_archive(archive=archive)
        return

    def try_ingest(self) -> None:
        """Tests ingestion of provided files to the Carbon Portal."""
        # Todo: Fix the way try_ingest() outputs stuff.W
        print(f'- Trying ingestion of files (This might take a while...)')
        subprocesses = self.settings.try_ingest_subprocesses
        archive = read_json(self.settings.archive_path)
        checks = 0
        command_list = [base_info["try_ingest_components"] for base_info in
                        archive.values()]
        lists_to_process = [
            command_list[i * subprocesses:(i + 1) * subprocesses]
            for i in range(
                (len(command_list) + subprocesses - 1) // subprocesses
            )
        ]
        results = list()
        for item_list in lists_to_process:
            # Create a process for each item in the sublist.
            with Pool(processes=len(item_list)) as pool:
                # Each item in the sublist is passed as an argument to
                # `execute_item()` function.
                pool_results = pool.map(self.execute_item, item_list)
                for pool_result in pool_results:
                    if pool_result['status_code'] != 200:
                        exiter.exit_zupload(exit_type='try_ingest',
                                            info=pool_result)
                    else:
                        checks += 1
                        self.maybe_show_progress_try_ingest(
                            (str(pool_result["file_name"]))
                        )
                        # tools.progress_bar(operation='try_ingest',
                        #                    current=checks,
                        #                    total=total,
                        #                    info=pool_result)
                results.extend(pool_results)
        return

    @staticmethod
    def execute_item(try_ingest_components: dict[str, str]) ->\
            dict[str, str | int]:
        """Used from processes spawned by try_ingest()."""
        try_ingest_response = requests.put(
            url=try_ingest_components['url'],
            data=open(file=try_ingest_components['file_path'], mode='rb'),
            params=try_ingest_components['params']
        )
        file_name = try_ingest_components['file_path'].split('/')[-1]
        return {'status_code': try_ingest_response.status_code,
                'text': try_ingest_response.text,
                'file_name': file_name}

    def maybe_save_archive(self, archive: dict[str, Any]) -> None:
        if Path(self.settings.archive_path).exists() and \
                self.settings.overwrite_archive:
            write_json(self.settings.archive_path, archive)

    def maybe_show_progress_try_ingest(self, file_name: str) -> None:
        if self.settings.show_progress_try_ingest:
            print(f"\tSuccessfully tried ingestion of {file_name} "
                  f"{ICON_CHECK}")
        return

    def maybe_show_progress_upload_meta_data(self, file_name: str,
                                             landing_page: str) -> None:
        if self.settings.show_progress_upload_meta_data:
            print(f"\tSuccessfully uploaded meta-data for {file_name} "
                  f"{ICON_CHECK} -> {landing_page}")
        return

    def maybe_show_progress_upload_data(self, file_name: str) -> None:
        if self.settings.show_progress_upload_data:
            print(f"\tSuccessfully uploaded data for {file_name} "
                  f"{ICON_CHECK}")
        return


def download_collections() -> Any:
    """SPARQL query for all collections."""
    with open(file="queries/collections.txt", mode="r") as query_handle:
        query = query_handle.read()
    return icos.meta.sparql_select(query=query)


def get_cte_hr_collections(interval: str) -> dict[str, str]:
    query_results = download_collections()
    collections: dict[str, str] = dict()
    if interval == "monthly":
        pattern = r"High-resolution, near-real-time fluxes over Europe " \
                  r"from CTE-HR for \d{4}-\d{2}$"
        for res in query_results.bindings:
            if match := re.search(pattern, res["title"].value):
                collections.setdefault(
                    match[0][-7:].replace("-", ""),
                    res["coll"].uri
                )
    elif interval == "yearly":
        pattern = r"High-resolution, near-real-time fluxes over Europe " \
                  r"from CTE-HR for \d{4}$"
        for res in query_results.bindings:
            if (match := re.search(pattern, res["title"].value)) and \
                    "R4FivCqCR62RruN3mvh5dG2b" not in res["coll"].uri:
                collections.setdefault(
                    match[0][-4:],
                    res["coll"].uri
                )
    else:
        pattern = r"High-resolution, near-real-time fluxes over Europe " \
                  r"from CTE-HR for \d{4}-\d{4}$"
        for res in query_results.bindings:
            if match := re.search(pattern, res["title"].value):
                collections.setdefault(
                    match[0][-9:].replace("-", "_"),
                    res["coll"].uri
                )
    return collections


def get_meta(landing_page: str) -> Any:
    """Returns metadata for a landing page"""
    json_url = f"{landing_page}/easter_egg.json"
    response = utils.handle_request(request='get', args={"url": json_url})
    return response.json()


def get_collection_members(landing_page: str) -> Any:
    """Returns a list of members"""
    return get_meta(landing_page=landing_page)["members"]


def sort_members(members: list[dict[Any, Any]]) -> list[str]:
    sorted_members = list()
    if all([member.keys() for member in members if "title" in member]):
        sorted_members = sorted(members, key=lambda x: x["title"])
    else:
        print("Not all members have a \"title\" key.")
        exiter.exit_zupload(exit_type="todo")
    return sorted_members


def upload_collection(json_file: str) -> Any:
    response = utils.handle_request(
        request="post",
        args={
            "url": METADATA_UPLOAD_URL,
            "data": open(file=json_file, mode="rb"),
            "headers": {"Content-Type": "application/json"},
            "cookies": utils.get_cookie_jar()
            }
    )
    if response.status_code == 200:
        print(response.text)
    else:
        print(response.status_code,
              response.text)
        exiter.exit_zupload(
            exit_type="upload_meta_data",
            info=dict({
                "status_code":
                    response.status_code,
                "text": response.text,
                "file_name": json_file
            })
        )
    return response.text


def get_previous_versions() -> dict[str, str]:
    with open(file="queries/anthropogenic_nrt.txt", mode="r") as query_handle:
        query = query_handle.read()
    query_results = icos.meta.sparql_select(query=query)
    versions = dict({
        res["fileName"].value: res["dobj"].uri.split("/")[-1]
        for res in query_results.bindings
    })
    return versions
