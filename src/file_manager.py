# Standard library imports.
from typing import Tuple, Any
import json
import pathlib
import re

# Related third party imports.
import xarray

# Local application/library specific imports.
from constants import endpoints as ce
from constants import general_settings as cgs
from constants import icons as ci
from exiter import exit_zupload
from json_manager import write_json
from rename_specs import YamlSettings
import meta_tools


class FileManager:

    def __init__(self, settings: YamlSettings) -> None:
        self.settings = settings
        self.input_data = self.retrieve_input_files()

    def retrieve_input_files(self) -> Tuple[pathlib.PosixPath]:
        print(f"- Retrieving data files using: \"{self.settings.data_dir}"
              f"{self.settings.pattern}\"")
        pathlib_files = list(pathlib.Path(self.settings.data_dir).
                             glob(pattern=self.settings.pattern))
        found_files, file_info = zip(
            *sorted(map(
                lambda file: (file,
                              f"{file.name} {file.stat().st_size} bytes"),
                pathlib_files))
        )
        self.maybe_show_input_files(file_info, len(found_files))
        return found_files

    def archive_files(self) -> None:
        print("- Archiving system information")
        archive_out = dict()
        for file in self.input_data:
            dataset_type, dataset_object_spec = meta_tools.get_spec(file.name)
            archive_out[file.stem] = {
                "file_path": str(file.resolve()),
                "file_name": file.name,
                "dataset_type": dataset_type,
                "dataset_object_spec": dataset_object_spec,
                "try_ingest_components": build_try_ingest(
                    str(file.resolve()), dataset_object_spec)
            }
            # Todo: Edit this for each new dataset type.
            if self.settings.reason in ["cte-hr", "cte-hr-202306"]:
                general_date = re.findall(r"\d{6}", file.name)
                if len(general_date) != 1:
                    exit_zupload(info={"file_name": file.name,
                                       "general_date": general_date})
                archive_out[file.stem]["year"] = general_date[0][0:4]
                archive_out[file.stem]["month"] = general_date[0][4:6]
            self.maybe_show_progress_archive_files(file.name)
        self.maybe_save_archive(archive_out)

    def maybe_show_input_files(self, file_info: tuple[str], total_files: int)\
            -> None:
        if self.settings.show_input_files:
            for info in file_info:
                print(f"\t{info}")
            # print(*file_info, sep="\n\t")
            print(f"\tTotal of {total_files} files.")

    def maybe_show_progress_archive_files(self, file_name: str) -> None:
        if self.settings.show_progress_archive_files:
            print(f"\tSuccessfully archived information for {file_name} "
                  f"{ci.ICON_CHECK}")

    def maybe_save_archive(self, archive: dict[str, Any]) -> None:
        if pathlib.Path(self.settings.archive_path).exists() and \
                self.settings.overwrite_archive:
            write_json(self.settings.archive_path, archive)
        return


def build_try_ingest(file_path: str, dataset_object_spec: str)\
        -> dict[str, str]:
    """Build the try-ingest command for each data file."""
    try:
        xarray_dataset = xarray.open_dataset(file_path)
    except ValueError as e:
        variables = None
    else:
        variable_list = list(
            variable for variable in xarray_dataset.data_vars
            if variable not in cgs.EXCLUDED_VARIABLES
        )
        # The variable list must be formatted like this:
        # '["variable_1", "variable_2", ...]'
        # Formatting like this e.g:
        # "['variable_1', 'variable_2', ...]"
        # will probably result in a try ingest error.
        # This is why we use json.dumps,
        # to create a specifically formatted string.
        variables = f"{json.dumps(variable_list)}"
    try_ingest_url = ce.CP_TRY_INGEST
    params = dict({"specUri": dataset_object_spec,
                   "varnames": variables})
    try_ingest_components = {"url": try_ingest_url,
                             "params": params,
                             "file_path": file_path}
    return try_ingest_components




# def validate_json(path: str = None, json_data: str = None):
#     """
#     Read dictionary from json file or validate json.
#
#     Can also be used to check for valid json either from file or from
#     another object like a response from a request.
#     """
#     if path:
#         with open(file=path, mode='r') as json_handle:
#             json_data = json.load(json_handle)
#     else:
#         json_data = json.loads(json_data)
#     return json_data




