# Standard library imports.
from datetime import datetime
from typing import Any, Optional, Union
import hashlib
import json
import pathlib
import re

# Related third party imports.
import xarray
from xarray.core.dataset import Dataset as XarrayDataset
from icoscp_core import icos
from icoscp_core.sparql import sparql_select, SparqlResults

import exiter
# Local application/library specific imports.
from constants.licences import ICOS_LICENSE
from constants.icons import ICON_CHECK
from constants.people import *
from constants.static_meta_paths import *
from constants.organizations import *
from constants.spatial_boxes import *
from constants.general_settings import EXCLUDED_VARIABLES
from rename_specs import YamlSettings


class JsonManager:

    def __init__(self, settings: YamlSettings) -> None:
        self.settings = settings

    # Todo: Maybe multi-process this.
    def archive_json(self) -> None:
        """Generates standalone .json files and adds to archive.

        Generates the standalone .json file for each data file and updates
        the archive with the regenerated json content. This function needs
        to be rerun each time we need to change something in the meta-data.
        If we decide to rerun this then it is mandatory that we also
        overwrite the `archive_in_nc.json` file using the function
        `store_current_archive()` at the end of the script.

        """
        print("- Archiving meta-data (Includes hash-sum calculation.)")
        self.make_json()

    def make_json(self) -> None:
        archive = read_json(self.settings.archive_path)
        for base_key, base_info in archive.items():
            xarray_dataset = xarray.open_dataset(base_info["file_path"])
            base_info["json"] = dict({
                "fileName": base_info["file_name"],
                "hashSum": get_hash_sum(file_path=base_info["file_path"],
                                        progress=False),
                "isNextVersionOf":
                    get_previous_version(reason=self.settings.reason,
                                         file_name=base_info["file_name"]),
                "objectSpecification": base_info["dataset_object_spec"],
                "references": {
                    "keywords": get_keywords(reason=self.settings.reason,
                                             dataset=xarray_dataset),
                    "licence": ICOS_LICENSE,
                },
                "specificInfo": {
                    "description":
                        get_description(
                            reason=self.settings.reason,
                            dataset=xarray_dataset
                        ),
                    "production": {
                        "contributors":
                            get_contributors(reason=self.settings.reason),
                        "creationDate":
                            get_creation_date(reason=self.settings.reason,
                                              dataset=xarray_dataset),
                        "creator": get_creator(reason=self.settings.reason),
                        "hostOrganization":
                            get_host_org(reason=self.settings.reason),
                        # Todo: rework this for future cte-hr versions.
                        "comment": get_comment(reason=self.settings.reason,
                                               file_name=base_info[
                                                   "file_name"]),
                        "sources": [],
                        "documentation":
                            get_documentation(reason=self.settings.reason),
                    },
                    "spatial": get_spatial_box(
                        reason=self.settings.reason,
                        file_name=base_info["file_name"],
                        dataset=xarray_dataset),
                    "temporal": {
                        "interval": {
                            "start": xarray_dataset.time[0].dt.strftime(
                                "%Y-%m-%dT%H:%M:%SZ").item(),
                            "stop": xarray_dataset.time[-1].dt.strftime(
                                "%Y-%m-%dT%H:%M:%SZ").item(),
                        },
                        "resolution":
                            get_resolution(reason=self.settings.reason,
                                           file_name=base_info["file_name"],
                                           dataset=xarray_dataset),
                    },
                    "title": get_title(reason=self.settings.reason,
                                       additional_info=base_info,
                                       dataset=xarray_dataset),
                    "variables": [
                        variable for variable in xarray_dataset.data_vars
                        if variable not in EXCLUDED_VARIABLES
                    ]
                },
                "submitterId": "CP",
            })
            base_info["json_file_path"] = \
                str(
                    pathlib.Path(self.settings.json_standalone_files,
                                 f"{base_key}.json"
                                 ).resolve()
                )
            write_json(base_info["json_file_path"], base_info["json"])
            self.maybe_show_progress_archive_json(base_info["file_name"])
        self.maybe_save_archive(archive)

    def maybe_save_archive(self, archive: dict[str, Any]) -> None:
        if pathlib.Path(self.settings.archive_path).exists() and \
                self.settings.overwrite_archive:
            write_json(self.settings.archive_path, archive)

    def maybe_show_progress_archive_json(self, file_name: str) -> None:
        if self.settings.show_progress_archive_json:
            print(f"\tSuccessfully archived json for "
                  f"{file_name} {ICON_CHECK}")

    def show_uploads(self) -> None:
        print("- Showing uploaded landing pages.")
        archive = read_json(self.settings.archive_path)
        for base_key, base_info in archive.items():
            if "file_metadata_url" in base_info:
                print(f"\t{base_info['file_metadata_url']}")
            else:
                print(f"\tnothing to show for {base_key}")
        return


def get_hash_sum(file_path: str, progress: bool = True) -> str:
    """Calculate and return hash-sum of given file."""
    sha256_hash = hashlib.sha256()
    with open(file=file_path, mode='rb') as file_handle:
        total = int(pathlib.Path(file_path).stat().st_size)
        # total = int(os.stat(file_path).st_size)
        current = int()
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: file_handle.read(4096), b""):
            sha256_hash.update(byte_block)
            current += len(byte_block)
            # Printing out the progress bar while calculating a
            # hash-sum of a big file is a strenuous task; thus limit
            # the output using multiples of 4096 and when all bytes
            # are read.
            if (current % 65535 == 0 or current == total) and progress:
                progress_bar(
                    operation='calculate_hash_sum', current=current,
                    total=total,
                    info=dict({
                        'file_name': file_path.split('/')[-1]
                    }))
    return sha256_hash.hexdigest()


def get_previous_version(reason: str, file_name: str) -> list[str | None]:
    with open(file="queries/get_previous_by_name.txt",
              mode="r") as query_handle:
        replacer = file_name.replace("050_monthly", "monthly_halfdeg") \
            if "050_monthly" in file_name else file_name
        query = query_handle.read().replace("#file_name#", replacer)
    sparql_results: SparqlResults = icos.meta.sparql_select(query=query)
    if reason in ["fluxcom-et", "fluxcom-et-t", "fluxcom-gpp", "fluxcom-nee"]\
            and not sparql_results.bindings:
        print(f"Warning! Previous version for {file_name} was not found!")
    return [sparql_results.bindings[0]["dobj"].uri.split("/")[-1]] if \
        sparql_results.bindings else []


def get_keywords(reason: str, dataset: XarrayDataset) -> list[str | None]:
    keywords: list[str | None] = []
    if reason == "cte-hr":
        keywords = ["carbon flux"]
    elif reason == "avengers":
        keywords = ["AVENGERS", "aerosols"]
    elif reason == "cte-gcp":
        keywords = dataset.keywords.split(", ")
    elif reason in ["fluxcom-et", "fluxcom-et-t", "fluxcom-gpp"]:
        keywords = dataset.keywords + ['FLUXCOM']
    else:
        print("Warning! Keywords field empty!")
    return keywords


def get_description(reason: str, dataset: XarrayDataset) -> Optional[str]:
    description = str()
    if reason == "cte-hr":
        description = dataset.comment
    elif reason == "avengers":
        description = (
            "This aerosol emission dataset is based on the CAMS-REG "
            "inventory version 5 (Kuenen et al, 2022, "
            "https://doi.org/10.5194/essd-14-491-2022) but modified "
            "to include the contribution from the condensable PM "
            "fraction for residential wood/coal combustion in a "
            "consistent way, as outlined in Denier van der Gon et "
            "al. (2015, https://doi.org/10.5194/acp-15-6503-2015). "
            "This dataset is prepared by TNO for the AVENGERS "
            "project in such a way that it can be directly nested "
            "in the HTAPv3 dataset (Crippa et al., 2023, "
            "https://doi.org/10.5194/essd-15-2667-2023)"
        )
    elif reason == "cte-gcp":
        description = (
            f"{dataset.summary}\n\n{dataset.source}\n\n{dataset.references}"
        )
    elif reason in ["gcp-inversions", "fluxcom-et", "fluxcom-et-t",
                    "fluxcom-gpp"]:
        description = dataset.summary
    else:
        print("Warning! Description field empty!")
    return description


def get_contributors(reason: str) -> list[str | None]:
    contributors: list[str | None] = []
    if reason == "cte-hr":
        contributors = \
            [INGRID_LUIJKX, NAOMI_SMITH, REMCO_DE_KOK, WOUTER_PETERS]
    elif reason == "avengers":
        contributors = [HUGO_DENIER, STIJN_DELLAERT, JEROEN_KUENEN]
    elif reason == "cte-gcp":
        contributors = [INGRID_LUIJKX, WOUTER_PETERS]
    elif reason == "gcp-inversions":
        contributors = [WOUTER_PETERS, CHRISTIAN_ROEDENBECK,
                        FREDERIC_CHEVALLIER, ZOE_LLORET, ANNE_COZIC,
                        YOSUKE_NIWA, ANDREW_JACOBSON, JUNJIE_LIU,
                        KEVIN_BOWMAN, JEONGMIN_YUN, BRENDAN_BYRNE,
                        ANTHONY_BLOOM, ZHE_JIN, XIANGJUN_TIAN, SHILONG_PIAO,
                        YILONG_WANG, HONGQIN_ZHANG, MIN_ZHAO, TAO_WANG,
                        JINZHI_DING, BO_ZHENG, ZHIQIANG_LIU, NING_ZENG,
                        FEI_JIANG, WEIMIN_JU, LIANG_FENG, PAUL_PALMER,
                        DONGXU_YANG, NAVEEN_CHANDRA, PRABIR_PATRA]
    elif reason in ["fluxcom-et", "fluxcom-et-t", "fluxcom-gpp"]:
        contributors = [JACOB_NELSON, SOPHIA_WALTHER, ULRICH_WEBER,
                        ZAYD_HAMDI, BASIL_KRAFT, WEIJIE_ZHANG,
                        GREGORY_DUVEILLER, MARTIN_JUNG]
    else:
        print("Warning! Contributors field empty!")
    return contributors


def get_creation_date(reason: str, dataset: XarrayDataset) -> Optional[str]:
    creation_date = None
    if reason == "cte-hr":
        creation_date = datetime \
            .strptime(dataset.creation_date, "%Y-%m-%d %H:%M") \
            .strftime("%Y-%m-%dT%H:%M:%SZ")
    elif reason == "avengers":
        creation_date = datetime \
            .strptime(dataset.creation_time, "%d/%m/%Y %H:%M:%S") \
            .strftime("%Y-%m-%dT%H:%M:%SZ")
    elif reason in ["cte-gcp", "gcp-inversions", "fluxcom-et",
                    "fluxcom-et-t", "fluxcom-gpp"]:
        creation_date = datetime \
            .strptime(dataset.creation_date, "%Y-%m-%d") \
            .strftime("%Y-%m-%dT11:00:00Z")
    if not creation_date:
        exiter.exit_zupload(info=dict({"message": "Creation date cannot be "
                                                  "empty"}))
    return creation_date


def get_creator(reason: str) -> Optional[str]:
    creator = None
    if reason == "cte-hr":
        creator = AUKE_WOUDE
    elif reason == "avengers":
        creator = HUGO_DENIER
    elif reason == "cte-gcp":
        creator = REMCO_DE_KOK
    elif reason == "gcp-inversions":
        creator = INGRID_LUIJKX
    elif reason in ["fluxcom-et", "fluxcom-et-t", "fluxcom-gpp"]:
        creator = FABIAN_GANS
    if not creator:
        exiter.exit_zupload(info=dict({"message": "Creator cannot be empty"}))
    return creator


def get_host_org(reason: str) -> Optional[str]:
    host_org = None
    if reason in ["cte-hr", "cte-gcp", "gcp-inversions"]:
        host_org = WUR
    elif reason == "avengers":
        host_org = TNO
    elif reason in ["fluxcom-et", "fluxcom-et-t", "fluxcom-gpp"]:
        host_org = CARBON_PORTAL
    else:
        print("Warning! Host organization field empty!")
    return host_org


def get_comment(reason: str, file_name: str) -> Optional[str]:
    comment = None
    if reason == "cte-hr":
        if file_name in ["anthropogenic.persector.202306.nc",
                         "anthropogenic.202306.nc"]:
            comment = (
                "In this version, anthropogenic fluxes have been "
                "corrected to avoid erroneous zero values for on-road"
                " emissions in Ukraine. Furthermore, a small number of "
                "hours of Public power were found to have zero values "
                "and were replaced with values of the previous hour. "
                "The previous version of this file is missing due to "
                "an error during the upload process. We are sorry for "
                "the inconvenience."
            )
        elif file_name in \
                ["fire.202306.nc", "nep.202306.nc", "ocean.202306.nc"]:
            pass
        # else:
        #     comment = (
        #         "In this version, anthropogenic fluxes have been "
        #         "corrected to avoid erroneous zero values for on-road"
        #         " emissions in Ukraine. Furthermore, a small number of "
        #         "hours of Public power were found to have zero values "
        #         "and were replaced with values of the previous hour."
        #     )
    return comment


def get_documentation(reason: str) -> Optional[str]:
    documentation = None
    if reason == "avengers":
        documentation = "mt_1_l8X7FUhaYtYbFEMCqXL"
    return documentation


def get_spatial_box(reason: str, file_name: str, dataset: XarrayDataset) \
        -> Optional[Union[str, dict[Any, Any]]]:
    spatial_box: Optional[Union[str, dict[Any, Any]]] = None
    if reason == "cte-hr":
        spatial_box = CTE_HR_BOX
    elif reason == "cte-gcp":
        if "transcom" in file_name:
            spatial_box = GLOBAL_BOX
        # Todo: What is this??
        elif "flux":
            spatial_box = CTE_HR_BOX
    elif reason == "avengers":
        spatial_box = AVENGERS_BOX
    elif reason == "gcp-inversions":
        lat_max = dataset.latitude.max().values.item()
        lat_min = dataset.latitude.min().values.item()
        lon_max = dataset.longitude.max().values.item()
        lon_min = dataset.longitude.min().values.item()
        spatial_box = {
            "_type": "LatLonBox",
            "geo": {
                "coordinates": [[
                    [lon_min, lat_min],
                    [lon_min, lat_max],
                    [lon_max, lat_max],
                    [lon_max, lat_min],
                    [lon_min, lat_min]
                ]],
                "type": "Polygon"
            },
            "max": {
                "lat": lat_max,
                "lon": lon_max
            },
            "min": {
                "lat": lat_min,
                "lon": lon_min
            }
        }
    elif reason in ["fluxcom-et", "fluxcom-et-t", "fluxcom-gpp"]:
        spatial_box = GLOBAL_BOX
    if not spatial_box:
        exiter.exit_zupload(info=dict({
            "message": "Spatial box cannot be empty"})
        )
    return spatial_box


def get_resolution(reason: str, file_name: str, dataset: XarrayDataset) -> \
        Optional[str]:
    resolution = None
    if reason == "cte-hr":
        resolution = "hourly"
    elif reason in ["avengers", "gcp-inversions"]:
        resolution = "monthly"
    elif reason == "cte-gcp":
        if "monthly" in file_name:
            resolution = "monthly"
        elif "yearly" in file_name:
            resolution = "yearly"
    elif reason in ["fluxcom-et", "fluxcom-et-t", "fluxcom-gpp"]:
        if dataset.frequency == "1h":
            resolution = "hourly"
        elif dataset.frequency == "1d":
            resolution = "daily"
        elif dataset.frequency == "1mo":
            resolution = "monthly"
    if not resolution:
        exiter.exit_zupload(info=dict({
            "message": "Resolution cannot be empty"})
        )
    return resolution


def get_title(reason: str, additional_info: dict[Any, Any],
              dataset: XarrayDataset) -> Optional[str]:
    title = None
    if reason == "cte-hr":
        title = (
            f"High-resolution, near-real-time fluxes over Europe "
            f"from CTE-HR: {additional_info['dataset_type']} "
            f"{additional_info['year']}-{additional_info['month']}"
        )
    elif reason == "avengers":
        species = additional_info["file_name"].split("_")[3]
        year = additional_info["file_name"].split("_")[2]
        title = f"TNO {species} emission inventory {year} for AVENGERS"
    elif reason in ["cte-gcp", "gcp-inversions"]:
        title = dataset.title
    elif reason in ["fluxcom-et", "fluxcom-et-t", "fluxcom-gpp"]:
        regex, variable, frequency = None, None, None
        if "ET_T" in additional_info["file_name"]:
            regex = r"ET_T|_|\.|nc"
            variable = "transpiration"
        elif "ET" in additional_info["file_name"]:
            regex = r"ET|_|\.|nc"
            variable = "evapotranspiration"
        elif "GPP" in additional_info["file_name"]:
            regex = r"GPP|_|\.|nc"
            variable = "gross primary productivity"
        elif "NEE" in additional_info["file_name"]:
            regex = r"NEE|_|\.|nc"
            variable = "net ecosystem exchange"
        if regex and variable:
            year, degree, frequency = list(
                filter(None, re.split(regex, additional_info["file_name"]))
            )
            if frequency == "monthly":
                title = f"FLUXCOM-X monthly {variable} on global " \
                        f"{float(degree)/100} degree grid for {year}"
            elif frequency == "daily":
                title = f"FLUXCOM-X daily {variable} on global " \
                        f"{float(degree)/100} degree grid for {year}"
            elif frequency == "monthlycycle":
                title = f"FLUXCOM-X monthly diurnal cycle of {variable} on" \
                        f" global {float(degree)/100} degree grid for {year}"
            assert title is not None
        else:
            exiter.exit_zupload(
                info=dict({
                    "message": "Something went wrong while creating the title"
                }))
    if not title:
        exiter.exit_zupload(info=dict({"message": "Title cannot be empty"}))
    return title


def write_json(path: str, content: dict[str, Any]) -> None:
    """Write dictionary to json file."""
    with open(file=path, mode="w+") as json_handle:
        json.dump(content, json_handle, indent=4)
    return


def read_json(path: str) -> Any:
    """Read dictionary from json file."""
    with open(file=path, mode="r") as json_handle:
        json_data = json.load(json_handle)
    return json_data


def make_monthly_cte_hr_collection(collection: dict[Any, Any]) \
        -> dict[Any, Any]:
    year, month = collection["key"][0:4], collection["key"][4:6]
    with open(file=CTE_HR_MONTHLY_DESCRIPTION, mode="r") as file_handle:
        static_description = file_handle.read()
    collection_json = {
        "description": static_description.replace("year_month",
                                                  f"{year}-{month}"),
        "members": collection["members"],
        "submitterId": "CP",
        "title": f"High-resolution, near-real-time fluxes over Europe "
                 f"from CTE-HR for {year}-{month}",
        "isNextVersionOf":
            collection["isNextVersionOf"] if "isNextVersionOf" in
                                             collection.keys()
            else []
    }
    return collection_json


def make_yearly_cte_hr_collection(collection: dict[Any, Any]) \
        -> dict[Any, Any]:
    with open(file=CTE_HR_YEARLY_DESCRIPTION, mode="r") as file_handle:
        static_description = file_handle.read()
    collection_json = {
        "description": static_description.replace("year",
                                                  collection["key"]),
        "members": collection["members"],
        "submitterId": "CP",
        "title": f"High-resolution, near-real-time fluxes over Europe from "
                 f"CTE-HR for {collection['key']}",

        "isNextVersionOf":
            collection["isNextVersionOf"] if "isNextVersionOf" in
                                             collection.keys()
            else []
    }
    return collection_json


def make_full_cte_hr_collection(collection: dict[Any, Any]) -> dict[Any, Any]:
    with open(file=CTE_HR_FULL_DESCRIPTION, mode="r") as file_handle:
        static_description = file_handle.read()
    collection_json = {
        "description": static_description,
        "members": collection["members"],
        "submitterId": "CP",
        "title": "High-resolution, near-real-time fluxes over Europe from "
                 "CTE-HR for 2017-2023",
        "isNextVersionOf":
            collection["isNextVersionOf"] if "isNextVersionOf" in
                                             collection.keys()
            else []
    }
    return collection_json
