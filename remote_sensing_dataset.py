# Format read from https://peps.python.org/pep-0008/#imports.
# Standard library imports.
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
from pprint import pprint
import hashlib
import json
import os
import re

# Related third party imports.
import humanize
import requests

# Local application/library specific imports.
import constants
import dataset
import tools
import zload_warnings


class RemoteSensingDataset(dataset.Dataset):
    def __init__(self, reason: str = None, interactive: bool = False):
        super().__init__(reason, interactive)
        self.atomic_spec = self.extract_atomic_spec()
        self.rest_countries = tools.obtain_rest_countries()
        # self.p_standard_mapping = os.path.join(
        #     self.archives_dir,
        #     f'{self.reason}_standard_mapping.json')
        # self.generate_standard_mapping()
        # Used to reinitialize archive_out file.
        # self.archive_out = tools.read_json(path=self.p_standard_mapping)
        self.zipped_files_dir = os.path.join(
            self.master_dir, 'zipped-files/')
        Path(self.zipped_files_dir).mkdir(parents=True, exist_ok=True)
        self.processed_input_data = [
            os.path.join(self.zipped_files_dir, file) for file in
            os.listdir(path=self.zipped_files_dir)
        ]
        # self.file_type = self.processed_input_data[0].split('.')[-1]
        # self.collections = self.get_collections()
        self.p_input_from_sophia = os.path.join(self.archives_dir,
                                                'input_from_sophia.json')
        # Todo: use this function to edit Sophia's input data
        #  (stations, stations' locations, country codes).
        self.input_from_sophia = self.get_input_from_sophia()
        return

    def archive_files(self):
        """Archive file paths, names, and other information if needed."""
        print(f'- Archiving system information of `.{self.file_type}` files... ', end='')
        for file_path in self.input_data:
            file_name = file_path.split('/')[-1]
            station = file_name.split('.')[0]
            country = station.split('-')[0]
            continent = self.rest_countries[country]['continent']
            continent_possession = \
                self.rest_countries[country]['continent_possession']
            station_exists_in_dataset = False
            # Group together files and information. This is achieved by
            # extracting the station name from the file name and then
            # searching the station name inside the archive. In many
            # cases stations within the same country or even within the
            # same continent, will be grouped together. Stations in the
            # US are an exception.
            for base_key, base_info in self.archive_out.items():
                if station in base_info['stations']:
                    station_exists_in_dataset = True
                    file_name = f'{self.atomic_spec}_{base_key}.zip'
                    dataset_type, dataset_object_spec = \
                        tools.get_specification(file_name)
                    base_info.update({
                        'archive_size': (base_info['archive_size'] +
                                         tools.get_size(path=file_path)),
                        # Todo: These two keys should only be updated
                        #  once!
                        'continent': continent,
                        'continent_possession': continent_possession,
                        'dataset_type': dataset_type,
                        'dataset_object_spec': dataset_object_spec,
                        'file_name': file_name,
                        'file_path': os.path.join(self.zipped_files_dir,
                                                  base_info['file_name']),
                        'files': sorted(
                            list(set(base_info['files'] + [file_path]))
                        ),
                        'try_ingest_components':
                            self.build_try_ingest_components(
                                file_path=base_info['file_path'],
                                dataset_object_spec=dataset_object_spec
                            )
                    })
            if not station_exists_in_dataset:
                # Todo: This should be calling a function with a
                #  message instead.
                exit(f'{station} does not exist in the dataset...')
        # TALK TO OLEG ABOUT THE INTERMEDIATE COLLECTION.
        # for base_key, base_info in self.archive_out.items():
        #     for key, info in self.collections['versions'].items():
        #         if all(part in base_info['file_name'] for part in info['file_name_parts']):
        #             base_info.update({
        #                 'versions': [info['latest_version']]
        #             })
        user_zip_files = 'n'
        if input('\n\tWould you like to zip files? (Y/n): ') == 'Y':
            self.zip_files()
        else:
            print('\tSkipping zipping of files...')
        # Sort archive's items.
        self.archive_out = dict(sorted(self.archive_out.items()))
        self.store_current_archive()
        return

    def zip_files(self):
        """Zip files that have been grouped together."""
        # Todo: Maybe multi-process this.
        for base_key, base_info in self.archive_out.items():
            # Skip zipping for datasets that no files have been grouped
            # together or for datasets that a zip file has already been
            # created.
            if not base_info['files'] or 'hash_sum' in base_info.keys():
                continue
            tools.zip_files(files=base_info['files'],
                            p_output_file=base_info['file_path'])
            self.processed_input_data.append(base_info['file_path'])
            base_info.update({
                'hash_sum': tools.get_hash_sum(base_info['file_path'])
            })
            print('\t-')
        return

    def build_try_ingest_components(self,
                                    file_path: str = None,
                                    dataset_object_spec: str = None) -> dict:
        """Build the try-ingest command for each data file."""
        try_ingest_url = 'https://data.icos-cp.eu/tryingest'
        params = dict({'specUri': dataset_object_spec})
        try_ingest_components = {'url': try_ingest_url,
                                 'params': params,
                                 'file_path': file_path}
        return try_ingest_components

    # todo: Maybe multi-process this.
    def archive_json(self):
        """Generates standalone .json files and adds to archive.

        Generates the standalone .json file for each data file and updates
        the archive with the regenerated json content. This function needs
        to be rerun each time we need to change something in the meta-data.
        If we decide to rerun this then it is mandatory that we also
        overwrite the `archive_in.json` file using the function
        `store_current_archive()` at the end of the script.

        """
        total = len(self.archive_out)
        for index, (base_key, base_info) in \
                enumerate(self.archive_out.items()):
            tools.progress_bar(operation='archive_meta_data', current=index+1,
                               total=total)
            if not base_info['handlers']['upload_metadata']:
                continue
            creation_date = datetime.fromtimestamp(
                os.path.getmtime(base_info['file_path']))
            base_info['json'] = dict({
                'fileName': base_info['file_name'],
                'hashSum': base_info['hash_sum'],
                'isNextVersionOf': (
                    [] if not base_info['versions']
                    else [base_info['versions'][-1].rsplit('/')[-1]]
                ),
                'objectSpecification': base_info['dataset_object_spec'],
                'references': {
                    'duplicateFilenameAllowed': True,
                    'keywords': self.get_keywords(),
                    'licence': constants.ICOS_LICENSE
                },
                'specificInfo': {
                    'description': self.get_description(
                        continent=base_info['continent'],
                        stations=base_info['stations']
                    ),
                    'production': self.get_production(creation_date),
                    'spatial': dict({
                        'features': self.get_features(base_info)
                    }),
                    'temporal': {
                        'interval': {
                            'start': (
                                '2000-01-01T00:00:00Z'
                                if self.reason == 'modis'
                                else '1984-01-31T00:00:00Z'
                            ),
                            'stop': '2022-12-31T00:00:00Z'
                        },
                        'resolution': (
                            'daily' if self.reason == 'modis'
                            else 'monthly'
                        )
                    },
                    'title': self.get_title(base_info)
                },
                'submitterId': 'CP'
            })
            json_file_name = base_key + '.json'
            json_file_path = os.path.join(self.json_standalone_files,
                                          json_file_name)
            base_info['json_file_path'] = json_file_path
            tools.write_json(path=json_file_path, content=base_info['json'])
        self.store_current_archive()
        return

    # Todo: This will probably need further editing for each new run
    #  of the zbunchpload for remote sensing data. Each new run,
    #  Sophia will probably give us back a different version of this
    #  file -> FLEO_sitelist_coords.txt which we will edit to produce
    #  input_from_sophia.json file.
    def get_input_from_sophia(self) -> dict:
        """Edit Sophia's input file."""
        sophia_input = dict()
        if os.path.exists(self.p_input_from_sophia):
            print(f'\tSophia\'s content read from: '
                  f'{self.p_input_from_sophia}... {constants.ICON_CHECK}')
            sophia_input = tools.read_json(path=self.p_input_from_sophia)
            # zload_warnings.warn_for_present_file(path=p_sophia_input)
            # continue_input = input(
            #     f'\tWould you like to continue? (y/n): '
            # )
            # tools.exit_zupload() if continue_input == 'n' else tools.pass_me()
        else:
            p_provided_file = \
                'input-files/landsat/in-out-archives/FLEO_sitelist_coords.txt'
            with open(file=p_provided_file) as txt_handle:
                file_content = txt_handle.read()
            file_content = file_content.split('\n')
            file_content.pop(0)
            sophia_input = dict()
            for line in file_content:
                station_info = line.split(',')
                country_code = station_info[0][0:2]
                station = station_info[0]
                sophia_input.update({
                    station: dict({
                        'lat': float(station_info[1]),
                        'lon': float(station_info[2]),
                        'country_code': country_code
                    })
                })
            tools.write_json(path=self.p_input_from_sophia,
                             content=sophia_input)
        return sophia_input

    def get_collections(self):
        """Extract remote sensing collections."""
        df_collections = tools.download_collections()
        df_remote_sensing = None
        if self.reason == 'modis':
            df_remote_sensing = df_collections[
                df_collections.title.str.contains('\(MODIS\)')
            ]
        elif self.reason == 'landsat':
            df_remote_sensing = df_collections[
                df_collections.title.str.contains('\(Landsat\)')
            ]
        single_digital_object = df_remote_sensing.coll.to_list()[0]
        meta_data = tools.get_request(
            url=f'{single_digital_object}/meta.json').json()
        latest_version = meta_data['latestVersion']
        meta_data = tools.get_request(
            url=f'{latest_version}/meta.json').json()
        collection_info = dict()
        collection_info.update({
            'latest_version': latest_version,
            'members': [member['res'] for member in meta_data['members']]
        })
        versions = dict()
        for member in collection_info['members']:
            member_meta_data = tools.get_request(f'{member}/meta.json').json()
            landing_page = member_meta_data['accessUrl'].replace('data',
                                                                 'meta')
            file_name_parts = list()
            if self.reason == 'modis':
                file_name_parts = re.split(r'MODIS_|_|.zip',
                                           member_meta_data['fileName'])
            elif self.reason == 'landsat':
                file_name_parts = re.split(r'Landsat_|_|.zip',
                                           member_meta_data['fileName'])
            file_name_parts = list(filter(None, file_name_parts))
            versions.update({
                member_meta_data['fileName']: {
                    'file_name_parts': file_name_parts,
                    'latest_version': landing_page
                }
            })
        collection_info.update({
            'versions': versions
        })
        return collection_info

    def build_something(self):
        d_continents = dict()
        for p_file in self.input_data:
            # Extract country code and station name from file path.
            country, station = p_file.split('/')[-1][0:6].split('-')
            # Cross-check with rest countries and find in which
            # continent each country-code belongs to.
            continent = self.d_rest_countries[country]['continent']
            if country not in self.d_modis[continent]['countries']:
                self.d_modis[continent].setdefault('new_countries', list())
                if country not in self.d_modis[continent]['new_countries']:
                    self.d_modis[continent]['new_countries'].append(country)
        for continent, continent_info in self.d_modis.items():
            if 'new_countries' in continent_info.keys():
                for country in continent_info['new_countries']:
                    print(continent, country, self.d_input_from_sophia[country])
            # d_continents.setdefault(continent, dict())
            # d_continents[continent].setdefault(country_code, {
            #     'file_path': p_file,
            # })
        return

    def generate_standard_mapping(self):
        """
        Generate current mapping of datasets and stations.

        This should be used in case of new upload series where stations
        and/or countries have diverged from previous uploads. The user
        then needs to enter manually in this new json file the new
        stations or countries that were added.
        """
        standard_mapping = dict()
        for dataset_key, dataset_info in self.archive_out.items():
            countries = dataset_info['countries']
            max_lats, max_lons, min_lats, min_lons = [], [], [], []
            for country in countries:
                max_lats.append(self.rest_countries[country]['max_lat'])
                max_lons.append(self.rest_countries[country]['max_lon'])
                min_lats.append(self.rest_countries[country]['min_lat'])
                min_lons.append(self.rest_countries[country]['min_lon'])
            standard_mapping.setdefault(dataset_key, dict({
                'archive_size': float(),
                'continent': str(),
                'countries': dataset_info['countries'],
                'dataset_type': str(),
                'dataset_object_spec': str(),
                'file_name': str(),
                'file_path': str(),
                'files': list(),
                'handlers': dict(),
                'max_lat': max(max_lats, key=lambda x: float(x)),
                'max_lon': max(max_lons, key=lambda x: float(x)),
                'min_lat': min(min_lats, key=lambda x: float(x)),
                'min_lon': min(min_lons, key=lambda x: float(x)),
                'stations': dataset_info['stations'],
                'try_ingest_components': str(),
                'versions': list()
            }))
        tools.write_json(path=self.p_standard_mapping,
                         content=standard_mapping)
        return

    def extract_atomic_spec(self):
        spec = None
        if self.reason == 'modis':
            spec = self.reason.upper()
        elif self.reason == 'landsat':
            spec = self.reason.capitalize()
        return spec

    def get_description(self, continent: str = None,
                        stations: list = None) -> str:
        description = None
        description_end_part = str(
            f' ZIP archive of netcdf files for stations in {continent}: '
            f'{", ".join(stations)}',
        )
        if self.reason == 'modis':
            description = (
                'This is version 2 of quality checked and gap-filled daily '
                'MODIS observations of surface reflectance and land surface '
                'temperature at global eddy co-variance sites for the time '
                'period 2000-2022. Two product versions: one features all '
                'MODIS pixels within 2km radius around a given site, and a '
                'second version consists of an average time series that '
                'represents the area within 1km2 around a site. All data '
                'layers have a complementary layer with gap-fill information.'
                ' MODIS data comprise 647 eddy covariance sites (see a '
                'detailed list in the README of version 2). FluxnetEO v2 '
                'MODIS reflectance products: enhanced vegetation index (EVI),'
                ' normalized difference vegetation index (NDVI), generalized '
                'NDVI (kNDVI), near infra-red reflectance of vegetation '
                '(NIRv), normalized difference water index (NDWI) with band '
                '5, 6, or 7 as reference, the scaled wide dynamic range '
                'vegetation index (sWDRVI), surface reflectance in MODIS '
                'bands 1-7. Based on the NASA MCD43A4 and MCD43A2 collection'
                ' 6 products with a pixel size of 500m. FluxnetEO v2 MODIS '
                'land surface temperature: Terra and Aqua, day and night, at'
                ' native viewing zenith angle as well as corrected to viewing'
                ' zenith angles of 0 and 40degrees (Ermida et al., 2018, RS,'
                ' https://www.mdpi.com/2072-4292/10/7/1114). Based on NASA'
                ' MOD11A1 and MYD11A1 collection 6 at a pixel size of 1km. '
                'This is version 2 of the data described in Walther* & '
                'Besnard* et al. 2022. A view from space on global flux '
                'towers by MODIS and Landsat: The FluxnetEO dataset, '
                'Biogeosciences, https://doi.org/10.5194/bg-19-2805-2022. '
                'Please refer to the README of version2 to understand the '
                'details of the updates between version1 (described in the '
                'paper) and version2. The data are separated in zip-files by '
                'continents and groups of countries.'
            )
        elif self.reason == 'landsat':
            description = (
                'This is version 2 of quality checked and gap-filled monthly '
                'Landsat observations of surface reflectance at global eddy '
                'co-variance sites for the time period 1984-2022. Two product'
                ' versions: one features all Landsat pixels within 2km radius'
                ' around a given site, and a second version consists of an '
                'average time series that represents the area within 1km2 '
                'around a site. All data layers have a complementary layer '
                'with gap-fill information. Reflectance products: enhanced '
                'vegetation index (EVI), normalized difference vegetation '
                'index (NDVI), generalized NDVI (kNDVI), near infra-red '
                'reflectance of vegetation (NIRv), normalized difference '
                'water index (NDWI) with both shortwave infra-red bands as '
                'reference, the scaled wide dynamic range vegetation index '
                '(sWDRVI), surface reflectance in individual Landsat bands. '
                'Based on the Landsat4,5,7,8, 9 collection 2 products with a '
                'pixel size of 30m. The data set comprises 645 eddy '
                'covariance sites (see a detailed list in the README of '
                'version 2). This is version 2 of the data described in '
                'Walther* & Besnard* et al. 2022. A view from space on global'
                ' flux towers by MODIS and Landsat: The FluxnetEO dataset, '
                'Biogeosciences, https://doi.org/10.5194/bg-19-2805-2022. '
                'Please refer to the README of version2 to understand the '
                'details of the updates between version1 (described in the '
                'paper) and version2.'
            )
        description += description_end_part
        return description

    def printer(self):
        print(f'- {constants.ICON_RECEIPT:3}Here\'s your meta-data:')
        # Used to align user output since keys can vary in length.
        base_key_max_length = len(max(self.archive_out.keys(), key=len))
        for base_key, base_info in self.archive_out.items():
            if base_info['file_path'] not in self.processed_input_data:
                continue
            if 'file_metadata_url' in base_info.keys():
                print(f'\t{base_key:{base_key_max_length}} {base_info["file_metadata_url"]}')
            else:
                print(f'\t{base_key:{base_key_max_length}} No info for file meta-data url')
        print(f'\tTotal of {len(self.processed_input_data)} items.')
        return

    def get_production(self, creation_date: datetime = None) -> dict:
        production = dict({
            'contributors': None,
            'creationDate': creation_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
            'creator': None,
            'documentation': 'j9fusMYfwG3-3oLrutSe4f8t',
            'hostOrganization': constants.MPI_BGC,
            'source': list([])
        })
        if self.reason == 'modis':
            production.update({
                'creator': constants.SOPHIA_WALTHER,
                'contributors': [
                    constants.SIMON_BESNARD,
                    constants.SOPHIA_WALTHER,
                    constants.ULRICH_WEBER,
                    constants.JACOB_NELSON
                ]
            })
        elif self.reason == 'landsat':
            production.update({
                'creator': constants.SIMON_BESNARD,
                'contributors': [
                    constants.SIMON_BESNARD,
                    constants.SOPHIA_WALTHER,
                    constants.ULRICH_WEBER
                ]
            })
        return production

    def get_features(self, base_info: dict = None) -> list:
        sites_info = list()
        if 'sites_info' in base_info.keys():
            sites_info = base_info['sites_info']
        else:
            for station in base_info['stations']:
                if station in self.input_from_sophia.keys():
                    sites_info.append(
                        dict({
                            'label': station,
                            'lat': self.input_from_sophia[station]['lat'],
                            'lon': self.input_from_sophia[station]['lon']
                        })
                    )
        return sites_info

    def get_title(self, base_info: dict = None) -> str:
        countries = [
            self.rest_countries[country]['name']
            for country in base_info['countries']
        ]
        continent_possession = base_info['continent_possession']
        file_name = base_info['file_name']
        splitter = re.split(r'_|.zip', file_name)
        splitter = list(filter(None, splitter))
        number = None
        if any([x in splitter for x in ['0', '1', '2', '3']]):
            number = splitter[-1]
        else:
            pass
        title = f'The FluxnetEO dataset ({self.atomic_spec}) '
        if len(countries) != 1:
            countries[-1] = f'and {countries[-1]}'
            title += (
                f'for {continent_possession} stations located in '
                f'{", ".join(countries)}'
            )
        elif len(countries) == 1 and countries[0] == 'United States':
            title += (
                f'for {continent_possession} stations located in '
                f'{", ".join(countries)} ({number})'
            )
        else:
            title += (
                f'for {continent_possession} stations located in '
                f'{", ".join(countries)}'
            )
        return title

    def get_keywords(self) -> list:
        keywords = list(['quality control', 'gap-filled',
                         'surface reflectance', 'eddy-covariance stations'])
        if self.reason == 'modis':
            keywords = \
                keywords + [self.atomic_spec, 'land surface temperature']
        elif self.reason == 'landsat':
            keywords.append(self.atomic_spec)
        return keywords
