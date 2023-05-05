# Format read from https://peps.python.org/pep-0008/#imports.
# Standard library imports.
from datetime import datetime
from multiprocessing import Pool
from pathlib import Path
import hashlib
import json
import os
import re

# Related third party imports.
import humanize
import requests
import xarray

# Local application/library specific imports.
import src.constants as constants
import src.dataset as dataset
import src.exiter as exiter
import src.tools as tools


class GcpInversionsDataset(dataset.Dataset):
    def __init__(self, reason: str = None, interactive: bool = False):
        super().__init__(reason, interactive)
        # Todo: Need a proper way to find and exclude variables.
        #       Perhaps read here the dataset, but this means we
        #       will keep it in memory from this point.
        # We need to exclude variables that are not preview-able on the
        # data portal.
        self.excluded_variables = list([
            'area', 'ensemble_member_name', 'cell_area'
        ])
        return

    def archive_files(self):
        """Archive file paths, names, and other information if needed."""
        print('- Archiving system information.')
        total = len(self.input_data)
        for index, file_path in enumerate(self.input_data):
            file_name = file_path.split('/')[-1]
            # Todo: This part was used for one specific type of dataset
            #  and we probably don't need it.
            # if (general_date := re.findall(r'\d{4}', file_name)) != 1:
            #     user_input = tools.input_handler(
            #         operation='picker',
            #         additional_info=dict({'iterable': general_date})
            #     )
            #     if user_input == 'e':
            #         exiter.exit_zupload()
            #     else:
            #         general_date = [user_input]
            # year = general_date[0][0:4]
            # month = general_date[0][4:6]
            dataset_type, dataset_object_spec = \
                tools.get_specification(file_name)
            base_key = file_name.rstrip('.nc')
            self.archive_out[base_key] = dict({
                'file_path': file_path,
                'file_name': file_name,
                'dataset_type': dataset_type,
                'dataset_object_spec': dataset_object_spec,
                # 'month': month,
                # 'year': year,
                'try_ingest_components':
                    self.build_try_ingest_components(
                        file_path=file_path,
                        dataset_object_spec=dataset_object_spec
                    ),
            })
            self.archive_out[base_key].setdefault(
                'handlers',
                dict({'archive_json': True,
                      'try_ingest': True,
                      'upload_metadata': True,
                      'upload_data': True})
            )
            self.archive_out[base_key].setdefault('versions', [])
            tools.progress_bar(
                operation='archive_system_info',
                current=index + 1, total=total,
                additional_info=dict({'file_name': file_name})
            )
        # Sort archive's items.
        self.archive_out = dict(sorted(self.archive_out.items()))
        self.store_current_archive()
        return

    def build_try_ingest_components(self, file_path: str = None,
                                    dataset_object_spec: str = None) -> dict:
        """Build the try-ingest command for each data file."""
        try:
            xarray_dataset = xarray.open_dataset(file_path)
        except ValueError as e:
            variables = None
        else:
            variable_list = list(
                variable for variable in xarray_dataset.data_vars
                if variable not in self.excluded_variables
            )
            # The variable list must be formatted like this:
            # '["variable_1", "variable_2", ...]'
            # Formatting like this e.g:
            # "['variable_1', 'variable_2', ...]"
            # will probably result in a try ingest error.
            # This is why we use json.dumps,
            # to create a specifically formatted string.
            variables = f'{json.dumps(variable_list)}'
        try_ingest_url = 'https://data.icos-cp.eu/tryingest'
        params = dict({'specUri': dataset_object_spec,
                       'varnames': variables})
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
        overwrite the `archive_in_nc.json` file using the function
        `store_current_archive()`.
        """
        print('- Archiving meta-data (Includes hash-sum calculation.)')
        total = len(self.archive_out)
        for index, (base_key, base_info) in \
                enumerate(self.archive_out.items()):
            if total != 1:
                tools.progress_bar(operation='archive_meta_data',
                                   current=index+1,
                                   total=total,
                                   additional_info=dict({
                                       'file_name': base_info['file_name']
                                   }))
            if not base_info['handlers']['upload_metadata']:
                continue
            xarray_dataset = xarray.open_dataset(base_info['file_path'])
            if len(xarray_dataset.creation_date) < 16:
                creation_date = datetime.strptime(
                    xarray_dataset.creation_date, '%Y-%m-%d')
            else:
                creation_date = datetime.strptime(
                    xarray_dataset.creation_date, '%Y-%m-%d %H:%M')
            base_info['json'] = dict({
                'fileName': base_info['file_name'],
                'hashSum':
                    tools.get_hash_sum(file_path=base_info['file_path'],
                                       progress=True),
                'isNextVersionOf': [] if not base_info['versions'] else
                base_info['versions'][-1].rsplit('/')[-1],
                'objectSpecification': base_info['dataset_object_spec'],
                'references': {
                    'keywords': [
                        'carbon flux',
                        'land carbon flux',
                        'ocean carbon flux',
                        'GCB2022',
                        'global carbon',
                        'project',
                        'atmospheric',
                        'inversions',
                        'monthly',
                        'co2'
                    ],
                    'licence': constants.ICOS_LICENSE
                },
                'specificInfo': {
                    'description': xarray_dataset.summary,
                    'production': {
                        'contributors': [
                            constants.FREDERIC_CHEVALLIER,
                            constants.CHRISTIAN_ROEDENBECK,
                            constants.YOSUKE_NIWA,
                            constants.JUNJIE_LIU,
                            constants.LIANG_FENG,
                            constants.PAUL_PALMER,
                            constants.KEVIN_BOWMAN,
                            constants.WOUTER_PETERS,
                            constants.XIANGJUN_TIAN,
                            constants.SHILONG_PIAO,
                            constants.BO_ZHENG
                        ],
                        'creationDate':
                            creation_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'creator': constants.INGRID_LUIJKX,
                        'hostOrganization': constants.WUR,
                        'sources': [],
                    },
                    'spatial': self.get_spatial(dataset=xarray_dataset),
                    # 'spatial': constants.GLOBAL_BOX,
                    'temporal': {
                        'interval': {
                            # 'start': '1980-01-01T00:00:00Z',
                            # 'stop': '2020-12-31T23:59:59Z',
                            'start': xarray_dataset.time[0].dt.strftime(
                                    '%Y-%m-%dT%H:%M:%SZ').item(),
                            'stop': xarray_dataset.time[-1].dt.strftime(
                                '%Y-%m-%dT%H:%M:%SZ').item(),
                        },
                        'resolution': 'monthly'
                    },
                    'title': xarray_dataset.title,
                    # 'title': 'GAW Data',
                    'variables': list(
                        variable for variable in xarray_dataset.data_vars
                        if variable not in self.excluded_variables
                    ),
                },
                'submitterId': constants.STANDARD_SUBMITTER
            })
            json_file_name = base_key + '.json'
            json_file_path = os.path.join(self.json_standalone_files,
                                          json_file_name)
            base_info['json_file_path'] = json_file_path
            tools.write_json(path=json_file_path, content=base_info['json'])
        # Sort archive's items.
        self.archive_out = dict(sorted(self.archive_out.items()))
        self.store_current_archive()
        return


    @staticmethod
    def get_spatial(dataset: xarray = None) -> dict:
        spatial = dict({
            '_type': 'LatLonBox',
            'min': {'lat': None, 'lon': None},
            'max': {'lat': None, 'lon': None}
        })
        if all(key in dataset for key in ['lat', 'lon']):
            spatial['min']['lat'] = dataset.lat.min().item()
            spatial['min']['lon'] = dataset.lon.min().item()
            spatial['max']['lat'] = dataset.lat.max().item()
            spatial['max']['lon'] = dataset.lon.max().item()
        elif all(key in dataset for key in ['latitude', 'longitude']):
            spatial['min']['lat'] = dataset.latitude.min().item()
            spatial['min']['lon'] = dataset.longitude.min().item()
            spatial['max']['lat'] = dataset.latitude.max().item()
            spatial['max']['lon'] = dataset.longitude.max().item()
        return spatial

