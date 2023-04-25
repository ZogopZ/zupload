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
import constants
import dataset
import exiter
import tools


class CteHrDataset(dataset.Dataset):
    def __init__(self, reason: str = None, interactive: bool = False):
        super().__init__(reason, interactive)
        return

    def archive_files(self):
        """Archive file paths, names, and other information if needed."""
        print('- Archiving system information.')
        total = len(self.input_data)
        for index, file_path in enumerate(self.input_data):
            file_name = file_path.split('/')[-1]
            general_date = re.findall(r'\d{6}', file_name)
            if len(general_date) != 1:
                exiter.exit_zupload(info=dict({'file_name': file_name,
                                               'general_date': general_date}))
            year = general_date[0][0:4]
            month = general_date[0][4:6]
            dataset_type, dataset_object_spec = self.get_file_info(file_name)
            base_key = file_name.rstrip('.nc')
            self.archive_out[base_key] = dict({
                'file_path': file_path,
                'file_name': file_name,
                'dataset_type': dataset_type,
                'dataset_object_spec': dataset_object_spec,
                'month': month,
                'year': year,
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
        xarray_dataset = xarray.open_dataset(file_path)
        variable_list = list(xarray_dataset.data_vars)
        # The variable list must be formatted like this:
        # '["variable_1", "variable_2", ...]'
        # Formatting like this e.g: "['variable_1', 'variable_2', ...]"
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

    @staticmethod
    def get_file_info(file_name: str = None) -> tuple:
        dataset_type = None
        dataset_object_spec = None
        if 'persector' in file_name:
            dataset_type = 'anthropogenic emissions per sector'
            dataset_object_spec = constants.OBJECT_SPECS[
                'anthropogenic_emission_model_results']
        elif 'anthropogenic' in file_name:
            dataset_type = 'anthropogenic emissions'
            dataset_object_spec = constants.OBJECT_SPECS[
                'anthropogenic_emission_model_results']
        elif 'nep' in file_name:
            dataset_type = 'biospheric fluxes'
            dataset_object_spec = constants.OBJECT_SPECS[
                'biospheric_model_results']
        elif 'fire' in file_name:
            dataset_type = 'fire emissions'
            dataset_object_spec = constants.OBJECT_SPECS[
                'file_emission_model_results']
        elif 'ocean' in file_name:
            dataset_type = 'ocean fluxes'
            dataset_object_spec = constants.OBJECT_SPECS[
                'oceanic_flux_model_results']
        return dataset_type, dataset_object_spec

    # todo: Maybe multi-process this.
    def archive_json(self):
        """Generates standalone .json files and adds to archive.

        Generates the standalone .json file for each data file and updates
        the archive with the regenerated json content. This function needs
        to be rerun each time we need to change something in the meta-data.
        If we decide to rerun this then it is mandatory that we also
        overwrite the `archive_in_nc.json` file using the function
        `store_current_archive()` at the end of the script.

        """
        print('- Archiving meta-data (Includes hash-sum calculation.)')
        total = len(self.archive_out)
        for index, (base_key, base_info) in \
                enumerate(self.archive_out.items()):
            tools.progress_bar(operation='archive_meta_data',
                               current=index+1,
                               total=total,
                               additional_info=dict({
                                   'file_name': base_info['file_name']
                               }))
            if not base_info['handlers']['upload_metadata']:
                continue
            xarray_dataset = xarray.open_dataset(base_info['file_path'])
            creation_date = datetime.strptime(xarray_dataset.creation_date,
                                              '%Y-%m-%d %H:%M')
            base_info['json'] = dict({
                'fileName': base_info['file_name'],
                'hashSum':
                    tools.get_hash_sum(file_path=base_info['file_path'],
                                       progress=False),
                'isNextVersionOf': [] if not base_info['versions'] else
                base_info['versions'][-1].rsplit('/')[-1],
                'objectSpecification': base_info['dataset_object_spec'],
                'references': {
                    'keywords': ['carbon flux'],
                    'licence': constants.ICOS_LICENSE
                },
                'specificInfo': {
                    'description': xarray_dataset.comment,
                    'production': {
                        'contributors': [
                            constants.INGRID_LUIJKX,
                            constants.NAOMI_SMITH,
                            constants.REMCO_DE_KOK,
                            constants.WOUTER_PETERS
                        ],
                        'creationDate':
                            creation_date.strftime('%Y-%m-%dT%H:%M:%SZ'),
                        'creator': constants.AUKE_WOUDE,
                        'hostOrganization': constants.WUR,
                        # Comment used for correct versions of
                        # anthropogenic & anthropogenic per sector
                        # files.
                        # 'comment': 'In the previous version, the file did not contain the correct Public Power and '
                        #            'Residential Heating from the degree-day model (see Van der Woude et al. '
                        #            'https://doi.org/10.5194/essd-2022-175), but rather contained the CAMS diurnal '
                        #            'profiles.',
                        'sources': [],
                    },
                    'spatial': constants.CTE_HR_BOX,
                    'temporal': {
                        'interval': {
                            'start': xarray_dataset.time[0].dt.strftime(
                                '%Y-%m-%dT%H:%M:%SZ').item(),
                            'stop': xarray_dataset.time[-1].dt.strftime(
                                '%Y-%m-%dT%H:%M:%SZ').item(),
                        },
                        'resolution': 'hourly'
                    },
                    'title': (
                        f'High-resolution, near-real-time fluxes over Europe '
                        f'from CTE-HR: {base_info["dataset_type"]} '
                        f'{base_info["year"]}-{base_info["month"]}'
                    ),
                    'variables':
                        [variable for variable in xarray_dataset.data_vars],
                },
                'submitterId': 'CP'
            })
            json_file_name = f'{base_key}.json'
            json_file_path = os.path.join(self.json_standalone_files,
                                          json_file_name)
            base_info['json_file_path'] = json_file_path
            tools.write_json(path=json_file_path, content=base_info['json'])
        # Sort archive's items.
        self.archive_out = dict(sorted(self.archive_out.items()))
        self.store_current_archive()
        return

