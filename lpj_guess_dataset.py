import dataset


class LpjGuessDataset(dataset.Dataset):
    def __init__(self, reason: str = None, interactive: bool = False):
        super().__init__(reason, interactive)
        self.dataset_object_spec = constants.OBJECT_SPECS['biosphere_modeling_spatial']

    def archive_files(self):
        """Archive file paths, names, and other information if needed."""
        print(f'- {constants.ICON_GEAR:3} Archiving system information of `.{self.file_type}` files... ', end='')
        for file_path in self.input_data:
            file_name = file_path.split('/')[-1]
            year = re.findall(r'\d{4}', file_name)
            if len(year) != 1:
                exit(f'\tError! Incorrect 4-digit year values: {year} where spotted in file: {file_name}.\n'
                     f'\tNeed to have only one 4-digit year value specified in file\'s name.\n'
                     f'\tZbunchpload will now exit.')
            variable = [variable for variable in ['rtot', 'gpp', 'nee'] if variable in file_name]
            if len(variable) != 1:
                exit(f'\tError! Incorrect number of variables detected: {variable} file: {file_name}.\n'
                     f'\tNeed to have only one variable specified in file\'s name.\n'
                     f'\tZbunchpload will now exit.')
            base_key = f'{variable[0]}_{year[0]}'
            self.archive_out[base_key] = dict({
                'file_path': file_path,
                'file_name': file_name,
                'variable': variable[0],
                'year': year[0],
                'try_ingest_components': self.build_try_ingest_components(file_path=file_path)})
        # Sort archive's items.
        self.archive_out = dict(sorted(self.archive_out.items()))
        print(constants.ICON_CHECK)
        return

    def build_try_ingest_components(self, file_path: str = None) -> dict:
        """Build the try-ingest command for each data file."""
        xarray_dataset = xarray.open_dataset(file_path)
        variable_list = list(xarray_dataset.data_vars)
        # The variable list must be formatted like this:
        # '["variable_1", "variable_2", ...]'
        # Formatting like this e.g: "['variable_1', 'variable_2', ...]"
        # will probably result in a try ingest error.
        variables = ', '.join(f'["{variable}"]' for variable in variable_list)
        try_ingest_url = 'https://data.icos-cp.eu/tryingest'
        params = dict({'specUri': self.dataset_object_spec,
                       'varnames': variables})
        try_ingest_components = {'url': try_ingest_url,
                                 'params': params,
                                 'file_path': file_path}
        return try_ingest_components

    @staticmethod
    def execute_item(try_ingest_components: dict = None) -> dict:
        """Used from processes spawned by try_ingest()."""
        try_ingest_response = requests.put(url=try_ingest_components['url'],
                                           data=open(file=try_ingest_components['file_path'], mode='rb'),
                                           params=try_ingest_components['params'])
        return {'status_code': try_ingest_response.status_code, 'text': try_ingest_response.text}

    def archive_json(self):
        """Generates standalone .json files and adds to archive.

        Generates the standalone .json file for each data file and updates
        the archive with the regenerated json content. This function needs
        to be rerun each time we need to change something in the meta-data.
        If we decide to rerun this then it is mandatory that we also
        overwrite the `archive_in_nc.json` file using the function
        `store_current_archive()` at the end of the script.

        """
        print(f'- {constants.ICON_GEAR:3} Archiving meta-data... ', end='')
        for base_key, base_info in self.archive_out.items():
            if not base_info['handlers']['archive_json']:
                continue
            xarray_dataset = xarray.open_dataset(base_info['file_path'])
            base_info['json'] = dict({
                'fileName': base_info['file_name'],
                'hashSum': self.get_hash_sum(file_path=base_info['file_path']),
                'isNextVersionOf': [],
                'objectSpecification': self.dataset_object_spec,
                'references': {
                    'keywords': [
                        'Carbon Dioxide', 'Carbon Cycle', 'Land Biogeochemistry', 'Terrestrial Ecosystems'
                    ],
                    'licence': 'http://meta.icos-cp.eu/ontologies/cpmeta/icosLicence'
                },
                'specificInfo': {
                    'description': (
                        'LPJ-GUESS (revision 6562) forced with hourly ERA5 climate datasets to simulate global '
                        'terrestrial NEE, GPP and total respiration in 0.5 degree. LPJ-GUESS is a process-based '
                        'dynamic global vegetation model, it uses time series data (e.g. climate forcing and '
                        'atmospheric carbon dioxide concentrations with WMO CO2 X2019 scale) as input to simulate the '
                        'effects of environmental change on vegetation structure and composition in terms of European '
                        'plant functional types (PFTs), soil hydrology and biogeochemistry '
                        '(Smith et al., 2001, https://web.nateko.lu.se/lpj-guess/).'
                    ),
                    'production': {
                        'contributors': [
                            'http://meta.icos-cp.eu/resources/people/Michael_Mischurow',
                            'http://meta.icos-cp.eu/resources/people/Paul_Miller'
                        ],
                        'creationDate': '2022-10-03T10:00:00Z',
                        'creator': 'http://meta.icos-cp.eu/resources/people/Zhendong_Wu',
                        'hostOrganization': 'http://meta.icos-cp.eu/resources/organizations/CP',
                        'sources': [],
                    },
                    'spatial': 'http://meta.icos-cp.eu/resources/latlonboxes/lpjGuessEuropeLatLonBox',
                    'temporal': {
                        'interval': {
                            'start': xarray_dataset.time[0].dt.strftime('%Y-%m-%dT%H:%M:%SZ').item(),
                            'stop': xarray_dataset.time[-1].dt.strftime('%Y-%m-%dT%H:%M:%SZ').item(),
                        },
                        'resolution': 'hourly'
                    },
                    'title': f'LPJ-GUESS Europe hourly {base_info["variable"].upper()} for {base_info["year"]}',
                    'variables': [variable for variable in xarray_dataset.data_vars],
                },
                'submitterId': 'CP'
            })
            json_file_name = base_key + '.json'
            json_file_path = os.path.join(self.json_standalone_files, json_file_name)
            base_info['json_file_path'] = json_file_path
            tools.write_json(path=json_file_path, content=base_info['json'])
        print(constants.ICON_CHECK, flush=True)
        return

    def printer(self):
        print(f'- {constants.ICON_RECEIPT:3}Here\'s your meta-data:')
        for base_key, base_info in self.archive_out.items():
            print('\t', base_info['file_metadata_url'])
        return
