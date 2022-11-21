from imports import *


class CteHrDataset(dataset.Dataset):
    def __init__(self, reason: str = None, interactive: bool = False):
        super().__init__(reason, interactive)
        self.data_object_spec = 'http://meta.icos-cp.eu/resources/cpmeta/biosphereModelingSpatial'

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
        params = dict({'specUri': self.data_object_spec,
                       'varnames': variables})
        try_ingest_components = {'url': try_ingest_url,
                                 'params': params,
                                 'file_path': file_path}
        return try_ingest_components

    def try_ingest(self):
        """Tests ingestion of provided files to the Carbon Portal."""
        # Get number of try-ingest subprocesses from user input.
        # Setting this to more than 1 sometimes might not work.
        n = int(input(f'- {constants.ICON_GEAR} Please select number of subprocesses: '))
        checks = 0
        try_ingest_string = (
            f'- {constants.ICON_GEAR} Trying ingestion of {self.file_type} data files (This might take a while.) '
            f'{checks:2}/{len(self.input_data):2} checks... ')
        print(try_ingest_string, end='\r')
        command_list = list()
        for key, value in self.archive_out.items():
            command_list.append(value['try_ingest_components'])
        # Break up the list into smaller lists of n items per sublist.
        # This way we can produce user output faster, so that the person
        # who executes this script has an idea of what's happening.
        # todo: Maybe make this part interactive using the self.interactive class attribute.
        lists_to_process = [command_list[i * n:(i + 1) * n] for i in range((len(command_list) + n - 1) // n)]
        results = list()
        for item_list in lists_to_process:
            pool_results = list()
            # Create a process for each item in the sublist.
            with Pool(processes=len(item_list)) as pool:
                # Each item in the sublist is passed as an argument to
                # `execute_item()` function.
                pool_results = pool.map(self.execute_item, item_list)
                for pool_result in pool_results:
                    if pool_result['status_code'] != 200:
                        exit(f'\n\tTry Ingest Error! {pool_result}.\n'
                             f'\tZbunchpload will now exit.')
                    else:
                        checks += 1
                        try_ingest_string = (
                            f'- {constants.ICON_GEAR} Trying ingestion of {self.file_type} data files '
                            f'(This might take a while.) {checks:2}/{len(self.input_data):2} checks... '
                            f'{checks * constants.ICON_CHECK}')
                        print(try_ingest_string) if checks == len(self.input_data) else \
                            print(try_ingest_string, end='\r')
                results.extend(pool_results)
        # Uncomment this line to print the results of the try-ingest.
        # print('')
        # [print(result) for result in results]
        return

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
            xarray_dataset = xarray.open_dataset(base_info['file_path'])
            base_info['json'] = dict({
                'fileName': base_info['file_name'],
                'hashSum': self.get_hash_sum(file_path=base_info['file_path']),
                'isNextVersionOf': [],
                'objectSpecification': self.data_object_spec,
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

    # todo: maybe move this to tools.py
    @staticmethod
    def get_hash_sum(file_path: str = None) -> str:
        sha256_hash = hashlib.sha256()
        with open(file=file_path, mode='rb') as file_handle:
            # Read and update hash string value in blocks of 4K
            for byte_block in iter(lambda: file_handle.read(4096), b""):
                sha256_hash.update(byte_block)
        return sha256_hash.hexdigest()

    def upload_metadata(self):
        checks = 0
        upload_metadata_string = (
            f'- {constants.ICON_LABEL} Uploading meta-data: {checks:2}/{len(self.archive_out):2} checks...')
        print(upload_metadata_string, end='\r')
        url = 'https://meta.icos-cp.eu/upload'
        for base_key, base_info in self.archive_out.items():
            data = open(file=base_info['json_file_path'], mode='rb')
            cookies = tools.load_cookie()
            headers = {'Content-Type': 'application/json'}
            upload_metadata_response = requests.post(url=url, data=data, headers=headers, cookies=cookies)
            if upload_metadata_response.status_code == 200:
                file_data_url = upload_metadata_response.text
                base_info['file_data_url'] = file_data_url
                base_info['file_metadata_url'] = file_data_url.replace('data', 'meta')
                checks += 1
                upload_metadata_string = (
                    f'- {constants.ICON_LABEL} Uploading meta-data: {checks:2}/{len(self.archive_out):2} checks...'
                    f'{checks * constants.ICON_CHECK}')
                print(upload_metadata_string) if checks == len(self.archive_out) else \
                    print(upload_metadata_string, end='\r')
        return

    def upload_data(self):
        upload_checks = 0
        hash_checks = 0
        upload_data_string = (
            f'- {constants.ICON_FLOPPY_DISK} Uploading data: (hash checks {hash_checks:2}/{len(self.archive_out):2})'
            f' {upload_checks:2}/{len(self.archive_out):2} checks...')
        print(upload_data_string, end='\r')
        for base_key, base_info in self.archive_out.items():
            json_hash_sum = base_info['json']['hashSum']
            live_file_hash_sum = self.get_hash_sum(base_info['file_path'])
            if json_hash_sum == live_file_hash_sum:
                hash_checks += 1
                upload_data_string = (
                    f'- {constants.ICON_FLOPPY_DISK} Uploading data: (hash checks '
                    f'{hash_checks:2}/{len(self.archive_out):2}) {upload_checks:2}/{len(self.archive_out):2} checks...')
                print(upload_data_string, end='\r')
            else:
                print(upload_data_string)
                exit(f'\tError during hash-sum validation. Different hash-sums were detected for file '
                     f'{base_info["file_name"]}.\n'
                     f'\t{"Hash sum read from archive:":30} {json_hash_sum}\n'
                     f'\t{"Hash sum calculated from file:":30} {live_file_hash_sum}\n'
                     f'\tzbunchpload will now exit.')
            url = base_info["file_data_url"]
            data = open(file=base_info['file_path'], mode='rb')
            cookies = tools.load_cookie()
            upload_data_response = requests.put(url=url, data=data, cookies=cookies)
            if upload_data_response.status_code == 200:
                base_info['pid'] = upload_data_response.text
                upload_checks += 1
                upload_data_string = (
                    f'- {constants.ICON_FLOPPY_DISK} Uploading data: (hash checks '
                    f'{hash_checks:2}/{len(self.archive_out):2}) {upload_checks:2}/{len(self.archive_out):2} checks...'
                    f'{upload_checks * constants.ICON_CHECK}')
                print(upload_data_string) if upload_checks == len(self.archive_out) else \
                    print(upload_data_string, end='\r')
            else:
                print(upload_data_string)
                print(upload_data_response.status_code, upload_data_response.text)
                print(f'\tWARNING! An error has occurred during data upload for file {base_info["file_name"]}.')
                input('\tYou can press ctrl+c to stop this program or press any other key to continue... ')
        return

    # todo: implement mode mode for the one_shot handler.
    #  Instead of boolean arguments have 0 or 1.
    def one_shot(self, handlers=None):
        if handlers is None:
            handlers = {}
        self.archive_files() if handlers['archive_files'] \
            else print(f'- {constants.ICON_HOLE:3} Skipping archiving of files...')
        self.try_ingest() if handlers['try_ingest'] \
            else print(f'- {constants.ICON_HOLE:3} Skipping try ingestion of files...')
        self.archive_json() if handlers['archive_json'] \
            else print(f'- {constants.ICON_HOLE:3} Skipping archiving of json...')
        tools.check_permissions()
        self.upload_metadata() if handlers['upload_metadata'] \
            else print(f'- {constants.ICON_HOLE:3} Skipping uploading of meta-data...')
        self.upload_data() if handlers['upload_data'] \
            else print(f'- {constants.ICON_HOLE:3} Skipping uploading of data...')
        self.store_current_archive()
        self.printer()

    def printer(self):
        print(f'- {constants.ICON_RECEIPT:3}Here\'s your meta-data:')
        for base_key, base_info in self.archive_out.items():
            print('\t', base_info['file_metadata_url'])
        return