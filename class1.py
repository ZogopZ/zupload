from imports import *


class Dataset:

    def __init__(self, reason: str = None, interactive: bool = False):
        if reason is None:
            # todo: Need to have a way for the user to
            #  specify archive_in name.
            print('Unspecified reason. Exiting...')
            return
        # Initialize needed directories.
        Path('input-files/in-out-archives').mkdir(parents=True, exist_ok=True)
        self.reason = reason
        self.input_data = self.get_input_files()
        # todo: Need to generalise this.
        self.file_type = self.input_data[0].split('.')[-1]
        self.interactive = interactive
        self.archive_in = f'input-files/in-out-archives/archive_in_{self.reason}.json'
        self.archive_out = self.read_static_data()

    # todo: Do I need to keep this as a method?
    @staticmethod
    def get_input_files():
        print(f'- {constants.ICON_DATA} Obtaining data files...')
        while True:
            # todo: Maybe make this part interactive using the self.interactive class attribute.
            # search_string = input('\tPlease enter files\' path followed by regular expression if needed: ')
            search_string = '/data/flexpart/output/LPJoutput/MarkoRun2022global/nc2022/*global*.nc'
            # search_string = '/data/flexpart/output/LPJoutput/MarkoRun2022global/nc2022/conv_lpj_hgpp_global_0.5deg_2018.nc'
            found_files = glob.glob(search_string)
            print(f'\tListing files...', *found_files, sep='\n\t\t')
            if input(f'\tTotal of {len(found_files)} files. Will these do? (Y/n): ') == 'Y':
                break
        return found_files

    def read_static_data(self) -> dict:
        archive_out = dict()
        while True:
            if not self.interactive:
                # Archive in file does not exist.
                if not os.path.exists(self.archive_in):
                    print(f'- {constants.ICON_ARCHIVE} Creating file {self.archive_in}... ', end='')
                    with open(file=self.archive_in, mode='w+'):
                        pass
                # Archive in file exists.
                else:
                    # Archive in is empty.
                    if os.stat(self.archive_in).st_size == 0:
                        print(f'- {constants.ICON_ARCHIVE} File {self.archive_in} exists but it is '
                              f'empty. Converting it to json... {constants.ICON_CHECK}')
                        with open(file=self.archive_in, mode='w') as archive_in_handle:
                            json.dump(dict(), archive_in_handle, indent=4)
                    print(f'- {constants.ICON_ARCHIVE} Reading static {self.reason} data from file '
                          f'{self.archive_in}... ', end='')
                    with open(file=self.archive_in, mode='r') as archive_in_handle:
                        archive_out = json.load(archive_in_handle)
                print(constants.ICON_CHECK)
                break
            # todo: Implement the user interaction of reading static files.
            #  Until then return an "error" message.
            elif self.interactive:
                print(f'- {constants.ICON_ARCHIVE} Sorry this is not yet implemented. Blame rando.')
                break
        return archive_out

    # todo: Do I need to keep this as a class method?
    def store_current_archive(self):
        user_input = 'n'
        if os.path.exists(self.archive_in):
            user_input = input(f'- {constants.ICON_ARCHIVE} Be careful!!! You are trying to overwrite an already '
                               f'existing {self.archive_in} file.\n\tAre you sure you want to overwrite?(Y/n): ')
        if user_input == 'Y':
            with open(file=self.archive_in, mode='w') as archive_out_handle:
                json.dump(self.archive_out, archive_out_handle, indent=4)
        return


class LPJDataset(Dataset):
    def __init__(self, reason: str = None, interactive: bool = False):
        super().__init__(reason, interactive)
        self.data_object_spec = 'http://meta.icos-cp.eu/resources/cpmeta/biosphereModelingSpatial'

    def archive_files(self):
        """Archive file paths, names, and other information if needed."""
        print(f'- {constants.ICON_GEAR} Archiving system information of `.{self.file_type}` files... ', end='')
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
                'try_ingest_components': self.build_try_ingest_components(file_path=file_path)})
        # Sort archive's items.
        self.archive_out = dict(sorted(self.archive_out.items()))
        print(constants.ICON_CHECK)
        return

    def build_try_ingest_components(self, file_path: str = None) -> dict:
        """Build the try-ingest command for each data file."""
        dataset = xarray.open_dataset(file_path)
        variable_list = list(dataset.data_vars)
        # The variable list must be formatted like this:
        # '["variable_1", "variable_2", ...]'
        # Any other way will probably result in try ingest error.
        variables = ', '.join(f'["{variable}"]' for variable in variable_list)
        try_ingest_url = 'https://data.icos-cp.eu/tryingest'
        params = dict({'specUri': self.data_object_spec,
                       'varnames': variables})
        try_ingest_components = {'call': requests.put,
                                 'url': try_ingest_url,
                                 'params': params,
                                 'file_path': file_path}
        return try_ingest_components

    def try_ingest(self):
        """Tests ingestion of provided files to the Carbon Portal."""
        print(f'- {constants.ICON_GEAR} Trying ingestion of {self.file_type} data files (This might take a while.) '
              f'Expecting {len(self.input_data):>2} checks... ', end='')
        command_list = list()
        for key, value in self.archive_out.items():
            command_list.append(value['try_ingest_components'].copy())
            # Remove the function call from the archive.
            # Functions stored in dictionaries cannot be json
            # serialized.
            value['try_ingest_components'].pop('call')
        # Break up the list into smaller lists of n items per sublist.
        # This way we can produce user output faster, so that the person
        # who executes this script has an idea of what's happening.
        # todo: Maybe make this part interactive using the self.interactive class attribute.
        n = 3
        lists_to_process = [command_list[i * n:(i + 1) * n] for i in range((len(command_list) + n - 1) // n)]
        results = list()
        for item_list in lists_to_process:
            # Create a process for each item in the sublist.
            with Pool(processes=len(item_list)) as pool:
                # Each item in the sublist is passed as an argument to
                # `execute_item()` function.
                results.extend(pool.map(self.execute_item, item_list))
        # Uncomment this line to print the results of the try-ingest.
        # [print(result) for result in results]
        checks = 0
        for result in results:
            if result['status_code'] == 200:
                checks += 1
        print(f'\n{67*" "}Received {checks:>2} checks')
        return

    @staticmethod
    def execute_item(try_ingest_components: dict = None) -> dict:
        """Used from processes spawned by try_ingest()."""
        try_ingest_request = try_ingest_components['call'](
            url=try_ingest_components['url'],
            data=open(file=try_ingest_components['file_path'], mode='rb'),
            params=try_ingest_components['params'])
        if try_ingest_request.status_code == 200:
            print(constants.ICON_CHECK, end='', flush=True)
        else:
            print('x', end='', flush=True)
        return {'status_code': try_ingest_request.status_code, 'text': try_ingest_request.text}

    def one_shot(self):
        self.archive_files()
        self.try_ingest()
        self.store_current_archive()


if __name__ == '__main__':
    LPJDataset(reason='zois').one_shot()
