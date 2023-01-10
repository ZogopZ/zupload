# Format read from https://peps.python.org/pep-0008/#imports.
# Standard library imports.
from multiprocessing import Pool
from pathlib import Path
from pprint import pprint
import hashlib
import json
import os

# Related third party imports.
import humanize
import requests

# Local application/library specific imports.
import constants
# import dataset
# import lpj_guess_dataset
# import cte_hr_dataset
import tools


class Dataset:

    def __init__(self, reason: str = None, interactive: bool = False):
        if reason is None:
            # todo: Need to have a way for the user to
            #  specify archive_in name.
            print('Unspecified reason. Exiting...')
            return
        self.reason = reason
        # Initialize needed directories & files.
        self.master_dir = f'input-files/{self.reason.replace("_", "-")}'
        self.archives_dir = os.path.join(self.master_dir, 'in-out-archives')
        self.archive_in = os.path.join(self.archives_dir, f'{self.reason}.json')
        self.json_standalone_files = os.path.join(self.master_dir,
                                                  f'json-standalone-files/{self.reason.replace("_", "-")}')
        Path(self.master_dir).mkdir(parents=True, exist_ok=True)
        Path(self.archives_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_standalone_files).mkdir(parents=True, exist_ok=True)
        self.interactive = interactive
        self.archive_out = self.read_static_data()
        self.input_data = None
        self.file_type = self.input_data[0].split('.')[-1]
        return

    @property
    def input_data(self):
        return self._input_data

    @input_data.setter
    def input_data(self, input_content=None):
        print(f'- {constants.ICON_DATA:3}Obtaining data files...')
        found_files = list()
        file_listing = list()
        while True:
            if input_content is None:
                # todo: Maybe make this part interactive using the self.interactive class attribute.
                # search_string = input('\tPlease enter files\' path followed by regular expression if needed: ')
                search_string = 'input-files/data-files/.*.nc'
                found_files = sorted(tools.find_files(search_string=search_string))
            else:
                found_files = sorted(input_content)
            file_listing = list()
            for file in found_files:
                file_listing.append(f'{file} ({humanize.naturalsize(os.stat(file).st_size)})')
            if input(f'\tWould you like to see the files? (Y/n): ') == 'Y':
                print(f'\tListing files...', *file_listing, sep='\n\t\t')
            if input(f'\tTotal of {len(found_files)} files. Will these do? (Y/n): ') == 'Y':
                break
        self._input_data = found_files
        return

    # todo: Do I need to keep this as a method?
    @staticmethod
    def get_input_files() -> list:
        print(f'- {constants.ICON_DATA:3}Obtaining data files...')
        while True:
            # todo: Maybe make this part interactive using the self.interactive class attribute.
            # search_string = input('\tPlease enter files\' path followed by regular expression if needed: ')
            # Home directory for cte-hr fluxes.
            # search_string = '/ctehires/upload/remco/
            # search_string = '/data/flexpart/output/LPJoutput/MarkoRun2022global/nc2022/.*global.*.nc'
            search_string = '/ctehires/upload/remco/.*202209.*.nc'
            found_files = sorted(tools.find_files(search_string=search_string))
            file_listing = list()
            for file in found_files:
                file_listing.append(f'{file} ({humanize.naturalsize(os.stat(file).st_size)})')
            if input(f'\tWould you like to see the files? (Y/n): ') == 'Y':
                print(f'\tListing files...', *file_listing, sep='\n\t\t')
            if input(f'\tTotal of {len(found_files)} files. Will these do? (Y/n): ') == 'Y':
                break
        return found_files

    def read_static_data(self) -> dict:
        archive_out = dict()
        while True:
            if not self.interactive:
                # Archive in file does not exist.
                if not os.path.exists(self.archive_in):
                    print(f'- {constants.ICON_ARCHIVE:3}Creating file {self.archive_in}... ', end='')
                    Path(self.archive_in).touch()
                    # todo: Maybe remove these next 2 lines.
                    # with open(file=self.archive_in, mode='w+'):
                    #     pass
                # Archive in file exists.
                else:
                    # Archive in is empty.
                    if os.stat(self.archive_in).st_size == 0:
                        print(f'- {constants.ICON_ARCHIVE:3}File {self.archive_in} exists but it is '
                              f'empty. Converting it to json... {constants.ICON_CHECK}')
                        with open(file=self.archive_in, mode='w') as archive_in_handle:
                            json.dump(dict(), archive_in_handle, indent=4)
                    print(f'- {constants.ICON_ARCHIVE:3}Reading static {self.reason} data from file '
                          f'{self.archive_in}... ', end='')
                    with open(file=self.archive_in, mode='r') as archive_in_handle:
                        archive_out = json.load(archive_in_handle)
                print(constants.ICON_CHECK)
                break
            # todo: Implement the user interaction of reading static files.
            #  Until then return an "error" message.
            elif self.interactive:
                print(f'- {constants.ICON_ARCHIVE:3}Sorry this is not yet implemented. Blame rando.')
                break
        return archive_out

    def fill_handlers(self):
        for base_key, base_info in self.archive_out.items():
            # Initialize handlers for your static read files.
            # Zbunchpload will use and update json parts in archive_in
            # only if their respective handlers are true.
            if base_info['file_path'] in self.input_data:
                base_info.setdefault('handlers', dict())
                base_info.update(handlers=dict({'archive_json': True,
                                                'try_ingest': True,
                                                'upload_metadata': True,
                                                'upload_data': True}))
            else:
                base_info.setdefault('handlers', dict())
                base_info.update(handlers=dict({'archive_json': False,
                                                'try_ingest': False,
                                                'upload_metadata': False,
                                                'upload_data': False}))
        return

    def archive_files(self):
        return

    def archive_json(self):
        return

    # todo: Do I need to keep this as a class method?
    # todo: Do I need to add a mode flag for this?
    def store_current_archive(self):
        user_input = 'n'
        if os.path.exists(self.archive_in):
            user_input = input(f'- {constants.ICON_ARCHIVE:2}Be careful!!! You are trying to overwrite an already '
                               f'existing {self.archive_in} file.\n\tAre you sure you want to overwrite?(Y/n): ')
        if user_input == 'Y':
            with open(file=self.archive_in, mode='w') as archive_out_handle:
                json.dump(self.archive_out, archive_out_handle, indent=4)
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

    def try_ingest(self):
        """Tests ingestion of provided files to the Carbon Portal."""
        # Get number of try-ingest subprocesses from user input.
        # Setting this to more than 1 sometimes might not work.
        n = int(input(f'- {constants.ICON_GEAR:3}Please select number of subprocesses: '))
        checks = 0
        try_ingest_string = (
            f'- {constants.ICON_GEAR:3}Trying ingestion of {self.file_type} data files (This might take a while.) '
            f'{checks:2}/{len(self.input_data):2} checks... ')
        print(try_ingest_string, end='\r')
        command_list = list()
        for base_key, base_info in self.archive_out.items():
            if base_info['handlers']['try_ingest']:
                command_list.append(base_info['try_ingest_components'])
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
                            f'- {constants.ICON_GEAR:3}Trying ingestion of {self.file_type} data files '
                            f'(This might take a while.) {checks:2}/{len(self.input_data):2} checks... '
                            f'{checks * constants.ICON_CHECK}')
                        print(try_ingest_string) if checks == len(self.input_data) else \
                            print(try_ingest_string, end='\r', flush=True)
                results.extend(pool_results)
        # Uncomment these lines to print the results of the try-ingest.
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

    def re_ingest(self):
        for base_key, base_info in self.archive_out.items():
            if not base_info['handlers']['upload_metadata']:
                continue
            re_ingest_command_list = \
                ["curl", "-s", "--cookie", "cookies.txt",
                 "-X", "POST",
                 base_info['file_data_url']]
            re_ingest_command = shlex.split(' '.join(re_ingest_command_list))
            print(re_ingest_command)
        return

    def upload_metadata(self):
        checks = 0
        upload_metadata_string = (
            f'- {constants.ICON_LABEL:2}Uploading meta-data: {checks:2}/{len(self.input_data):2} checks...')
        print(upload_metadata_string, end='\r')
        url = 'https://meta.icos-cp.eu/upload'
        for base_key, base_info in self.archive_out.items():
            if not base_info['handlers']['upload_metadata']:
                continue
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
                    f'- {constants.ICON_LABEL:2}Uploading meta-data: {checks:2}/{len(self.input_data):2} checks...'
                    f'{checks * constants.ICON_CHECK}')
                print(upload_metadata_string) if checks == len(self.archive_out) else \
                    print(upload_metadata_string, end='\r')
            else:
                print(upload_metadata_string)
                print(upload_metadata_response.text, upload_metadata_response.status_code)
                # todo: Implement exit in case of incorrect upload of
                #  meta-data.
        print(upload_metadata_string)
        return

    def upload_data(self):
        upload_checks = 0
        hash_checks = 0
        upload_data_string = (
            f'- {constants.ICON_FLOPPY_DISK:2}Uploading data: (hash checks {hash_checks:2}/{len(self.input_data):2})'
            f' {upload_checks:2}/{len(self.input_data):2} checks...')
        print(upload_data_string, end='\r')
        for base_key, base_info in self.archive_out.items():
            if not base_info['handlers']['upload_data']:
                continue
            json_hash_sum = base_info['json']['hashSum']
            live_file_hash_sum = self.get_hash_sum(base_info['file_path'])
            if json_hash_sum == live_file_hash_sum:
                hash_checks += 1
                upload_data_string = (
                    f'- {constants.ICON_FLOPPY_DISK} Uploading data: (hash checks '
                    f'{hash_checks:2}/{len(self.input_data):2}) {upload_checks:2}/{len(self.input_data):2} checks...')
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
                    f'{hash_checks:2}/{len(self.input_data):2}) {upload_checks:2}/{len(self.input_data):2} checks...'
                    f'{upload_checks * constants.ICON_CHECK}')
                print(upload_data_string) if upload_checks == len(self.archive_out) else \
                    print(upload_data_string, end='\r')
            else:
                print(upload_data_string)
                print(upload_data_response.status_code, upload_data_response.text)
                print(f'\tWARNING! An error has occurred during data upload for file {base_info["file_name"]}.')
                input('\tYou can press ctrl+c to stop this program or press any other key to continue... ')
                # todo: Implement exit in case of incorrect data upload.
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

    def printer(self):
        print(f'- {constants.ICON_RECEIPT:3}Here\'s your meta-data:')
        # Used to align user output since keys can vary in length.
        base_key_max_length = len(max(self.archive_out.keys(), key=len))
        for base_key, base_info in self.archive_out.items():
            if base_info['file_path'] not in self.input_data:
                continue
            if 'file_metadata_url' in base_info.keys():
                print(f'\t{base_key:{base_key_max_length}} {base_info["file_metadata_url"]}')
            else:
                print(f'\t{base_key:{base_key_max_length}} No info for file meta-data url')
        print(f'\tTotal of {len(self.input_data)} items.')
        return

    # todo: implement mode mode for the one_shot handler.
    #  Instead of boolean arguments have 0 or 1.
    def one_shot(self, handlers=None):
        if handlers is None:
            handlers = {}
        self.archive_files() if handlers['archive_files'] \
            else print(f'- {constants.ICON_HOLE:3}Skipping archiving of files...')
        self.fill_handlers() if handlers['fill_handlers'] \
            else print(f'- {constants.ICON_HOLE:3}Skipping handler filling...')
        self.try_ingest() if handlers['try_ingest'] \
            else print(f'- {constants.ICON_HOLE:3}Skipping try ingestion of files...')
        self.archive_json() if handlers['archive_json'] \
            else print(f'- {constants.ICON_HOLE:3}Skipping archiving of json...')
        tools.check_permissions()
        self.upload_metadata() if handlers['upload_metadata'] \
            else print(f'- {constants.ICON_HOLE:3}Skipping uploading of meta-data...')
        self.upload_data() if handlers['upload_data'] \
            else print(f'- {constants.ICON_HOLE:3}Skipping uploading of data...')
        self.store_current_archive() if handlers['store_current_archive'] \
            else print(f'- {constants.ICON_HOLE:3}Skipping storing of archive...')
        self.printer()
