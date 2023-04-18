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
# import cte_hr_dataset
# import dataset
# import lpj_guess_dataset
import constants
import exiter
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
                                                  f'json-standalone-files')
        Path(self.master_dir).mkdir(parents=True, exist_ok=True)
        Path(self.archives_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_standalone_files).mkdir(parents=True, exist_ok=True)
        self.interactive = interactive
        self.archive_out = self.read_static_data()
        self.input_data = None
        self.processed_input_data = None
        # Todo: probably make this an argument. For remote sensing
        #  data, the input files are .nc but the actual uploaded files
        #  are zips.
        self.file_type = \
            self.input_data[0].split('.')[-1] if self.input_data \
                else 'unknown file'
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
                search_string = 'input-files/data-files/ctehires/.*.nc'
                found_files = sorted(tools.find_files(search_string=search_string))
            else:
                found_files = sorted(input_content)
            file_listing = list()
            for file in found_files:
                file_listing.append(f'{file} ({humanize.naturalsize(os.stat(file).st_size)})')
            # view_files_input = 'n'
            view_files_input = input(
                f'\tWould you like to see the files? (Y/n/e): '
            )
            if view_files_input == 'n':
                break
            elif view_files_input == 'Y':
                print(f'\tListing files...', *file_listing, sep='\n\t\t')
                confirmation_input = input(
                    f'\tTotal of {len(found_files)} files. '
                    f'Will these do? (Y/n): '
                )
                if confirmation_input == 'Y':
                    break
            elif view_files_input == 'e':
                exit('Exiting abruptly')
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
        """Read static data from existing archive."""
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
            # only if their respective handlers are true. For remote
            # sensing datasets, the actual input files are the zip
            # archives and not the .nc files themselves. That's why we
            # use the self.processed_input_data instance variable
            # instead oof self.input_data.
            if (
                    (base_info['file_path'] in self.input_data)
                    or
                    (
                        (self.reason == 'modis' or self.reason == 'landsat')
                        and
                        (base_info['file_path'] in self.processed_input_data)
                    )
            ):
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
    #
    # def archive_files(self):
    #     return
    #
    # def archive_json(self):
    #     return

    # todo: Do I need to keep this as a class method?
    # todo: Do I need to add a mode flag for this?
    def store_current_archive(self):
        user_input = 'n'
        if (
                os.path.exists(self.archive_in)
            and
                tools.input_handler(operation='store_current_archive',
                                    additional_info=dict(
                                        {'archive': self.archive_in})) == 'Y'
        ):
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
        # Todo: Fix the way try_ingest() outputs stuff.
        # Get number of try-ingest subprocesses from user input.
        # Setting this to more than 1 sometimes might not work.
        print(f'- Trying ingestion of files (This might take a while.)')
        subprocesses = int(tools.input_handler(operation='try_ingest'))
        checks = 0
        command_list = list()
        for base_key, base_info in self.archive_out.items():
            if base_info['handlers']['try_ingest']:
                command_list.append(base_info['try_ingest_components'])
        # Break up the list into smaller lists of n items per sublist.
        # This way we can produce user output faster, so that the person
        # who executes this script has an idea of what's happening.
        # todo: Maybe make this part interactive using the self.interactive class attribute.
        lists_to_process = [
            command_list[i * subprocesses:(i + 1) * subprocesses]
            for i in range(
                (len(command_list) + subprocesses - 1) // subprocesses
            )
        ]
        results = list()
        total = len(lists_to_process)
        for item_list in lists_to_process:
            pool_results = list()
            # Create a process for each item in the sublist.
            with Pool(processes=len(item_list)) as pool:
                # Each item in the sublist is passed as an argument to
                # `execute_item()` function.
                pool_results = pool.map(self.execute_item, item_list)
                for pool_result in pool_results:
                    if pool_result['status_code'] != 200:
                        exiter.exit_zupload(reason='Try ingest error')
                    else:
                        checks += 1
                        tools.progress_bar(operation='try_ingest',
                                           current=checks, total=total,
                                           additional_info=pool_result)
                results.extend(pool_results)
        # Uncomment these lines to print the results of the try-ingest.
        # print('')
        # [print(result) for result in results]
        return

    @staticmethod
    def execute_item(try_ingest_components: dict = None) -> dict:
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
        print('- Uploading meta-data.')
        total = len(self.archive_out)
        for index, (base_key, base_info) in \
                enumerate(self.archive_out.items()):
            if base_info['handlers']['upload_metadata']:
                data = open(file=base_info['json_file_path'], mode='rb')
                cookies = tools.load_cookie()
                headers = {'Content-Type': 'application/json'}
                upload_metadata_response = requests.post(
                    url=constants.META_DATA_UPLOAD_URL,
                    data=data,
                    headers=headers,
                    cookies=cookies
                )
                if upload_metadata_response.status_code == 200:
                    file_data_url = upload_metadata_response.text
                    base_info['file_data_url'] = file_data_url
                    base_info['file_metadata_url'] = \
                        file_data_url.replace('data', 'meta')
                else:
                    exit(f'{upload_metadata_response.text},'
                         f'{upload_metadata_response.status_code}')
                    # todo: Implement exit in case of incorrect upload of
                    #  meta-data.
            tools.progress_bar(operation='upload_meta_data', current=index+1,
                               total=total)
        self.store_current_archive()
        return

    def upload_data(self):
        total = len(self.archive_out)
        for index, (base_key, base_info) in \
                enumerate(self.archive_out.items()):
            if not base_info['handlers']['upload_data'] or \
                    'file_data_url' not in base_info.keys():
                tools.progress_bar(operation='upload_data', current=index + 1,
                                   total=total)
                continue
            json_hash_sum = base_info['json']['hashSum']
            url = base_info['file_data_url']
            data = open(file=base_info['file_path'], mode='rb')
            cookies = tools.load_cookie()
            upload_data_response = requests.put(
                url=url,
                data=data,
                cookies=cookies
            )
            if upload_data_response.status_code == 200:
                base_info['pid'] = upload_data_response.text
            else:
                exit(f'{upload_data_response.status_code}, '
                     f'{upload_data_response.text}')
            tools.progress_bar(operation='upload_data', current=index+1,
                               total=total)
        self.store_current_archive()
        return

    def printer(self):
        print(f'- Here\'s your meta-data:')
        # Used to align user output since keys can vary in length.
        base_key_max_length = len(max(self.archive_out.keys(), key=len))
        for base_key, base_info in self.archive_out.items():
            if base_info['file_path'] not in self.input_data:
                continue
            if 'file_metadata_url' in base_info.keys():
                print(f'\t{base_key:{base_key_max_length}} '
                      f'{base_info["file_metadata_url"]}')
            else:
                print(f'\t{base_key:{base_key_max_length}} '
                      f'No info for file meta-data url')
        print(f'\tTotal of {len(self.input_data)} items.')
        return

    def archive_files(self):
        return

    def archive_json(self):
        return

    # todo: implement mode mode for the one_shot handler.
    #  Instead of boolean arguments have 0 or 1.
    def one_shot(self, handlers=None):
        if handlers is None:
            handlers = {}
        self.archive_files() if handlers['archive_files'] \
            else print(f'- Skipping archiving of files.')
        # self.fill_handlers() if handlers['fill_handlers'] \
        #     else print(f'- {constants.ICON_HOLE:3}Skipping handler filling...')
        self.try_ingest() if handlers['try_ingest'] \
            else print(f'- Skipping try ingestion of files.')
        self.archive_json() if handlers['archive_json'] \
            else print(f'- Skipping archiving of json.')
        tools.check_permissions()
        self.upload_metadata() if handlers['upload_metadata'] \
            else print(f'- Skipping uploading of meta-data.')
        self.upload_data() if handlers['upload_data'] \
            else print(f'- Skipping uploading of data.')
        self.store_current_archive() if handlers['store_current_archive'] \
            else print(f'- Skipping storing of archive.')
        self.printer()
