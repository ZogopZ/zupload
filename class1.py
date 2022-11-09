import constants
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
            search_string = input('\tPlease enter files\' path followed by regular expression if needed: ')
            found_files = glob.glob(search_string)
            if input(f'\tWill these files do? {found_files} (Y/n): ') == 'Y':
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
                               f'existing {self.archive_in} file.\n\tAre you sure you want to overwrite?(Y/n)')
        if user_input == 'Y':
            with open(file=self.archive_in, mode='w') as archive_out_handle:
                json.dump(self.archive_out, archive_out_handle, indent=4)
        return

    def archive_files(self):
        """Archive file paths, names, and other information if needed."""
        print(f'- {constants.ICON_GEAR} Archiving system information of `.{self.file_type}` files... ', end='')
        for file_path in self.input_data:
            file_name = file_path.split('/')[-1]
            base_key = file_name.split('_STILT')[0]
            year = base_key.split('_')[1]
            obtained_height = str(static_stilt[base_key.split('_')[0]]['alt'])
            station_height = obtained_height if len(obtained_height) == 3 else '0' + obtained_height
            other_specs = dict()
            other_specs['station_height'] = station_height
            with open(file=file_path, mode='r') as csv_handle:
                other_specs['rows'] = sum(1 for line in csv_handle) - 1
            data_object_spec = 'http://meta.icos-cp.eu/resources/cpmeta/stiltMoleFracTimeSer'
            archive_out[base_key] = dict({
                'file_path': file_path,
                'file_name': file_name,
                'station_height': station_height,
                'year': year,
                'try_ingest_command': build_try_ingest_command(file_path=file_path,
                                                               data_object_spec=data_object_spec,
                                                               other_specs=other_specs),
                'other_specs': other_specs
            })
        print(check)
        return

    def one_shot(self):
        self.archive_files()


if __name__ == '__main__':
    dataset = Dataset(reason='zois').one_shot()


