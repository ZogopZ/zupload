from imports import *


class Dataset:

    def __init__(self, reason: str = None, interactive: bool = False):
        if reason is None:
            # todo: Need to have a way for the user to
            #  specify archive_in name.
            print('Unspecified reason. Exiting...')
            return
        self.reason = reason
        # Initialize needed directories & files.
        self.archives_dir = 'input-files/in-out-archives'
        self.archive_in = f'input-files/in-out-archives/archive_in_{self.reason}.json'
        self.json_standalone_files = 'input-files/json-standalone-files'
        Path(self.archives_dir).mkdir(parents=True, exist_ok=True)
        Path(self.json_standalone_files).mkdir(parents=True, exist_ok=True)
        self.input_data = self.get_input_files()
        # todo: Need to generalise this.
        self.file_type = self.input_data[0].split('.')[-1]
        self.interactive = interactive
        self.archive_out = self.read_static_data()

    # todo: Do I need to keep this as a method?
    @staticmethod
    def get_input_files() -> list:
        print(f'- {constants.ICON_DATA:3} Obtaining data files...')
        while True:
            # todo: Maybe make this part interactive using the self.interactive class attribute.
            # search_string = input('\tPlease enter files\' path followed by regular expression if needed: ')
            # search_string = '/data/flexpart/output/LPJoutput/MarkoRun2022global/nc2022/*global*.nc'
            # search_string = '/data/flexpart/output/LPJoutput/MarkoRun2022global/nc2022/conv_lpj_hgpp_global_0.5deg_2018.nc'
            search_string = '/data/flexpart/output/LPJoutput/MarkoRun2022eu/nc2022/*eu*.nc'
            found_files = sorted(glob.glob(search_string))
            file_listing = list()
            for file in found_files:
                file_listing.append(f'{file} ({humanize.naturalsize(os.stat(file).st_size)})')
            print(f'\tListing files...', *file_listing, sep='\n\t\t')
            if input(f'\tTotal of {len(found_files)} files. Will these do? (Y/n): ') == 'Y':
                break
        return sorted(found_files)

    def read_static_data(self) -> dict:
        archive_out = dict()
        while True:
            if not self.interactive:
                # Archive in file does not exist.
                if not os.path.exists(self.archive_in):
                    print(f'- {constants.ICON_ARCHIVE:3} Creating file {self.archive_in}... ', end='')
                    Path(self.archive_in).touch()
                    # todo: Maybe remove these next 2 lines.
                    # with open(file=self.archive_in, mode='w+'):
                    #     pass
                # Archive in file exists.
                else:
                    # Archive in is empty.
                    if os.stat(self.archive_in).st_size == 0:
                        print(f'- {constants.ICON_ARCHIVE:3} File {self.archive_in} exists but it is '
                              f'empty. Converting it to json... {constants.ICON_CHECK}')
                        with open(file=self.archive_in, mode='w') as archive_in_handle:
                            json.dump(dict(), archive_in_handle, indent=4)
                    print(f'- {constants.ICON_ARCHIVE:3} Reading static {self.reason} data from file '
                          f'{self.archive_in}... ', end='')
                    with open(file=self.archive_in, mode='r') as archive_in_handle:
                        archive_out = json.load(archive_in_handle)
                print(constants.ICON_CHECK)
                break
            # todo: Implement the user interaction of reading static files.
            #  Until then return an "error" message.
            elif self.interactive:
                print(f'- {constants.ICON_ARCHIVE:3} Sorry this is not yet implemented. Blame rando.')
                break
        return archive_out

    # todo: Do I need to keep this as a class method?
    # todo: Do I need to add a mode flag for this?
    def store_current_archive(self):
        user_input = 'n'
        if os.path.exists(self.archive_in):
            user_input = input(f'- {constants.ICON_ARCHIVE:3} Be careful!!! You are trying to overwrite an already '
                               f'existing {self.archive_in} file.\n\tAre you sure you want to overwrite?(Y/n): ')
        if user_input == 'Y':
            with open(file=self.archive_in, mode='w') as archive_out_handle:
                json.dump(self.archive_out, archive_out_handle, indent=4)
        return


if __name__ == '__main__':
    skipping_handlers = tools.parse_arguments(sys.argv[1])
    LpjGuessDataset(reason='lpj_guess_eu').one_shot(skipping_handlers)
    # LPJDataset(reason='lpj_guess_eu').one_shot(
    #     skip_archive_files=True,
    #     skip_try_ingest=True,
    #     skip_archive_json=True,
    # #     skip_upload_metadata=True,
    #     skip_upload_data=True
    # )
    # LPJDataset(reason='zois').one_shot()
    # dataset = LPJDataset(reason='zois')
    # LPJDataset(reason='lpj_guess_eu').one_shot()
