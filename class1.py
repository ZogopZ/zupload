from imports import os


class Dataset:

    def __init__(self, reason: str = None, interactive: bool = True):
        if reason is None:
            # todo: Need to have a way for the user to
            #  specify archive_in name.
            print('Unspecified reason. Exiting...')
            return
        self.archive_in = f'archive_in_{reason}.json'
        self.archive_out = self.read_static_data(interactive, self.archive_in)

    @staticmethod
    def read_static_data(interactive: bool, archive_in: str) -> dict:
        while True:
            if interactive:
                # Archive in file does not exist.
                if not os.path.exists(archive_in):
                    print(f'- {archive} Creating file {archive_in}... ', end='')
                    with open(file=archive_in, mode='w+'):
                        pass
                # Archive in file exists.
                else:
                    # Archive in is empty.
                    if os.stat(archive_in).st_size == 0:
                        print(f'- {archive} File {archive_in} exists but it is '
                              f'empty. Converting it to json... {check}')
                        with open(file=archive_in, mode='w') as archive_in_handle:
                            json.dump(dict(), archive_in_handle, indent=4)
                    print(f'- {archive} Reading static {reason} data from file '
                          f'{archive_in}... ', end='')
                    with open(file=archive_in, mode='r') as archive_in_handle:
                        archive_out = json.load(archive_in_handle)
                print(check)
                return archive_out
            elif not cli_flag:
                break
            elif user_input == 'e':
                exit('User exited')
            else:
                continue
        return {}
        print(interactive)
        return
    # def read_static_data(self, interactive=True: bool) -> :


if __name__ == '__main__':
    Dataset().read_static_data(interactive=False)


