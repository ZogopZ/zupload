def exit_zupload(reason: str = None, info: dict = None):
    exit_message = str()
    if info and \
            all(key in info.keys() for key in ['general_date', 'file_name']):
        exit_message = (
            f'\tError! Incorrect 6-digit date values: {info["general_date"]}'
            f' where spotted in file: {info["file_name"]}.\n'
            f'\tNeed to have only one 6-digit date value specified in file\'s'
            f' name.\n'
            f'\tZupload will now exit.'
        )
    elif reason:
        exit_message = f'\tZupload will now exit due to: {reason}.'
    else:
        exit_message = 'Zupload will now exit.'
    exit(exit_message)
    return
