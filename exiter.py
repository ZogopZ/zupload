def exit_zupload(exit_type: str = None, info: dict = None):
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
    elif exit_type:
        if exit_type == 'upload_data':
            exit_message = (
                f'\tError while uploading data.\n'
                f'\tStatus code: {info["status_code"]}\n'
                f'\tContent: {info["text"]}\n'
                f'\tZupload will now exit.'
            )
        elif exit_type == 'upload_meta_data':
            exit_message = (
                f'\tError while uploading meta-data.\n'
                f'\tStatus code: {info["status_code"]}\n'
                f'\tContent: {info["text"]}\n'
                f'\tZupload will now exit.'
            )
        elif exit_type == 'try_ingest':
            exit_message = (
                f'\tError while trying ingestion.\n'
                f'\tZupload will now exit.'
            )
    else:
        exit_message = 'Zupload will now exit.'
    exit(exit_message)
    return
