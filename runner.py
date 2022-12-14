import sys
import tools
import stilt_timeseries_dataset
import lpj_guess_dataset
import cte_hr_dataset

if __name__ == '__main__':
    if len(sys.argv) == 2:
        skipping_handlers = tools.parse_arguments(sys.argv[1])
    else:
        static_mode = '111111'
        skipping_handlers = tools.parse_arguments(static_mode)
<<<<<<< Updated upstream
    # my_class = cte_hr_dataset.CteHrDataset(reason='cte_hr').one_shot(skipping_handlers)
    # my_class = lpj_guess_dataset.LpjGuessDataset(reason='lpj_guess_global').one_shot(skipping_handlers)
    my_class = stilt_timeseries_dataset.StiltTimeseriesDataset(reason='stilt_timeseries').one_shot(skipping_handlers)
=======

    # LPJ-GUESS
    # my_class = lpj_guess_dataset.LpjGuessDataset(reason='lpj_guess_global').one_shot(skipping_handlers)

    # CTE-HR
    my_class = cte_hr_dataset.CteHrDataset(reason='cte_hr').one_shot(skipping_handlers)


    # skipping_handlers = tools.parse_arguments(sys.argv[1])
    # # backup_archive = tools.read_json(path='input-files/in-out-archives/archive_in_nc_2022_07.json')
    # current_archive = tools.read_json(path='input-files/in-out-archives/archive_in_cte_hr.json')
    # for key, value in current_archive.items():
    #     value.update(handlers=dict({'archive_json': False, 'try_ingest': False, 'upload_metadata': False, 'upload_data': False}))
    #     if value['versions']:
    #         if value['file_metadata_url'] == value['versions'][-1]:
    #             print(constants.ICON_CHECK, end='')
    #         else:
    #             print(key)
    # if 'anthropogenic' in key:
    #     value.setdefault('handlers', dict({'try_ingest': True, 'upload_metadata': True, 'upload_data': True}))
    # else:
    #     value.setdefault('handlers', dict({'try_ingest': False, 'upload_metadata': False, 'upload_data': False}))
    #         value.update(versions=[value['file_metadata_url']])
    # #     else:
    # #         value.update(versions=[])
    # # pprint(current_archive)
    # # # keys_found = 0
    # # # for backup_key, backup_value in backup_archive.items():
    # # #     if backup_key in current_archive.keys():
    # # #         if 'file_data_url' in current_archive[backup_key].keys() or 'file_metadata_url' in current_archive[backup_key].keys():
    # # #             print('WARNING!!!')
    # # #             break
    # # #         else:
    # # #             keys_found += 1
    # # #             current_archive[backup_key]['file_data_url'] = backup_archive[backup_key]['file_data_url']
    # # #             current_archive[backup_key]['file_metadata_url'] = backup_archive[backup_key]['file_metadata_url']
    # # #     else:
    # # #         print('SOMETHING IS MISSING')
    # # #
    # # # print(keys_found)
    # tools.write_json('input-files/in-out-archives/archive_in_cte_hr.json', content=current_archive)
>>>>>>> Stashed changes
