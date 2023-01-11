import sys
import tools
import lpj_guess_dataset
import cte_hr_dataset
import one_time_dataset

if __name__ == '__main__':
    if len(sys.argv) == 2:
        skipping_handlers = tools.parse_arguments(sys.argv[1])
    else:
        # Bit handler 1: archive files.
        # Bit handler 2: fill handlers. This should always be 1.
        # Bit handler 3: try-ingest files.
        # Bit handler 4: archive json.
        # Bit handler 5: upload meta-data.
        # Bit handler 6: upload data.
        # Bit handler 7: overwrite archive in.
        #              1234567
        static_mode = '0100000'
        skipping_handlers = tools.parse_arguments(static_mode)
    # LPJ-GUESS
    # my_class = lpj_guess_dataset.LpjGuessDataset(reason='lpj_guess_global').one_shot(skipping_handlers)

    # CTE-HR
    # my_class = cte_hr_dataset.CteHrDataset(reason='cte_hr').one_shot(skipping_handlers)

    # ONE-TIME
    # my_class = one_time_dataset.OneTimeDataset(reason='one_time').one_shot(skipping_handlers)
