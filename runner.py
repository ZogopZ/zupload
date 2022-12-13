import sys
import tools
import stilt_timeseries_dataset
import lpj_guess_dataset
import cte_hr_dataset

if __name__ == '__main__':
    if len(sys.argv) == 2:
        skipping_handlers = tools.parse_arguments(sys.argv[1])
    else:
        static_mode = '000000'
        skipping_handlers = tools.parse_arguments(static_mode)
    # my_class = cte_hr_dataset.CteHrDataset(reason='cte_hr').one_shot(skipping_handlers)
    # my_class = lpj_guess_dataset.LpjGuessDataset(reason='lpj_guess_global').one_shot(skipping_handlers)
    my_class = stilt_timeseries_dataset.StiltTimeseriesDataset(reason='stilt_timeseries').one_shot(skipping_handlers)
