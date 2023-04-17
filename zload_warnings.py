import warnings
warnings.filterwarnings('always',
                        category=Warning,
                        module='remote_sensing_dataset')


def warn_for_present_file(path: str = None) -> None:
    warning = (
        f'\t\nFile {path} already present.\n'
        f'\tThis warning is positioned here because this file is supposed\n'
        f'\tto have different content for different runs of this kind of'
        f' upload.\n '
        f'\tProceed with caution.'
    )
    warnings.warn(warning, category=UserWarning)
    return
