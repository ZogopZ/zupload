# Standard library imports.
from pathlib import Path
# Related third party imports.
import yaml

# Local application/library specific imports.
from rename_specs import YamlSettings
from constants.general_settings import YAML_SETTINGS


class Settings:

    def __init__(self) -> None:
        self.settings = self.read_settings()
        self.init_files()
        return

    @staticmethod
    def read_settings() -> YamlSettings:
        with open(file=YAML_SETTINGS, mode="r") as yaml_handler:
            return YamlSettings(**yaml.safe_load(yaml_handler))

    def init_files(self) -> None:
        Path(self.settings.master_dir).mkdir(parents=True, exist_ok=True)
        Path(self.settings.archive_path).touch(exist_ok=True)
        Path(self.settings.json_standalone_files).mkdir(parents=True,
                                                        exist_ok=True)
        Path(self.settings.json_collection_standalone_files).\
            mkdir(parents=True, exist_ok=True)
