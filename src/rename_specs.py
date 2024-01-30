from dataclasses import dataclass


@dataclass(frozen=True)
class YamlSettings:
    reason: str
    archive_files: bool
    try_ingest: bool
    archive_json: bool
    upload_meta_data: bool
    upload_data: bool
    master_dir: str
    archive_path: str
    json_standalone_files: str
    json_collection_standalone_files: str
    data_dir: str
    pattern: str
    previous_collection: str | None
    try_ingest_subprocesses: int
    show_input_files: bool
    show_progress_archive_files: bool
    show_progress_try_ingest: bool
    show_progress_archive_json: bool
    show_progress_upload_meta_data: bool
    show_progress_upload_data: bool
    upload_to_production: bool
    overwrite_archive: bool
