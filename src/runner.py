from file_manager import FileManager
from json_manager import JsonManager, get_previous_version
from portal_interactor import PortalInteractor
from settings import Settings

settings = Settings().settings
file_manager = FileManager(settings)
json_manager = JsonManager(settings)
portal_interactor = PortalInteractor(settings)

file_manager.archive_files() if settings.archive_files \
    else print(f"- Skipping archiving of files.")
portal_interactor.try_ingest() if settings.try_ingest \
    else print(f"- Skipping try ingestion of files.")
json_manager.archive_json() if settings.archive_json \
    else print(f"- Skipping archiving of json.")
portal_interactor.upload_metadata() if settings.upload_meta_data \
    else print(f"- Skipping uploading of meta-data.")
portal_interactor.upload_data() if settings.upload_data \
    else print(f"- Skipping uploading of data.")
json_manager.show_uploads()


# coll_1 = "https://meta.icos-cp.eu/collections/R4FivCqCR62RruN3mvh5dG2b"
# coll_2 = "https://meta.icos-cp.eu/collections/Svr5_VyfI9qDyiTQsxuLb_Jc"
# new_members = list(
#     [member["res"] for member in get_meta(landing_page=coll_2)["members"]]
# )
# meta = get_meta(landing_page=coll_1)
# for member in meta["members"]:
#     if member["res"] in new_members:
#         print(f"Member: {member['res']} already in members...")
#     else:
#         print(f"Who is this guy: {member['res']}?")
# settings = Settings().settings
# full_collection = get_cte_hr_collections(interval="full")
# for full_collection_key, landing_page in full_collection.items():
#     meta = get_meta(landing_page=landing_page)
#     new_members = list()
#     for member in meta["members"]:
#         if member["res"] == member["latestVersion"]:
#             print(f"For {full_collection_key} collection, {member['res']}"
#                   f"does not have a new version. Appending the current one...")
#             new_members.append(member["res"])
#         else:
#             new_members.append(member["latestVersion"])
#     new_collection = dict({
#         "key": full_collection_key,
#         "members": new_members,
#         "isNextVersionOf": meta["res"].split("/")[-1]
#     })
#     file_path = Path(settings.json_collection_standalone_files,
#                      f"{full_collection_key}.json")
#     json_content = make_full_cte_hr_collection(collection=new_collection)
#     pprint(json_content)
#     write_json(path=str(file_path.resolve()),
#                content=json_content)
#     upload_collection(json_file=str(file_path.resolve()))





# collection_files = "/srv/git/zupload/src/cte-hr/json-collection-standalone-files-backup-1"
# collection_files = sorted(
#     [str(file.resolve()) for file in Path(collection_files).glob("*.json")]
# )
# for file in collection_files:
#     print(file)
#     upload_monthly_cte_hr_collection(json_file=file)
#     json = read_json(path=file)
#     members = json["members"]
#     for index, member in enumerate(members):
#         landing_page = f"https://meta.icos-cp.eu/objects/{member}"
#         json_url = f"{landing_page}/easter_egg.json"
#         response = utils.handle_request(request='get', args={"url": json_url})
#         members[index] = response.json()["accessUrl"].replace("data", "meta")
#     json["members"] = members
#     write_json(path=file, content=json)
#     print(ICON_CHECK, end="")












#             members[index] = response.json()["hash"]
#     json["members"] = members
#     write_json(path=file, content=json)
# archive = read_json(path=settings.archive_path)
# for key, collection in get_monthly_cte_hr_collections().items():
#     members = list()
#     for member in get_collection_members(landing_page=collection):
#         found_member = False
#         for base_key, base_info in archive.items():
#             if member[0:24] == base_info["json"]["isNextVersionOf"]:
#                 found_member = True
#                 members.append(base_info["file_data_url"].split("/")[-1])
#         if not found_member:
#             members.append(member)
#     new_collection = dict({
#         "key": key,
#         "members": members,
#         "isNextVersionOf": collection.split("/")[-1]
#     })
#     upload_monthly_cte_hr_collection(collection=new_collection)

