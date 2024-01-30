from json_manager import make_yearly_cte_hr_collection
from json_manager import make_full_cte_hr_collection
from json_manager import make_monthly_cte_hr_collection
import exiter
from portal_interactor import get_cte_hr_collections
from portal_interactor import get_meta
from portal_interactor import get_collection_members
from portal_interactor import sort_members
from portal_interactor import upload_collection
from pathlib import Path
from pprint import pprint
from settings import Settings
from json_manager import read_json, write_json
import utils
from constants.icons import ICON_CHECK
from constants.endpoints import CP_COLLECTIONS
from icoscp_core import icos


# Todo: DO NOT DELETE THIS CODE BLOCK, it is meant to be integrated
#  somehow.
#  1. Make the necessary changes to the script below to upload the
#     new monthly 5-component collection.
#  2. Use the uploadgui afterwards to manually upload the new yearly
#     version. For example deprecate the yearly 2023 collection that
#     includes months 01-09 with a yearly 2023 collection that
#     includes months 01-10.
#  3. Use the uploadgui to deprecate the full collection found here
#     https://doi.org/10.18160/20Z1-AYJ2 by replacing the newly
#     uploaded yearly collection (2023 (01-10)) or by appending the
#     newly uploaded yearly collection (2024 (01)). Don't forget to
#     fill in the preexisting doi field.
#  4. Update the target url of the doi to the newly uploaded full
#     collection.
# settings = Settings().settings
# file_path = Path(settings.json_collection_standalone_files, f"202310.json")
# json_content = make_monthly_cte_hr_collection(collection=dict({
#     "key": "202310",
#     "members": [
#         "https://meta.icos-cp.eu/objects/y5EWtWgVT69nN6iyx4c3su7s",
#         "https://meta.icos-cp.eu/objects/ILelQyE6H9OKdqdOHAAito2C",
#         "https://meta.icos-cp.eu/objects/ow_JOQLEGh4NK7fqwx--ULV7",
#         "https://meta.icos-cp.eu/objects/iODrZIgv5bBibTlaE2rfdiRR",
#         "https://meta.icos-cp.eu/objects/4AJm1DYzQ23DPD-JbNZt08rM"
#     ],
#     # "isNextVersionOf": "mplampla"
# }))
# write_json(path=str(file_path.resolve()), content=json_content)
# upload_collection(json_file=str(file_path.resolve()))
# Todo: Ends here.

# Todo: DO NOT DELETE THIS CODE BLOCK, it is meant to be integrated
#  somehow. It was used to upload the new yearly collection versions
#  AFTER THE STANDALONE COMPONENTS AND MONTHLY COLLECTIONS HAVE BEEN
#  DEPRECATED. It was used when we deprecated a bunch of the
#  anthropogenic files, so it needs some tweaking to get it to work
#  correctly. PPRINT BEFORE UPLOADING ANYTHING.
# settings = Settings().settings
# collections = get_cte_hr_collections(interval="yearly")
# for yearly_collection_key, landing_page in collections.items():
#     meta = get_meta(landing_page=landing_page)
#     new_members = list()
#     for member in meta["members"]:
#         if member["res"] == member["latestVersion"]:
#             print(
#                 f"For {yearly_collection_key} collection, {member['res']} "
#                 f"does not have a new version. Appending the current one..."
#             )
#             new_members.append(member["res"])
#         else:
#             new_members.append(member["latestVersion"])
#     new_collection = dict({
#         "key": yearly_collection_key,
#         "members": new_members,
#         "isNextVersionOf": meta["res"].split("/")[-1]
#     })
#     file_path = Path(settings.json_collection_standalone_files,
#                      f"{yearly_collection_key}.json")
#     json_content = make_yearly_cte_hr_collection(collection=new_collection)
#     write_json(path=str(file_path.resolve()),
#                content=json_content)
#      upload_collection(json_file=file_path)
# Todo: Ends here.
