# Before you run anything MAKE SURE to update the file settings.yml

# Guidelines for meta-data needed for upload.
*Most of the specifications of the meta-data below already exist in the file
constants.py, while others you may need to define in the [ICOS meta-data 
editor](https://meta.icos-cp.eu/edit/cpmeta/) (Login and permissions
required. [Click here](#icos-meta-data-editor) for more information regarding
new definitions in ICOS meta-data editor). In the latter case don't forget to
also update the file `constants.py` with the newly added specifications.*
1. Data object specification: The specification for each file is set in the 
`dataset.py` file using the `archive_files` method. It is usually predefined
from previous versions (grab from `constants.py`). If the selection of data
object specification is unclear, here are some general rules:
   1. The up-scaling of measurements from local point measurements to gridded
   maps using machine learning seems well described by modeling.
2. Previous version: Is there a previous version?
3. Keywords: Keywords may be mentioned in the dataset's attributes or manually
coded.  
We made a commitment to try and use the keywords from a standard vocabulary
from NASA called [GCMD keywords](
https://gcmd.earthdata.nasa.gov/KeywordViewer/). Quite often they don't have
what we need, and in that case it's ok to invent our own, but then we should
try and re-use the same keywords as much as we can instead of inventing unique
ones for every data product we create. Basically, the recommendation is to
first check if there is something close to what you want already. And in any
case, please refrain from using long phrases as keywords, they are very likely
to be one-offs, and thus not very useful.  
4. License: License until now has always been `constants.ICOS_LICENSE` (grab 
from `constants.py`).
5. Description: Description may be mentioned in the dataset's attributes but
may also be a combination of what exists in the dataset's attributes and other
complementary parts.
6. Contributors: Contributors will most likely be mentioned in the dataset's
attributes (grab from `constants.py`).
7. Creation date: Creation date is usually mentioned in the dataset's
attributes, but may also be manually coded.
8. Creator: Creator may be mentioned directly in the dataset's attributes, or
mentioned in the list of contributors, or manually coded (grab from 
`constants.py`). 
9. Host organization: Host organization until now has always been 
`constants.CARBON_PORTAL` (grab from `constants.py`).
10. Sources: Sources is usually an empty list (need more information about
this).
11. Spatial resolution: Spatial resolution may be mentioned directly in the
dataset's attributes (as global, or europe, or e.t.c), or may be extracted
from the NetCDF's coordinates using `min` and `max` values for latitude and
longitude. In the first case grab it from `constants.py`.
12. Temporal resolution (interval): Interval (start and stop) is usually
extracted from the dataset's coordinates using `min` and `max` values for
time.
13. Temporal resolution (resolution): Resolution may be mentioned directly
in the dataset's attributes, or may be logically extracted by having a look at
the data intervals within the dataset.
14. Title: Title may be mentioned in the dataset's attributes but may also be
a combination of what exists in the dataset's attributes and other
complementary parts.
15. Variables: Variables should be extracted using `dataset.variables` also we
need to make sure that these variables (those which are going to be 
preview-able) exist in the ICOS meta-data editor (try-ingest and data upload
will not work otherwise). Variables are essential for the try-ingest and 
upload processes. If the variables are not defined in the ICOS meta-data
editor, then both of these steps will fail.
16. Submitter ID: Submitter ID is always hard-coded as `CP`.

# ICOS meta-data editor
### Adding a new person
1. First go to the Membership entry and click the plus icon on the other side
of the entries column.
2. The id of the new instance should follow this format:
[institute's abbreviation]\_[person's role]\_[person's surname]. Here's an 
example id: LU_Researcher_Zogopoulos.
3. Fill in the organization of the person using the autocomplete features of
the interface.
4. Fill in the role of the person using the autocomplete features of the 
interface.
5. Fill in the label following this format: 
[person's surname] as [person's role] at [Institute's abbreviation]. Here's an
example: Zogopoulos as Researcher at LU


# This project and the ICOS upload gui/staging upload gui.
Always keep in mind that this project and the services
[upload gui](https://meta.icos-cp.eu/uploadgui/) and
[staging upload gui](https://metastaging.icos-cp.eu/uploadgui/), essentially, 
do the same thing; upload meta-data and data to the ICOS Carbon Portal. This
project is used as a more scripted-like approach of the aforementioned
services.

# Tools
- To download a directory from an ftp server use: 
`wget -r ftp://user:pass@server.com/` You can try using "anonymous" in place of 
"user" if no other option is provided.

<hr>
Credits to: Oleg Mirzov, Ute Karstens, Margareta Hellström 