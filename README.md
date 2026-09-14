# zupload

## What is zupload?
`zupload` is a command-line tool for uploading scientific datasets and their
metadata to ICOS / ENVRI data portals in a structured and reproducible way.  

It uses a **spreadsheet-driven approach** to describe datasets, their metadata,
and the target portal. The tool then validates, prepares, and uploads both 
metadata and data files using the appropriate service APIs.

`zupload` is designed for:
- batch uploads of many files,
- preparing metadata in a way that is easy to review and reproduce,
- scripted or semi-automated ingestion workflows.

`zupload` is implemented as a set of **Typer-based CLI
commands** and uses an Excel (`.xlsx`) file as its primary input.

## How it works

At a high level, `zupload` turns a spreadsheet into a series of upload actions.

1. **You prepare an Excel file**  
   The spreadsheet describes your datasets: where the files are located, which
   metadata belongs to each file, and which ICOS / ENVRI portal to use.

2. **`zupload` reads the input**  
   The tool loads the spreadsheet and prepares the metadata in the format
   expected by the target services. The upload flow itself does not check the
   input, so a missing field surfaces as a raw error; you can check the input
   up front with the `validate` command (described later).

3. **Metadata is converted to JSON**  
   For each data file, `zupload` builds a metadata JSON payload based on the
   spreadsheet contents. This step can also be run on its own, without 
   uploading anything.

4. **Metadata is uploaded first**  
   The metadata JSON is sent to the portal's metadata service. If this
   succeeds, the service returns an upload URL for the actual data file.

5. **Data files are uploaded**  
   The data files are uploaded directly to the returned URL, completing the
   ingestion process.

This separation between metadata and data uploads makes it easier to validate,
debug, and reproduce uploads, especially when working with many files.

## Installation

`zupload` is a Python-based command-line tool.

Clone the repository and install it into a virtual environment of your choice:

```bash
git clone https://github.com/ICOS-Carbon-Portal/zupload.git
cd zupload
python -m venv .venv
source .venv/bin/activate
pip install .
```

This will install `zupload` and its dependencies and make the CLI commands
available in your environment.

**Note:** zupload is currently intended to be used from a Python environment
rather than as a standalone binary.

## Authentication

`zupload` relies on the standard ICOS authentication flow provided by the
`icoscp_core` library. This means you don't have to worry about manually
handling tokens: as long as you're logged in with ICOS credentials, the 
library will handle retrieving and attaching the necessary authentication to
every request.

To authenticate, follow the instructions in the `icoscp_core` documentation:
https://icos-carbon-portal.github.io/pylib/icoscp/authentication/

If authentication is missing or invalid, upload requests will fail with an
authorization error.

## Usage

`zupload` is used from the command line and operates on an Excel (`.xlsx`)
spreadsheet that describes the datasets to upload.

After activating your Python virtual environment and installing the library,
the `zupload` commands are available directly on the command line.

At a minimum, you point `zupload` to a spreadsheet file and run the upload
command. If no file is provided, `zupload` uses the single `.xlsx` file in the
current directory when there is exactly one; if there are several `.xlsx` files
it stops with an error and asks you to specify which one.

Typical usage looks like this:

```bash
zupload
```
or

```bash
zupload /path/to/spreadsheet.xlsx
```

This will:
- read the spreadsheet,
- prepare metadata for each listed data file,
- upload the metadata to the target portal,
- upload the corresponding data files.

Use `--metadata-only` to upload the metadata but skip uploading the actual data
file.

```bash
zupload /path/to/spreadsheet.xlsx --metadata-only
```

Use `--rows` to restrict the run to specific rows of the `upload_meta` sheet, by
that sheet's row number. `--rows 5` runs a single row, and `--rows 5-12` runs a
contiguous, inclusive range. This works on both the default upload command and
the `validate` command.

```bash
zupload /path/to/spreadsheet.xlsx --rows 5-12
```

By default `zupload` uploads to the portal's production environment. Use
`--staging` to send the metadata to the portal's staging environment instead,
which is useful for testing. Because production uploads are hard to undo, the
tool asks you to confirm before a production upload; pass `--yes` to skip that
confirmation, for example in scripts. Staging must already be set up for your
submitter on the portal side, otherwise the upload is rejected.

```bash
zupload /path/to/spreadsheet.xlsx --staging
```

Additional commands are available for preparing metadata without uploading
data, and for validating an upload before you run it.

When you want to prepare metadata without uploading, there are two options.
`--no-upload` does a dry run: it builds the metadata and prints it, but uploads
nothing and writes no files. `--extract-json` instead writes each row's
metadata JSON next to its data file.

```bash
zupload /path/to/spreadsheet.xlsx --extract-json
```

The `validate` command
inspects the spreadsheet rows and reports any problems without uploading
anything or changing the spreadsheet. It separates findings into errors
(clearly wrong input, such as a missing required field) and warnings (things
that look suspicious but may be fine). It checks the metadata only, so it works
even when the data files are not present locally. It also reports, for each
row, whether it can find the data file at its `fileLocation`; when a file
cannot be found it tells you how to rerun with `--data-dir` to locate the
files and fill in the missing details.

Unlike the upload command, `validate` takes the spreadsheet as the
`--spreadsheet` option rather than as a positional argument.

```bash
zupload validate --spreadsheet /path/to/spreadsheet.xlsx
```

If the data files are available locally but the spreadsheet is missing their
`hashSum` or `fileLocation`, point `validate` at the folder that contains them
with `--data-dir`. It finds each file by name (searching subfolders as well)
and fills in `fileLocation` and `hashSum` directly in the spreadsheet. If it
finds no matching files under that folder, it leaves the spreadsheet unchanged.

```bash
zupload validate --spreadsheet /path/to/spreadsheet.xlsx --data-dir /path/to/data
```

The `fetch` command retrieves the existing metadata for an object from the
portal and prints it. It is a read-only lookup and uploads nothing. You can pass
a PID, a hash, or a landing-page URL.

```bash
zupload fetch <pid|hash|landing-url>
```

The `generate` command scaffolds a new upload spreadsheet from a directory of
data files. It is currently specialized for ICOS Cities footprint NetCDF files,
so most users preparing a normal upload should start from an existing working
spreadsheet instead.

```bash
zupload generate /path/to/directory
```

## Harvesting metadata from the portal (`harvest`)

`harvest` does the reverse of `zupload`: it retrieves metadata from the ICOS or Cities portal and writes it to a `zupload` spreadsheet. It does not upload anything.

`harvest` is installed as a separate command rather than as a `zupload` subcommand.

It can either:

* create a new spreadsheet from one or more landing page URIs, or
* fill metadata into an existing spreadsheet containing `landingPageURI` values.

### Create a spreadsheet from landing pages

Use `--landing-page` to create a new spreadsheet directly from portal objects:

```bash
harvest --landing-page https://meta.icos-cp.eu/objects/6TNdmGyjojb8iLTQ3acDs3-F
harvest --landing-page <uri-a> --landing-page <uri-b> --output my_sheet.xlsx
```

The flag can be repeated to harvest several objects. Unless `--output` is given, the result is written to `harvest.xlsx`.

The generated workbook contains the sheets and metadata columns expected by `zupload`. Values that cannot be obtained from the portal are left blank.

In particular, `fileLocation` must be supplied locally before uploading. If the files are available on disk, it can be filled automatically with:

```bash
zupload validate --spreadsheet <sheet> --data-dir <dir>
```

The portal is inferred from the landing page URI. All objects in one generated spreadsheet must belong to the same portal.

For uploads, keeping different dataset types in separate spreadsheets is recommended.

### Fill an existing spreadsheet

`harvest` can also populate an existing `.xlsx` file whose `upload_meta` sheet already contains `landingPageURI` values:

```bash
harvest
harvest /path/to/spreadsheet.xlsx
harvest sheet.xlsx --rows 5
harvest sheet.xlsx --rows 5-12
harvest sheet.xlsx --dry-run
harvest sheet.xlsx --overwrite
```

By default, `harvest` fills blank cells only. Existing values that differ from the portal are kept and reported. Use `--overwrite` if you want portal values to replace them.

`--dry-run` reports what would change without modifying the workbook.

As with `zupload`, omitting the spreadsheet path uses the single `.xlsx` file in the current directory.

### Before uploading a harvested spreadsheet

A harvested spreadsheet can be reviewed, edited, and uploaded again with `zupload`.

```bash
zupload validate --spreadsheet /path/to/spreadsheet.xlsx
zupload /path/to/spreadsheet.xlsx
```

Keep in mind:

* `fileLocation` is never harvested because it refers to a local directory.
* Uploading a harvested row updates the metadata of the existing object; it does not create a new version.
* `isNextVersionOf` is copied from the portal and should normally be left unchanged.
* Staging may reject references that are valid in production if the staging metadata store is not fully synchronized.

`harvest` supports both the ICOS and Cities portals and writes run logs under `./logs/harvest-<timestamp>/`.

## Input spreadsheet

`zupload` uses an Excel (`.xlsx`) spreadsheet as its main input. The 
spreadsheet describes which files should be uploaded, which metadata belongs to
each file, and which portal the upload targets.

Each upload workflow may require a slightly different spreadsheet layout,
depending on the type of data and the target service. For this reason, the
spreadsheet format is intentionally not fully fixed or documented in detail
here.

Example spreadsheets are not yet included with the project. Until they are, base
your new spreadsheet on an existing working spreadsheet, and use its
`instructions` sheet (described below) as your guide to the required sheets,
columns, and value formats.

Spreadsheets also contain an **`instructions` sheet** with additional guidance
and explanations for the different fields. This sheet is meant for human
readers only and is ignored by `zupload` when processing the file.

When preparing a spreadsheet:
- each row typically corresponds to a single data file,
- file paths must be accessible from the machine running `zupload`,
- some fields may contain JSON-formatted values (for example lists of variables
  or keywords).

Basing your spreadsheet on an existing working spreadsheet, and following its
`instructions` sheet, is the recommended way to ensure your spreadsheet matches
what `zupload` expects.

## Things to be aware of

- `zupload` expects the spreadsheet structure and column names to match what
  the tool reads internally. Basing your spreadsheet on an existing working
  spreadsheet, and following its `instructions` sheet, is strongly recommended.
- Some spreadsheet fields are expected to contain valid JSON (for example lists
  of variables or keywords). Make sure these values use proper JSON syntax.
- A spreadsheet produced by `harvest` still needs `fileLocation`, which is
  never harvested because it is a local path rather than portal metadata. Run
  `zupload validate --spreadsheet <sheet>` on a harvested sheet before
  uploading it.
- Uploading a harvested sheet updates the metadata of the object it was
  harvested from; it does not create a new version of it.
- Metadata is uploaded before data files. If metadata upload fails, the data
  file will not be uploaded.
- Data files must be accessible from the machine running `zupload` at the paths
  specified in the spreadsheet.
- Authentication must be set up before running `zupload`. Missing or expired
  credentials will cause uploads to fail.

## Credits

`zupload` was developed within the ICOS / Carbon Portal ecosystem.

Contributors:
- Jonathan Schenk  
- Jonathan Thiry  
- Maggie Hellström  
- Oleg Mirzov  
- Ute Karstens  
- Claude
