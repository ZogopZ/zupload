# Format read from https://peps.python.org/pep-0008/#imports.
# Standard library imports.
from getpass import getpass
import collections
import hashlib
import json
import os
import pandas
import pickle
import re


# Related third party imports.
import requests
from icoscp.sparql.runsparql import RunSparql

# Local application/library specific imports.
import src.constants as constants
import src.exiter as exiter
import src.tools as tools


def read_json(path: str = None, json_data: str = None):
    """
    Read dictionary from json file.

    Can also be used to check for valid json either from file or from
    another object like a response from a request.
    """
    if path:
        with open(file=path, mode='r') as json_handle:
            json_data = json.load(json_handle)
    else:
        json_data = json.loads(json_data)
    return json_data


def write_json(path=None, content=None):
    """Write dictionary to json file"""
    with open(file=path, mode='w+') as json_handle:
        json.dump(content, json_handle, indent=4)
    return


def regenerate_full_archive(components_dir=None):
    """Combine the archives of each individual component"""
    full_archive = dict()
    for file in os.listdir(path=components_dir):
        json_path = os.path.join(components_dir, file)
        component_key = file.split('_', maxsplit=3)[-1].split('.')[0]
        full_archive[component_key] = read_json(path=json_path)
    write_json(path='full_archive.json', content=full_archive)
    return


def check_permissions():
    valid_cookie = False
    print(f'- Authenticating user.')
    while not valid_cookie:
        if os.path.exists(constants.COOKIES) and validate_cookie():
            valid_cookie = True
        elif os.path.exists(constants.COOKIES) and not validate_cookie():
            user_input = input_handler(operation='authentication')
            if user_input == 'r':
                valid_cookie = True if curl_cookie() else False
            elif user_input == 'e':
                exiter.exit_zupload(exit_type='authentication')
        else:
            valid_cookie = curl_cookie()
    return


def curl_cookie():
    validation = False
    url = 'https://cpauth.icos-cp.eu/password/login'
    data = {'mail': tools.input_handler(operation='username'),
            'password': tools.input_handler(operation='password')}
    cookie_response = requests.post(url=url, data=data)
    if cookie_response.status_code == 200:
        validation = True
        save_cookie(cookie_response.cookies)
        validate_cookie()
    else:
        print('\tSomething went wrong. Please try again...')
    return validation


def validate_cookie():
    """Validate existing cookie."""
    validation = False
    cookie_validation_response = requests.get(constants.WHO_AM_I,
                                              cookies=load_cookie())
    if cookie_validation_response.status_code == 200:
        validation = True
        print(f'\t{constants.ICON_ARROW_DOWN_RIGHT} Hello '
              f'{cookie_validation_response.json()["email"]}!')
    return validation


def save_cookie(cookie_jar: requests.cookies.RequestsCookieJar = None):
    with open(constants.COOKIES, 'wb') as cookie_handle:
        pickle.dump(cookie_jar, cookie_handle)


def load_cookie():
    with open(constants.COOKIES, 'rb') as cookie_handle:
        return pickle.load(cookie_handle)


def parse_arguments(mode: str = None) -> dict:
    skipping_handlers = collections.OrderedDict({
        'archive_files': True,
        'fill_handlers': True,
        'try_ingest': True,
        'archive_json': True,
        'upload_metadata': True,
        'upload_data': True,
        'store_current_archive': True
    })
    print(f'- Mode parsing ({mode}).')
    for handler, handler_items in zip(mode, skipping_handlers.items()):
        boolean_handler = bool(int(handler))
        print(
            f'\t{" ".join(handler_items[0].split("_")):21} =  '
            f'{boolean_handler}')
        skipping_handlers[handler_items[0]] = bool(int(handler))
    return skipping_handlers


def find_files(search_string: str = None) -> list:
    split_search_string = search_string.rsplit('/', 1)
    directory = split_search_string[0]
    regex = split_search_string[1]
    pattern = re.compile(f'{regex}')
    found_files = list()
    for file_name in os.listdir(directory):
        if pattern.match(file_name):
            found_files.append(os.path.join(directory, file_name))
    return sorted(found_files)


def request_rest_countries():
    countries = requests.get('https://restcountries.com/v3.1/all')
    with open(file='delete_1', mode='w') as json_file:
        json.dump(countries.json(), json_file)
    return


# Todo: Maybe rename this function.
def get_request(url: str = None):
    """Send and handle a get request."""
    response = None
    try:
        response = requests.get(url=url)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        print(e)
    else:
        if response.status_code == 200:
            pass
    return response


def pass_me():
    pass
    return


def download_collections() -> pandas.DataFrame:
    """SPARQL query for all collections."""
    sparql_query = (
        '''
            prefix cpmeta: <http://meta.icos-cp.eu/ontologies/cpmeta/>
            prefix dcterms: <http://purl.org/dc/terms/>
            select ?coll ?title where{
                ?coll a cpmeta:Collection .
                OPTIONAL{?coll cpmeta:hasDoi ?doi}
                ?coll dcterms:title ?title .
                FILTER NOT EXISTS {[] cpmeta:isNextVersionOf ?coll}
                OPTIONAL{?coll cpmeta:hasCitationString ?citation}
                OPTIONAL{?doc cpmeta:hasBiblioInfo ?bibinfo}
                FILTER(STRSTARTS(str(?coll), "https://meta.icos-cp.eu/"))
            }
            order by ?title
        '''
    )
    return RunSparql(sparql_query=sparql_query, output_format='pandas').run()


def get_size(path: str = None, units='bytes') -> float:
    """
    Returns the size of the given path in specified units.

    By-default it will return the output of os.path.getsize(), namely
    a number of bytes for the given path.
    """
    size = None
    b_size = os.path.getsize(path)
    if units == 'bytes':
        size = b_size
    elif units == 'kilobytes':
        size = b_size / (2**10)
    elif units == 'megabytes':
        size = b_size / (2**20)
    elif units == 'gigabytes':
        size = b_size / (2**30)
    return size


def zip_files(files: list = None, p_output_file: str = None):
    """
    Zip incoming file list.

    It is recommended to not use any compression level or type since it
    might slow down accessing zip archives on the data portal (Oleg's
    suggestion).
    Outlaws acting on their own volition may use:
      compress_type=ZIP_DEFLATED
        and
      compresslevel=Z_BEST_COMPRESSION,
    in the zip_file.write() function to produce smaller size zips.
    """
    files = sorted(files)
    with ZipFile(file=p_output_file, mode='w') as zip_file:
        total = len(files)
        for index, file in enumerate(files):
            archive_name = file.split('/')[-1]
            zip_file.write(filename=file,
                           arcname=archive_name)
            # Pirates use this instead.
            # zip_file.write(filename=file,
            #                arcname=archive_name,
            #                compress_type=ZIP_DEFLATED,
            #                compresslevel=Z_BEST_COMPRESSION)
            progress_bar(operation='zip_files',
                         current=index + 1,
                         total=total,
                         additional_info=({
                             'target_zip': p_output_file.split('/')[-1],
                             'source_file': archive_name
                         }))
    return


def get_hash_sum(file_path: str = None, progress: bool = True) -> str:
    """Calculate and return hash-sum of given file."""
    sha256_hash = hashlib.sha256()
    with open(file=file_path, mode='rb') as file_handle:
        total = int(os.stat(file_path).st_size)
        current = int()
        # Read and update hash string value in blocks of 4K
        for byte_block in iter(lambda: file_handle.read(4096), b""):
            sha256_hash.update(byte_block)
            current += len(byte_block)
            # Printing out the progress bar while calculating a
            # hash-sum of a big file is a strenuous task; thus limit
            # the output using multiples of 4096 and when all bytes
            # are read.
            if (current % 65535 == 0 or current == total) and progress:
                progress_bar(
                    operation='calculate_hash_sum', current=current,
                    total=total,
                    additional_info=dict({
                        'source_file': file_path.split('/')[-1]
                    }))
    return sha256_hash.hexdigest()


def get_specification(file_name: str = None) -> tuple:
    dataset_type = None
    dataset_object_spec = None
    if 'persector' in file_name:
        dataset_type = 'anthropogenic emissions per sector'
        dataset_object_spec = \
            constants.OBJECT_SPECS['anthropogenic_emission_model_results']
    elif 'anthropogenic' in file_name:
        dataset_type = 'anthropogenic emissions'
        dataset_object_spec = \
            constants.OBJECT_SPECS['anthropogenic_emission_model_results']
    elif 'nep' in file_name:
        dataset_type = 'biospheric fluxes'
        dataset_object_spec = \
            constants.OBJECT_SPECS['biospheric_model_results']
    elif 'fire' in file_name:
        dataset_type = 'fire emissions'
        dataset_object_spec = \
            constants.OBJECT_SPECS['file_emission_model_results']
    elif 'ocean' in file_name:
        dataset_type = 'ocean fluxes'
        dataset_object_spec = \
            constants.OBJECT_SPECS['oceanic_flux_model_results']
    elif any(part in file_name for part in ['CSR', 'LUMIA', 'Priors', 'GCP']):
        dataset_type = 'inversion modeling spatial'
        dataset_object_spec = \
            constants.OBJECT_SPECS['inversion_modeling_spatial']
    elif 'zip' in file_name:
        dataset_type = 'model data archive'
        dataset_object_spec = \
            constants.OBJECT_SPECS['model_data_archive']
    elif 'VPRM' in file_name:
        dataset_type = 'biosphere modeling spatial'
        dataset_object_spec = \
            constants.OBJECT_SPECS['biosphere_modeling_spatial']
    return dataset_type, dataset_object_spec


def obtain_rest_countries():
    """Read or download rest countries information."""
    print(f'- Obtaining rest countries information...')
    rest_countries = None
    continent_possession = {
        'Africa': 'African',
        'Americas': 'American',
        'Antarctic': 'Antarctic',
        'Asia': 'Asian',
        'Eurasia': 'Eurasian',
        'Europe': 'European',
        'Oceania': 'Oceanian'
    }
    try:
        rest_countries = tools.read_json(path=constants.P_REST_COUNTRIES)
    except FileNotFoundError as e:
        pass
    finally:
        if rest_countries:
            print(f'\tContent read from {constants.P_REST_COUNTRIES}... '
                  f'{constants.ICON_CHECK}')
        # Rest countries file exists but it doesn't contain any
        # json data.
        else:
            download_boxes = False
            if input_handler(operation='download_rest_countries') == 'Y':
                download_boxes = True
            downloaded_rest_countries = \
                tools.get_request(url=constants.REST_COUNTRIES).json()
            rest_countries = dict()
            total = len(downloaded_rest_countries)
            for index, country in enumerate(downloaded_rest_countries):
                country_code = country['cca2']
                if country_code == 'GB':
                    country_code = 'UK'
                country_name = country['name']['common']
                country_continent = country['region']
                country_name_nominatim = None
                # Svalbard and Jan is under the full sovereignty of
                # Norway.
                if country_code == 'SJ':
                    country_name_nominatim = 'Norway'
                #
                elif country_code == 'GF':
                    country_name_nominatim = 'France'
                elif country_code == 'RU':
                    country_name_nominatim = country_name
                    country_continent = 'Eurasia'
                else:
                    country_name_nominatim = country_name
                bounding_box = []
                if download_boxes:
                    # Request country's bounding box from nominatim.
                    # This can be empty.
                    box_url = (
                        'https://nominatim.openstreetmap.org/search?'
                        f'country={country_name_nominatim}'
                        f'&format=json&polygon=0'
                    )
                    country_coordinates = tools.get_request(box_url).json()
                    bounding_box = country_coordinates[0]['boundingbox'] \
                        if country_coordinates else []
                # Fill in the countries' dictionary with country name,
                # country continent, country's bounding box (if
                # available.) The dictionary will have as keys the
                # cca2 countries' codes.
                rest_countries.setdefault(
                    country_code,
                    {
                        'name': country_name,
                        'continent': country_continent,
                        'continent_possession':
                            continent_possession[country_continent],
                        'bounding_box_from': country_name_nominatim,
                        'min_lat':
                            float(bounding_box[0]) if bounding_box
                            else None,
                        'max_lat':
                            float(bounding_box[1]) if bounding_box
                            else None,
                        'min_lon':
                            float(bounding_box[2]) if bounding_box
                            else None,
                        'max_lon':
                            float(bounding_box[3]) if bounding_box
                            else None
                    })
                progress_bar(operation='download_rest_countries',
                             current=index+1,
                             total=total)
            tools.write_json(path=constants.P_REST_COUNTRIES,
                             content=rest_countries)
            print(f'{constants.ICON_CHECK}', flush=True)
    return rest_countries


def progress_bar(operation: str = None, current: int = None,
                 total: int = None, bar_length: int = 20,
                 additional_info: dict = None):
    """
    Outputs a loading-like bar for various operations.

    Credits to: https://stackoverflow.com/a/37630397
    """
    prepender = str()
    if operation == 'archive_system_info':
        prepender = (
            f'\tArchiving {additional_info["file_name"]}'
        )
    elif operation == 'download_rest_countries':
        prepender = (
            f'\tDownloading rest countries from '
            f'{constants.REST_COUNTRIES}:'
        )
    elif operation == 'zip_files':
        prepender = (
            f'\tZipping  |{additional_info["source_file"]}|  to  '
            f'|{additional_info["target_zip"]}|'
        )
    elif operation == 'calculate_hash_sum':
        prepender = (
            f'\tCalculating hash sum of {additional_info["source_file"]}'
        )
    elif operation == 'archive_meta_data':
        prepender = (
            f'\tArchiving meta-data for file {additional_info["file_name"]}'
        )
    elif operation == 'try_ingest':
        prepender = (
            f'\tTried ingestion of file: {additional_info["file_name"]}'
        )
    elif operation == 'upload_meta_data':
        prepender = (
            f'\tUploaded meta-data for file: {additional_info["file_name"]}'
        )
    elif operation == 'upload_data':
        prepender = (
            f'\tUploaded data for file: {additional_info["file_name"]}'
        )
    elif operation == 'chunk':
        prepender = (
            f'\tUploading data: '
        )
    fraction = current / total
    arrow = int(fraction * bar_length - 1) * '-' + '>'
    padding = int(bar_length - len(arrow)) * ' '
    if current == total:
        prepender = '\tCompletion'
        ending = f' {constants.ICON_CHECK}\n'
    else:
        ending = '\r'
    progress = str(
        f'{prepender} [{arrow}{padding}] {int(fraction*100)}%'
    )
    print(200*' ', end='\r', flush=True)
    print(progress, end=ending, flush=True)
    return


def input_handler(operation: str = None, additional_info: dict = None) -> str:
    input_prepender = str()
    if operation == 'download_rest_countries':
        input_prepender = (
            '\tWe download bounding boxes from nominatim. According to\n'
            '\tnominatim\'s usage policy there is an absolute maximum of 1\n'
            '\trequest per second; Thus this operation will take about 3-4\n'
            '\tto complete.\n'
            '\tWould you like to also download bounding boxes? (Y,n): '
        )
    elif operation == 'try_ingest':
        input_prepender = '\tPlease select number of subprocesses: '
    elif operation == 'store_current_archive':
        input_prepender = (
            '\tBe careful!!! You are trying to overwrite an already existing '
            f'{additional_info["archive"]}\n'
            f'\tAre you sure you want to continue? (Y/n): '
        )
    elif operation == 'authentication':
        input_prepender = (
            f'\tFile {constants.COOKIES} exists in the current working '
            f'directory but it is outdated:\n'
            f'\t- Continue with current cookie {constants.ICON_ARROW} c\n'
            f'\t- Regenerate cookie {constants.ICON_ARROW} r\n'
            f'\tPlease enter a selection (c/r/e to exit): ')
    elif operation == 'picker':
        input_prepender = (
            '\tPlease type year and month for your dataset (e.g.: 202312)'
            '.\n\tThese values might end up in the meta-data title.'
            ' (e to exit): ')
    elif operation == 'username':
        input_prepender = '\tPlease enter your e-mail: '
    elif operation == 'password':
        input_prepender = '\tPlease enter your password: '
    user_input = input(input_prepender) \
        if operation != 'password' else getpass(prompt=input_prepender)
    return user_input
