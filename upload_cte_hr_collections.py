import time

import pandas
import requests
import constants
from pprint import pprint
import src.tools as tools
import os
import subprocess
from _collections import OrderedDict
from icoscp.sparql.runsparql import RunSparql

monthly_collections = OrderedDict()


def extract_monthly_collections(archive=None):
    # Collect members from each component's monthly dataset.
    # Each new collection will contain 5 components per month.
    for monthly_key, monthly_component in archive.items():
        component, reconstructed_key = monthly_key.rsplit('.', maxsplit=1)

        monthly_collections.setdefault(reconstructed_key, dict({'members': dict(), 'sorted_members': list()}))
        monthly_collections[reconstructed_key]['members'].setdefault(component, monthly_component['file_metadata_url'])
    # Archive meta-data for each collection.
    current_monthly_collections = tools.read_json('monthly_collections.json')
    json_collection_files = 'input-files/json-standalone-collection-files'
    collections_to_upload = 0
    for collection_key, collection_info in monthly_collections.items():
        if collection_key not in current_monthly_collections.keys():
            collections_to_upload += 1
        year, month = collection_key[0:4], collection_key[4:6]
        sorted_members = list(collection_info['members'][key] for key in sorted(collection_info['members'].keys()))
        collection_info['sorted_members'] = sorted_members
        collection_info['json'] = {
            "description":
                f"Monthly collection of hourly CO2 fluxes for {year}-{month}, containing hourly "
                f"estimates of biospheric fluxes, anthropogenic emissions (total and per sector), "
                f"GFAS fire emissions and Jena CarboScope ocean fluxes, all re-gridded to match "
                f"the resolution of the biospheric fluxes.\n\nNet ecosystem productivity (gross "
                f"primary production minus respiration). Positive fluxes are emissions, negative "
                f"mean uptake. These fluxes are the result of the SiB4 (Version 4.2-COS, hash "
                f"1e29b25, https://doi.org/10.1029/2018MS001540) biosphere model, driven by ERA5 "
                f"reanalysis data at a 0.5x0.5 degree resolution. The NEP per plant functional "
                f"type are distributed according to the high resolution CORINE land-use map "
                f"(https://land.copernicus.eu/pan-european/corine-land-cover), and aggregated to "
                f"CTE-HR resolution.\n\n"
                f"Anthropogenic emissions include contributions from public power, industry, "
                f"households, ground transport, aviation, shipping, and calcination of cement. "
                f"Our product does not include carbonation of cement and human respiration. Public "
                f"power is based on ENTSO-E data (https://transparency.entsoe.eu/), Industry, "
                f"Ground transport, Aviation, and Shipping is based on Eurostat data "
                f"(https://ec.europa.eu/eurostat/databrowser/). Household emissions are based on a "
                f"degree-day model, driven by ERA5 reanalysis data. Spatial distributions of the "
                f"emissions are based on CAMS data (https://doi.org/10.5194/essd-14-491-2022). "
                f"Cement emissions are taken from GridFED V.2021.3 "
                f"(https://zenodo.org/record/5956612#.YoTmvZNBy9F).\n\n"
                f"GFAS fire emissions (https://doi.org/10.5194/acp-18-5359-2018), re-gridded to "
                f"match the resolution of the biosphere, fossil fuel, and ocean fluxes of the CTE-HR "
                f"product. Please always cite the original GFAS data when using this file, and use "
                f"the original data when only fire emissions are required. For more information, see "
                f"https://doi.org/10.5281/zenodo.6477331 Contains modified Copernicus Atmosphere "
                f"Monitoring Service Information [2020].\n\nOcean fluxes, based on a climatology of "
                f"Jena CarboScope fluxes (https://doi.org/10.17871/CarboScope-oc_v2020, "
                f"https://doi.org/10.5194/os-9-193-2013). An adjustment, based on windspeed and "
                f"temperature, is applied to obtain hourly fluxes at the CTE-HR resolution. Positive "
                f"fluxes are emissions and negative fluxes indicate uptake. Please always cite the "
                f"original Jena CarboScope data when using this file, and use the original data when "
                f"only low resolution ocean fluxes are required.\n\n"
                f"For more information, see https://doi.org/10.5281/zenodo.6477331",
            "members": collection_info['sorted_members'],
            "submitterId": "CP",
            "title": f"High-resolution, near-real-time fluxes over Europe from CTE-HR for {year}-{month}",
            "isNextVersionOf": [] if collection_key not in current_monthly_collections.keys()
            else current_monthly_collections[collection_key].rsplit('/')[-1]
        }
        collection_info['versions'] = [collection_info['json']['isNextVersionOf']] if collection_info['json']['isNextVersionOf'] else []
        json_file_name = collection_key + '.json'
        json_file_path = os.path.join(json_collection_files, json_file_name)
        collection_info['json_file_path'] = json_file_path
        tools.write_json(path=json_file_path, content=collection_info['json'])
    print(f'- {constants.ICON_GEAR:3}Uploading monthly collections... '
          f'(Expecting {collections_to_upload} checks)... ')
    url = 'https://meta.icos-cp.eu/upload'
    for collection_key, collection_info in monthly_collections.items():
        if collection_key in current_monthly_collections.keys():
            continue
        data = open(file=collection_info['json_file_path'], mode='rb')
        cookies = tools.load_cookie()
        headers = {'Content-Type': 'application/json'}
        upload_metadata_response = requests.post(url=url, data=data, headers=headers, cookies=cookies)
        time.sleep(0.5)
        if upload_metadata_response.status_code == 200:
            collection_info['versions'].append(upload_metadata_response.text)
            print(upload_metadata_response.text)
        else:
            print(upload_metadata_response.status_code, upload_metadata_response.text)
            exit('ERROR')
    tools.write_json(path='zois.json', content=monthly_collections)
    return


def extract_yearly_collections(monthly_collections=None):
    pprint(monthly_collections)
    return
    # Read SPARQLed yearly collections.
    current_yearly_collections = tools.read_json('yearly_collections.json')
    json_collection_files = 'input-files/json-standalone-collection-files'
    # Collect members for a yearly collection.
    yearly_collections = dict()
    for monthly_key, collection_content in monthly_collections.items():
        year = monthly_key[0:4]
        if year in current_yearly_collections.keys():
            pass
        month = monthly_key[4:]
        yearly_collections.setdefault(year, dict({'json': dict(), 'members': list(), 'versions': list()}))
        yearly_collections[year]['members'].append(collection_content["versions"][-1])
        pprint(yearly_collections)
    return
    for yearly_key, yearly_collection_content in yearly_collections.items():
        yearly_collection_content['versions'] = [] if yearly_key not in current_yearly_collections.keys() \
            else [current_yearly_collections[yearly_key].rsplit('/')[-1]]
        # Archive collection's meta-data.
        yearly_collection_content['json'] = {
            "description":
                f"Yearly collection of hourly CO2 fluxes for {yearly_key}, containing hourly estimates of biospheric fluxes, "
                f"anthropogenic emissions (total and per sector), GFAS fire emissions and Jena CarboScope ocean "
                f"fluxes, all re-gridded to match the resolution of the biospheric fluxes.\n\nNet ecosystem "
                f"productivity (gross primary production minus respiration). Positive fluxes are emissions, negative "
                f"mean uptake. These fluxes are the result of the SiB4 (Version 4.2-COS, hash 1e29b25, "
                f"https://doi.org/10.1029/2018MS001540) biosphere model, driven by ERA5 reanalysis data at a 0.5x0.5 "
                f"degree resolution. The NEP per plant functional type are distributed according to the high "
                f"resolution CORINE land-use map (https://land.copernicus.eu/pan-european/corine-land-cover), and "
                f"aggregated to CTE-HR resolution.\n\nAnthropogenic emissions include contributions from public power, "
                f"industry, households, ground transport, aviation, shipping, and calcination of cement. Our product "
                f"does not include carbonation of cement and human respiration. Public power is based on ENTSO-E data "
                f"(https://transparency.entsoe.eu/), Industry, Ground transport, Aviation, and Shipping is based on "
                f"Eurostat data (https://ec.europa.eu/eurostat/databrowser/). Household emissions are based on a "
                f"degree-day model, driven by ERA5 reanalysis data. Spatial distributions of the emissions are based "
                f"on CAMS data (https://doi.org/10.5194/essd-14-491-2022). Cement emissions are taken from GridFED "
                f"V.2021.3 (https://zenodo.org/record/5956612#.YoTmvZNBy9F).\n\nGFAS fire emissions "
                f"(https://doi.org/10.5194/acp-18-5359-2018), re-gridded to match the resolution of the biosphere, "
                f"fossil fuel, and ocean fluxes of the CTE-HR product. Please always cite the original GFAS data when "
                f"using this file, and use the original data when only fire emissions are required. For more "
                f"information, see https://doi.org/10.5281/zenodo.6477331 Contains modified Copernicus Atmosphere "
                f"Monitoring Service Information [2020].\n\nOcean fluxes, based on a climatology of Jena CarboScope "
                f"fluxes (https://doi.org/10.17871/CarboScope-oc_v2020, https://doi.org/10.5194/os-9-193-2013). "
                f"An adjustment, based on windspeed and temperature, is applied to obtain hourly fluxes at the CTE-HR "
                f"resolution. Positive fluxes are emissions and negative fluxes indicate uptake. Please always cite "
                f"the original Jena CarboScope data when using this file, and use the original data when only low "
                f"resolution ocean fluxes are required.\n\nFor more information, see "
                f"https://doi.org/10.5281/zenodo.6477331",
            "members": yearly_collection_content['members'],
            "submitterId": "CP",
            "title": f"High-resolution, near-real-time fluxes over Europe from CTE-HR for {yearly_key}",
            "isNextVersionOf": [] if yearly_key not in current_yearly_collections.keys() else
            current_yearly_collections[yearly_key].rsplit('/')[-1]
        }
        # Archive meta-data for each collection.
        json_file_name = f'{yearly_key}.json'
        json_file_path = os.path.join(json_collection_files, json_file_name)
        yearly_collection_content['json_file_path'] = json_file_path
        tools.write_json(path=json_file_path, content=yearly_collection_content['json'])
    pprint(yearly_collections)
    return
    # THE REST WAS DONE BY HAND!!!!
    print(f'- {constants.ICON_GEAR:3}Uploading yearly collections... '
          f'(Expecting {len(yearly_collections.items())} checks)... ')
    url = 'https://meta.icos-cp.eu/upload'
    for collection_key, collection_info in yearly_collections.items():
        data = open(file=collection_info['json_file_path'], mode='rb')
        cookies = tools.load_cookie()
        headers = {'Content-Type': 'application/json'}
        upload_metadata_response = requests.post(url=url, data=data, headers=headers, cookies=cookies)
        time.sleep(0.5)
        if upload_metadata_response.status_code == 200:
            collection_info['versions'].append(upload_metadata_response.text.rsplit('/')[-1])
            print(upload_metadata_response.text)
        else:
            print(upload_metadata_response.status_code, upload_metadata_response.text)
            exit('ERROR')
    tools.write_json(path='zois_yearly.json', content=yearly_collections)
    return


def download_collections() -> pandas.DataFrame:
    """SPARQL query for all collections."""
    sparql_query = '''
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
    return RunSparql(sparql_query=sparql_query, output_format='pandas').run()


def extract_cte_hr_collections(df_collections: pandas.DataFrame = None):
    """Extract cte-hr collections to files."""
    df_cte_hr = df_collections[df_collections.title.str.contains(
        'High-resolution, near-real-time fluxes over Europe from CTE-HR')]
    d_collections = dict()
    for index in df_cte_hr.T:
        key = df_cte_hr.T[index].title.split('for ')[-1]
        if '-' in key:
            key = key.replace('-', '')
        meta = df_cte_hr.T[index].coll
        if meta == 'https://meta.icos-cp.eu/collections/3Oqzho4DBNlfuFYrRrIpZmHR':
            continue
        d_collections.setdefault(key, meta)

    d_full_collection, d_yearly_collections, d_monthly_collections = dict(), dict(), dict()
    for key, value in d_collections.items():
        if len(key) == 6:
            d_monthly_collections.setdefault(key, value)
        elif len(key) == 4:
            d_yearly_collections.setdefault(key, value)
        else:
            d_full_collection.setdefault(key, value)
    tools.write_json(path='monthly_collections.json', content=d_monthly_collections)
    tools.write_json(path='yearly_collections.json', content=d_yearly_collections)
    tools.write_json(path='full_collection.json', content=d_full_collection)
    return


if __name__ == '__main__':
    # Download all collections and extract the cte-hr collections.
    extract_cte_hr_collections(download_collections())
    archive_in = tools.read_json(path='input-files/cte-hr/in-out-archives/cte_hr.json')

    # Construct and upload monthly collections.
    extract_monthly_collections(archive=archive_in)

    # You just uploaded a new monthly collection of 5 components.
    # REST BY HAND
    ### 1.
    # a. Upload a new yearly version that also includes the
    # aforementioned newly uploaded monthly collection if it belongs
    # to the same year.
    # or
    # b. Upload a new yearly collection that includes the aforementioned
    # newly uploaded monthly collection.
    ### 2. https://doi.org/10.18160/20Z1-AYJ2
    # a. Upload a new version for the aforementioned DOI. The last
    # yearly collection of the DOI must be updated to its new version.
    # or
    # b. Upload a new version for the aforementioned DOI. Add the new
    # yearly collection to the list of collections.
    ### 3. https://doi.org/10.18160/20Z1-AYJ2
    # Update the Target URL in the doi app to the latest version of the full collection.

    # Read monthly collections from json file generated by
    # extract_monthly_collections() function.
    # monthly_collections = tools.read_json(path='zois.json')
    # Construct and upload yearly collections.
    # extract_yearly_collections(monthly_collections)


    # archive_json_curl()
    # pprint(monthly_collections)
    # tools.check_permissions()
    # upload_collections()
    # tools.write_json(path='backup/monthly_collections_2022_07.json', content=monthly_collections)
