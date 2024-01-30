# Standard library imports.
from typing import Tuple, Optional

# Related third party imports.

# Local application/library specific imports.
from constants.obj_specs import OBJECT_SPECS


def get_spec(file_name: str) -> Tuple[str, str]:
    if 'persector' in file_name:
        dataset_type = 'anthropogenic emissions per sector'
        dataset_object_spec = \
            OBJECT_SPECS['anthropogenic_emission_model_results']
    elif 'anthropogenic' in file_name:
        dataset_type = 'anthropogenic emissions'
        dataset_object_spec = \
            OBJECT_SPECS['anthropogenic_emission_model_results']
    elif 'nep' in file_name:
        dataset_type = 'biospheric fluxes'
        dataset_object_spec = \
            OBJECT_SPECS['biospheric_model_results']
    elif 'fire' in file_name:
        dataset_type = 'fire emissions'
        dataset_object_spec = \
            OBJECT_SPECS['file_emission_model_results']
    elif 'ocean' in file_name:
        dataset_type = 'ocean fluxes'
        dataset_object_spec = \
            OBJECT_SPECS['oceanic_flux_model_results']
    elif 'transcom' in file_name:
        dataset_type = 'inversion time-series'
        dataset_object_spec = \
            OBJECT_SPECS['inversion_modeling_time_series']
    elif any(part in file_name for part in ['CSR', 'LUMIA', 'Priors', 'GCP']):
        dataset_type = 'inversion modeling spatial'
        dataset_object_spec = \
            OBJECT_SPECS['inversion_modeling_spatial']
    elif 'zip' in file_name:
        dataset_type = 'model data archive'
        dataset_object_spec = \
            OBJECT_SPECS['model_data_archive']
    elif any(part in file_name for part in [
        'VPRM', 'lpj', 'ET', 'ET_T', 'GPP', 'NEE'
    ]):
        dataset_type = 'biosphere modeling spatial'
        dataset_object_spec = \
            OBJECT_SPECS['biosphere_modeling_spatial']
    elif 'traceRadon' in file_name:
        dataset_type = 'radon flux map'
        dataset_object_spec = \
            OBJECT_SPECS['radon_flux_map']
    elif 'EDGAR' in file_name:
        dataset_type = 'EDGAR anthropogenic emissions'
        dataset_object_spec = \
            OBJECT_SPECS['co2_emission_inventory']
    elif "AVENGERS" in file_name:
        dataset_type = 'AVENGERS aerosol emissions'
        dataset_object_spec = \
            OBJECT_SPECS['cf_compliant_netcdf']
    else:
        dataset_type = 'Easter Egg'
        dataset_object_spec = 'Easter Egg'
    return dataset_type, dataset_object_spec
