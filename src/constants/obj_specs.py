from constants.endpoints import CP_META


OBJECT_SPECS = {
    # Used for uploading LPJ-GUESS & VPRM datasets.
    "biosphere_modeling_spatial": f"{CP_META}biosphereModelingSpatial",
    # Used for uploading CTE-HR datasets.
    "anthropogenic_emission_model_results":
        f"{CP_META}anthropogenicEmissionModelResults",
    "biospheric_model_results": f"{CP_META}biosphericModelResults",
    "file_emission_model_results": f"{CP_META}fireEmissionModelResults",
    "oceanic_flux_model_results": f"{CP_META}oceanicFluxModelResults",
    #
    "inversion_modeling_time_series": f"{CP_META}inversionModelingTimeseries",
    "inversion_modeling_spatial": f"{CP_META}inversionModelingSpatial",
    # Used for uploading remote sensing datasets (Landsat & MODIS).
    "model_data_archive": f"{CP_META}modelDataArchive",
    # Used for uploading TRACE-RADON dataset.
    "radon_flux_map": f"{CP_META}radonFluxSpatialL3",
    # Used for uploading EDGAR anthropogenic emissions datasets.
    "co2_emission_inventory": f"{CP_META}co2EmissionInventory",
    "cf_compliant_netcdf": f"{CP_META}arbitraryCfNetcdf"
}
