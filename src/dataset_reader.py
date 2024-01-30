import xarray
from pprint import pprint
from file_manager import FileManager
from settings import Settings
from datetime import datetime


settings = Settings().settings
file_manager = FileManager(settings)

for file in file_manager.input_data:
    dataset = xarray.open_dataset(filename_or_obj=file)
    pprint(dataset.attrs)
    break


    # for var in dataset.variables:
    #     print("***")
    #     print(var)
    #     print(dataset.variables[var])
    #     print("***")
    #     input()
    # pprint(dataset.variables["cell_area"])
    # print(f"{dataset.variables}")
    # print(f"{dataset.variables}  \n{dataset.source}\n\n{dataset.references}")
    # if "2022" in str(file.resolve()):
    #     print(file, dataset.data_vars)
