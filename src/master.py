##############################################
# 00 - Loading libraries and packages
##############################################

import os
import pandas as pd

import tools.manage_files as mf

from treaty import Treaty

##############################################
# 01 - Setting configuration
##############################################

print("01 - Setting configuration")
# Setting global parameters
root = "/indicator"
data_folder = os.path.join(root, "data")
conf_folder = os.path.join(data_folder, "conf")
# Inputs
inputs_folder = os.path.join(data_folder, "inputs")
workspace_folder = os.path.join(data_folder, "workspace")

# Creating folders
print("Creating folders")
mf.mkdir(inputs_folder)
mf.mkdir(workspace_folder)

print("Loading configurations")
conf_xls = pd.ExcelFile(os.path.join(conf_folder,"conf.xlsx"))
conf_general = conf_xls.parse("general")

print("Extracting global parameters")
treaty_encoding = conf_general.loc[conf_general["variable"] == "treaty_encoding","value"].values[0]
treaty_file = conf_general.loc[conf_general["variable"] == "treaty_file","value"].values[0]
treaty_sheet = conf_general.loc[conf_general["variable"] == "treaty_sheet","value"].values[0]
treaty_key_crop = conf_general.loc[conf_general["variable"] == "treaty_key_crop","value"].values[0]
treaty_key_country_origin = conf_general.loc[conf_general["variable"] == "treaty_key_country_origin","value"].values[0]
treaty_key_country_recipient = conf_general.loc[conf_general["variable"] == "treaty_key_country_recipient","value"].values[0]
treaty_year = conf_general.loc[conf_general["variable"] == "treaty_year","value"].values[0]
treaty_key_germplasm = conf_general.loc[conf_general["variable"] == "treaty_key_germplasm","value"].values[0]
plant_treaty_fields = str(conf_general.loc[conf_general["variable"] == "plant_treaty_fields","value"].values[0]).split(',')
plant_treaty_key_crop = conf_general.loc[conf_general["variable"] == "plant_treaty_key_crop","value"].values[0]
countries_file = conf_general.loc[conf_general["variable"] == "countries_file","value"].values[0]
countries_sheet = conf_general.loc[conf_general["variable"] == "countries_sheet","value"].values[0]
countries_fields = str(conf_general.loc[conf_general["variable"] == "countries_fields","value"].values[0]).split(',')
countries_key_country = conf_general.loc[conf_general["variable"] == "countries_key_country","value"].values[0]
income_file = conf_general.loc[conf_general["variable"] == "income_file","value"].values[0]
income_sheet = conf_general.loc[conf_general["variable"] == "income_sheet","value"].values[0]
income_key = conf_general.loc[conf_general["variable"] == "income_key","value"].values[0]
income_years = [int(y) for y in str(conf_general.loc[conf_general["variable"] == "income_years","value"].values[0]).split(',')]
germplasm_file = conf_general.loc[conf_general["variable"] == "germplasm_file","value"].values[0]
germplasm_sheet = conf_general.loc[conf_general["variable"] == "germplasm_sheet","value"].values[0]
germplasm_key = conf_general.loc[conf_general["variable"] == "germplasm_key","value"].values[0]
fao_file = conf_general.loc[conf_general["variable"] == "fao_file","value"].values[0]
fao_years = str(conf_general.loc[conf_general["variable"] == "fao_years","value"].values[0]).split(',')
fao_elements_col = conf_general.loc[conf_general["variable"] == "fao_elements_col","value"].values[0]
fao_key_crop = conf_general.loc[conf_general["variable"] == "fao_key_crop","value"].values[0]
fao_encoding = conf_general.loc[conf_general["variable"] == "fao_encoding","value"].values[0]
##############################################
# 02 - Processing Treaty Data
##############################################

print("02 - Processing Treaty Data")
treaty = Treaty(inputs_folder, treaty_file, treaty_sheet, workspace_folder)
print("Merging with plant treaty data")
treaty.merge_plant_treaty(conf_xls.parse("plant_treaty"), plant_treaty_fields, plant_treaty_key_crop, treaty_key_crop)
print("Merging with countries data")
treaty.merge_countries(treaty_key_country_origin,treaty_key_country_recipient,countries_file,countries_sheet, countries_fields,countries_key_country)
print("Merging with income data")
treaty.merge_income(treaty_key_country_origin,treaty_key_country_recipient, treaty_year,income_file,income_sheet,income_key,income_years)
print("Merging with germplasm data")
treaty.merge_germplasm(treaty_key_germplasm,germplasm_file, germplasm_sheet,germplasm_key)
print("Merging with fato data")
treaty.merge_fao(treaty_key_crop,fao_file,fao_years,fao_elements_col,fao_key_crop,fao_encoding)

print("Fixing columns names")
treaty.change_names(conf_xls.parse("treaty_columns"))


