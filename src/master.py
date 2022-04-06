##############################################
# 00 - Loading libraries and packages
##############################################

import os
import pandas as pd

import tools.manage_files as mf

from treaty import Treaty
from fao_views import FAOViews

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
treaty_name_country_origin = conf_general.loc[conf_general["variable"] == "treaty_name_country_origin","value"].values[0]
treaty_name_country_recipient = conf_general.loc[conf_general["variable"] == "treaty_name_country_recipient","value"].values[0]
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
fao_files = str(conf_general.loc[conf_general["variable"] == "fao_files","value"].values[0]).split(',')
fao_years = str(conf_general.loc[conf_general["variable"] == "fao_years","value"].values[0]).split(',')
fao_elements_col = conf_general.loc[conf_general["variable"] == "fao_elements_col","value"].values[0]
fao_key_crop = conf_general.loc[conf_general["variable"] == "fao_key_crop","value"].values[0]
fao_encoding = conf_general.loc[conf_general["variable"] == "fao_encoding","value"].values[0]
nagoya_file = conf_general.loc[conf_general["variable"] == "nagoya_file","value"].values[0]
nagoya_sheet = conf_general.loc[conf_general["variable"] == "nagoya_sheet","value"].values[0]
nagoya_key = conf_general.loc[conf_general["variable"] == "nagoya_key","value"].values[0]
nagoya_key_year = conf_general.loc[conf_general["variable"] == "nagoya_key_year","value"].values[0]
member_file = conf_general.loc[conf_general["variable"] == "member_file","value"].values[0]
member_sheet = conf_general.loc[conf_general["variable"] == "member_sheet","value"].values[0]
member_key = conf_general.loc[conf_general["variable"] == "member_key","value"].values[0]
member_key_year = conf_general.loc[conf_general["variable"] == "member_key_year","value"].values[0]
######################
fao_views_encoding = conf_general.loc[conf_general["variable"] == "fao_views_encoding","value"].values[0]
fao_views_file = conf_general.loc[conf_general["variable"] == "fao_views_file","value"].values[0]
fao_views_sheet = conf_general.loc[conf_general["variable"] == "fao_views_sheet","value"].values[0]
fao_views_key_crop = conf_general.loc[conf_general["variable"] == "fao_views_key_crop","value"].values[0]
fao_views_key_country_origin = conf_general.loc[conf_general["variable"] == "fao_views_key_country_origin","value"].values[0]
fao_views_year = conf_general.loc[conf_general["variable"] == "fao_views_year","value"].values[0]
fao_views_year_end = conf_general.loc[conf_general["variable"] == "fao_views_year_end","value"].values[0]
fao_views_key_germplasm = conf_general.loc[conf_general["variable"] == "fao_views_key_germplasm","value"].values[0]
fao_views_name_country_origin = conf_general.loc[conf_general["variable"] == "fao_views_name_country_origin","value"].values[0]


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
print("Merging with fao data")
treaty.merge_fao(treaty_key_crop,fao_files,fao_years,fao_elements_col,fao_key_crop,fao_encoding)
print("Merging with nagoya")
treaty.merge_nagoya(treaty_name_country_origin,treaty_name_country_recipient, treaty_year, nagoya_file,nagoya_sheet,nagoya_key,nagoya_key_year)
print("Merging with members of treaty")
treaty.merge_members_treaty(treaty_key_country_origin,treaty_key_country_recipient, treaty_year, member_file,member_sheet,member_key,member_key_year)
print("Fixing columns names")
treaty.change_names(conf_xls.parse("treaty_columns"))

##############################################
# 03 - Processing FAO Views Data
##############################################

print("03 - Processing FAO Views Data")
fao_views = FAOViews(inputs_folder, fao_views_file, fao_views_sheet, workspace_folder)
#print("Merging with plant treaty data")
#fao_views.merge_plant_treaty(conf_xls.parse("plant_treaty"), plant_treaty_fields, plant_treaty_key_crop, fao_views_key_crop)
print("Merging with countries data")
fao_views.merge_countries(fao_views_key_country_origin,countries_file,countries_sheet, countries_fields,countries_key_country)
print("Merging with income data")
fao_views.merge_income(fao_views_key_country_origin, fao_views_year,income_file,income_sheet,income_key,income_years)
print("Merging with germplasm data")
fao_views.merge_germplasm(fao_views_key_germplasm,germplasm_file, germplasm_sheet,germplasm_key)
print("Merging with fao data")
fao_views.merge_fao(fao_views_key_crop,fao_files,fao_years,fao_elements_col,fao_key_crop,fao_encoding)
print("Merging with nagoya")
fao_views.merge_nagoya(fao_views_name_country_origin, fao_views_year_end, nagoya_file,nagoya_sheet,nagoya_key,nagoya_key_year)
print("Merging with members of treaty")
fao_views.merge_members_treaty(fao_views_key_country_origin, fao_views_year_end, member_file,member_sheet,member_key,member_key_year)
print("Fixing columns names")
fao_views.change_names(conf_xls.parse("fao_views_columns"))

