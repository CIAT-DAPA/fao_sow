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
plant_treaty_fields = str(conf_general.loc[conf_general["variable"] == "plant_treaty_fields","value"].values[0]).split(',')
plant_treaty_key_crop = conf_general.loc[conf_general["variable"] == "plant_treaty_key_crop","value"].values[0]

##############################################
# 02 - Processing Treaty Data
##############################################

print("02 - Processing Treaty Data")
treaty = Treaty(inputs_folder, treaty_file, treaty_sheet, workspace_folder)
treaty.merge_uses(conf_xls.parse("plant_treaty"), plant_treaty_fields, plant_treaty_key_crop, treaty_key_crop, force=True)

