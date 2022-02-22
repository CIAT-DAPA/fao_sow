import os
import re
import glob

from pandas.core import groupby
import numpy as np
import pandas as pd

import tools.manage_files as mf

class Treaty(object):

    inputs_folder = ''
    input_file = ''
    input_sheet = ''
    workspace_root = ''
    encoding = ''
    outputs = ''

    # Properties
    def getworkspace(self):
        return os.path.join(self.workspace_root,'treaty')

    def getinputfile(self):
        return os.path.join(self.inputs_folder,self.input_file)

    ## Method construct
    # (string) input: Name of the raw data
    # (string) workspace: Place where data processed should saved
    # (string) encoding: Encoding files. By default it is ISO-8859-1
    def __init__(self, inputs_folder, input_file, input_sheet, workspace, outputs= 'treaty', encoding="ISO-8859-1"):
        self.inputs_folder = inputs_folder
        self.input_file = input_file
        self.input_sheet = input_sheet
        self.workspace_root = workspace
        self.encoding = encoding
        self.outputs = outputs
        mf.mkdir(self.getworkspace())

    ## Method that merge the raw data with plant treaty.
    # It adds new fields which are needed for the analysis
    # (dataframe) plant_treaty: Dataframe with the information of
    # (string[]) plant_treaty_fields: fields of the dataframe uses which will be merged with the core data
    # (string) plant_treaty_key: Key field of plant_treaty data for merging with data
    # (string) crop_key: Key field of data for merging with plant treaty
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def merge_uses(self, plant_treaty, plant_treaty_fields, plant_treaty_key, crop_key, step="01",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path,sm=False)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):
            print("\tProcessing ", self.getinputfile())
            data_xls = pd.ExcelFile(self.getinputfile())
            df = data_xls.parse(self.input_sheet)
            fields_list = plant_treaty_fields
            fields_list.append(plant_treaty_key)
            df_merged = pd.merge(df,plant_treaty[fields_list],how='left',left_on=[crop_key],right_on=[plant_treaty_key])
            print("\tNumber rows: treaty=",df.shape[0],"plant treaty=",plant_treaty.shape[0],"merged=",df_merged.shape[0])
            # Updating the Uses for treaty data
            df_merged.loc[df_merged["Use - detailed_x"].isna(),"Use - detailed_x"] = df_merged.loc[df_merged["Use - detailed_x"].isna(),:]["Use - detailed_y"]
            df_merged["Use - detailed"] = df_merged["Use - detailed_x"]
            df_merged = df_merged.drop(['Use - detailed_x', 'Use - detailed_y'], axis=1)

            # Saving
            print("\tSaving ", final_file)
            df_merged.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: ",final_file)

    ## Method that merge the raw data with countries.
    # It allows to add new information like region and subregions
    # (string) step: prefix of the output files. By default it is 02
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def merge_countries(self, step="02",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path,sm=False)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):
            """ print("\tProcessing ", self.getinputfile())
            data_xls = pd.ExcelFile(self.getinputfile())
            df = data_xls.parse(self.input_sheet)
            fields_list = plant_treaty_fields
            fields_list.append(plant_treaty_key)
            df_merged = pd.merge(df,plant_treaty[fields_list],how='left',left_on=[crop_key],right_on=[plant_treaty_key])
            print("\tNumber rows: treaty=",df.shape[0],"plant treaty=",plant_treaty.shape[0],"merged=",df_merged.shape[0])
            # Updating the Uses for treaty data
            df_merged.loc[df_merged["Use - detailed_x"].isna(),"Use - detailed_x"] = df_merged.loc[df_merged["Use - detailed_x"].isna(),:]["Use - detailed_y"]
            df_merged["Use - detailed"] = df_merged["Use - detailed_x"]
            df_merged = df_merged.drop(['Use - detailed_x', 'Use - detailed_y'], axis=1)

            # Saving
            print("\tSaving ", final_file)
            df_merged.to_csv(final_file, index = False, encoding = self.encoding) """

        else:
            print("\tNot processed: ",final_file)
