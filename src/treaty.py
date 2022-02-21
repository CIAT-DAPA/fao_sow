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

    ## Method that merge fix the raw data into a dataframe that can be processed
    # (string) step: prefix of the output files. By default it is 01
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def merge_uses(self, uses, step="01",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path,sm=False)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):
            print("\tReading ", self.getinputfile())
            data_xls = pd.ExcelFile(self.getinputfile())
            df = data_xls.parse(self.input_sheet)
            df_merged = pd.merge(df,uses[["Common name - main","Use - detailed"]],how='left',left_on=['Crop list equivalent'],right_on=['Common name - main'])

            # Updating the Uses for treaty data
            df_merged.loc[df_merged["Use - detailed_x"].isna(),"Use - detailed_x"] = df_merged.loc[df_merged["Use - detailed_x"].isna(),:]["Use - detailed_y"]
            df_merged["Use - detailed"] = df_merged["Use - detailed_x"]
            df_merged = df_merged.drop(['Use - detailed_x', 'Use - detailed_y'], axis=1)

            # Saving
            print("\tWriting ", final_file)
            df_merged.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: ",final_file)
