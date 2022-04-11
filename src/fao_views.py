import os
import re
import glob

from pandas.core import groupby
import numpy as np
import pandas as pd

import tools.manage_files as mf

class FAOViews(object):

    inputs_folder = ''
    input_file = ''
    input_sheet = ''
    workspace_root = ''
    encoding = ''
    outputs = ''

    # Properties
    def getworkspace(self):
        return os.path.join(self.workspace_root,'fao_views')

    def getinputfile(self):
        return os.path.join(self.inputs_folder,self.input_file)

    def getworkspacestep(self,step,folder="OK"):
        return os.path.join(self.getworkspace(),step,folder)

    ## Method construct
    # (string) input: Name of the raw data
    # (string) workspace: Place where data processed should saved
    # (string) encoding: Encoding files. By default it is ISO-8859-1
    def __init__(self, inputs_folder, input_file, input_sheet, workspace, outputs= 'fao_views', encoding="utf-8"):
        self.inputs_folder = inputs_folder
        self.input_file = input_file
        self.input_sheet = input_sheet
        self.workspace_root = workspace
        self.encoding = encoding
        self.outputs = outputs
        mf.mkdir(self.getworkspace())

    # Method with replace values for similar columns for records which has NA's data.
    # When merge two dataframe with similar columns, pandas set the name x for left dataframe
    # and y for right dataframe. It join both columns in just one
    # (dataframe) df: Dataframe which has the columns that will be update
    # (string) col: Column name
    def update_columns_data(self, df,col):
        x = col + "_x"
        y = col + "_y"
        df.loc[df[x].isna(),x] = df.loc[df[x].isna(),:][y]
        df[col] = df[x]
        df = df.drop([x, y], axis=1)
        return df

    ## Method that merge the raw data with countries.
    # It allows to add new information like region and subregions
    # (string) origin: Field name for key of country from origin
    # (string) countries_file: Files name of countries. It should be into inputs folder
    # (string) countries_sheet: Sheet name
    # (string) countries_fields: List of field which will be used
    # (string) countries_key: Key field for merging data with countries
    # (string) step: prefix of the output files. By default it is 02
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def merge_countries(self, origin, countries_file, countries_sheet, countries_fields, countries_key, step="01",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):
            countries_path = os.path.join(self.inputs_folder,countries_file)
            print("\tProcessing countries=", countries_path,"data=",self.getinputfile())
            countries_xls = pd.ExcelFile(countries_path)
            df_countries = countries_xls.parse(countries_sheet)
            c_fields_list = countries_fields
            df_countries = df_countries[c_fields_list]

            data_xls = pd.ExcelFile(self.getinputfile())
            df = data_xls.parse(self.input_sheet)

            print("\tMerging origin")
            df_merged = pd.merge(df,df_countries,how="left",left_on=[origin],right_on=[countries_key])
            print("\tNumber rows: treaty=",df.shape[0],"countries=",df_countries.shape[0],"merged=",df_merged.shape[0])

            # Saving
            print("\tSaving ", final_file)
            df_merged.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: ",final_file)


    ## Method that merge the data with income for each country.
    # It add income for origin and recipient
    # (string) origin: Field name for key of country from origin
    # (string) fao_views_year: Field name for year of transaction
    # (string) income_file: File name of income for countries. It should be into inputs folder
    # (string) income_sheet: Sheet name
    # (string) income_key: Key field for merging data from income
    # (int[]) income_years: Arrays of years
    # (string) step: prefix of the output files. By default it is 02
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def merge_income(self, origin, fao_views_year, income_file, income_sheet, income_key, income_years, step="02",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path, sm=False)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):
            income_path = os.path.join(self.inputs_folder,income_file)
            print("\tProcessing income=", income_path,"data=",os.path.join(self.getworkspacestep("01"),self.outputs + ".csv"))

            income_xls = pd.ExcelFile(income_path)
            df_income = income_xls.parse(income_sheet)

            # Pivoting columns into rows
            df_income = pd.melt(df_income, id_vars=[income_key], value_vars=income_years)
            df_income["variable"] = df_income["variable"].astype(str)

            df = pd.read_csv(os.path.join(self.getworkspacestep("01"),self.outputs + ".csv"), encoding = self.encoding)
            # Fixing periods of year for this dataset
            df[['year_start','year_end']] = df[fao_views_year].str.split('-',expand=True)

            print("\tMerging origin")
            df_merged = pd.merge(df,df_income,how="left",left_on=[origin,'year_start'],right_on=[income_key,"variable"])
            print("\tNumber rows: fao_views=",df.shape[0],"income=",df_income.shape[0],"merged=",df_merged.shape[0])

            # Saving
            print("\tSaving ", final_file)
            df_merged.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: ",final_file)

    ## Method that merge the data with germplasm for each crop.
    # (string) fao_views_key_germplasm: Field name for key of country from origin
    # (string) germplasm_file: File name of income for countries. It should be into inputs folder
    # (string) germplasm_sheet: Sheet name
    # (string) germplasm_key: Key field for merging data from income
    # (string) step: prefix of the output files. By default it is 03
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def merge_germplasm(self, fao_views_key_germplasm, germplasm_file, germplasm_sheet, germplasm_key, step="03",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path, sm=False)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):
            germ_path = os.path.join(self.inputs_folder,germplasm_file)
            print("\tProcessing germplasm=", germ_path,"data=",os.path.join(self.getworkspacestep("02"),self.outputs + ".csv"))
            germ_xls = pd.ExcelFile(germ_path)
            df_germ = germ_xls.parse(germplasm_sheet)
            df = pd.read_csv(os.path.join(self.getworkspacestep("02"),self.outputs + ".csv"), encoding = self.encoding)

            print("\tMerging by crops")
            df_merged = pd.merge(df,df_germ,how="left",left_on=[fao_views_key_germplasm],right_on=[germplasm_key])
            print("\tNumber rows: fao_views=",df.shape[0],"income=",df_germ.shape[0],"merged=",df_merged.shape[0])

            # Removing key id
            df_merged = df_merged.drop([germplasm_key], axis=1)

            # Saving
            print("\tSaving ", final_file)
            df_merged.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: ",final_file)

    ## Method that merge the data with fao for each crop.
    # (string) fao_views_key_crop: Field name for key of crop
    # (string[]) fao_files: List of files with fao data. They should be into inputs folder
    # (string) fao_years: List of year to calculate average
    # (string) fao_elements_col: Column name which has elements or variables
    # (string) fao_key_crop: Column names for key crop in fao
    # (string) fao_encoding: Encoding of fao file
    # (string) step: prefix of the output files. By default it is 03
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def merge_fao(self, fao_views_key_crop, fao_files, fao_years, fao_elements_col,fao_key_crop, fao_encoding, step="04",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path, sm=False)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):
            # Loading data
            print("\tProcessing data=",os.path.join(self.getworkspacestep("03"),self.outputs + ".csv"))
            df = pd.read_csv(os.path.join(self.getworkspacestep("03"),self.outputs + ".csv"), encoding = self.encoding)
            # Loop for each fao file
            for fao_file in fao_files:
                fao_path = os.path.join(self.inputs_folder,fao_file)
                print("\t\tProcessing fao=", fao_path)
                # Load fao file
                df_fao = pd.read_csv(fao_path,encoding=fao_encoding)
                # Calculate average through years
                df_fao["value"] = df_fao[fao_years].mean(axis=1)
                # Pivot Elements to columns
                df_fao = df_fao.pivot_table(index=[fao_key_crop], columns=[fao_elements_col], values="value")
                df_fao.reset_index(level=[fao_key_crop], inplace=True)
                print("\tMerging by crops")
                df_merged = pd.merge(df,df_fao,how="left",left_on=[fao_views_key_crop],right_on=[fao_key_crop])
                print("\tNumber rows: df=",df.shape[0],"fao=",df_fao.shape[0],"merged=",df_merged.shape[0])
                # Saving data
                df = df_merged

            # Create final dataframe with all data
            df_merged = df
            # Saving
            print("\tSaving ", final_file)
            df_merged.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: ",final_file)

    ## Method that validate if a transfer was part of treaty or not
    # It does for origin and recipient
    # (string) origin: Field name for key of country from origin
    # (string) file: File name of member for countries. It should be into inputs folder
    # (string) sheet: Sheet name
    # (string) key: Key field for merging data from nagoya
    # (string) step: prefix of the output files. By default it is 06
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def validate_transfer_was_part(self, df, origin,  year, name, file, sheet, key_country, key_year):
        path = os.path.join(self.inputs_folder, file)
        print("\tProcessing file=", path)

        xls = pd.ExcelFile(path)
        df_transfer = xls.parse(sheet)

        print("\tMerging origin")
        #print(df_transfer.head())
        df_merged = pd.merge(df,df_transfer,how="left",left_on=[origin],right_on=[key_country])
        print("\tNumber rows: data=",df.shape[0],name +"=",df_transfer.shape[0],"merged=",df_merged.shape[0])

        # Setting if transfer was part of treaty or not
        df_merged["origin_" + name + "_transfer"] = df_merged[key_year] <= df_merged[year]

        return df_merged

    ## Method that merge the data with nagoya for each country.
    # (string) origin: Field name for key of country from origin
    # (string) year: Field name for key of transfer year
    # (string) nagoya_file: File name of nagoya for countries. It should be into inputs folder
    # (string) nagoya_sheet: Sheet name
    # (string) nagoya_key: Key field for merging data from nagoya
    # (string) nagoya_key_year: Field name in which country started to be parted
    # (string) step: prefix of the output files. By default it is 06
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def merge_nagoya(self, origin, year, nagoya_file, nagoya_sheet, nagoya_key, nagoya_key_year, step="05",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path, sm=False)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):

            # Reading data
            df = pd.read_csv(os.path.join(self.getworkspacestep("04"),self.outputs + ".csv"), encoding = self.encoding)
            # Calculating
            df_merged = self.validate_transfer_was_part(df, origin, year, "nagoya", nagoya_file, nagoya_sheet, nagoya_key, nagoya_key_year)

            # Saving
            print("\tSaving ", final_file)
            df_merged.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: ",final_file)

     ## Method that merge the data with member of treaty for each country.
    # (string) origin: Field name for key of country from origin
    # (string) year: Field name for key of transfer year
    # (string) member_file: File name of nagoya for countries. It should be into inputs folder
    # (string) member_sheet: Sheet name
    # (string) member_key: Key field for merging data from nagoya
    # (string) member_key_year: Field name in which country started to be parted
    # (string) step: prefix of the output files. By default it is 07
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def merge_members_treaty(self, origin,  year, member_file, member_sheet, member_key, member_key_year, step="06",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path, sm=False)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):

            # Reading data
            df = pd.read_csv(os.path.join(self.getworkspacestep("05"),self.outputs + ".csv"), encoding = self.encoding)
            # Calculating
            df_merged = self.validate_transfer_was_part(df, origin, year, "member_treaty", member_file, member_sheet, member_key, member_key_year)

            # Saving
            print("\tSaving ", final_file)
            df_merged.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: ",final_file)

    ## Method that change the columns names of dataset
    # (string) origin: Field name for key of country from origin
    # (string) recipient: Field name for key of country from recipient
    # (string) countries_file: Files name of countries. It should be into inputs folder
    # (string) countries_sheet: Sheet name
    # (string) countries_fields: List of field which will be used
    # (string) countries_key: Key field for merging data with countries
    # (string) step: prefix of the output files. By default it is 02
    # (bool) force: Set if the process have to for the execution of all files even if the were processed before.
    #               By default it is False
    def change_names(self, columns, step="07",force=False):
        final_path = os.path.join(self.getworkspace(),step)
        mf.create_review_folders(final_path, er=False,sm=False)
        # It checks if files should be force to process again or if the path exist
        final_file = os.path.join(final_path,"OK",self.outputs + ".csv")
        if force or not os.path.exists(final_file):
            print("\tProcessing data=",os.path.join(self.getworkspacestep("06"),self.outputs + ".csv"))
            df = pd.read_csv(os.path.join(self.getworkspacestep("06"),self.outputs + ".csv"), encoding = self.encoding)
            df.columns = columns["new"]
            #print(columns.loc[columns["drop"]==0:"new"].head())
            columns_final = columns.loc[columns["drop"]==0,"new"]
            df = df[columns_final]
            # Saving
            print("\tSaving ", final_file)
            df.to_csv(final_file, index = False, encoding = self.encoding)

        else:
            print("\tNot processed: ",final_file)
