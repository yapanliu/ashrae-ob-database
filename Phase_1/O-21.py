'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-21-Marilena De Simone-Zhao

1. this dataset was first cleaned in excel
2. but the character symbol '-' was found in all the datasets
3. this code will replace all the symbols with -999

'''

import os
import glob
import string
import datetime
import pandas as pd

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-06-11/21-Done-Done/sql/data_1st_round_cleaned/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-06-11/21-Done-Done/sql/'
# read templates into pandas
window_df = pd.read_csv(data_path + 'Window_Status.csv')

# check data
print(window_df.columns)
print(temp_df.dtypes)

# get all the files under root folder
root_folders = next(os.walk(data_path))[1]
root_files = next(os.walk(data_path))[2]

for index, name in enumerate(root_files):
    print(f'dealing with file {name}')
    temp_df = pd.read_csv(data_path + name)
    temp_df = temp_df.replace(['-'], [-999])
    print(temp_df['Electric_Power'].unique())
    temp_df['Date_Time'] = pd.to_datetime(temp_df['Date_Time'], format="%Y-%m-%d %H:%M:%S")
    temp_df.convert_dtypes().dtypes

''' Not completed ! '''



