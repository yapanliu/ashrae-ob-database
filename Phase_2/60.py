'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-60 Jared

1. this code changes the column name of the survey data
'''

import os
import math
import glob
import string
import datetime
import pandas as pd

# specify the path
data_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/60-Done-Done-Survey/Survey/"
save_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/60-Done-Done-Survey/"

# read data
df = pd.read_csv(data_path + 'Study_26.csv')
headings_df = pd.read_csv(data_path + 'Study_26_Dict.csv')

print(df.columns)
print(df.head(10))

df_head = df.head(10)

# save portion of the data to csv
df_head.to_csv(save_path+'df_head.csv', index=False)

# assign new headings
len(df.columns)
df.columns = headings_df['col_names']
df.to_csv(data_path + 'Study_26.csv', index=False)