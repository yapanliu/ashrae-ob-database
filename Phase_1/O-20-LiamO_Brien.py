'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-20-Liam O_Brien

1. this dataset is event based, the original blank cells were filled by -999
2. Delete those blank data instead
3. this processing is based on the cleaned data in the first round
'''

import os
import glob
import string
import datetime
import pandas as pd
import matplotlib.pyplot as plt

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-06-11/20-Done-Done/data-to-sql/'

# read templates into pandas
lighting_df = pd.read_csv(data_path + 'Ligthing_Status_fillna.csv')
occ_df = pd.read_csv(data_path + 'Occupancy_Measurement_fillna.csv')

# check data
print(lighting_df.columns)
print(occ_df.columns)

lighting_df.columns = ['Lighting_Status_ID', 'Date_Time', 'Lighting_Status', 'Room_ID']  # Room ID was assigned to lighting zone ID
lighting_df['Lighting_Zone_ID'] = 1
# reorder the columns
lighting_df = lighting_df[['Lighting_Status_ID', 'Date_Time', 'Lighting_Status', 'Lighting_Zone_ID', 'Room_ID']]

print(lighting_df['Lighting_Status'].unique())
print(occ_df['Occupancy_Measurement'].unique())

# drop rows with -999 measurements
lighting_df = lighting_df[lighting_df['Lighting_Status'] != -999].copy()
lighting_df.reset_index(drop=True, inplace=True)
occ_df = occ_df[occ_df['Occupancy_Measurement'] != -999].copy()
occ_df.reset_index(drop=True, inplace=True)

# check data types
print(lighting_df.dtypes)
print(occ_df.dtypes)

lighting_df['Date_Time'] = pd.to_datetime(lighting_df['Date_Time'], format="%m/%d/%Y %H:%M")
lighting_df['Lighting_Status'] = lighting_df['Lighting_Status'].astype(int)

occ_df['Date_Time'] = pd.to_datetime(occ_df['Date_Time'], format="%Y-%m-%d %H:%M")
occ_df['Occupancy_Measurement'] = occ_df['Occupancy_Measurement'].astype(int)
occ_df['Room_ID'] = occ_df['Room_ID'].astype(int)

# save data
lighting_df.to_csv(data_path+'LightingStatus.csv', index=False)
occ_df.to_csv(data_path+'Occupancy_Measurement.csv', index=False)
