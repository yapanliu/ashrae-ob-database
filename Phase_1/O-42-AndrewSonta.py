'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-42-Andrew Sonta

1. all plug load type of data

'''

import os
import glob
import string
import datetime
import pandas as pd

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-42-Andrew Sonta/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-42-Andrew Sonta/_yapan_processing/'

# read templates into pandas
template_appliance = pd.read_csv(template_path + 'Appliance_Usage.csv')

''' read the data into pandas '''
combined_df = pd.read_csv(data_path + 'rwc-clean-trimmed.csv')
combined_df = combined_df.rename(columns={combined_df.columns[0]: 'Date_Time'})
combined_df = combined_df[combined_df['Date_Time'].notnull()]
plug_list = list(combined_df.columns[1:])  # get the list of all the plug names

# concat all the columns
plug_temp_df = pd.DataFrame()  # create empty dataframe for combining date time and plug load
for index, col_name in enumerate(plug_list):
    # store data into the dataframe
    plug_temp_df['Date_Time'] = combined_df['Date_Time']
    plug_temp_df['Electric_Power'] = combined_df[col_name]
    plug_temp_df['Appliance_ID'] = int(col_name[-3:])
    plug_temp_df['Room_ID'] = 1
    plug_temp_df['Building_ID'] = 1

    template_appliance = pd.concat([template_appliance, plug_temp_df], ignore_index=True)

# check data before saving
# check the unique room ids and building ids
print(template_appliance['Appliance_ID'].unique())
print(len(template_appliance['Appliance_ID'].unique()))

# check missing data
print('check null values')
print(template_appliance.isnull().sum())
print(template_appliance[template_appliance.isnull().any(axis=1)])  # check missing data in all columns
print(template_appliance[template_appliance['Electric_Power'].isnull()])  # check missing data in a column
# fill null with -999
template_appliance['Electric_Power'].fillna(-999, inplace=True)

# check datatypes
print(template_appliance.dtypes)
# assign data types
template_appliance['Date_Time'] = pd.to_datetime(template_appliance['Date_Time'], format='%m/%d/%y %H:%M')
template_appliance['Building_ID'] = template_appliance['Building_ID'].astype(int)
template_appliance['Room_ID'] = template_appliance['Room_ID'].astype(int)
template_appliance['Appliance_ID'] = template_appliance['Appliance_ID'].astype(int)

# save data
template_appliance.to_csv(save_path + 'Appliance_Usage.csv', index=False)



