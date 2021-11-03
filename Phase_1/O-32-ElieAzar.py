'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-32-Elie Azar

1. 8 desks and 8 occupancy measurements, but only 6 desks with 6 appliance IDs under each desk
3. 2 workstations have occupancy measurements and 6 plugs
4. 1 middle desk has occupancy measurements only
5. all the data are in the same .xlsx sheets
'''

import os
import glob
import string
import datetime
import pandas as pd

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-32-Elie Azar/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-32-Elie Azar/_yapan_processing/'

# read templates into pandas
template_occupancy = pd.read_csv(template_path + 'Occupancy_Measurement.csv')
template_appliance = pd.read_csv(template_path + 'Appliance_Usage.csv')
template_indoor = pd.read_csv(template_path + 'Indoor_Measurement.csv')

''' read the data into pandas '''

combined_df = pd.read_excel(save_path + 'EA_DATA-TEST_processed.xlsx')
# combined_df.columns

# clean data before processing
columns_list = combined_df.columns[-2:]
combined_df.drop(columns_list, axis=1, inplace=True)
combined_df = combined_df[combined_df['TimeStamp'].notnull()]
combined_df = combined_df.rename(columns={'TimeStamp': 'Date_Time'})
combined_df.fillna(-999, inplace=True)
# combined_df.columns
print(f'Total columns: {len(combined_df.columns)}')
print(f'Check null vlaues: {combined_df.isnull().sum().sum()}')

# process the data by desk and workstation, get the data by each table/workstation
occ_list = ['Occupant' + str(i) for i in range(1, 9)]  # generate the occupants' names
occ_list.extend(['Workstation1', 'Workstation2', 'MiddleTable'])
# get the column names with appliance data
indoor_list = ['AirTemp', 'RelativeHumidity', 'Illiminance']  # indoor measurement columns

# i = 0
# process the data by desk/workstation
for desk_id, occ_name in enumerate(occ_list):
    occ_columns = list(combined_df.filter(regex=f'({occ_name})', axis=1))
    # process data by occupancy and appliance
    for index, column in enumerate(occ_columns):
        occ_temp_df = combined_df[['Date_Time', column]].copy()
        if index == 0:  # occupancy measurements
            occ_temp_df.columns = ['Date_Time', 'Occupancy_Measurement']
            occ_temp_df['Room_ID'] = 1
            occ_temp_df['Building_ID'] = 1
            occ_temp_df['Desk_ID'] = desk_id + 1  # get the desk ID
            template_occupancy = pd.concat([template_occupancy, occ_temp_df], ignore_index=True)

        else:  # appliance usage
            occ_temp_df.columns = ['Date_Time', 'Electric_Power']
            occ_temp_df['Desk_ID'] = desk_id + 1  # get the desk ID
            occ_temp_df['Appliance_ID'] = index  # get the desk ID
            occ_temp_df['Room_ID'] = 1
            occ_temp_df['Building_ID'] = 1
            template_appliance = pd.concat([template_appliance, occ_temp_df], ignore_index=True)

    # print(len(occ_columns))  # check the if those two loops covered all the columns
    # i += len(occ_columns)

# process indoor data
indoor_df = pd.DataFrame()  # store indoor data before concat with template
for index, indoor_name in enumerate(indoor_list):
    indoor_columns = list(combined_df.filter(regex=f'({indoor_name})', axis=1))
    indoor_columns.extend(['Date_Time'])
    indoor_temp_df = combined_df[indoor_columns].copy()

    if index == 0:  # indoor t
        indoor_temp_df['Indoor_Temp'] = indoor_temp_df.loc[:, indoor_temp_df.columns != 'Date_Time'].mean(axis=1)
        indoor_temp_df['Room_ID'] = 1
        indoor_temp_df['Building_ID'] = 1
        indoor_temp_df = indoor_temp_df[['Date_Time', 'Indoor_Temp', 'Room_ID', 'Building_ID']]

    if index == 1:  # indoor rh
        indoor_temp_df['Indoor_RH'] = indoor_temp_df.loc[:, indoor_temp_df.columns != 'Date_Time'].mean(axis=1)
        indoor_temp_df = indoor_temp_df['Indoor_RH']

    if index == 2:  # indoor Illiminance
        indoor_temp_df['Indoor_Illuminance'] = indoor_temp_df.loc[:, indoor_temp_df.columns != 'Date_Time'].mean(axis=1)
        indoor_temp_df['Room_ID'] = 1
        indoor_temp_df['Building_ID'] = 1
        indoor_temp_df = indoor_temp_df['Indoor_Illuminance']

    indoor_df = pd.concat([indoor_df, indoor_temp_df], axis=1)  # concat by column and keep column names

template_indoor = pd.concat([template_indoor, indoor_df], ignore_index=True, axis=0)

# print(len(indoor_columns))
# i += len(indoor_columns)

# check data before saving
# check the unique room ids and building ids
print(template_occupancy['Desk_ID'].unique())
print(template_appliance['Desk_ID'].unique())
print(template_appliance['Appliance_ID'].unique())

# check datatypes
# check the unique room ids and building ids
print(template_occupancy.dtypes)
print(template_appliance.dtypes)
print(template_indoor.dtypes)

# assign data types
template_occupancy['Building_ID'] = template_occupancy['Building_ID'].astype(int)
template_occupancy['Room_ID'] = template_occupancy['Room_ID'].astype(int)
template_occupancy['Desk_ID'] = template_occupancy['Desk_ID'].astype(int)
template_occupancy['Occupancy_Measurement'] = template_occupancy['Occupancy_Measurement'].astype(int)

template_appliance['Building_ID'] = template_appliance['Building_ID'].astype(int)
template_appliance['Room_ID'] = template_appliance['Room_ID'].astype(int)
template_appliance['Desk_ID'] = template_appliance['Desk_ID'].astype(int)
template_appliance['Appliance_ID'] = template_appliance['Appliance_ID'].astype(int)

template_indoor['Building_ID'] = template_indoor['Building_ID'].astype(int)
template_indoor['Room_ID'] = template_indoor['Room_ID'].astype(int)

print('check null values')
print(template_occupancy.isnull().sum())
print(template_appliance.isnull().sum())
print(template_indoor.isnull().sum())

# save data
template_occupancy.to_csv(save_path + 'Occupancy_Measurement.csv', index=False)
template_appliance.to_csv(save_path + 'Appliance_Usage.csv', index=False)
template_indoor.to_csv(save_path + 'Indoor_Measurement.csv', index=False)
