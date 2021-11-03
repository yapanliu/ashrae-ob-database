'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-44-Leticia Neves

1. one office building
2. 3 different office rooms
3. window, ac, indoor
4. MRT: mean radiant remperature; DBT: dry bulb temperature; TOP: opearative temperature.
5. use DBT as indoor temp.
'''

import os
import glob
import string
import datetime
import pandas as pd

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-44-Leticia Neves/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-44-Leticia Neves/_yapan_processing/'

# read templates into pandas
template_indoor = pd.read_csv(template_path + 'Indoor_Measurement.csv')
template_hvac = pd.read_csv(template_path + 'HVAC_Measurement.csv')
template_window = pd.read_csv(template_path + 'Window_Status.csv')

# read data into dataframe
combined_df = pd.DataFrame()
for i in range(1,4):
    temp_df = pd.read_excel(data_path+f'Observed_data_office0{i}.xlsx')
    temp_df['Room_ID'] = i
    combined_df = pd.concat([combined_df, temp_df], ignore_index=True)

# combined_df['Room_ID'].unique()
# combined_df.columns
col_names = ['date', 'hour', 'Windows (0=closed; 1=opened)', 'AC (0=off; 1=on)', 'DBT (°C)', 'RH (%)', 'Room_ID']
combined_df = combined_df[col_names]  # keep useful data only
combined_df.columns = ['Date', 'Time', 'Window_Status', 'Cooling_Status', 'Indoor_Temp', 'Indoor_RH', 'Room_ID']  # rename columns

# get datetime
combined_df.loc[:, 'Date_Time'] = pd.to_datetime(combined_df['Date']) + combined_df['Time'].astype('timedelta64[h]')

combined_df['Building_ID'] = 1

# check data
# check dataframes
print(combined_df.columns)
print(combined_df.dtypes)
print(combined_df.isnull().sum())

# combine data into templates
indoor_df = combined_df[['Date_Time', 'Indoor_Temp', 'Indoor_RH', 'Room_ID', 'Building_ID']].copy()
window_df = combined_df[['Date_Time', 'Window_Status', 'Room_ID', 'Building_ID']].copy()
window_df['Window_ID'] = 1
hvac_df = combined_df[['Date_Time', 'Cooling_Status', 'Room_ID', 'Building_ID']].copy()
hvac_df['HVAC_Zone_ID'] = 1

template_indoor = pd.concat([template_indoor, indoor_df], ignore_index=True)
template_hvac = pd.concat([template_hvac, hvac_df], ignore_index=True)
template_window = pd.concat([template_window, window_df], ignore_index=True)

# check data
# check dataframes
print(template_window.columns)
print(template_window.dtypes)
print(template_window.isnull().sum())

print(template_hvac.columns)
print(template_hvac.dtypes)
print(template_hvac.isnull().sum())

print(template_indoor.columns)
print(template_indoor.dtypes)
print(template_indoor.isnull().sum())

# assign data types
template_indoor[['Room_ID', 'Building_ID']] = template_indoor[['Room_ID', 'Building_ID']].apply(pd.to_numeric)

template_hvac[['Cooling_Status', 'HVAC_Zone_ID', 'Room_ID', 'Building_ID']] = \
    template_hvac[['Cooling_Status', 'HVAC_Zone_ID', 'Room_ID', 'Building_ID']].apply(pd.to_numeric)
template_hvac['Room_ID'] = template_hvac['Room_ID'].astype(int)
template_hvac['Building_ID'] = template_hvac['Room_ID'].astype(int)

template_window[['Window_Status', 'Window_ID', 'Room_ID', 'Building_ID']] = \
    template_window[['Window_Status', 'Window_ID', 'Room_ID', 'Building_ID']].apply(pd.to_numeric)

# fill na
template_indoor['Indoor_RH'].fillna(-999, inplace=True)

# save data
template_indoor.to_csv(save_path + 'Indoor_Measurement.csv', index=False)
template_window.to_csv(save_path + 'Window_Status.csv', index=False)
template_hvac.to_csv(save_path + 'HVAC_Measurement.csv', index=False)



