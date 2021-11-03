'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-45-Andreas Wagner

1. this code further processes the dataset from Xuezheng
2. combine all the csv files in the same behavior type
3. combine the files with templates
4. window status data didn't contain room ids, room 1-15: 1-30, each room has two data sets; 16-31, 17-32, 33
'''

import os
import math
import glob
import string
import datetime
import pandas as pd
import matplotlib.pyplot as plt

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-06-11/45-Done-Done/_archived/Data to SQL-XZ/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-06-11/45-Done-Done/_sql/'

# read the templates
template_indoor = pd.read_csv(template_path + 'Indoor_Measurement.csv')
template_window = pd.read_csv(template_path + 'Window_Status.csv')
template_occ = pd.read_csv(template_path + 'Occupancy_Measurement.csv')

# read data
os.chdir(data_path)
root_files = next(os.walk('.'))[2]  # get the files under root path
indoor_list = list(filter(lambda name: 'Indoor' in name, root_files))
# read data into pandas
combined_indoor = pd.concat([pd.read_excel(data_path+f) for f in indoor_list], ignore_index=True)
combined_occ = pd.read_excel(data_path+root_files[3])
combined_window = pd.read_excel(data_path+root_files[4])

# change column names to standard names
print(combined_indoor.columns)
print(combined_occ.columns)
print(combined_window.columns)

print(template_indoor.columns)
print(template_occ.columns)
print(template_window.columns)

combined_indoor.columns = ['Indoor_Measurement_ID', 'Date_Time', 'Indoor_Temp',
                           'Indoor_RH', 'Indoor_CO2', 'Indoor_VOC', 'Indoor_Air_Speed', 'Room_ID']
combined_occ.columns = ['Occupancy_Measurement_ID', 'Date_Time', 'Occupancy_Measurement', 'Room_ID']
combined_window.drop(['shade_status'], axis=1, inplace=True)  # drop column by name
combined_window.columns = ['Window_Status_ID', 'Date_Time', 'Window_Status', 'Window_ID']

# change column names to standard names
print(combined_indoor.columns)
print(combined_occ.columns)
print(combined_window.columns)

# check measurements
print(combined_indoor['Room_ID'].unique())
print(combined_occ['Occupancy_Measurement'].unique())
print(combined_window['Window_Status'].unique())

combined_window['Window_Status'].replace([3], [-999], inplace=True)  # replace the abnormal values

# combine the data with templates
template_indoor = pd.concat([template_indoor, combined_indoor], ignore_index=True)
template_window = pd.concat([template_window, combined_window], ignore_index=True)
template_occ = pd.concat([template_occ, combined_occ], ignore_index=True)

# check null values and data types
print(template_indoor.isnull().sum())
print(template_occ.isnull().sum())
print(template_window.isnull().sum())

print(template_indoor.dtypes)
print(template_occ.dtypes)
print(template_window.dtypes)

# assign room ids based on window ids
for i in range(template_window.shape[0]):
    if template_window.loc[i, 'Window_ID'] != 31:  # room 1-15, 17
        template_window.loc[i, 'Room_ID'] = math.ceil(template_window.loc[i, 'Window_ID'] / 2)
    elif template_window.loc[i, 'Window_ID'] == 31:  # room 16
        template_window.loc[i, 'Room_ID'] = 16
    else:
        pass
print(template_window['Window_ID'].unique())
print(template_window['Room_ID'].unique())

# check data types
print(template_indoor.dtypes)
print(template_occ.dtypes)
print(template_window.dtypes)

# assign data types
template_indoor['Date_Time'] = pd.to_datetime(template_indoor['Date_Time'], format='%d.%m.%Y %H:%M:%S')
template_occ['Date_Time'] = pd.to_datetime(template_occ['Date_Time'], format='%d.%m.%Y %H:%M:%S')
template_window['Date_Time'] = pd.to_datetime(template_window['Date_Time'], format='%d.%m.%Y %H:%M:%S')

template_indoor['Building_ID'] = 1
template_occ['Building_ID'] = 1
template_window['Building_ID'] = 1

template_indoor['Building_ID'] = template_indoor['Building_ID'].astype(int)
template_indoor['Room_ID'] = template_indoor['Room_ID'].astype(int)

template_occ['Occupancy_Measurement'] = template_occ['Occupancy_Measurement'].astype(int)
template_occ['Building_ID'] = template_occ['Building_ID'].astype(int)
template_occ['Room_ID'] = template_occ['Room_ID'].astype(int)

template_window['Window_Status'] = template_window['Window_Status'].astype(int)
template_window['Window_ID'] = template_window['Window_ID'].astype(int)
template_window['Building_ID'] = template_window['Building_ID'].astype(int)
template_window['Room_ID'] = template_window['Room_ID'].astype(int)

# save data
template_indoor.to_csv(save_path+'Indoor_Measurement.csv', index=False)
template_occ.to_csv(save_path+'Occupancy_Measurement.csv', index=False)
template_window.to_csv(save_path+'Window_Status.csv', index=False)






