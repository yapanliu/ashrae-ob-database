'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-39-Xin Zhou

1. Room A, B, and C belong to three different buildings. 1, 2, 3
2. office A has no occupancy information, no ac status info
3. office A - indoor(CO2, temp,rh), outdoor(temp,rh), windows
4. office B - indoor(temp,rh), outdoor(temp,rh), window, occupancy, ac
5. office C - indoor(CO2, temp,rh), outdoor(temp,rh), windows, occupancy, ac

process in csv
1. copy data
2. replace blank cells with -999
3. format data

'''

import os
import glob
import string
import datetime
import pandas as pd

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-39-Xin Zhou/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-39-Xin Zhou/_yapan_processing/'

''' Read all the  manually processed data and fillna, change data types '''
os.chdir(save_path)  # pwd
sub_folders = next(os.walk('.'))[1]  # get the names of the child directories, different rooms
root_files = next(os.walk('.'))[2]  # get the files under root path
root_files[:5]

# read all the data
template_hvac = pd.read_csv(save_path+root_files[0])
template_indoor = pd.read_csv(save_path+root_files[1])
template_occ = pd.read_csv(save_path+root_files[2])
template_outdoor = pd.read_csv(save_path+root_files[3])
template_window = pd.read_csv(save_path+root_files[4])

# check columns
print('Check columns:')
print(template_hvac.columns)
print(template_indoor.columns)
print(template_occ.columns)
print(template_outdoor.columns)
print(template_window.columns)

# check null values
print('check null values')
print(template_hvac.isnull().sum())
print(template_indoor.isnull().sum())
print(template_occ.isnull().sum())
print(template_outdoor.isnull().sum())
print(template_window.isnull().sum())

# fill na with -999
template_hvac['Cooling_Status'].fillna(-999, inplace=True)

template_indoor['Indoor_Temp'].fillna(-999, inplace=True)
template_indoor['Indoor_RH'].fillna(-999, inplace=True)
template_indoor['Indoor_CO2'].fillna(-999, inplace=True)

template_occ['Occupancy_Measurement'].fillna(-999, inplace=True)

template_outdoor['Outdoor_Temp'].fillna(-999, inplace=True)
template_outdoor['Outdoor_RH'].fillna(-999, inplace=True)

template_window['Window_Status'].fillna(-999, inplace=True)


# check null values
print('check data types')
print(template_hvac.dtypes)
print(template_indoor.dtypes)
print(template_occ.dtypes)
print(template_outdoor.dtypes)
print(template_window.dtypes)

# save data
template_hvac.to_csv(save_path+'/_sql/'+root_files[0], index=False)
template_indoor.to_csv(save_path+'/_sql/'+root_files[1], index=False)
template_occ.to_csv(save_path+'/_sql/'+root_files[2], index=False)
template_outdoor.to_csv(save_path+'/_sql/'+root_files[3], index=False)
template_window.to_csv(save_path+'/_sql/'+root_files[4], index=False)