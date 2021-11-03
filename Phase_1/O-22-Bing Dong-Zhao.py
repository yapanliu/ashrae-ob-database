'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-22-Bing Dong-Zhao

1. all occupancy data
2. three different buildings, each building has its own folder
3. combine all the csv files within the same folder
4. drop the empty rows, since this dataset is event based, not time series
'''

import os
import glob
import datetime
import pandas as pd
import matplotlib.pyplot as plt

# specify the path
data_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-22-Bing Dong-Zhao/"
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-22-Bing Dong-Zhao/_yapan_processing/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'

'''
1. walk through all the folders and combine the csvs under each folder
'''
# folder_names = ['ACC']
# folder_names = ['Residential_OCC_Schedule']
folder_names = ['ACC', 'Container', 'Residential_OCC_Schedule']

begin_time = datetime.datetime.now()
combined_csv = pd.DataFrame()
# this folder has 1-6 sub-folders, combine all the data under each sub-folder, assign the id as subfolder name
for index, name in enumerate(folder_names):
    # folder index
    print(f'Dealing with data under folder {name}')
    # set up the working directory
    os.chdir(data_path+f'/{name}')
    sub_folders = os.listdir()  # list out all the sub folders in current working directory
    # walk through each sub folder and combine csv files
    for room_id, folder in enumerate(sub_folders):
        os.chdir(data_path+f'/{name}/'+folder)
        file_names = [i for i in glob.glob('*.{}'.format('csv'))]  # get all file names
        if file_names != []:
            col_names = ['Date_Time', 'Occupancy_Measurement']
            temp_df = pd.concat([pd.read_csv(f, skiprows=2, usecols=[1, 2], names=col_names) for f in file_names])
            temp_df['Room_ID'] = room_id+1
            temp_df['Building_ID'] = index+1
            combined_csv = pd.concat([combined_csv, temp_df], ignore_index=True)
            combined_csv.columns = temp_df.columns

# check missing values, and sum missing value count by column
print('Check missing values in : window_combined')
print(combined_csv.isnull().sum())

# drop rows which do not have data for occupancy_Measurement
combined_csv = combined_csv[combined_csv['Occupancy_Measurement'].notna()]

# fill null values with -999
# combined_csv = combined_csv.fillna(-999)

# read Occupancy_Measurement.csv
template_occupancy = pd.read_csv(template_path+'Occupancy_Measurement.csv')
# save data to template
template_occupancy = pd.concat([template_occupancy, combined_csv], ignore_index=True)

# convert data format
template_occupancy.dtypes
template_occupancy = template_occupancy.fillna('')
template_occupancy['Date_Time'] = pd.to_datetime(template_occupancy['Date_Time'], format="%m/%d/%y %H:%M:%S %p")
template_occupancy['Occupancy_Measurement'] = template_occupancy['Occupancy_Measurement'].astype(int)
template_occupancy['Room_ID'] = template_occupancy['Room_ID'].astype(int)
template_occupancy['Building_ID'] = template_occupancy['Building_ID'].astype(int)

# save data
template_occupancy.to_csv(save_path+'Occupancy_Measurement.csv', index=False)

print(f'Total running time: {datetime.datetime.now() - begin_time}')

# check data
print(template_occupancy.Room_ID.unique())
print(template_occupancy.Building_ID.unique())
print(template_occupancy.Occupancy_Measurement.unique())
