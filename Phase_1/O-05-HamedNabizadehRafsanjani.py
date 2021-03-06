'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-05-Hamed Nabizadeh Rafsanjani
Sub-folders:
    NIOLM data
    Plug-in meters data - unprocessed
    Smart meter data - unprocessed

1. this code will read all the immediate subfolders under root path,
    and list out all the empty folders, nonempty folders.
2. combine all the csv files within the same folder
3. combine all the same time csv files generated by step 2;
    fill missing values by -999
    Assign IDs for different rooms, buildings, plugs, windows, ...
'''

import os
import glob
import datetime
import subprocess as sp
import pandas as pd

# specify the path
root_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-05-Hamed Nabizadeh Rafsanjani/'
processed_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-05-Hamed Nabizadeh Rafsanjani/_yapan_processing/'

'''
1. NIOLM data
This dataset is the processed results... Not raw data
combine csv files using pandas takes a lot of time, should use windows shell command
'''
# # set up the working directory
# os.chdir(root_path+'NIOLM data')
#
# # find all the files within this folder
# all_filenames = [i for i in glob.glob('*.{}'.format('csv'))]
#
# # pandas combine all the csv files wihtin the folder
# combined_csv = pd.concat([pd.read_csv(f) for f in all_filenames ])

'''
2. Plug-in meters data
Question: How to normalize the datetime? What is the timezone of the data?
'''

# begin_time = datetime.datetime.now()
# # this folder has 1-6 sub-folders, combine all the data under each sub-folder, assign the id as subfolder name
# for index in range(1,7):
#     # folder index
#     print(f'Dealing with data under folder {index}')
#
#     # set up the working directory
#     os.chdir(root_path+f'Plug-in meters data - unprocessed/{index}')
#     # combine all the csv files under the working directory
#     file = "_combined.csv"
#     combine_cmd = f"copy *.csv {file}"
#     os.system(f'{combine_cmd}') # combine all the csv into one csv files
#     print("Successfully combined all the csv files!")
#
#     # read combined csv into pd dataframe
#     combined_csv = pd.read_csv(file)
#     combined_csv = combined_csv[combined_csv['DATE/TIME'] != 'DATE/TIME']  # drop duplicated headers caused by cmd merge
#     # delete the generated file after using
#     del_cmd = f"del {file}"
#     os.system(f'{del_cmd}') # delete the generated csv files after importing into pandas
#     print("Deleted combined csv file!")
#
#     # select desire data
#     cols_to_keep = ['DATE/TIME', 'WATTS']
#     combined_csv = combined_csv.loc[:, cols_to_keep]
#     combined_csv.columns = ['Date_Time', 'Electric_Power'] # rename the columns as standard
#     # clean data, remove last wired row and duplicates
#     combined_csv = combined_csv[combined_csv['Date_Time'].notna()]
#     combined_csv = combined_csv.drop_duplicates()
#
#     combined_csv['Date_Time'] = pd.to_datetime(combined_csv['Date_Time'], format="%Y-%m-%d %H:%M:%S") # convert object to datetime
#     combined_csv['Appliance_ID'] = index # assign the ID to this data
#     # breakpoint()
#     # save data to a csv file
#     combined_csv.to_csv(processed_path+f'Plug-in meters data - combined-{index}.csv', index=False)
#
# print(f'Total running time: {datetime.datetime.now() - begin_time}')


'''
3. Smart meter data
'''
# begin_time = datetime.datetime.now()
# # set up the working directory
# os.chdir(root_path+'Smart meter data - unprocessed')
# # combine all the csv files under the working directory
# file = "_combined.csv"
# combine_cmd = f"copy *.csv {file}"
# os.system(f'{combine_cmd}') # combine all the csv into one csv files
# print("Successfully combined all the csv files!")
#
# # read combined csv into pd dataframe
# combined_csv = pd.read_csv(file)
# print(f'Combined_csv columns: {combined_csv.columns}')
# combined_csv = combined_csv[combined_csv[' Time'] != ' Time']  # drop duplicated headers caused by cmd merge
# # delete the generated file after using
# del_cmd = f"del {file}"
# os.system(f'{del_cmd}') # delete the generated csv files after importing into pandas
# print("Deleted combined csv file!")
# # combined_csv.dtypes
#
# # keep the desired data
# cols_to_keep = [combined_csv.columns[0], combined_csv.columns[1], combined_csv.columns[2]]
# combined_csv = combined_csv.loc[:, cols_to_keep]
# combined_csv.columns = ['Appliance_ID', 'Date_Time', 'Electric_Power'] # rename the columns as standard
# combined_csv = combined_csv[['Date_Time', 'Electric_Power', 'Appliance_ID']]
# combined_csv = combined_csv[combined_csv['Date_Time'].notna()]
# combined_csv = combined_csv.drop_duplicates()
# # breakpoint()
# # specify the format of the time could significantly speed up the processing time
# combined_csv['Date_Time'] = pd.to_datetime(combined_csv['Date_Time'], format="%m/%d/%Y %H:%M:%S") # convert object to datetime
# # save data
# combined_csv.to_csv(processed_path+f'Smart meter data - combined.csv', index=False)
#
# print(f'Total running time: {datetime.datetime.now() - begin_time}')
#

'''
Final step - combine all the sheets; do a round of final data cleaning
'''
begin_time = datetime.datetime.now()
# set up the working directory
os.chdir(processed_path)
# combine all the csv files under the working directory
file = "_combined.csv"
combine_cmd = f"copy *.csv {file}"
os.system(f'{combine_cmd}') # combine all the csv into one csv files
print("Successfully combined all the csv files!")

# read combined csv into pd dataframe
combined_csv = pd.read_csv(file)
print(f'Combined_csv columns: {combined_csv.columns}')
combined_csv = combined_csv[combined_csv['Date_Time'] != 'Date_Time']  # drop duplicated headers caused by cmd merge
# or use
# combined_csv.drop(combined_csv.tail(1).index, inplace=True)  # drop the last row generated by cmd combine operation
# delete the generated file after using
del_cmd = f"del {file}"
os.system(f'{del_cmd}') # delete the generated csv files after importing into pandas
print("Deleted combined csv file!")

# keep the desired data
combined_csv.columns = ['Date_Time', 'Electric_Power', 'Appliance_ID'] # rename the columns as standard
combined_csv = combined_csv[combined_csv['Appliance_ID'].notna()]
combined_csv = combined_csv.drop_duplicates()
# combined_csv.dtypes

# change data types from object to datetime, float, int...
combined_csv = combined_csv.replace({'MTU1': 7})  # Assign the ID to MTU1
combined_csv['Appliance_ID'] = combined_csv['Appliance_ID'].astype(str).astype(int)
combined_csv['Electric_Power'] = combined_csv['Electric_Power'].astype(str).astype(float)
combined_csv['Appliance_Usage_ID'] = ''
combined_csv['Date_Time'] = pd.to_datetime(combined_csv['Date_Time'], format="%Y-%m-%d %H:%M:%S")
combined_csv = combined_csv[['Appliance_Usage_ID', 'Date_Time', 'Electric_Power', 'Appliance_ID']] # re-order columns

# check missing values, sum all the missing value counts by column
print('Check missing values: ')
print(combined_csv.isnull().sum())
# save data
combined_csv.to_csv(processed_path+f'_final_all_in_one-to-SQL.csv', index=False)

print(f'Total running time: {datetime.datetime.now() - begin_time}')
print("Finished this data folder! Congrats!")
