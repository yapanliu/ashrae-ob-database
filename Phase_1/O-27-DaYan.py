'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-27-Da Yan

semi-automate code, needs some hands work. LOL But God is so good to me.

1. 9 different buildings in this dataset, and each building has different rooms
3. each room has different window, door, ac, indoor, outdoor info
4. I processed building A to F by hand, then figured out that I can rename the files first, then use code to process
5. rename the file by type and number, such as window1, indoor1, ac1, door1, etc.
6. code automated G, H, I
7. the folder has multiple types of data, csv and xlsx, figure out the file type, then rean into pandas
8. concat the outdoor datetime and temperature with ac data, then judge if the ac is on or off

'''

import os
import glob
import string
import datetime
import pandas as pd
import matplotlib.pyplot as plt

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-27-Da Yan/_yapan_processing/processed/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-27-Da Yan/_yapan_processing/_sql/'

# generate the name of different building folders
alphabet_string = string.ascii_uppercase
alphabet_list = list(alphabet_string)
building_names = alphabet_list[:9]

''' 1. process data by folders '''
begin_time = datetime.datetime.now()

# create dataframe to store the data
combined_window = pd.DataFrame()
combined_door = pd.DataFrame()
combined_hvac = pd.DataFrame()
combined_indoor = pd.DataFrame()
combined_outdoor = pd.DataFrame()

''' process outdoor data '''
print(f'Process outdoor data')
os.chdir(data_path)  # pwd
sub_folders = next(os.walk('.'))[1]  # get the names of the child directories, different rooms
root_files = next(os.walk('.'))[2]  # get the files under root path

outdoor_files = list(filter(lambda name: 'outdoor_building' in name, root_files))  # filter out the door status files
combined_outdoor = pd.concat([pd.read_csv(f) for f in outdoor_files])

''' manual processed data '''
print(f'Process manually processed data')
building_names_1 = building_names[:6]
# unit test
# i = 0
# folder_name = building_names_1[i]

for index, bld_name in enumerate(building_names_1):
    print(f'Reading the data under building folder {bld_name}')
    building_path = data_path + bld_name + '/'
    os.chdir(building_path)  # pwd
    sub_folders = next(os.walk('.'))[1]  # get the names of the child directories, different rooms
    root_files = next(os.walk('.'))[2]  # get the files under root path

    # combine
    indoor_files = list(filter(lambda name: 'indoor' in name, root_files))  # filter out the indoor files
    window_files = list(filter(lambda name: 'window' in name, root_files))  # filter out the window files
    hvac_files = list(filter(lambda name: 'hvac' in name, root_files))  # filter out the ac files
    door_files = list(filter(lambda name: 'door_status' in name, root_files))  # filter out the door status files

    # read anc combine the files under this folder
    if indoor_files:  # make sure it is not empty
        indoor_temp_df = pd.concat([pd.read_csv(f) for f in indoor_files])
        combined_indoor = pd.concat([combined_indoor, indoor_temp_df], ignore_index=True)  # concat the data
    else:
        pass
    if window_files:
        window_temp_df = pd.concat([pd.read_csv(f) for f in window_files])
        combined_window = pd.concat([combined_window, window_temp_df], ignore_index=True)  # concat the data
    else:
        pass
    if hvac_files:
        hvac_temp_df = pd.concat([pd.read_csv(f) for f in hvac_files])
        combined_hvac = pd.concat([combined_hvac, hvac_temp_df], ignore_index=True)  # concat the data
        # print(combined_hvac.isnull().sum())
        # print(index)
    else:
        pass
    if door_files:
        door_temp_df = pd.concat([pd.read_csv(f) for f in door_files])
        combined_door = pd.concat([combined_door, door_temp_df], ignore_index=True)  # concat the data
        # print(combined_door.isnull().sum())
        # print(index)
    else:
        pass

''' auto mated process by building level '''
building_names = ['G', 'H', 'I']
building_ids = [7, 8, 9]

for index, bld_name in enumerate(building_names):
    print(f'Dealing with data under building folder {bld_name}')
    building_path = data_path + bld_name + '/'
    os.chdir(building_path)  # pwd
    sub_folders = next(os.walk('.'))[1]  # get the names of the child directories, different rooms
    root_files = next(os.walk('.'))[2]  # get the files under root path

    '''' room level '''
    for room_id in sub_folders:
        print(f'Dealing with data under room folder {room_id}')
        room_path = building_path + room_id + '/'
        os.chdir(room_path)  # pwd
        file_names = os.listdir()  # get all the file names
        window_files = list(filter(lambda name: 'window' in name, file_names))  # filter out the window files
        hvac_files = list(filter(lambda name: 'ac' in name, file_names))  # filter out the ac files
        door_files = list(filter(lambda name: 'door' in name, file_names))  # filter out the door files

        # read and combine files
        if window_files:
            for window_name in window_files:
                name, extension = os.path.splitext(window_name)  # get the path and extension of a file
                if extension == '.CSV':  # if the file is csv file
                    temp_df = pd.read_csv(window_name, usecols=[0, 1])
                    temp_df.columns = ['Date_Time', 'Window_Status']  # rename the columns
                else:
                    temp_df = pd.read_excel(window_name, usecols=[0, 1])
                    temp_df.columns = ['Date_Time', 'Window_Status']

                temp_df['Window_ID'] = int(name.split('_')[0][6:])
                temp_df['Room_ID'] = int(room_id)  # assign Room_ID
                temp_df['Building_ID'] = building_ids[index]  # assign Building_ID

                combined_window = pd.concat([combined_window, temp_df], ignore_index=True)  # concat the data
        else:
            pass

        if door_files:
            for door_name in door_files:
                name, extension = os.path.splitext(door_name)  # get the path and extension of a file
                if extension == '.CSV':  # if the file is csv file
                    temp_df = pd.read_csv(door_name, usecols=[0, 1])
                    temp_df.columns = ['Date_Time', 'Door_Status']  # rename the columns
                else:
                    temp_df = pd.read_excel(door_name, usecols=[0, 1])
                    temp_df.columns = ['Date_Time', 'Door_Status']

                temp_df['Door_ID'] = int(name.split('_')[0][4:])
                temp_df['Room_ID'] = int(room_id)  # assign Room_ID
                temp_df['Building_ID'] = building_ids[index]  # assign Building_ID

                combined_door = pd.concat([combined_door, temp_df], ignore_index=True)  # concat the data
        else:
            pass

        if hvac_files:
            for hvac_name in hvac_files:
                name, extension = os.path.splitext(hvac_name)  # get the path and extension of a file
                if extension == '.CSV':  # if the file is csv file
                    temp_df = pd.read_csv(hvac_name, usecols=[0, 1])
                    temp_df.columns = ['Date_Time', 'yapan_supply _t']
                else:
                    temp_df = pd.read_excel(hvac_name, usecols=[0, 1])
                    temp_df.columns = ['Date_Time', 'yapan_supply _t']

                temp_df['HVAC_Zone_ID'] = int(name.split('_')[0][2:])  # get the number of ac
                temp_df['Room_ID'] = int(room_id)  # assign Room_ID
                temp_df['Building_ID'] = building_ids[index]  # assign Building_ID

                combined_hvac = pd.concat([combined_hvac, temp_df], ignore_index=True)  # concat the data
        else:
            pass

# drop na rows when specific column is null
combined_indoor = combined_indoor[combined_indoor['Date_Time'].notnull()]
combined_outdoor = combined_outdoor[combined_outdoor['Date_Time'].notnull()]
combined_window = combined_window[combined_window['Date_Time'].notnull()]
combined_door = combined_door[combined_door['Date_Time'].notnull()]
combined_hvac = combined_hvac[combined_hvac['Date_Time'].notnull()]

# process windows, door open/close data
combined_door['Door_Status'] = combined_door['Door_Status'].replace([0, 1, 2], [1, 0, 0])
combined_window['Window_Status'] = combined_window['Window_Status'].replace([0, 1, 2], [1, 0, 0])

# format datetime
print("Formatting datetime!")
combined_indoor['Date_Time'] = pd.to_datetime(combined_indoor['Date_Time'], format='%m/%d/%Y %H:%M')
combined_outdoor['Date_Time'] = pd.to_datetime(combined_outdoor['Date_Time'], format='%m/%d/%Y %H:%M')
combined_window['Date_Time'] = pd.to_datetime(combined_window['Date_Time'], infer_datetime_format=True)
combined_door['Date_Time'] = pd.to_datetime(combined_door['Date_Time'], infer_datetime_format=True)
combined_hvac['Date_Time'] = pd.to_datetime(combined_hvac['Date_Time'], infer_datetime_format=True)

# format data type
print(combined_indoor.dtypes)
print(combined_outdoor.dtypes)
print(combined_window.dtypes)
print(combined_door.dtypes)
print(combined_hvac.dtypes)

combined_indoor['Building_ID'] = combined_indoor['Building_ID'].astype(int)
combined_indoor['Room_ID'] = combined_indoor['Room_ID'].astype(int)

combined_outdoor['Building_ID'] = combined_outdoor['Building_ID'].astype(int)

combined_window['Building_ID'] = combined_window['Building_ID'].astype(int)
combined_window['Room_ID'] = combined_window['Room_ID'].astype(int)
combined_window['Window_ID'] = combined_window['Window_ID'].astype(int)

combined_door['Building_ID'] = combined_door['Building_ID'].astype(int)
combined_door['Room_ID'] = combined_door['Room_ID'].astype(int)
combined_door['Door_ID'] = combined_door['Door_ID'].astype(int)

combined_hvac['Building_ID'] = combined_hvac['Building_ID'].astype(int)
combined_hvac['Room_ID'] = combined_hvac['Room_ID'].astype(int)
combined_hvac['HVAC_Zone_ID'] = combined_hvac['HVAC_Zone_ID'].astype(int)

# replace null with empty


# # check combined data
# print('check null values')
# print(combined_window.isnull().sum())
# print(combined_door.isnull().sum())
# print(combined_hvac.isnull().sum())
#
# # check the unique IDs
# print(combined_window.Window_ID.unique())
# print(combined_door.Door_ID.unique())
# print(combined_hvac.HVAC_Zone_ID.unique())
#
# print(combined_hvac.Building_ID.unique())
# print(combined_window.Building_ID.unique())
# print(combined_door.Building_ID.unique())

# save data
combined_indoor.to_csv(save_path + 'combined_indoor.csv', index=False)
combined_outdoor.to_csv(save_path + 'combined_outdoor.csv', index=False)
combined_window.to_csv(save_path + 'combined_window.csv', index=False)
combined_door.to_csv(save_path + 'combined_door.csv', index=False)
combined_hvac.to_csv(save_path + 'combined_hvac.csv', index=False)

''' read templates and save data into the standard templates '''
# data
combined_indoor = pd.read_csv(save_path + 'combined_indoor.csv')
combined_outdoor = pd.read_csv(save_path + 'combined_outdoor.csv')
combined_window = pd.read_csv(save_path + 'combined_window.csv')
combined_door = pd.read_csv(save_path + 'combined_door.csv')
combined_hvac = pd.read_csv(save_path + 'combined_hvac.csv')

# templates
# read templates into pandas
template_window = pd.read_csv(template_path+'Window_Status.csv')
template_door = pd.read_csv(template_path+'Door_Status.csv')
template_hvac = pd.read_csv(template_path+'HVAC_Measurement.csv')
template_indoor = pd.read_csv(template_path+'Indoor_Measurement.csv')
template_outdoor = pd.read_csv(template_path+'Outdoor_Measurement.csv')

# columns
print(template_window.columns)
print(combined_window.columns)

print(template_door.columns)
print(combined_door.columns)

print(template_hvac.columns)
print(combined_hvac.columns)

print(template_indoor.columns)
print(combined_indoor.columns)

print(template_outdoor.columns)
print(combined_outdoor.columns)

# concat data
template_window = pd.concat([template_window, combined_window], ignore_index=True)
template_door = pd.concat([template_door, combined_door], ignore_index=True)
template_hvac = pd.concat([template_hvac, combined_hvac], ignore_index=True)
template_indoor = pd.concat([template_indoor, combined_indoor], ignore_index=True)
template_outdoor = pd.concat([template_outdoor, combined_outdoor], ignore_index=True)

template_door = template_door.drop(columns=['Study_ID'])
template_outdoor = template_outdoor.drop(columns=['Buiulding_ID'])
# columns
print(template_window.columns)
print(template_door.columns)
print(template_hvac.columns)
print(template_indoor.columns)
print(template_outdoor.columns)

# data types
print(template_window.dtypes)
print(template_door.dtypes)
print(template_hvac.dtypes)
print(template_indoor.dtypes)
print(template_outdoor.dtypes)


# format datetime
print("Formatting datetime!")
template_indoor['Date_Time'] = pd.to_datetime(template_indoor['Date_Time'], format='%Y-%m-%d %H:%M:%S')
template_outdoor['Date_Time'] = pd.to_datetime(template_outdoor['Date_Time'], format='%Y-%m-%d %H:%M:%S')
template_window['Date_Time'] = pd.to_datetime(template_window['Date_Time'], format='%Y-%m-%d %H:%M:%S')
template_door['Date_Time'] = pd.to_datetime(template_door['Date_Time'], format='%Y-%m-%d %H:%M:%S')
template_hvac['Date_Time'] = pd.to_datetime(template_hvac['Date_Time'], format='%Y-%m-%d %H:%M:%S')

# format data types
template_indoor['Building_ID'] = template_indoor['Building_ID'].astype(int)
template_indoor['Room_ID'] = template_indoor['Room_ID'].astype(int)

template_outdoor['Building_ID'] = template_outdoor['Building_ID'].astype(int)

template_window['Building_ID'] = template_window['Building_ID'].astype(int)
template_window['Room_ID'] = template_window['Room_ID'].astype(int)
template_window['Window_ID'] = template_window['Window_ID'].astype(int)

template_door['Building_ID'] = template_door['Building_ID'].astype(int)
template_door['Room_ID'] = template_door['Room_ID'].astype(int)
template_door['Door_ID'] = template_door['Door_ID'].astype(int)

template_hvac['Building_ID'] = template_hvac['Building_ID'].astype(int)
template_hvac['Room_ID'] = template_hvac['Room_ID'].astype(int)
template_hvac['HVAC_Zone_ID'] = template_hvac['HVAC_Zone_ID'].astype(int)

# save data
template_window.to_csv(save_path+'Window_Status.csv', index=False)
template_door.to_csv(save_path+'Door_Status.csv', index=False)
template_hvac.to_csv(save_path+'HVAC_Measurement.csv', index=False)
template_indoor.to_csv(save_path+'Indoor_Measurement.csv', index=False)
template_outdoor.to_csv(save_path+'Outdoor_Measurement.csv', index=False)

# check the unique room ids and building ids
print(template_window['Room_ID'].unique())
print(template_window['Building_ID'].unique())

print(template_door['Room_ID'].unique())
print(template_door['Building_ID'].unique())

print(template_hvac['Room_ID'].unique())
print(template_hvac['Building_ID'].unique())

print(template_indoor['Room_ID'].unique())
print(template_indoor['Building_ID'].unique())

print(template_outdoor['Building_ID'].unique())

''' convert ac measurement to on/off status '''
# read data
template_hvac = pd.read_csv(save_path+'HVAC_Measurement.csv')
template_outdoor = pd.read_csv(save_path+'Outdoor_Measurement.csv')

# check columns
print(template_hvac.columns)
print(template_outdoor.columns)

# check the buildings have ac data and outdoor data
template_hvac.groupby(['Room_ID', 'Building_ID']).size().reset_index()
template_outdoor.groupby(['Building_ID']).size().reset_index()

# check datetime
template_hvac['Date_Time']
template_outdoor['Date_Time']

# merge two dataframes together
hvac_df = template_hvac.merge(template_outdoor, how='left', on=['Building_ID', 'Date_Time'])

# use below two columns to calculate ac status
# hvac_df[['yapan_supply _t', 'Outdoor_Temp']]
hvac_df = hvac_df[hvac_df['Outdoor_Temp'].notnull()]
hvac_df['Cooling_Status'] = hvac_df.loc[:, 'Outdoor_Temp'] - hvac_df.loc[:, 'yapan_supply _t']
# convert negative values to 0-off, positive values to 1-on
hvac_df.loc[hvac_df['Cooling_Status'] < 0, 'Cooling_Status'] = 0
hvac_df.loc[hvac_df['Cooling_Status'] > 0, 'Cooling_Status'] = 1

# save data
cols = list(template_hvac)  # get the column names as a slit
hvac_df = hvac_df[cols]  # keep only desired columns
hvac_df.drop(['yapan_supply _t'], axis=1, inplace=True)  # drop a column

hvac_df.to_csv(save_path+'/final/HVAC_Measurement.csv', index=False)











