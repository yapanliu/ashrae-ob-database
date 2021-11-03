'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-12-Mikkel Kjærgaard
Sub-folders created by Yapan:
    Indoor_Measurement
        combine all the information into one
    Occupancy_Measurement
        only consider the occupant_count_room_1.csv, 2, 3 files

1. vav_room_1.csv , 2, 3 don't fit into the templates, so they were not considered.
2. the challenge is how to assign day to the datetime info. because all the dayIDs were randomly assigned
    to anonymize the data.
3. combine all the same time csv files generated by step 2;
    fill missing values by -999
    Assign IDs for different rooms, buildings, plugs, windows, ...
'''

import os
import glob
import datetime
import pandas as pd
import random, calendar

# specify the path
data_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-12-Mikkel Kjærgaard/_yapan_processing/"


def randomDate(year, month, ndays):
    ''' This function generate a list of random workdays from a given specific month '''
    day_count = calendar.monthrange(year, month)[1]
    days = []
    # generate the days
    for day in range(0, ndays):
        while True:
            t = random.choice(pd.date_range(f"{year}-{month}-01", f"{year}-{month}-{day_count}", freq='D'))
            # check if the day is weekday and was different with previously generated days
            if t.dayofweek != 5 and t.dayofweek != 6 and t not in days:
                days.append(t)
                break
    days.sort()  # sort the days
    return days

'''
1. Indoor_Measurement
combine all the csv files by row, add them together from left side to right side
pick useful column and drop the other columns
generate date time, randomly pick day in that month from work day and assign it to dayID
'''

begin_time = datetime.datetime.now()

# set up the working directory
os.chdir(data_path + 'Indoor_Measurement')

# find all the files within this folder
all_filenames = [i for i in glob.glob('*.{}'.format('csv'))]

# pandas combine all the csv files wihtin the folder based on data type
combined_co2 = pd.concat([pd.read_csv(f) for f in all_filenames if 'co2' in f], axis=1)
combined_humidity = pd.concat([pd.read_csv(f) for f in all_filenames if 'humidity' in f], axis=1)
combined_Illuminance = pd.concat([pd.read_csv(f) for f in all_filenames if 'Illuminance' in f], axis=1)
combined_temperature = pd.concat([pd.read_csv(f) for f in all_filenames if 'temperature' in f], axis=1)

combined_co2 = combined_co2.loc[:, ~combined_co2.columns.duplicated()]  # remove the duplicated columns
# combined_co2['Workday'].unique()  # all are work days

# get the unique year, month and dayid
DayIds = combined_co2.groupby(['Year', 'Month', 'DayId']).size().reset_index(name = "Group_Count")
DayIds_count = DayIds.groupby(['Year', 'Month']).size().reset_index(name = "ndays")
# DayIds_count

# loop through DayIds_count and generate dates based on number of days
workdays = []
for i in range(0,DayIds_count.shape[0]):
    # get year, month and ndays
    year = DayIds_count.iloc[i,0]
    month = DayIds_count.iloc[i,1]
    ndays = DayIds_count.iloc[i,2]

    workdays.append(randomDate(year, month, ndays))  # get all the workdays

workdays = [item for sublist in workdays for item in sublist]  # flat the list of lists
# print(workdays)
DayIds['workday'] = workdays  # assign the randomly selected workday to the DayId


# iterate over the DayIds dataframe and assign work days to combined_co2
combined_co2['Workday'] = ""
for i in range(0,DayIds.shape[0]):
    # get year, month and ndays
    year = DayIds.iloc[i, 0]
    month = DayIds.iloc[i, 1]
    dayId = DayIds.iloc[i, 2]
    workday = DayIds.iloc[i, 4]
    # select specific days and assign workday datetime
    combined_co2.loc[(combined_co2['Year'] == year) & (combined_co2['Month'] == month) &
                     (combined_co2['DayId'] == dayId), 'Workday'] = workday

# added Time to Workdays, because all the Workdays at 00:00:00
combined_co2['Time'] = pd.to_datetime(combined_co2['Time'], format='%H:%M:%S').dt.time  # convert time only
combined_co2['Workday'] = pd.to_datetime(combined_co2['Workday'], format="%Y-%m-%d %H:%M:%S") # convert to date time
combined_co2['Date_Time'] = combined_co2['Workday'].astype(str) + ' ' + combined_co2['Time'].astype(str)  # add date and time
combined_co2['Date_Time'] = pd.to_datetime(combined_co2['Date_Time'], format="%Y-%m-%d %H:%M:%S")  # convert to datetime

combined_co2.to_csv(data_path + '_combined_co2.csv', index=False)

print(f'Total running time: {datetime.datetime.now() - begin_time}')

# check missing values, sum all the missing value counts by column
combined_indoor =  pd.read_csv(data_path+'Indoor_Measurement.csv')
combined_occupancy = pd.read_csv(data_path+'Occupancy_Measurement.csv')
print('Check missing values in : Indoor_Measurement.csv')
print(combined_indoor.isnull().sum())
print('Check missing values in : Occupancy_Measurement.csv')
print(combined_occupancy.isnull().sum())