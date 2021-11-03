'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-67-David Webber

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
import matplotlib.pyplot as plt

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-67-David Webber/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-67-David Webber/_yapan_processing/'

# read templates into pandas
template_occ_num = pd.read_csv(template_path + 'Occupant_Number_Measurement.csv')

# read data
combined_df = pd.read_csv(data_path+'observations.csv')

# manipulate data
combined_df.columns = ['Date_Time', 'dur', 'enter', 'exit']  # rename columns
combined_df = combined_df[['Date_Time', 'enter', 'exit']]
combined_df['Occupant_Number_Measurement'] = None
combined_df['Occupant_Number_Measurement_negative'] = None

combined_df['Room_ID'] = 1
combined_df['Building_ID'] = 1

# find null values and their locations
combined_df.isnull().sum()

# change data types
combined_df.dtypes
combined_df['Date_Time'] = pd.to_datetime(combined_df['Date_Time'], format="%Y-%m-%d %H:%M:%S")
combined_df = combined_df.sort_values(by='Date_Time', ascending=True)  # sort the dataframe by datetime
combined_df['Date'] = pd.to_datetime(combined_df['Date_Time'], format="%Y-%m-%d %H:%M:%S").dt.date

combined_df = combined_df[combined_df['Date_Time'] > pd.to_datetime('2019-11-22 23:59:59')].copy()   # drop first day data
combined_df.reset_index(drop=True, inplace=True)  # reset index


# loop through the rows and calculate the occupancy numbers
begin_time = datetime.datetime.now()

occ_num = 0  # initial number of people in the room
combined_df.loc[0, 'Occupant_Number_Measurement'] = 1  # first row of data
occ_num += 1

# processed - change negative to zero
for index in range(1, combined_df.shape[0]):
    occ_change = combined_df.loc[index, 'enter'] - combined_df.loc[index, 'exit']
    occ_num += occ_change
    combined_df.loc[index, 'Occupant_Number_Measurement'] = occ_num

    if combined_df.loc[index, 'Date'] > combined_df.loc[index-1, 'Date']:
        occ_num = 0
        combined_df.loc[index, 'Occupant_Number_Measurement'] = occ_num

    else:
        pass
    # deal with negative values
    if combined_df.loc[index, 'Occupant_Number_Measurement'] < 0:
        combined_df.loc[index, 'Occupant_Number_Measurement'] = 0
        occ_num = combined_df.loc[index, 'Occupant_Number_Measurement']
    else:
        pass

# unprocessed - keep negative
occ_num = 0  # initial number of people in the room
combined_df.loc[0, 'Occupant_Number_Measurement_negative'] = 1  # first row of data
occ_num += 1
for index in range(1, combined_df.shape[0]):
    occ_change = combined_df.loc[index, 'enter'] - combined_df.loc[index, 'exit']
    occ_num += occ_change
    combined_df.loc[index, 'Occupant_Number_Measurement_negative'] = occ_num

    if combined_df.loc[index, 'Date'] > combined_df.loc[index-1, 'Date']:
        occ_num = 0
        combined_df.loc[index, 'Occupant_Number_Measurement_negative'] = occ_num

    else:
        pass

# plot results
plt.plot(combined_df['Occupant_Number_Measurement_negative'], linewidth=1, label="unprocessed")
plt.plot(combined_df['Occupant_Number_Measurement'], linewidth=1, label="Processed")
plt.legend()
plt.show()


# save data
combined_df.to_csv(save_path + 'combined_df.csv', index=False)

''' manually process in excel'''
