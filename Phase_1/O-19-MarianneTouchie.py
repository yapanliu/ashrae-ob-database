# O-19-Marianne Touchie

'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-19-Marianne Touchie

1. one office building
2. Xuezheng processed outdoor and indoor data
3. use the same room ids as Xuezheng used for window and door status data
'''

import os
import glob
import string
import time
import datetime
import pandas as pd
import matplotlib.pyplot as plt

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-19-Marianne Touchie/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-19-Marianne Touchie/_yapan_processing/'

# read templates into pandas
template_occ_num = pd.read_csv(template_path + 'Occupant_Number_Measurement.csv')
template_indoor = pd.read_csv(template_path + 'Indoor_Measurement.csv')
template_hvac = pd.read_csv(template_path + 'HVAC_Measurement.csv')
template_window = pd.read_csv(template_path + 'Window_Status.csv')
template_door = pd.read_csv(template_path + 'Door_Status.csv')

# read data
combined_df = pd.read_excel(data_path+'Master Data File - Door operation (2).xlsx', sheet_name='MASTER')
room_ids = [' 1504 LivRm  Temp ', ' 1504 LrgBed Temp ', ' 1503 Hall Temp ',
            ' 1503 BthRm Temp ', ' 11th Floor Corridor Temp ',
            ' 12th Floor Corridor Temp ', ' 14th Floor Corridor Temp ',
            ' 1405 LivRm Temp ', ' 1403 LrgBed Temp ', ' 1403 LivRm Temp ',
            ' 1503 SmlBed Temp ', ' 1503 LrgBed Temp ', ' 1503 Kit Temp ',
            ' 1503 LivRm Inner Temp ', ' 1503 LivRm Outer Temp ',
            ' 1404 LrgBed Temp ', ' 1203 SmlBed Temp ', ' 1404 LivRm Temp ',
            ' 1203 LrgBed Temp ', ' 1203 Kit Temp ', ' 1203 LivRm Outer Temp ',
            ' 1203 LivRm Inner Temp ', ' 1203 Hall Temp ', ' 1105 Bed Temp ',
            ' 1105 BthRm Temp ', ' 1105 LivRm Outer Temp ', ' 1105 Kit Temp ',
            ' 1105 LivRm Inner Temp ', ' 1203 BthRm Temp ', ' 1105 Hall Temp ']

# combined_df.columns

# extract window info and door info
window_list = list(combined_df.filter(regex=r'(Wndw)', axis=1))  # columns of window info
window_df = combined_df[window_list].copy()

door_list = list(combined_df.filter(regex=r'(Dr)', axis=1))  # columns of door info
door_df = combined_df[door_list].copy()

# check data
print(window_df.columns)
print(door_df.columns)

print(window_df.isnull().sum())
print(door_df.isnull().sum())

# fill na
window_df.fillna(-999, inplace=True)  # fill na with -999
door_df.fillna(-999, inplace=True)
window_df.replace(['NULL', ' NULL '], [-999, -999], inplace=True)  # replace NULL with -999
door_df.replace(['NULL', ' NULL '], [-999, -999], inplace=True)
door_df.mask(door_df > 99999, -999, inplace=True)  # replace abnormal values with -999
# negative values to positive
window_df = window_df.abs()  # changed -999 to 999
door_df = door_df.abs()

window_df.replace([999], [-999], inplace=True)
door_df.replace([999], [-999], inplace=True)  # change back

''' process door status data '''
# change door status to 0 and 1
door_df[door_df > 0].quantile(0.1)  # if less than 10% quantile, then treat it as closed. Otherwise, open.

door_columns = list(door_df.columns)  # door IDs: 15, 21, 26
# 2nd column all negative values
door_df[door_columns[0]].mask((door_df[door_columns[0]] > 0.0) & (door_df[door_columns[0]] <= 2.0),
                              0, inplace=True)  # 0 is off
door_df[door_columns[0]].mask(door_df[door_columns[0]] > 2.0, 1, inplace=True)  # replace the positive values by 1 to standardize

door_df[door_columns[2]].mask((door_df[door_columns[2]] > 0.0) & (door_df[door_columns[2]] <= 3.0),
                              0, inplace=True)  # o is off
door_df[door_columns[2]].mask(door_df[door_columns[2]] > 3.0, 1, inplace=True)  # replace the positive values by 1 to standardize


# check converted values
print(door_df[door_columns[0]].unique())
print(door_df[door_columns[2]].unique())

# save data
door_df.to_csv(save_path+'_door_df.csv', index=False)

# check the door status values
plt.plot(door_df[door_columns[0]])
plt.plot(door_df[door_columns[1]])
plt.plot(door_df[door_columns[2]])

''' process window status data '''
window_columns = list(window_df.columns)  # door IDs: 15, 21, 26
# fill na
# window_df.replace(['NULL', ' NULL '], [-999, -999], inplace=True)
window_df.mask(window_df > 99999, -999, inplace=True)  # replace abnormal values with -999


# check the unique values of each column and judge if the column has dynamically change data
for name in window_columns: print(len(window_df[name].unique()))

for index, name in enumerate(window_columns):
    # change door status to 0 and 1
    # if less than 10% quantile, then treat it as closed. Otherwise, open.
    len_unique = len(window_df[name].unique())
    if len_unique > 1:
        off_value = window_df[window_df[name] > 0][name].quantile(0.1)  # get the threshold

        # replace the positive values by 1 to standardize
        window_df[name].mask((window_df[name] > 0.0) & (window_df[name] <= off_value), 0, inplace=True)
        # replace the positive values by 1 to standardize
        window_df[name].mask(window_df[name] > off_value, 1, inplace=True)




# check converted values
for name in window_columns: print(window_df[name].unique())

# save data
window_df.to_csv(save_path+'_window_df.csv', index=False)

# len(room_ids)
# room_ids[10]
window_ids_list = [[11,1], [12,1], [14,2], [14,1], [17,1], [19,1], [22,3], [22,2], [22,1], [24,1], [28,1], [28,2]]
for index, name in enumerate(window_columns):
    window_temp_df = pd.DataFrame()
    window_temp_df['Date_Time'] = combined_df[combined_df.columns[0]]
    window_temp_df['Window_Status'] = window_df[name]
    window_temp_df['Window_ID'] = window_ids_list[index][1]
    window_temp_df['Room_ID'] = window_ids_list[index][0]
    window_temp_df['Building_ID'] = 1

    template_window = pd.concat([template_window, window_temp_df], ignore_index=True)

# save data
template_window.to_csv(save_path+'Window_Status.csv', index=False)