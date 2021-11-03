'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-62-Zoltan Nagy _ June Young Park

1. occupancy data
2. light switch data
3. indoor illuminance data
4. four different rooms in an academic office bulding
5. different time ranges
	Data_1: 5/7/2018 – 6/30/2018
	Data_2: 5/7/2018 – 6/30/2018
	Data_3: 5/16/2018 – 6/30/2018
	Data_4: 5/7/2018 – 6/30/2018
	Data_5: 5/12/2018 – 6/30/2018
'''

import os
import glob
import string
import datetime
import pandas as pd

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-62-Zoltan Nagy _ June Young Park/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-62-Zoltan Nagy _ June Young Park/_yapan_processing/'

# read templates into pandas
template_occupancy = pd.read_csv(template_path + 'Occupancy_Measurement.csv')
template_light = pd.read_csv(template_path + 'Ligthing_Status.csv')
template_indoor = pd.read_csv(template_path + 'Indoor_Measurement.csv')

# read data into pandas
data_1 = pd.read_csv(data_path + 'data_1.csv')
data_2 = pd.read_csv(data_path + 'data_2.csv')
data_3 = pd.read_csv(data_path + 'data_3.csv')
data_4 = pd.read_csv(data_path + 'data_4.csv')
data_5 = pd.read_csv(data_path + 'data_5.csv')

# change column names
col_names = ['Time', 'Ligthing_Status', 'Occupancy_Measurement', 'Indoor_Illuminance']
data_1.columns = col_names
data_2.columns = col_names
data_3.columns = col_names
data_4.columns = col_names
data_5.columns = col_names

# change the format of Time column
# data_1.dtypes
# add columns
data_1['Time'] = pd.to_datetime(data_1['Time'], format='%H:%M:%S').dt.time
data_1['Hour'] = pd.to_datetime(data_1['Time'], format='%H:%M:%S').dt.hour
data_1['Date'] = None

data_2['Time'] = pd.to_datetime(data_2['Time'], format='%H:%M:%S').dt.time
data_2['Hour'] = pd.to_datetime(data_2['Time'], format='%H:%M:%S').dt.hour
data_2['Date'] = None

data_3['Time'] = pd.to_datetime(data_3['Time'], format='%H:%M:%S').dt.time
data_3['Hour'] = pd.to_datetime(data_3['Time'], format='%H:%M:%S').dt.hour
data_3['Date'] = None

data_4['Time'] = pd.to_datetime(data_4['Time'], format='%H:%M:%S').dt.time
data_4['Hour'] = pd.to_datetime(data_4['Time'], format='%H:%M:%S').dt.hour
data_4['Date'] = None

data_5['Time'] = pd.to_datetime(data_5['Time'], format='%H:%M:%S').dt.time
data_5['Hour'] = pd.to_datetime(data_5['Time'], format='%H:%M:%S').dt.hour
data_5['Date'] = None

# data_1.shape
date_list_1 = pd.date_range('2018-05-07', '2018-07-01', freq='1D')
date_list_2 = date_list_1
date_list_3 = pd.date_range('2018-05-16', '2018-07-01', freq='1D')
date_list_4 = date_list_1
date_list_5 = pd.date_range('2018-05-12', '2018-07-01', freq='1D')

# data 1
# loop through the rows and compare the hours, assign the dates
begin_time = datetime.datetime.now()

date_index = 0
for index in range(data_1.shape[0]-1):
    data_1.loc[index, 'Date'] = date_list_1[date_index].date()
    if (data_1.loc[index, 'Hour'] == 23) and (data_1.loc[index + 1, 'Hour'] == 0):
        date_index += 1
        data_1.loc[index + 1, 'Date'] = date_list_1[date_index].date()
    else:
        data_1.loc[index + 1, 'Date'] = date_list_1[date_index].date()
        pass

# create Date_Time column based on Date and Time
data_1['Date_Time'] = data_1['Date'].astype(str) + ' ' + data_1['Time'].astype(str)
data_1['Date_Time'] = pd.to_datetime(data_1['Date_Time'], format="%Y-%m-%d %H:%M:%S")
print('Finished processing data 1')


# data 2
# loop through the rows and compare the hours, assign the dates
date_index = 0
for index in range(data_2.shape[0]-1):
    data_2.loc[index, 'Date'] = date_list_2[date_index].date()
    if (data_2.loc[index, 'Hour'] == 23) and (data_2.loc[index + 1, 'Hour'] == 0):
        date_index += 1
        data_2.loc[index + 1, 'Date'] = date_list_2[date_index].date()
    else:
        data_2.loc[index + 1, 'Date'] = date_list_2[date_index].date()

# create Date_Time column based on Date and Time
data_2['Date_Time'] = data_2['Date'].astype(str) + ' ' + data_2['Time'].astype(str)
data_2['Date_Time'] = pd.to_datetime(data_2['Date_Time'], format="%Y-%m-%d %H:%M:%S")
print('Finished processing data 2')

# data 3
# loop through the rows and compare the hours, assign the dates
date_index = 0
for index in range(data_3.shape[0]-1):
    data_3.loc[index, 'Date'] = date_list_3[date_index].date()
    if (data_3.loc[index, 'Hour'] == 23) and (data_3.loc[index + 1, 'Hour'] == 0):
        date_index += 1
        data_3.loc[index + 1, 'Date'] = date_list_3[date_index].date()
    else:
        data_3.loc[index + 1, 'Date'] = date_list_3[date_index].date()

# create Date_Time column based on Date and Time
data_3['Date_Time'] = data_3['Date'].astype(str) + ' ' + data_3['Time'].astype(str)
data_3['Date_Time'] = pd.to_datetime(data_3['Date_Time'], format="%Y-%m-%d %H:%M:%S")
print('Finished processing data 3')

# data 4
# loop through the rows and compare the hours, assign the dates
date_index = 0
for index in range(data_4.shape[0]-1):
    data_4.loc[index, 'Date'] = date_list_4[date_index].date()
    if (data_4.loc[index, 'Hour'] == 23) and (data_4.loc[index + 1, 'Hour'] == 0):
        date_index += 1
        data_4.loc[index + 1, 'Date'] = date_list_4[date_index].date()
    else:
        data_4.loc[index + 1, 'Date'] = date_list_4[date_index].date()

# create Date_Time column based on Date and Time
data_4['Date_Time'] = data_4['Date'].astype(str) + ' ' + data_4['Time'].astype(str)
data_4['Date_Time'] = pd.to_datetime(data_4['Date_Time'], format="%Y-%m-%d %H:%M:%S")
print('Finished processing data 4')

# data 5
# loop through the rows and compare the hours, assign the dates
date_index = 0
for index in range(data_5.shape[0]-1):
    data_5.loc[index, 'Date'] = date_list_5[date_index].date()
    if (data_5.loc[index, 'Hour'] == 23) and (data_5.loc[index + 1, 'Hour'] == 0):
        date_index += 1
        data_5.loc[index + 1, 'Date'] = date_list_5[date_index].date()
    else:
        data_5.loc[index + 1, 'Date'] = date_list_5[date_index].date()

# create Date_Time column based on Date and Time
data_5['Date_Time'] = data_5['Date'].astype(str) + ' ' + data_5['Time'].astype(str)
data_5['Date_Time'] = pd.to_datetime(data_5['Date_Time'], format="%Y-%m-%d %H:%M:%S")
print('Finished processing data 5')

print(f'Total running time: {datetime.datetime.now() - begin_time}')


# assign Room_ID, Building_ID, etc
data_1['Room_ID'] = 1
data_1['Building_ID'] = 1
data_2['Room_ID'] = 2
data_2['Building_ID'] = 1
data_3['Room_ID'] = 3
data_3['Building_ID'] = 1
data_4['Room_ID'] = 4
data_4['Building_ID'] = 1
data_5['Room_ID'] = 5
data_5['Building_ID'] = 1

# concat data based on the columns in the templates
occ_temp_df = template_occupancy  # temperate dataframe
light_temp_df = template_light  # temperate dataframe
indoor_temp_df = template_indoor  # temperate dataframe

''' data_1 '''
# data_1, first extract useful columns from processed data, then store them into template
occ_df = pd.concat([occ_temp_df, data_1], join='inner', ignore_index=True)  # only contains same columns, first column was dropped
light_df = pd.concat([light_temp_df, data_1], join='inner', ignore_index=True)
indoor_df = pd.concat([indoor_temp_df, data_1], join='inner', ignore_index=True)

template_occupancy = pd.concat([template_occupancy, occ_df], ignore_index=True) # concat to the template
template_light = pd.concat([template_light, light_df], ignore_index=True)
template_indoor = pd.concat([template_indoor, indoor_df], ignore_index=True)

''' data_2 '''
# data_2, first extract useful columns from processed data, then store them into template
occ_df = pd.concat([occ_temp_df, data_2], join='inner', ignore_index=True)  # only contains same columns, first column was dropped
light_df = pd.concat([light_temp_df, data_2], join='inner', ignore_index=True)
indoor_df = pd.concat([indoor_temp_df, data_2], join='inner', ignore_index=True)

template_occupancy = pd.concat([template_occupancy, occ_df], ignore_index=True) # concat to the template
template_light = pd.concat([template_light, light_df], ignore_index=True)
template_indoor = pd.concat([template_indoor, indoor_df], ignore_index=True)

''' data_3 '''
# data_3, first extract useful columns from processed data, then store them into template
occ_df = pd.concat([occ_temp_df, data_3], join='inner', ignore_index=True)  # only contains same columns, first column was dropped
light_df = pd.concat([light_temp_df, data_3], join='inner', ignore_index=True)
indoor_df = pd.concat([indoor_temp_df, data_3], join='inner', ignore_index=True)

template_occupancy = pd.concat([template_occupancy, occ_df], ignore_index=True) # concat to the template
template_light = pd.concat([template_light, light_df], ignore_index=True)
template_indoor = pd.concat([template_indoor, indoor_df], ignore_index=True)

''' data_4 '''
# data_4, first extract useful columns from processed data, then store them into template
occ_df = pd.concat([occ_temp_df, data_4], join='inner', ignore_index=True)  # only contains same columns, first column was dropped
light_df = pd.concat([light_temp_df, data_4], join='inner', ignore_index=True)
indoor_df = pd.concat([indoor_temp_df, data_4], join='inner', ignore_index=True)

template_occupancy = pd.concat([template_occupancy, occ_df], ignore_index=True) # concat to the template
template_light = pd.concat([template_light, light_df], ignore_index=True)
template_indoor = pd.concat([template_indoor, indoor_df], ignore_index=True)

''' data_5 '''
# data_5, first extract useful columns from processed data, then store them into template
occ_df = pd.concat([occ_temp_df, data_5], join='inner', ignore_index=True)  # only contains same columns, first column was dropped
light_df = pd.concat([light_temp_df, data_5], join='inner', ignore_index=True)
indoor_df = pd.concat([indoor_temp_df, data_5], join='inner', ignore_index=True)

template_occupancy = pd.concat([template_occupancy, occ_df], ignore_index=True) # concat to the template
template_light = pd.concat([template_light, light_df], ignore_index=True)
template_indoor = pd.concat([template_indoor, indoor_df], ignore_index=True)

# check dataframes
print(template_occupancy.columns)
print(template_light.columns)
print(template_indoor.columns)

print(template_occupancy.isnull().sum())
print(template_light.isnull().sum())
print(template_indoor.isnull().sum())

template_light['Lighting_Zone_ID'] = 1

print(template_occupancy.dtypes)
print(template_light.dtypes)
print(template_indoor.dtypes)

# assign data types
# using apply method
template_occupancy[['Occupancy_Measurement', 'Room_ID', 'Building_ID']] \
    = template_occupancy[['Occupancy_Measurement', 'Room_ID', 'Building_ID']].apply(pd.to_numeric)

template_light[['Ligthing_Status', 'Room_ID', 'Building_ID']] \
    = template_light[['Ligthing_Status', 'Room_ID', 'Building_ID']].apply(pd.to_numeric)

template_indoor[['Indoor_Illuminance', 'Room_ID', 'Building_ID']] \
    = template_indoor[['Indoor_Illuminance', 'Room_ID', 'Building_ID']].apply(pd.to_numeric)

# save data
template_occupancy.to_csv(save_path + 'Occupancy_Measurement.csv', index=False)
template_light.to_csv(save_path + 'Ligthing_Status.csv', index=False)
template_indoor.to_csv(save_path + 'Indoor_Measurement.csv', index=False)