'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-15-Cristina Piselli-Zeng

1. weather data stored in multiple sheets and needs to be combined
2. read the data and append to the templates

'''

import os
import glob
import datetime
import pandas as pd

# specify the path
data_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-15-Cristina Piselli-Zeng/_yapan_processing/"
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
begin_time = datetime.datetime.now()

'''
1. read the two excel files into pandas and clean the data
'''
# read the data from excel and combine all the worksheets
combined_indoor = pd.concat(pd.read_excel(data_path + 'LivingEAPLAB_indoor-dataset.xlsx', sheet_name=None),
                            ignore_index=True)
combined_outdoor = pd.concat(pd.read_excel(data_path + 'LivingEAPLAB_weather-dataset.xlsx', sheet_name=None),
                             ignore_index=True)

# check missing values, and sum missing value count by column
print('Check missing values in : LivingEAPLAB_indoor-dataset.xlsx')
print(combined_indoor.isnull().sum())
print('Check missing values in : LivingEAPLAB_weather-dataset.xlsx')
print(combined_outdoor.isnull().sum())

# print out rows which contain nan values
is_NaN = combined_indoor.isnull()
row_has_NaN = is_NaN.any(axis=1)
rows_with_NaN = combined_indoor[row_has_NaN]
print('rows with missing values')
print(rows_with_NaN)

# data cleaning
combined_indoor.drop(combined_indoor.columns[[2, 3, 4, 9]], axis=1, inplace=True)   # drop unused columns
# drop the ID and Datetime that have nan values
combined_indoor = combined_indoor[combined_indoor['ID_office'].notna() &
                                  combined_indoor['Date [dd/MM/yyyy hh:mm:ss]'].notna()].copy()
combined_indoor.fillna(value=-999, inplace=True)  # fill missing values with -999
combined_indoor.reset_index(drop=True, inplace=True)

# check if there are any duplicated headers
duplicates = combined_outdoor['Date'] != 'Date'
duplicates.unique()  # check if False exist
# combined_outdoor = combined_outdoor[combined_outdoor['Date'] != 'Date']  # remove duplicated headers
# combined_outdoor.columns

'''
2. save the data into templates
'''
# create roomInfo dataframe to assign room Ids
roomIds = list(combined_indoor['ID_office'].unique())
roomNums = list(range(1,6))
roomInfo = pd.DataFrame({'roomId': roomIds, 'roomNumber': roomNums})
# replace roomIds with roomNums
combined_indoor.replace(roomIds, roomNums, inplace=True)

# read templates into pandas
template_appliance = pd.read_csv(template_path+'Appliance_Usage.csv')
template_appliance['Room_ID'] = ''
template_door = pd.read_csv(template_path+'Door_Status.csv')
template_window = pd.read_csv(template_path+'Window_Status.csv')
template_indoor = pd.read_csv(template_path+'Indoor_Measurement.csv')
template_outdoor = pd.read_csv(template_path+'Outdoor_Measurement.csv')

''' 2.1 Appliance_Usage.csv'''
# adding data into templates
rowNum = combined_indoor.shape[0]
id1 = pd.Series([1] * rowNum)  # create a series of ids
id2 = pd.Series([2] * rowNum)  # create a series of ids

# append appliance usage data of datalogger 1
applicance_df = template_appliance.copy()
applicance_df['Date_Time'] = combined_indoor[combined_indoor.columns[1]]  # datetime
applicance_df['Electric_Power'] = combined_indoor['El-1 [A]']*230  # power usage
applicance_df['Appliance_ID'] = id1  # appliance ID
applicance_df['Room_ID'] = combined_indoor['ID_office']  # room ID
# concat the two dataframe
template_appliance = pd.concat([template_appliance, applicance_df], ignore_index=True, sort=False)

# append appliance usage data of datalogger 2
applicance_df = template_appliance.copy()
applicance_df['Date_Time'] = combined_indoor[combined_indoor.columns[1]]
applicance_df['Electric_Power'] = combined_indoor['El-2 [A]']*230
applicance_df['Appliance_ID'] = id2
applicance_df['Room_ID'] = combined_indoor['ID_office']

# concat the two dataframe
template_appliance = pd.concat([template_appliance, applicance_df], ignore_index=True, sort=False)

template_appliance.replace([-999*230, -999.0*230], [-999, -999], inplace=True)  # replace the scaled missing value
# double check: -999.0*230 in template_appliance.values or -999*230 in template_appliance.values

# final check before saving the data
print(template_appliance.isnull().sum())  # check null values in the dataframe
# template_appliance.dtypes
template_appliance['Date_Time'] = pd.to_datetime(template_appliance['Date_Time'], format="%Y-%m-%d %H:%M:%S")
template_appliance['Electric_Power'] = template_appliance['Electric_Power'].astype(str).astype(float)
template_appliance['Appliance_ID'] = template_appliance['Appliance_ID'].astype(str).astype(int)
template_appliance['Room_ID'] = template_appliance['Room_ID'].astype(str).astype(int)
template_appliance['Appliance_Usage_ID'] = ''

# save Appliance_Usage.csv
template_appliance.to_csv(data_path+'Appliance_Usage.csv', index=False)

''' 2.2 Door_Status.csv '''
# add data to the dataframe
template_door['Date_Time'] = combined_indoor[combined_indoor.columns[1]]
template_door['Door_Status'] = combined_indoor['Door open [-]']
template_door['Room_ID'] = combined_indoor['ID_office']
# check null values
print(template_door.isnull().sum())
template_door.fillna('', inplace=True)  # fill nan values with empty

# change type of the column
# template_door.dtypes
template_door['Date_Time'] = pd.to_datetime(template_door['Date_Time'], format="%Y-%m-%d %H:%M:%S")
template_door['Door_Status'] = template_door['Door_Status'].astype(int)
template_door['Room_ID'] = template_door['Room_ID'].astype(int)
print(template_door.isnull().sum())

# save Door_Status.csv
template_door.to_csv(data_path+'Door_Status.csv', index=False)

''' 2.3 Window_Status.csv '''
# add data to the dataframe
template_window['Date_Time'] = combined_indoor[combined_indoor.columns[1]]
template_window['Window_Status'] = combined_indoor['Win open [-]']
template_window['Room_ID'] = combined_indoor['ID_office']
# check null values
print(template_window.isnull().sum())
template_window.fillna('', inplace=True)  # fill nan values with empty

# change type of the column
# template_door.dtypes
template_window['Date_Time'] = pd.to_datetime(template_window['Date_Time'], format="%Y-%m-%d %H:%M:%S")
template_window['Window_Status'] = template_window['Window_Status'].astype(int)
template_window['Room_ID'] = template_window['Room_ID'].astype(int)
print(template_window.isnull().sum())

# save Door_Status.csv
template_window.to_csv(data_path+'Window_Status.csv', index=False)

print(f'Total running time: {datetime.datetime.now() - begin_time}')

''' 2.4 Indoor_Measurement.csv '''
# add data to the dataframe
template_indoor['Date_Time'] = combined_indoor[combined_indoor.columns[1]]
template_indoor['Indoor_Temp'] = combined_indoor['Air T [°C]']
template_indoor['Indoor_Illuminance'] = combined_indoor['Illum [lux]']
template_indoor['Room_ID'] = combined_indoor['ID_office']
# check null values
print(template_indoor.isnull().sum())  # check the missing values of columns which have data inside
template_indoor.fillna('', inplace=True)  # fill nan values with empty

# change type of the column
# template_door.dtypes
template_indoor['Date_Time'] = pd.to_datetime(template_indoor['Date_Time'], format="%Y-%m-%d %H:%M:%S")
template_indoor['Indoor_Temp'] = template_indoor['Indoor_Temp'].astype(float)
template_indoor['Indoor_Illuminance'] = template_indoor['Indoor_Illuminance'].astype(float)
template_indoor['Room_ID'] = template_indoor['Room_ID'].astype(int)
print(template_indoor.isnull().sum())

# save Door_Status.csv
template_indoor.to_csv(data_path+'Indoor_Measurement.csv', index=False)

''' 2.5 Outdoor_Measurement.csv '''
# add data to the dataframe
combined_outdoor.isnull().sum()  # no missing values in this dataframe
# drop unuseful columns
columns_list = list(combined_outdoor.filter(regex=r'(Max|Min|StDev|RisDir|RisVel|StdDevDir|Direct)', axis=1))  # drop columns contain Max, Min, StDev
combined_outdoor.drop(columns_list, axis=1, inplace=True)
print(combined_outdoor.columns)


# rename the columns same as templates
combined_outdoor.columns =  ['Date_Time', 'Outdoor_RH', 'Outdoor_Temp', 'Solar_Radiation', 'Wind_Direction',
                             'Wind_Speed', 'Precipitation']
template_outdoor = pd.concat([template_outdoor, combined_outdoor])

# check null values
print(template_outdoor.isnull().sum())  # check the missing values of columns which have data inside
template_outdoor.fillna('', inplace=True)  # fill nan values with empty

# change type of the column
# template_outdoor.dtypes
columns_list = list(combined_outdoor.filter(regex=r'(Outdoor_Temp|Outdoor_RH|Wind_Speed|Wind_Direction'
                                                  r'|Solar_Radiation|Precipitation)', axis=1))  # drop columns contain Max, Min, StDev
template_outdoor[columns_list] = template_outdoor[columns_list].apply(pd.to_numeric, axis=1)
template_outdoor['Date_Time'] = pd.to_datetime(template_outdoor['Date_Time'], format="%d/%m/%Y %H.%M")

# missing values were filled with -999999 in this dataset, need to be replaced with -999
# -999999 in combined_outdoor.values
template_outdoor.replace(-999999, -999, inplace=True)
template_outdoor.fillna('', inplace=True)  # fill nan values with empty
print(template_outdoor.isnull().sum())
# save Door_Status.csv
template_outdoor.to_csv(data_path+'Outdoor_Measurement.csv', index=False)

