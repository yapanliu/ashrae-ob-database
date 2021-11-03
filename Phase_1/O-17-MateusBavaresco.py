'''
This code will clean the OB datasets and combine all the cleaned data into one
Dataset name: O-17-Mateus Bavaresco

1. two excel files for room 1 and room 2
2. each excel file has multiple sheets in it
3. extract different information from the excel file
4. store data in the templates

'''

import os
import glob
import datetime
import pandas as pd

# specify the path
data_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/2021-05-28-1130-raw-data/Annex 79 Data Collection/O-17-Mateus Bavaresco/_yapan_processing/"
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
begin_time = datetime.datetime.now()

'''
1. read the two excel files into pandas and clean the data
'''
# read the data from excel all in one
combined_room1 = pd.ExcelFile(data_path + 'Room 01 modified.xlsx')
combined_room2 = pd.ExcelFile(data_path + 'Room 02 modified.xlsx')

# parse the data
sheet_names1 = combined_room1.sheet_names  # get the sheet names in the excel file
sheet_names2 = combined_room2.sheet_names  # get the sheet names in the excel file

# filter out the desired data and combine them
window1 = list(filter(lambda name: 'Wind-' in name, sheet_names1))
light1 = list(filter(lambda name: 'Light-' in name, sheet_names1))
ac1 = list(filter(lambda name: 'AC-' in name, sheet_names1))
indoor1 = list(filter(lambda name: 'Central' in name, sheet_names1))
outdoor1 = list(filter(lambda name: 'Outdoor' in name, sheet_names1))

window2 = list(filter(lambda name: 'Window' in name, sheet_names2))
light2 = list(filter(lambda name: 'Light-' in name, sheet_names2))
indoor2 = list(filter(lambda name: 'Central' in name, sheet_names2))
outdoor2 = list(filter(lambda name: 'Outdoor' in name, sheet_names2))

''' 2. Data Processing'''
# read templates into pandas
template_window = pd.read_csv(template_path+'Window_Status.csv')
template_light = pd.read_csv(template_path+'Ligthing_Status.csv')
template_hvac = pd.read_csv(template_path+'HVAC_Measurement.csv')
template_indoor = pd.read_csv(template_path+'Indoor_Measurement.csv')
template_outdoor = pd.read_csv(template_path+'Outdoor_Measurement.csv')


''' 2.1 Window_Status.csv '''
# read and combine data by category and add IDs when combining
window_combined = pd.DataFrame()
# combine data from room 1 and assign room ID
for index, name in enumerate(window1):
    temp_df = pd.read_excel(combined_room1, sheet_name=name)
    temp_df['Window_ID'] = index+1
    temp_df['Room_ID'] = 1
    window_combined = pd.concat([window_combined, temp_df], ignore_index=True)
    # print(index)

# combine data from room 2 and assign room ID
for index, name in enumerate(window2):
    temp_df = pd.read_excel(combined_room2, sheet_name=name)
    temp_df['Window_ID'] = index+1
    temp_df['Room_ID'] = 2
    window_combined = pd.concat([window_combined, temp_df], ignore_index=True)
    # print(index)

# this column has mixed datetime and string data, convert all to datetime
window_combined.DATE = pd.to_datetime(window_combined['DATE'], infer_datetime_format=True)
# combine date and time columns together
window_combined['Date_Time'] = window_combined['DATE'].astype(str) + ' ' + window_combined['TIME'].astype(str)  # add date and time
window_combined['Date_Time'] = pd.to_datetime(window_combined['Date_Time'], format="%Y-%m-%d %H:%M:%S")  # convert to datetime
window_combined = window_combined[['Date_Time', 'STATUS', 'Window_ID', 'Room_ID']]  # re-order columns
window_combined.columns = ['Date_Time', 'Window_Status', 'Window_ID', 'Room_ID']  # rename the column names
window_combined = window_combined.replace(['OPEN', 'open', 'CLOSED', 'Closed'], [1, 1, 0, 0], inplace=False)  # convert window status to values
window_combined['Window_Status'].unique()  # check if all the text has been replaced

# concat the combined data to the template
template_window = pd.concat([template_window, window_combined], ignore_index=True)
# assign data type to each columns
# template_window.dtypes
template_window['Window_Status_ID'] = ''
template_window['Window_Status'] = template_window['Window_Status'].astype(int)
template_window['Window_ID'] = template_window['Window_ID'].astype(int)
template_window['Room_ID'] = template_window['Room_ID'].astype(int)

# sort the dataframe
# cannot sort by three columns by ascending, because of the Date_Time
# template_window.sort_values(by=['Date_Time', 'Window_ID', 'Room_ID'], ascending=True)

# check missing values, and sum missing value count by column
print('Check missing values in : window_combined')
print(template_window.isnull().sum())

# save
# save Window_Status.csv
template_window.to_csv(data_path+'Window_Status.csv ', index=False)



''' 2.2 Ligthing_Status.csv '''
# read and combine data by category and add IDs when combining
light_combined = pd.DataFrame()
# combine data from room 1 and assign room ID
for index, name in enumerate(light1):
    temp_df = pd.read_excel(combined_room1, sheet_name=name)
    temp_df['Lighting_Zone_ID'] = index+1
    temp_df['Room_ID'] = 1
    light_combined = pd.concat([light_combined, temp_df], ignore_index=True)
    # print(index)

# combine data from room 2 and assign room ID
for index, name in enumerate(light2):
    temp_df = pd.read_excel(combined_room2, sheet_name=name)
    temp_df['Lighting_Zone_ID'] = index+1
    temp_df['Room_ID'] = 2
    light_combined = pd.concat([light_combined, temp_df], ignore_index=True)
    # print(index)

# this column has mixed datetime and string data, convert all to datetime
light_combined.DATE = pd.to_datetime(light_combined['DATE'], infer_datetime_format=True)
# combine date and time columns together
light_combined['Date_Time'] = light_combined['DATE'].astype(str) + ' ' + light_combined['TIME'].astype(str)  # add date and time
light_combined['Date_Time'] = pd.to_datetime(light_combined['Date_Time'], format="%Y-%m-%d %H:%M:%S")  # convert to datetime
light_combined = light_combined[['Date_Time', 'STATUS', 'Lighting_Zone_ID', 'Room_ID']]  # re-order columns
light_combined.columns = ['Date_Time', 'Ligthing_Status', 'Lighting_Zone_ID', 'Room_ID']  # rename the column names
light_combined['Ligthing_Status'].unique()  # check if all the text has been replaced
light_combined = light_combined.replace(['ON', 'OFF'], [1, 0], inplace=False)  # convert window status to values
light_combined['Ligthing_Status'].unique()  # check if all the text has been replaced

# concat the combined data to the template
template_light = pd.concat([template_light, light_combined], ignore_index=True)
# assign data type to each columns
# template_light.dtypes
template_light['Lighting_Status_ID'] = ''
template_light['Ligthing_Status'] = template_light['Ligthing_Status'].astype(int)
template_light['Lighting_Zone_ID'] = template_light['Lighting_Zone_ID'].astype(int)
template_light['Room_ID'] = template_light['Room_ID'].astype(int)

# sort the dataframe
# cannot sort by three columns by ascending, because of the Date_Time
# template_light.sort_values(by=['Date_Time', 'Window_ID', 'Room_ID'], ascending=True)

# check missing values, and sum missing value count by column
print('Check missing values in : light_combined')
print(template_light.isnull().sum())

# save
# save Window_Status.csv
template_light.to_csv(data_path+'Ligthing_Status.csv ', index=False)



''' 2.3 HVAC_Measurement.csv '''
# template_hvac; 'HVAC_Measurement.csv'
# only room 1 has hvac measurement data
# read and combine data by category and add IDs when combining
hvac_combined = pd.DataFrame()
# combine data from room 1 and assign room ID
for index, name in enumerate(ac1):
    temp_df = pd.read_excel(combined_room1, sheet_name=name)
    temp_df['HVAC_Zone_ID'] = int(name[-1])  # ac 1,2,4; ac3 is missing
    temp_df['Room_ID'] = 1
    hvac_combined = pd.concat([hvac_combined, temp_df], ignore_index=True)
    # print(index)

# this column has mixed datetime and string data, convert all to datetime
hvac_combined.DATE = pd.to_datetime(hvac_combined['DATE'], infer_datetime_format=True)
# combine date and time columns together
hvac_combined['Date_Time'] = hvac_combined['DATE'].astype(str) + ' ' + hvac_combined['TIME'].astype(str)  # add date and time
hvac_combined['Date_Time'] = pd.to_datetime(hvac_combined['Date_Time'], format="%Y-%m-%d %H:%M:%S")  # convert to datetime
hvac_combined = hvac_combined[['Date_Time', 'STATUS', 'HVAC_Zone_ID', 'Room_ID']]  # re-order columns
hvac_combined.columns = ['Date_Time', 'Cooling_Status', 'HVAC_Zone_ID', 'Room_ID']  # rename the column names
hvac_combined['Cooling_Status'].unique()  # check if all the text has been replaced
hvac_combined = hvac_combined.replace(['ON', 'OFF'], [1, 0], inplace=False)  # convert window status to values
hvac_combined['Cooling_Status'].unique()  # check if all the text has been replaced

# concat the combined data to the template
template_hvac = pd.concat([template_hvac, hvac_combined], ignore_index=True)

# check missing values, and sum missing value count by column
print('Check missing values in : hvac_combined')
print(template_hvac.isnull().sum())
# no missing values in the combined raw data

# assign data type to each columns
# template_hvac.dtypes
template_hvac = template_hvac.fillna('')
template_hvac['Cooling_Status'] = template_hvac['Cooling_Status'].astype(int)
template_hvac['HVAC_Zone_ID'] = template_hvac['HVAC_Zone_ID'].astype(int)
template_hvac['Room_ID'] = template_hvac['Room_ID'].astype(int)

# sort the dataframe
# cannot sort by three columns by ascending, because of the Date_Time
# template_hvac.sort_values(by=['Date_Time', 'Window_ID', 'Room_ID'], ascending=True)

# check missing values, and sum missing value count by column
print('Check missing values in : hvac_combined')
print(template_hvac.isnull().sum())

# save
# save Window_Status.csv
template_hvac.to_csv(data_path+'HVAC_Measurement.csv ', index=False)


''' 2.4 Indoor_Measurement.csv '''
# template_indoor; 'Indoor_Measurement.csv'
# read and combine data by category and add IDs when combining
indoor_combined = pd.DataFrame()
# combine data from room 1 and assign room ID
for index, name in enumerate(indoor1):
    temp_df = pd.read_excel(combined_room1, sheet_name=name)
    temp_df.columns = ['DATE', 'TIME', 'Indoor_Temp', 'Indoor_RH']  # indoor 1 and indoor 2 have different column names
    temp_df['Room_ID'] = 1
    indoor_combined = pd.concat([indoor_combined, temp_df], ignore_index=True)
    # print(index)

''' Room 1'''
# indoor 1 TIME column has different format of timestamp
# format time
indoor_combined['DATE'] = pd.to_datetime(indoor_combined['DATE'], infer_datetime_format=True)
indoor_combined.isnull().sum()  # check null values
indoor_combined.dropna(subset=["Indoor_Temp"], inplace=True)  # drop rows have null values

# check how many hours of data in one day
indoor_combined['DATE'].value_counts()  # most have 96 rows of data which is 15 minutes interval, 24 hours' data
days = list(indoor_combined['DATE'].value_counts().index)
# indoor_combined[indoor_combined['DATE'] == days[1]]['TIME']

# change the time to desired time format
for day in days:
    print(f'Day: {day}')
    # process one day's data at one time
    time_one_day = indoor_combined[indoor_combined['DATE'] == day]['TIME'].copy()
    time_one_day.reset_index(drop=True, inplace=True)
    # check firt row of data, get the hour
    start_hour = int(time_one_day[0].split('h')[0])

    # 2019-04-26    starts at afternoon
    # 2019-12-12    starts at afternoon
    # 2019-06-11    starts with 12 at the morning

    if start_hour != 12:
        for index, i in enumerate(time_one_day):
            time_row = i
            old_hour = time_row.split('h')[0]
            new_hour = str(int(old_hour)+12)  # afternoon time
            time_one_day[index] = i.replace(old_hour+'h', new_hour+'h')
        # assign data back to the dataframe, using vlues of the series, avoid index matching problem
        indoor_combined.loc[indoor_combined['DATE'] == day, ['TIME']] = time_one_day.values

    else:  # start_hour = 12
        am_flag = 0
        pm_flag = 0  # 0 is morning, 1 is afternoon
        for index, i in enumerate(time_one_day):
            time_row = i
            old_hour = time_row.split('h')[0]
            if not am_flag:
                if int(old_hour) == 12:  # if it is early mooring
                    new_hour = str(int(old_hour)-12)  # early morning time
                    time_one_day[index] = i.replace(old_hour, new_hour)
                else:  # if it is am before noon
                    am_flag = 1

            elif not pm_flag:
                if int(old_hour) == 12:  # noon, change pm_flag to 1
                    pm_flag = 1
            elif pm_flag:  # afternoon
                if int(old_hour) < 12:
                    new_hour = str(int(old_hour)+12)  # afternoon time
                    time_one_day[index] = i.replace(old_hour+'h', new_hour+'h')
            # assign data back to the dataframe
            indoor_combined.loc[indoor_combined['DATE'] == day, ['TIME']] = time_one_day.values

# this column has mixed datetime and string data, convert all to datetime
# format time
indoor_combined['TIME'] = indoor_combined['TIME'].str.replace('h', ':')
indoor_combined['TIME'] = indoor_combined['TIME'].str.replace('min', ':')
indoor_combined['TIME'] = indoor_combined['TIME'].str.replace('s', '')

''' Room 2'''
# DATE	TIME	TEMPERATURE (°C)	RELATIVE HUMIDITY (%)	ILLUMINANCE - HORIZONTAL (LUX)	ILLUMINANCE - VERTICAL (LUX)
# combine data from room 2 and assign room ID
for index, name in enumerate(indoor2):
    temp_df = pd.read_excel(combined_room2, sheet_name=name)
    temp_df.drop(['ILLUMINANCE - VERTICAL (LUX)'], axis=1, inplace=True)
    temp_df.columns = ['DATE', 'TIME', 'Indoor_Temp', 'Indoor_RH',
                       'Indoor_Illuminance']  # indoor 1 and indoor 2 have different column names
    temp_df['Room_ID'] = 2
    indoor_combined = pd.concat([indoor_combined, temp_df], ignore_index=True)
    # print(index)

# indoor_combined.columns
# format time
indoor_combined['DATE'] = pd.to_datetime(indoor_combined['DATE'], infer_datetime_format=True)
indoor_combined.isnull().sum()  # check null values

# combine the date and time
indoor_combined['Date_Time'] = indoor_combined['DATE'].dt.date.astype(str) + ' ' + indoor_combined['TIME'].astype(str)  # add date and time
indoor_combined['Date_Time'] = pd.to_datetime(indoor_combined['Date_Time'], format="%Y-%m-%d %H:%M:%S")  # convert to datetime

indoor_combined = indoor_combined[['Date_Time', 'Indoor_Temp', 'Indoor_RH', 'Indoor_Illuminance', 'Room_ID']]  # re-order columns
# indoor_combined.columns

# concat the combined data to the template
template_indoor = pd.concat([template_indoor, indoor_combined], ignore_index=True)

# check missing values, and sum missing value count by column
print('Check missing values in : indoor_combined')
print(template_indoor.isnull().sum())
# no missing values in the combined raw data

# assign data type to each columns
template_indoor.dtypes
template_indoor = template_indoor.fillna('')
template_indoor['Indoor_Temp'] = template_indoor['Indoor_Temp'].astype(float)
template_indoor['Indoor_RH'] = template_indoor['Indoor_RH'].astype(float)
# template_indoor['Indoor_Illuminance'] = template_indoor['Indoor_Illuminance'].astype(float)  #
template_indoor['Room_ID'] = template_indoor['Room_ID'].astype(int)
# check missing values, and sum missing value count by column
print('Check missing values in : indoor_combined')
print(template_indoor.isnull().sum())

# save
# save Window_Status.csv
template_indoor.to_csv(data_path+'Indoor_Measurement.csv ', index=False)










''' 2.5 Outdoor_Measurement.csv '''
# template_outdoor; 'Outdoor_Measurement.csv'
outdoor_combined = pd.DataFrame()
# combine data from room 1 and assign room ID
for index, name in enumerate(outdoor1):
    temp_df = pd.read_excel(combined_room1, sheet_name=name)
    temp_df['Room_ID'] = 1
    outdoor_combined = pd.concat([outdoor_combined, temp_df], ignore_index=True)
    # print(index)

# combine data from room 2 and assign room ID
for index, name in enumerate(outdoor2):
    temp_df = pd.read_excel(combined_room2, sheet_name=name)
    temp_df['Room_ID'] = 2
    outdoor_combined = pd.concat([outdoor_combined, temp_df], ignore_index=True)
    # print(index)

outdoor_combined.columns
# this column has mixed datetime and string data, convert all to datetime
outdoor_combined['DATE (MM/DD/YY)'] = pd.to_datetime(outdoor_combined['DATE (MM/DD/YY)'], infer_datetime_format=True)
# combine date and time columns together
outdoor_combined['Date_Time'] = outdoor_combined['DATE (MM/DD/YY)'].astype(str) + ' ' + outdoor_combined['TIME'].astype(str)  # add date and time
outdoor_combined['Date_Time'] = pd.to_datetime(outdoor_combined['Date_Time'], format="%Y-%m-%d %H:%M:%S")  # convert to datetime
outdoor_combined = outdoor_combined[['Date_Time', 'SOLAR RADIATION - SUPERIOR PYRANOMETER (J/m²)',
                                     'AIR TEMPERATURE (°C)', 'RELATIVE HUMIDITY (%)', 'Room_ID']]  # re-order columns
outdoor_combined.columns = ['Date_Time', 'Solar_Radiation', 'Outdoor_Temp', 'Outdoor_RH', 'Room_ID']  # rename the column names


# concat the combined data to the template
template_outdoor = pd.concat([template_outdoor, outdoor_combined], ignore_index=True)
template_outdoor['Building_ID'] = 1

# check missing values, and sum missing value count by column
print('Check missing values in : outdoor_combined')
print(template_outdoor.isnull().sum())
# no missing values in the combined raw data

# assign data type to each columns
template_outdoor.dtypes
template_outdoor = template_outdoor.fillna('')
template_outdoor['Outdoor_Temp'] = template_outdoor['Outdoor_Temp'].astype(float)
template_outdoor['Outdoor_RH'] = template_outdoor['Outdoor_RH'].astype(float)
template_outdoor['Solar_Radiation'] = template_outdoor['Solar_Radiation'].astype(float)
template_outdoor['Building_ID'] = template_outdoor['Building_ID'].astype(int)
template_outdoor['Room_ID'] = template_outdoor['Room_ID'].astype(int)

template_outdoor.drop(['Room_ID'], axis=1, inplace=True)  # drop Room_ID, only one bulding
# check missing values, and sum missing value count by column
print('Check missing values in : outdoor_combined')
print(template_outdoor.isnull().sum())

# save
# save Window_Status.csv
template_outdoor.to_csv(data_path+'Outdoor_Measurement.csv ', index=False)