import pandas as pd
import numpy as np
import os

# specify the path
data_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-06-03-raw-data/Annex 79 Data Collection/O-18-Nan Gao/CornishCollege_CleanEXPORT (6)/'
template_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/OB Database Consolidation/Templates/'
save_path = 'D:/yapan_office_D/Data/Annex-79-OB-Database/2021-06-03-raw-data/Annex 79 Data Collection/O-18-Nan Gao/_yapan_processing/'

# read templates into pandas
template_occ_num = pd.read_csv(template_path + 'Occupant_Number_Measurement.csv')
template_outdoor = pd.read_csv(template_path + 'Outdoor_Measurement.csv')
template_hvac = pd.read_csv(template_path + 'HVAC_Measurement.csv')

os.chdir(data_path)  # pwd

df_1 = pd.read_csv('19.csv')
df_2 = pd.read_csv('20.csv')
df_3 = pd.read_csv('27.csv')
df_4 = pd.read_csv('28.csv')
df_5 = pd.read_csv('29.csv')
df_6 = pd.read_csv('30.csv')
df_7 = pd.read_csv('31.csv')
df_8 = pd.read_csv('40.csv')
df_9 = pd.read_csv('41.csv')
df_10 = pd.read_csv('43.csv')
df_11 = pd.read_csv('KB1.csv')
df_12 = pd.read_csv('KB2.csv')
df_13 = pd.read_csv('KB3.csv')
df_14 = pd.read_csv('KB4.csv')
df_15 = pd.read_csv('KB5.csv')
df_16 = pd.read_csv('KB6.csv')

df_1['Room_ID'] = 1
df_2['Room_ID'] = 2
df_3['Room_ID'] = 3
df_4['Room_ID'] = 4
df_5['Room_ID'] = 5
df_6['Room_ID'] = 6
df_7['Room_ID'] = 7
df_8['Room_ID'] = 8
df_9['Room_ID'] = 9
df_10['Room_ID'] = 10
df_11['Room_ID'] = 11
df_12['Room_ID'] = 12
df_13['Room_ID'] = 13
df_14['Room_ID'] = 14
df_15['Room_ID'] = 15
df_16['Room_ID'] = 16

df = pd.concat([df_1, df_2, df_3, df_4, df_5, df_6, df_7, df_8, df_9, df_10, df_11, df_12, df_13, df_14, df_15, df_16], ignore_index=True)


Date_Time = df['Unnamed: 0']
Occupancy_Measurement = df['Occupied']
Tin = df['IndoorTemperature']
RHin = df['IndoorHumidity']
CO2in = df['IndoorCO2']
Tout = df['OutdoorTemperature']
RHout = df['OutdoorHumidity']
Wind_Direction = df['OutdoorWindDirection']
Wind_Speed = df['OutdoorWindSpeed']
Room_ID = df['Room_ID']

tem_1 = pd.read_csv('Indoor_Measurement.csv')
tem_1['Date_Time'] = Date_Time
tem_1['Indoor_Temp'] = Tin
tem_1['Indoor_RH'] = RHin
tem_1['Indoor_CO2'] = CO2in
tem_1['Room_ID'] = Room_ID

tem_1['Date_Time'].fillna(-999)
tem_1['Date_Time'].fillna(-999)
tem_1['Indoor_Temp'].fillna(-999)
tem_1['Indoor_RH'].fillna(-999)
tem_1['Indoor_CO2'].fillna(-999)
tem_1['Room_ID'].fillna(-999)
tem_1.to_csv('Indoor_Measurement_18.csv', index=False, header=True)

tem_2 = pd.read_csv('Occupancy_Measurement.csv')
tem_2['Date_Time'] = Date_Time
tem_2['Occupancy_Measurement'] = Occupancy_Measurement
tem_2['Room_ID'] = Room_ID
tem_2['Date_Time'].fillna(-999)
tem_2['Room_ID'].fillna(-999)
tem_2['Occupancy_Measurement'].fillna(-999)
tem_2.to_csv('Occupancy_Measurement_18.csv', index=False, header=True)

tem_3 = pd.read_csv('Outdoor_Measurement.csv')
tem_3['Date_Time'] = Date_Time
tem_3['Outdoor_Temp'] = Tout
tem_3['Outdoor_RH'] = RHout
tem_3['Wind_Speed'] = Wind_Speed
tem_3['Wind_Direction'] = Wind_Direction
tem_3['Building_ID'] = 1
tem_3['Date_Time'].fillna(-999)
tem_3['Outdoor_Temp'].fillna(-999)
tem_3['Outdoor_RH'].fillna(-999)
tem_3['Wind_Speed'].fillna(-999)
tem_3['Wind_Direction'].fillna(-999)
tem_3.to_csv('Outdoor_Measurement_18.csv', index=False, header=True)


''' yapan added '''
df = df.rename(columns={'Unnamed: 0': 'Date_Time'})
# indoor_cols = ['Date_Time', 'IndoorTemperature', 'IndoorHumidity', 'IndoorCO2', 'Room_ID']
outdoor_cols = ['Date_Time', 'OutdoorTemperature', 'OutdoorHumidity', 'OutdoorWindDirection',
                'OutdoorWindSpeed', 'Precipitation', 'SolarRadiation', 'Room_ID']
# occ_cols = ['Date_Time', 'Occupied', 'Room_ID']
hvac_cols = ['Date_Time', 'HeatingState', 'CoolingState', 'Room_ID']

outdoor_df = df[outdoor_cols]
outdoor_df.columns = ['Date_Time', 'Outdoor_Temp', 'Outdoor_RH', 'Wind_Direction', 'Wind_Speed',
                      'Precipitation',  'Solar_Radiation', 'Room_ID']

template_outdoor = pd.concat([template_outdoor, outdoor_df], ignore_index=True)
template_outdoor['Building_ID'] = 1


template_hvac.columns
hvac_df = df[hvac_cols]
hvac_df.columns = ['Date_Time', 'Heating_Status', 'Cooling_Status', 'Room_ID']
template_hvac = pd.concat([template_hvac, hvac_df], ignore_index=True)
template_hvac['Building_ID'] = 1
template_hvac['HVAC_Zone_ID'] = 1

# check data
print(template_outdoor.isnull().sum())
print(template_outdoor.dtypes)

print(template_hvac.isnull().sum())
print(template_hvac.dtypes)

# change data types
template_outdoor['Date_Time'] = pd.to_datetime(template_outdoor['Date_Time'], format="%Y-%m-%d %H:%M:%S")
template_hvac['Date_Time'] = pd.to_datetime(template_hvac['Date_Time'], format="%Y-%m-%d %H:%M:%S")

template_outdoor['Building_ID'] = template_outdoor['Building_ID'].astype(int)
template_outdoor['Room_ID'] = template_outdoor['Room_ID'].astype(int)

template_hvac['Heating_Status'] = template_hvac['Heating_Status'].astype(int)
template_hvac['Cooling_Status'] = template_hvac['Cooling_Status'].astype(int)
template_hvac['Building_ID'] = template_hvac['Building_ID'].astype(int)
template_hvac['Room_ID'] = template_hvac['Room_ID'].astype(int)
template_hvac['HVAC_Zone_ID'] = template_hvac['HVAC_Zone_ID'].astype(int)

# save data

template_outdoor.to_csv(save_path + 'Outdoor_Measurement.csv', index=False)
template_hvac.to_csv(save_path + 'HVAC_Measurement.csv', index=False)