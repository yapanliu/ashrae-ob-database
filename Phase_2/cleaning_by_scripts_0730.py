''' This code changes heading of the large non-survey datasets '''

import os
import pandas as pd

''' 05 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-29/37/"
# template_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/Dataset_Templates/Appliance_Usage.csv"

# get all the file list under root folder
os.chdir(file_path)  # pwd
sub_folders = next(os.walk('.'))[1]  # get the names of the child directories, different rooms
root_files = next(os.walk('.'))[2]  # get the files under root path
print(f'root folders: {sub_folders}')
print(f'root files: {root_files}')

for file in root_files:
    df = pd.read_csv(file_path + file)
    # change column to pandas datetime
    df['Date_Time'] = pd.to_datetime(df['Date_Time'], infer_datetime_format=True)
    # print(df.columns)
    print(df['Date_Time'])
    df.to_csv(file_path + file, index=False)


''' 42 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-29/42/"
# template_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/Dataset_Templates/Appliance_Usage.csv"

# get all the file list under root folder
os.chdir(file_path)  # pwd
sub_folders = next(os.walk('.'))[1]  # get the names of the child directories, different rooms
root_files = next(os.walk('.'))[2]  # get the files under root path
print(f'root folders: {sub_folders}')
print(f'root files: {root_files}')

file = root_files[0]
df = pd.read_csv(file_path + file)
print(df)
# round values to 3 decimals
df['Electric_Power[w]'] = df['Electric_Power[w]'].round(3)
df.to_csv(file_path + file, index=False)

''' 45 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-29/45/"
# template_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/Dataset_Templates/Appliance_Usage.csv"

# get all the file list under root folder
os.chdir(file_path)  # pwd
sub_folders = next(os.walk('.'))[1]  # get the names of the child directories, different rooms
root_files = next(os.walk('.'))[2]  # get the files under root path
print(f'root folders: {sub_folders}')
print(f'root files: {root_files}')

file = root_files[0]
df = pd.read_csv(file_path + file)
print(df)
print(df.columns)
# round values to 3 decimals
df['Indoor_Temp[C]'] = df['Indoor_Temp[C]'].round(2)
df['Indoor_CO2[ppm]'] = df['Indoor_CO2[ppm]'].round(0)
print(df['Indoor_Temp[C]'].unique())
print(df['Indoor_CO2[ppm]'].unique())
df.to_csv(file_path + file, index=False)

''' 60 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-29/60/"
# template_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/Dataset_Templates/Appliance_Usage.csv"

# get all the file list under root folder
os.chdir(file_path)  # pwd
sub_folders = next(os.walk('.'))[1]  # get the names of the child directories, different rooms
root_files = next(os.walk('.'))[2]  # get the files under root path
print(f'root folders: {sub_folders}')
print(f'root files: {root_files}')

file = root_files[0]
df = pd.read_csv(file_path + file)
print(df)
print(df.columns)
# round values to 3 decimals
name_list1 = ['Indoor_Temp [C]', 'Indoor_RH [%]', 'INDOORMeanRadiantTemp [C]', 'Outdoor_Temp [C]', 'OUTDoor_RH [%]',
              'Temp_Setpoint [C]', 'BaseThermostatCOOLINGSetpoint [C]', 'CurrentThermostatHEATINGSetpoint [C]',
              'BaseThermostatHEATINGSetpoint [C]']  # 2 digits
name_list2 = ['Indoor_Air_Speed [m/s]', 'Outdoor_Air_Speed [m/s]']  # 3 digits
name_list3 = ['INDOORLumens [LUX]', 'Indoor_CO2 [ppm]']  # 0 digits

for name in name_list1:
    df[name] = df[name].round(2)
    print(df[name].unique())

for name in name_list2:
    df[name] = df[name].round(3)
    print(df[name].unique())

for name in name_list3:
    df[name] = df[name].round(0)
    print(df[name].unique())

df.to_csv(file_path + file, index=False)