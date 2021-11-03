''' This code changes heading of the large non-survey datasets '''

import pandas as pd

''' 05 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/05-Done-Done/sql/"
template_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/Dataset_Templates/Appliance_Usage.csv"
file_name = 'Appliance_Usage.csv'

df = pd.read_csv(file_path + file_name)
headings_df = pd.read_csv(template_path)
print(df.columns)

# drop empty columns
df['Room_ID'] = 1
df['Building_ID'] = 1
df.columns = headings_df.columns
print(df.columns)

# drop empty columns
df.dropna(how='all', axis=1, inplace=True)
# save data
df.to_csv(file_path + file_name, index=False)


''' 15 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/15-Done-Done/sql/"
template_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/Dataset_Templates/Appliance_Usage.csv"
file_name = 'Appliance_Usage.csv'

df = pd.read_csv(file_path + file_name)
headings_df = pd.read_csv(template_path)
print(df.columns)

# drop empty columns
# df['Room_ID'] = 1
df['Building_ID'] = 1
df.columns = headings_df.columns
print(df.columns)

# round values to 3 decimals
df['Electric_Power[w]'] = df['Electric_Power[w]'].round(3)

# drop empty columns
df.dropna(how='all', axis=1, inplace=True)
# save data
df.to_csv(file_path + file_name, index=False)

''' 27 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/27-Done-Done/sql/"
template_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/Dataset_Templates/"
file_name = 'Window_Status.csv'

df = pd.read_csv(file_path + file_name)
headings_df = pd.read_csv(template_path + file_name)
print(df.columns)
print(headings_df.columns)

# drop empty columns
# df['Room_ID'] = 1
# df['Building_ID'] = 1
df.columns = headings_df.columns
print(df.columns)

# drop empty columns
df.dropna(how='all', axis=1, inplace=True)
# save data
df.to_csv(file_path + file_name, index=False)

''' 42 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/42-Done-Done/sql/"
template_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/Dataset_Templates/"
file_name = 'Appliance_Usage.csv'

df = pd.read_csv(file_path + file_name)
headings_df = pd.read_csv(template_path + file_name)
print(df.columns)
print(headings_df.columns)

# drop empty columns
# df['Room_ID'] = 1
# df['Building_ID'] = 1
df.columns = headings_df.columns
print(df.columns)

# drop empty columns
df.dropna(how='all', axis=1, inplace=True)
# save data
df.to_csv(file_path + file_name, index=False)


''' 45 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/45-Done-Done/sql/"
template_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-22/Dataset_Templates/"
file_name = 'Indoor_Measurement.csv'

df = pd.read_csv(file_path + file_name)
headings_df = pd.read_csv(template_path + file_name)
print(df.columns)
print(headings_df.columns)

print(len(df.columns))
print(len(headings_df.columns))

# drop empty columns
# df['Room_ID'] = 1
# df['Building_ID'] = 1
df.columns = headings_df.columns
print(df.columns)

# drop empty columns
df.dropna(how='all', axis=1, inplace=True)
# save data
df.to_csv(file_path + file_name, index=False)


