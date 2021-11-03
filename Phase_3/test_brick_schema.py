''' This code will collect column names from large datasets '''

import pandas as pd

''' 05 '''
file_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-07-29/05/"
save_path = "C:/Users/yliu88/OneDrive - Syracuse University/PublicWorkSpace/Project/Annex/Annex 79/OB Database/papers and report/BRICK/"
file_name = 'Appliance_Usage.csv'

# read the dataset
df = pd.read_csv(file_path + file_name)
print(df.columns)

# save column names
col_names = df.columns.to_series()
col_names.to_csv(save_path + 'col_names_temp.csv', index=False)
