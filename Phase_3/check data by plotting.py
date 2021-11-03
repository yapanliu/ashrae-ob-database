""" check ob dataset 18 and plot the occ by room id """
data_name = "07"
data_path = "D:/yapan_office_D/Data/Annex-79-OB-Database/Backup/OB Database Consolidation 2021-10-21/In-Situ/"

import pandas as pd
import matplotlib.pyplot as plt

occ_df = pd.read_csv(data_path + data_name +  '/Occupancy_Measurement.csv')
print(occ_df.columns)

bldg_list = list(occ_df['Building_ID'].unique())
room_list = list(occ_df['Room_ID'].unique())

for id in room_list:
    temp_df = occ_df.loc[occ_df['Room_ID']==id, ]
    plt.plot(temp_df['Occupancy_Measurement[0-Unoccupied;1-Occupied]'], label=id)

plt.legend()
plt.show()




