import pandas as pd
import numpy as np
from datetime import datetime, timedelta


def reformat_data(node):
    f = f'/home/marko/PROJECTS/WatchPlant/MU/mu_interface/mu_interface/Utilities/data/{node}.csv'

    df = pd.read_csv(f, sep=',', header=0)

    # print(df.head())

    df['temp-PCB'] = round(df['temp-PCB'] / 10000, 2)
    df['mag_X'] = round(df['mag_X'] / 10, 3)
    df['mag_Y'] = round(df['mag_Y'] / 10, 3)
    df['mag_Z'] = round(df['mag_Z'] / 10, 3)
    df['mag_total'] = round(np.sqrt(df['mag_X']**2 + df['mag_Y']**2 + df['mag_Z']**2), 3)
    df['temp-external'] = round(df['temp-external'] / 10000, 2)
    df['light-external'] = round(df['light-external'] / 799.4 - 0.75056, 2)
    df['humidity-external'] = round((df['humidity-external'] * 3 / 4200000-0.1515) / (0.006707256-0.0000137376 * (df['temp-external'] / 10000.0)), 2)
    df['air_pressure'] = round(df['air_pressure'] / 100, 2)
    # df['differential_potential_CH1'] = df['differential_potential_CH1'] - 0
    # df['differential_potential_CH2'] = df['differential_potential_CH2'] - 0
    df['transpiration'] = round(df['transpiration'] / 1000, 2)
    # df['soil_moisture'] = df['soil_moisture'] - 0
    df['soil_temperature'] = round(df['soil_temperature'] / 10, 2)
    
    df = df.rename(columns={
        'temp-external': 'temp_external',
        'light-external': 'light_external',
        'humidity-external': 'humidity_external',
        'differential_potential_CH1': 'differential_potential_ch1',
        'differential_potential_CH2': 'differential_potential_ch2',
        'RF_power_emission': 'rf_power_emission',
    })

    # print(df.head())

    # Get yesterday's date
    yesterday = datetime.now()
    yesterday = yesterday.replace(hour=0, minute=0, second=0, microsecond=0)

    # Calculate the number of days since the last entry in the dataframe and yesterday
    last_entry = datetime.fromisoformat('2021-08-24 11:22:00')
    days_since_last_entry = (yesterday - last_entry).days + 1

    # Add the difference to all timestamps in the dataframe
    df['timestamp'] = pd.to_datetime(df['timestamp']) + timedelta(days=days_since_last_entry, hours=-1)

    # Print the updated dataframe
    # print(df.head())
    # print(df.tail())

    df.to_csv(f'/home/marko/PROJECTS/WatchPlant/MU/mu_interface/mu_interface/Utilities/data/{node}_reformatted.csv', index=False)
    
    
if __name__ == '__main__':
    for i in range(4):
        reformat_data(f'rpi{i}')