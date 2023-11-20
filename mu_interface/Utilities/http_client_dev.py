import json
import time
from pathlib import Path

import tqdm
import yaml


def upload_data(client, node):
    import pandas as pd
    
    data_file = Path(__file__).parent.absolute() / f"data/{node}_reformatted.csv"
    config_file = Path(__file__).parent.absolute() / "config/custom_data_fields.yaml"
    
    client.add_node(node, node)
    
    with open(config_file) as cf:
        config = yaml.safe_load(cf)
        keys = [key for key in config if config[key] is True]
        
    df = pd.read_csv(data_file, sep=',', header=0)
    df = df[keys]
    df = df.tail(int(len(df) / 4))
    df = df.iloc[::10]
    
    for i in tqdm.trange(len(df), desc='Data'):
        data_line = df.iloc[i]
        timestamp = data_line['timestamp']
        data_line = data_line.drop('timestamp')
        data = data_line.to_dict()
        client.add_data2(data, timestamp, node)
        time.sleep(1)
        
def make_json(node):
    import pandas as pd
    
    data_file = Path(__file__).parent.absolute() / f"data/{node}_reformatted.csv"
    config_file = Path(__file__).parent.absolute() / "config/custom_data_fields.yaml"
    
    with open(config_file) as cf:
        config = yaml.safe_load(cf)
        keys = [key for key in config if config[key] is True]
        
    df = pd.read_csv(data_file, sep=',', header=0)
    
    # Create a new column 'data' by selecting the columns of interest
    df['data'] = df[keys].apply(lambda row: row.to_dict(), axis=1)

    # Create the final JSON structure
    json_entries = df.apply(lambda row: {
        'node_handle': row['sender_hostname'],
        'date': row['timestamp'],
        'created_at': row['timestamp'],
        'updated_at': row['timestamp'],
        'data': row['data']
    }, axis=1).tolist()

    # Write the JSON data to a file
    json_file_path = Path(__file__).parent.absolute() / f"data/{node}.json"
    with open(json_file_path, 'w') as json_file:
        json.dump(json_entries, json_file, indent=2)
        
def make_csv(node):
    import pandas as pd
    
    data_file = Path(__file__).parent.absolute() / f"data/{node}_reformatted.csv"
    config_file = Path(__file__).parent.absolute() / "config/custom_data_fields.yaml"
    
    with open(config_file) as cf:
        config = yaml.safe_load(cf)
        keys = [key for key in config if config[key] is True]
        
    df = pd.read_csv(data_file, sep=',', header=0)
    
    # Create a new column 'data' by selecting the columns of interest
    df = df[keys]
    df = df.rename(columns={'sender_hostname': 'node_handle', 'timestamp': 'date'})
    df['created_at'] = df['date']
    df['updated_at'] = df['date']    

    csv_out_file_path = Path(__file__).parent.absolute() / f"data/{node}_new.csv"
    df.to_csv(csv_out_file_path, index=False)
    
def make_weird_json_csv(node):
    """
    Output file format example:
    "id";"node_handle";"date";"created_at";"updated_at";"data"
    "71388";"rpi1";"2023-11-03 03:52:12";"2023-11-07 13:18:11";"2023-11-07 13:18:11";"{""mag_total"":67.976,""temp_external"":28.49,""light_external"":0.68,""humidity_external"":36.91,""differential_potential_ch1"":500868,""differential_potential_ch2"":520368,""rf_power_emission"":16645,""transpiration"":2.26,""air_pressure"":999.48,""soil_moisture"":264,""soil_temperature"":26.8}"
    "71387";"rpi1";"2023-11-03 03:50:29";"2023-11-07 13:18:10";"2023-11-07 13:18:10";"{""mag_total"":67.137,""temp_external"":28.49,""light_external"":0.68,""humidity_external"":36.88,""differential_potential_ch1"":500638,""differential_potential_ch2"":520447,""rf_power_emission"":16643,""transpiration"":2.3,""air_pressure"":999.46,""soil_moisture"":264,""soil_temperature"":26.6}"
    "71386";"rpi1";"2023-11-03 03:48:47";"2023-11-07 13:18:08";"2023-11-07 13:18:08";"{""mag_total"":67.477,""temp_external"":28.49,""light_external"":0.68,""humidity_external"":36.9,""differential_potential_ch1"":500270,""differential_potential_ch2"":519860,""rf_power_emission"":16639,""transpiration"":2.28,""air_pressure"":999.46,""soil_moisture"":265,""soil_temperature"":26.6}"

    """
    import csv

    import pandas as pd
    
    data_file = Path(__file__).parent.absolute() / f"data/{node}_reformatted.csv"
    config_file = Path(__file__).parent.absolute() / "config/custom_data_fields.yaml"
    
    with open(config_file) as cf:
        config = yaml.safe_load(cf)
        data_keys = [key for key in config if config[key] is True]
        
    all_keys = data_keys + ['sender_hostname', 'timestamp']
        
    df = pd.read_csv(data_file, sep=',', header=0)
    df = df[all_keys]
    
    df = df.rename(columns={'sender_hostname': 'node_handle', 'timestamp': 'date'})
    df['created_at'] = df['date']
    df['updated_at'] = df['date']
    df = df.astype(object) 
    
    # Create a new column 'data' by selecting the columns of interest
    df['data'] = df[data_keys].apply(lambda row: row.to_json(), axis=1)
    df.drop(data_keys, axis=1, inplace=True)

    # Write the JSON data to a file
    out_path = Path(__file__).parent.absolute() / f"data/{node}_json.csv"
    df.to_csv(out_path, sep=';', index=True, index_label='id', quoting=csv.QUOTE_ALL)