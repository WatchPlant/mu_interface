#!/usr/bin/env python3
# -*- coding: utf-8 -*-
#from datetime import datetime
import datetime
import json
import logging
import os
import time
from pathlib import Path

import requests
import tqdm
import yaml
from requests.adapters import HTTPAdapter, Retry

# TODO: remove
from mu_interface.Utilities.log_formatter import setup_logger

# TODO: error handling

url = os.environ["WP_API_URL"]
headers = {"Content-Type": "application/json", "Authorization": os.environ["WP_API_AUTH"]}


class HTTPClient(object):
    def __init__(self, node_handle, display_name) -> None:
        # Get the list of nodes currently on the website.
        self.node_handle = node_handle
        self.display_name = display_name
        
        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5)
        self.session.mount('http://', HTTPAdapter(max_retries=retries))
        self.session.mount('https://', HTTPAdapter(max_retries=retries))
        
        self.known_data_fields = None
        self.known_nodes = None
                
        # if not self.node_exists():
        #     self.register_node()
        
    # DONE
    def get_nodes(self):
        """Get the list of nodes currently on the website."""
        query = 'nodes'

        try:
            response = self.session.get(url + query, headers=headers)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error("Timeout while getting the list of nodes.")
            return None
        
        parsed = json.loads(response.text)
        self.known_nodes = [x['handle'] for x in parsed['data']]
        return parsed['data']

    # DONE
    def node_exists(self, node_handle=None):
        """
        Check if a given node already exists on the website.

        Args:
            node_handle (str): Handle of the node to check. If None checks self.

        Returns:
            True if node exists, False otherwise.
        """
        if node_handle is None:
            node_handle = self.node_handle

        if self.known_nodes is None:
            self.get_nodes()
        return node_handle in self.known_nodes

    # DONE
    def add_node(self, node_handle, display_name):
        """
        Add a new node to the website.

        Args:
            node_handle (str): Internal identifier of the node. Important: The string has to contain letters
                                to avoid an error on the website. Don't only use numbers, even if they are 
                                formatted as string it will still lead to problems.
            display_name (str): Name of the node that is shown on the website.

        Returns:
            True if a successful response is received, False otherwise.
        """
        query = 'nodes'
        payload = {
            'handle': node_handle,
            'name': display_name
        }
        
        if self.node_exists(node_handle):
            logging.info(f"Node {node_handle} already exists.")
            return False
        
        response = None
        try:
            response = requests.request("POST", url + query, json=payload, headers=headers)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error("Timeout while waiting to add the new node.")
            return False
        
        if not response.ok:
            logging.error(f"Failed to add node {node_handle}. Status code {response.status_code}")
            return False
            
        return True

    # DONE
    def register_node(self):
        """Add a node with parameters specified in the constructor."""
        self.add_node(self.node_handle, self.display_name)

    # DONE
    def delete_node(self, node_handle):
        """
        Delete the node with the matching handle.

        Args:
            node_handle (str): Handle of node to delete.

        Returns:
            True if a successful response is received, False otherwise.
        """
        query = 'nodes/delete'
        payload = {
            "handle": node_handle
        }

        try:
            response = self.session.post(url + query, json=payload, headers=headers)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error("Timeout while waiting to delete node.")
            return False
        
        return True

    # DONE
    def get_data_fields(self):
        """Return all available data fields on the website."""
        query = 'data-field'
        response = None
        try:
            response = self.session.get(url + query, headers=headers)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error("Timeout while waiting for the current list of data fields.")
            return None

        parsed = json.loads(response.text)
        self.known_data_fields = [x['handle'] for x in parsed['data']]
        return parsed['data']

    # DONE
    def add_data_field(self, field_handle, field_name, unit):
        """
        Specify a new data field to be displayed on the website.

        Args:
            field_handle (str): Internal identifier of the data field.
            field_name (str): Name of the data field that is shown on the website.
            unit (str): Unit of the data field that is shown on the website.

        Returns:
            True if a successful response is received, False otherwise.
        """
        query = 'data-field'
        payload = {
            "name": field_name,
            "handle": field_handle,
            "unit": unit
        }

        try:
            response = self.session.post(url + query, json=payload, headers=headers)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error("Timeout while waiting to add the new data field.")
            return False
                
        if not response.ok:
            logging.error(f"Failed to add data field {field_handle}. Status code {response.status_code}")
            return False
        
        return True
    
    # DONE
    def delete_data_field(self, data_handle):
        """
        Delete data field with matching handle.

        Args:
            data_handle (str): Handle of data field to delete.

        Returns:
            True if a successful response is received, False otherwise.
        """
        query = 'data-field/delete'
        payload = {
            "handle": data_handle
        }

        try:
            response = self.session.post(url + query, json=payload, headers=headers)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error("Timeout while waiting to delete data field.")
            return False
        
        return True
    
    # FIXME: why is data type needed
    def get_data(self, data_type, date_range, node_handles=None,):
        """
        Return all data entries from specified nodes.

        Args:
            data_type (str): Type of data to retrieve.
            date_range (str): Time range to retrieve data from.
                              Possible values: 'last_hour', 'last_day', 'month', 'twelve_months'.
            node_handles (List[str]): List of node_handles whose data shall be extracted.

        Returns:
            Dictionary with all data entries from all given node_handles.
            Format:
            Dictionary with node_handles as keys
                List with all data entries per node_handle
                    Dictionary with metadata keys and one 'data' key
                        'data' contains a dictionary with the actual measurements 
        """
        if node_handles is None:
            node_handles = [self.node_handle]
        if not isinstance(node_handles, list):
            node_handles = [node_handles]
        if self.known_data_fields is None:
            self.get_data_fields()
        if data_type not in self.known_data_fields:
            logging.error(f"Data type {data_type} is not known. Current limitation of the API is that only " 
                           "data of specific type can be retrieved.")
            return False
        if date_range not in ['last_hour', 'last_day', 'month', 'twelve_months']:
            logging.error(f"Date range {date_range} is not known.")
            return False
        
        query = 'sensordata-multiple'
        payload = {
            "node_handles": node_handles,
            "date_range": date_range
        }

        try:
            response = self.session.post(f"{url}{query}?data_type={data_type}", json=payload, headers=headers)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error("Timeout while waiting to retrieve data.")
            return False

        if not response.ok:
            logging.error(f"Failed to retrieve data. Status code {response.status_code}")
            return False

        parsed = json.loads(response.text)
        return parsed['data']
        
    def add_data(self, data, additional_sensors, node_handle=None):
        """
        Push a single measurement set to the database.
        
        Args:
            data (np.array): Collected dataline from the MU
            node_handle (str): Node that collected the dataset, if None, it assumes itself as collector

        Returns:
            True if a successful response is received, False otherwise
        """
        if node_handle is None:
            node_handle = self.node_handle

        data = [self.node_handle] + data.tolist()
        time = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")#datetime.fromtimestamp(data[3]).strftime("%Y-%m-%d %H:%M:%S")

        #if config_file is None:
        config_file = Path(__file__).parent.absolute() / "data_fields.yaml"

        with open(config_file) as stream:
            config = yaml.safe_load(stream)

        additional_sensors = []

        keys = [key for key in config if config[key] is True] + (additional_sensors if additional_sensors != False else [])
        data_filter = [i for i, x in enumerate(config.values()) if x] + ([j + len(config) for j in range(len(additional_sensors))] if additional_sensors != False else [])
        filtered_data = [data[i] for i in data_filter]

        query = 'sensordata'
        payload = {
            "node_handle": filtered_data[0],
            "date": time,
            "data": dict(zip(keys[1:], filtered_data[1:]))
        }
        """
            {
                "temp_pcb": "0",
                "mag_x": "0",
                "mag_y": "0",
                "mag_z": "0",
                "temp_external": "50",
                "light_external": "0",
                "humidity_external": "4",
                "differential_potential_ch1": "10",
                "differential_potential_ch2": "0",
                "rf_power_emission": "0",
                "transpiration": "0",
                "air_pressure": "0",
                "soil_moisture": "0",
                "soil_temperature": "0",
                "mu_mm": "0",
                "mu_id": "0",
                "sender_hostname": "rpi0",
                "ozone": "0",
            }
        }
        """
        response = None
        while response is None:
            try:
                response = requests.request("POST", url + query, json=payload, headers=headers, timeout=2.0)
            except requests.exceptions.Timeout:
                print("Timeout while waiting to POST the new data field. Trying again.")
        
        if not response.ok:
            print(f"ERROR: Adding data. Status code {response.status_code}")
            return False
        
        return True

    # FIXME: does not work?
    def add_data2(self, data, timestamp, node_handle=None):
        """
        Add a single measurement set to the website.
        
        Args:
            node_handle (str): Node that collected the data. If None, it assumes itself as collector.
        
        Returns:
            True if a successful response is received, False otherwise.
        """
        if node_handle is None:
            node_handle = self.node_handle
        
        query = 'sensordata'
        payload = {
            "node_handle": node_handle,
            "data": data,
            "date": timestamp
        }
        
        try:
            response = self.session.post(url + query, json=payload, headers=headers)
        except requests.exceptions.Timeout:
            logging.error("Timeout while waiting to add the new data")
            return False
        
        if not response.ok:
            logging.error(f"Failed to add data. Status code {response.status_code}")
            return False
        
        return True
        

def add_data_fields_from_yaml(client, yaml_file_path):
    with open(yaml_file_path, 'r') as stream:
        try:
            data_fields = yaml.safe_load(stream)
            for field, settings in data_fields.items():
                if settings['show']:
                    client.add_data_field(field, settings['name'], settings['unit'])
        except yaml.YAMLError as exc:
            print(exc)


def main():
    from http_client_dev import make_weird_json_csv
    
    setup_logger('TEST', level=logging.INFO)
    logging.info('Starting HTTP client.')
    
    # client = HTTPClient('dev', 'Development Node')
    # add_data_fields_from_yaml(client, Path(__file__).parent.absolute() / "config/default_data_units.yaml")
    
    # for i in range(4):
    #     client.delete_node(f'rpi{i}')
    
    for i in tqdm.trange(4, desc='Node'):
        make_weird_json_csv(f'rpi{i}')
    
    # data = client.get_data('temp_external', 'month', [f'rpi{i}' for i in range(4)])
    # print(data)

if __name__ == '__main__':
    main()