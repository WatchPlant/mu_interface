#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# from datetime import datetime
import datetime
import json
import logging
import os
import queue
import threading
from collections import deque
from enum import Enum

import requests
import yaml
from requests.adapters import HTTPAdapter, Retry
from func_timeout import func_timeout, FunctionTimedOut


class DateRange(Enum):
    LAST_HOUR = "last_hour"
    LAST_DAY = "last_day"
    LAST_MONTH = "month"
    LAST_YEAR = "twelve_months"


url = os.environ["WP_API_URL"]
headers = {"Content-Type": "application/json", "Accept": "application/json", "Authorization": os.environ["WP_API_AUTH"]}
SLOW_TIMEOUT = 5
FAST_TIMEOUT = 1


website_mapping = {
    # "OB_MU ID (handle)": "descriptive name",
    "OB-ZAG-0_CYB1": "Zagreb Test Node",
    "OB-BCN-2_CYB1": "BCN IDAEA Ivy",
    "OB-BCN-1_CYB1": "BCN Eixample Ivy",
    "OB-BCN-3_CYB1": "BCN Vic Ivy",
}


class HTTPClient(object):
    timestamp_format = "%Y-%m-%d %H:%M:%S"

    def __init__(self, node_handle, display_name=None) -> None:
        self.node_handle = node_handle
        self.display_name = website_mapping.get(node_handle, display_name or node_handle)

        self.enabled = True
        if node_handle.startswith("OB") and node_handle not in website_mapping:
            self.enabled = False
            return

        self.session = requests.Session()
        retries = Retry(total=3, backoff_factor=0.5)
        self.session.mount("http://", HTTPAdapter(max_retries=retries))
        self.session.mount("https://", HTTPAdapter(max_retries=retries))

        self.known_data_fields = None
        self.known_nodes = None
        # Count how many times the last 10 data additions were successful.
        self.success_tracker = deque([True] * 10, maxlen=10)

        # TODO: what if this hangs?
        self.register_node()

        # Separate thread for adding data.
        self.queue = queue.Queue(maxsize=10)
        self._stop_event = threading.Event()
        self.thread = threading.Thread(target=self._data_processing_thread, daemon=True)
        self.thread_started = False

    def __getattribute__(self, name):
        """
        Oh, this is such a hacky way to silently ignore method calls.

        I use it because I want to distribute the same code to all orange
        boxes and run for all connected MU devices, but I don't want all of them
        to send data to the website. I don't want to add if statements to every
        method call, so I just disable the whole thing automatically.
        """
        attr = object.__getattribute__(self, name)

        # If the accessed attribute is a method, we need to check 'enabled'
        if callable(attr) and name != "__init__":
            def wrapper(*args, **kwargs):
                # Only call the method if 'enabled' is True
                if self.enabled:
                    return attr(*args, **kwargs)
                logging.debug(f"Attempted to call HTTPClient method '{name}'. Method is disabled.")
            return wrapper
        return attr

    def get_nodes(self):
        """Get the list of nodes currently on the website."""
        query = "nodes"

        try:
            response = self.session.get(url + query, headers=headers, timeout=SLOW_TIMEOUT)
            logging.debug(response.text)
        except (requests.exceptions.Timeout, FunctionTimedOut):
            logging.error("Timeout while getting the list of nodes from the website.")
            return None

        if not response.ok:
            logging.error(f"Failed to get existing nodes from the website. Status code {response.status_code}")
            return None

        parsed = json.loads(response.text)
        self.known_nodes = [x["handle"] for x in parsed["data"]]
        return parsed["data"]

    def node_exists(self, node_handle=None, force_refresh=False):
        """
        Check if a given node already exists on the website.

        Args:
            node_handle (str): Handle of the node to check. If None checks self.

        Returns:
            True if node exists, False otherwise.
        """
        if node_handle is None:
            node_handle = self.node_handle

        if self.known_nodes is None or force_refresh:
            self.get_nodes()

        if self.known_nodes is None:
            logging.error("Could not get list of nodes from the website.")
            return False

        return node_handle in self.known_nodes

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
        query = "nodes"
        payload = {"handle": node_handle, "name": display_name}

        if self.node_exists(node_handle):
            logging.info(f"Node {node_handle} already exists.")
            return False

        response = None
        try:
            response = requests.request("POST", url + query, json=payload, headers=headers, timeout=SLOW_TIMEOUT)
            logging.debug(response.text)
        except (requests.exceptions.Timeout, FunctionTimedOut):
            logging.error(f"Timeout while waiting to add node {node_handle} to the website.")
            return False

        if not response.ok:
            logging.error(f"Failed to add node {node_handle} to the website. Status code {response.status_code}")
            return False

        return True

    def register_node(self):
        """Add a node with parameters specified in the constructor."""
        try:
            func_timeout(SLOW_TIMEOUT, self.add_node, args=(self.node_handle, self.display_name))
        except FunctionTimedOut:
            logging.error("func_timeout had to terminate the add_node or some downstream function.")

    def delete_node(self, node_handle):
        """
        Delete the node with the matching handle.

        Args:
            node_handle (str): Handle of node to delete.

        Returns:
            True if a successful response is received, False otherwise.
        """
        query = "nodes/delete"
        payload = {"handle": node_handle}

        try:
            response = self.session.post(url + query, json=payload, headers=headers, timeout=SLOW_TIMEOUT)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error(f"Timeout while waiting to delete node {node_handle} from the website.")
            return False

        if not response.ok:
            logging.error(f"Failed to delete node {node_handle} from the website. Status code {response.status_code}")
            return False

        return True

    def get_data_fields(self):
        """Return all available data fields on the website."""
        query = "data-field"
        response = None
        try:
            response = self.session.get(url + query, headers=headers, timeout=SLOW_TIMEOUT)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error("Timeout while waiting for the current list of data fields from the website.")
            return None

        if not response.ok:
            logging.error(f"Failed to get existing data fields from the website. Status code {response.status_code}")
            return None

        parsed = json.loads(response.text)
        self.known_data_fields = [x["handle"] for x in parsed["data"]]
        return parsed["data"]

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
        query = "data-field"
        payload = {"name": field_name, "handle": field_handle, "unit": unit}

        try:
            response = self.session.post(url + query, json=payload, headers=headers, timeout=SLOW_TIMEOUT)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error(f"Timeout while waiting to add data field {field_handle} to the website.")
            return False

        if not response.ok:
            logging.error(f"Failed to add data field {field_handle} to the website. Status code {response.status_code}")
            return False

        return True

    def delete_data_field(self, field_handle):
        """
        Delete data field with matching handle.

        Args:
            data_handle (str): Handle of data field to delete.

        Returns:
            True if a successful response is received, False otherwise.
        """
        query = "data-field/delete"
        payload = {"handle": field_handle}

        try:
            response = self.session.post(url + query, json=payload, headers=headers, timeout=SLOW_TIMEOUT)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error(f"Timeout while waiting to delete data field {field_handle} from the website.")
            return False

        if not response.ok:
            logging.error(f"Failed to delete field {field_handle} from the website. Status code {response.status_code}")
            return False

        return True

    def get_data(self, date_range: DateRange, node_handles=None):
        """
        Return all data entries from specified nodes.

        Args:
            date_range (DateRange): Time range to retrieve data from.
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
        if not isinstance(date_range, DateRange):
            logging.error(f"Date range {date_range} is not known.")
            return False

        query = "sensordata-multiple"
        payload = {"node_handles": node_handles, "date_range": date_range.value}

        try:
            response = self.session.post(f"{url}{query}", json=payload, headers=headers, timeout=SLOW_TIMEOUT)
            logging.debug(response.text)
        except requests.exceptions.Timeout:
            logging.error("Timeout while waiting to retrieve data from the website.")
            return False

        if not response.ok:
            logging.error(f"Failed to retrieve data from the website. Status code {response.status_code}")
            return False

        parsed = json.loads(response.text)
        return parsed["data"]

    def add_data(self, timestamp, data, node_handle=None):
        """
        Add a single measurement set to the website.

        Args:
            timestamp (datetime or str): UTC timestamp of the data in "%Y-%m-%d %H:%M:%S" format.
            data (dict): Dictionary with the data to be added.
            node_handle (str): Node that collected the data. If None, it assumes itself as collector.

        Returns:
            True if a successful response is received, False otherwise.
        """
        if node_handle is None:
            node_handle = self.node_handle

        try:
            timestamp = self.validate_timestamp(timestamp)
        except ValueError as e:
            logging.error(e)
            self.success_tracker.append(False)
            return False

        query = "sensordata"
        payload = {"node_handle": node_handle, "data": data, "date": timestamp}

        try:
            response = self.session.post(url + query, json=payload, headers=headers, timeout=FAST_TIMEOUT)
        except (
            requests.exceptions.Timeout,
            requests.exceptions.ConnectionError,
            requests.exceptions.RequestException,
            FunctionTimedOut,
        ) as e:
            logging.error(f"Error while adding new data to the website: {e}")
            self.success_tracker.append(False)
            return False

        if not response.ok:
            logging.error(f"Failed to add data to the website. Status code {response.status_code}")
            self.success_tracker.append(False)
            return False

        self.success_tracker.append(True)
        return True

    def send(self, timestamp, data):
        try:
            self.queue.put_nowait((timestamp, data))
        except queue.Full:
            raise RuntimeError("HTTP client queue is full. This means the client is stuck.")

        return self.success_tracker.count(False) / len(self.success_tracker)

    def _data_processing_thread(self):
        while not self._stop_event.is_set():
            try:
                timestamp, data = self.queue.get()
                if data is None:  # Gracefully stop the thread if None is received.
                    break
                func_timeout(SLOW_TIMEOUT, self.add_data, args=(timestamp, data))
            except FunctionTimedOut:
                logging.error("func_timeout had to terminate the add_data function.")
            except Exception as e:
                logging.error(
                    f"Uncaught exception in _data_processing_thread: {e}\n"
                    f"Continuing because this is not a fatal error."
                )
            finally:
                self.queue.task_done()

    def start(self):
        self.thread.start()
        self.thread_started = True

    def stop(self):
        if self.enabled and self.thread_started:
            self._stop_event.set()
            self.queue.put((None, None))
            self.thread.join()
            self.thread_started = False

    @staticmethod
    def validate_timestamp(timestamp):
        if isinstance(timestamp, datetime.datetime):
            return timestamp.astimezone(datetime.timezone.utc).strftime(HTTPClient.timestamp_format)
        elif isinstance(timestamp, str):
            datetime.datetime.strptime(timestamp, HTTPClient.timestamp_format)
            return timestamp
        else:
            raise ValueError("Timestamp must be either a datetime object or a string.")


def add_data_fields_from_yaml(client, yaml_file_path):
    with open(yaml_file_path, "r") as stream:
        try:
            data_fields = yaml.safe_load(stream)
            for field, settings in data_fields.items():
                if settings["show"]:
                    client.add_data_field(field, settings["name"], settings["unit"])
        except yaml.YAMLError as exc:
            print(exc)


def main():
    from http_client_dev import sim_real_time

    setup_logger("TEST", level=logging.DEBUG)
    logging.info("Starting HTTP client.")

    client = HTTPClient("dev", "Development Node")
    sim_real_time(client)


def test():
    """It would be better to write using unittest, but we are actually interested to see how the API responds."""
    import datetime
    import time
    from pathlib import Path

    import pandas as pd

    # Setup logging.
    setup_logger("TEST", level=logging.INFO)
    logging.info("Starting HTTP client.")

    # Create the client.
    client = HTTPClient("dev", "Development Node")

    # Test listing the existing nodes.
    print(json.dumps(client.get_nodes(), indent=2))

    # Test adding and deleting nodes.
    print(f"Node 'rpi0' exists: {client.node_exists('rpi0')}")
    print(f"Node 'test' exists: {client.node_exists('test')}")
    print("Adding node 'test'...")
    client.add_node("test", "Testing node")
    print(f"Node 'test' exists: {client.node_exists('test', force_refresh=True)}")
    input("Press Enter to continue...")
    print("Deleting node 'test'...")
    client.delete_node("test")
    print(f"Node 'test' exists: {client.node_exists('test', force_refresh=True)}")

    input("Press Enter to continue...")

    print("Registering node...")
    client.register_node()

    # Test adding and deleting data fields.
    print(json.dumps(client.get_data_fields(), indent=2))
    print("Adding data field 'test'...")
    client.add_data_field("test", "Test field", "T")
    print(json.dumps(client.get_data_fields(), indent=2))
    input("Press Enter to continue...")
    print("Deleting data field 'test'...")
    client.delete_data_field("test")
    print(json.dumps(client.get_data_fields(), indent=2))
    input("Press Enter to continue...")

    # Test getting data.
    print("Getting data...")
    print(json.dumps(client.get_data(DateRange.LAST_YEAR), indent=2))
    print(json.dumps(client.get_data(DateRange.LAST_YEAR, "rpi0"), indent=2))

    # Test adding data.
    print("Adding data...")
    data_file = Path(__file__).parent.absolute() / "data/rpi1_reformatted.csv"
    config_file = Path(__file__).parent.absolute() / "config/custom_data_fields.yaml"
    with open(config_file) as cf:
        config = yaml.safe_load(cf)
        keys = [key for key in config if config[key] is True]
    df = pd.read_csv(data_file, sep=",", header=0)
    df = df[keys]

    for i in range(6):
        data_line = df.iloc[i]
        timestamp = datetime.datetime.now(datetime.timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
        data = data_line.to_dict()
        client.add_data(data, timestamp)
        time.sleep(10)

    print(json.dumps(client.get_data(DateRange.LAST_HOUR), indent=2))

    input("Press Enter to continue...")
    client.delete_node("dev")


if __name__ == "__main__":
    from mu_interface.Utilities.log_formatter import setup_logger

    test()
