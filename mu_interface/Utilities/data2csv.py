#!/usr/bin/env python3
import os
import csv
import yaml
from pathlib import Path


class CsvStorage:
    transformations = {
        "temp_external": (lambda d, c: d[c] / 10000),                                     # Degrees Celsius
        "temp_PCB": (lambda d, c: d[c] / 10000),                                          # Degrees Celsius
        "soil_temperature": (lambda d, c: d[c] / 10),                                     # Degrees Celsius
        "mag_X": (lambda d, c: d[c] / 1000 * 100),                                        # Micro Tesla
        "mag_Y": (lambda d, c: d[c] / 1000 * 100),                                        # Micro Tesla
        "mag_Z": (lambda d, c: d[c] / 1000 * 100),                                        # Micro Tesla
        "light_external": (lambda d, c: d[c] / 799.4 - 0.75056),                          # Lux
        "humidity_external": (lambda d, c: (d[c] * 3 / 4200000 - 0.1515) / 0.00636),      # Percent (Honeywell HIH-5031)
        "air_pressure": (lambda d, c: d[c] / 100),                                        # Mili Bars
        "differential_potential_CH1": (lambda d, c: (d[c] - 512000) / 1000),              # Mili Volts
        "differential_potential_CH2": (lambda d, c: (d[c] - 512000) / 1000),              # Mili Volts
        "transpiration": (lambda d, c: d[c] / 1000)                                       # Percent
    }

    rounding = {
        "temp_external": 2,
        "temp_PCB": 2,
        "soil_temperature": 2,
        "mag_X": 2,
        "mag_Y": 2,
        "mag_Z": 2,
        "light_external": 1,
        "humidity_external": 2,
        "air_pressure": 2,
        "differential_potential_CH1": 3,
        "differential_potential_CH2": 3,
        "transpiration": 2,
    }

    limits = {
        "temp_external": (0, 60),
        "humidity_external": (0, 100),
    }

    def __init__(self, file_path, file_name, additionalSensors, config_file=None):
        self.file_path = Path(file_path)
        self.file_path.mkdir(parents=True, exist_ok=True)
        self.file_name = file_name
        self.additionalSensors = additionalSensors

        if config_file is None:
            config_file = Path(__file__).parent.absolute() / "config/custom_data_fields.yaml"
            if not config_file.exists():
                config_file = Path(__file__).parent.absolute() / "config/default_data_fields.yaml"

        with open(config_file) as stream:
            config = yaml.safe_load(stream)

        # Names of stored columns.
        self.header = [key for key in config if config[key]] + additionalSensors
        # Indices of stored columns.
        self.filter = [i for i, x in enumerate(config.values()) if x] + list(
            range(len(config), len(config) + len(additionalSensors))
        )

        with open(self.file_path / self.file_name, "w", newline="") as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(["datetime"] + self.header)

    def fix_ownership(self):
        """Change the owner of the file to SUDO_UID"""
        uid = os.environ.get("SUDO_UID")
        gid = os.environ.get("SUDO_GID")
        if uid is not None and gid is not None:
            full_path = self.file_path / self.file_name
            os.chown(full_path, int(uid), int(gid))
            for p in list(full_path.parents)[:-3]:
                os.chown(p, int(uid), int(gid))

    def write(self, timestamp, data):
        data4csv = [timestamp] + data
        with open(self.file_path / self.file_name, "a", newline="") as csvfile:
            csvwriter = csv.writer(csvfile)
            csvwriter.writerow(data4csv)

    def transform_data(self, raw_data):
        """
        Transform the raw data to real values.

        1. Filter the data based on selection which columns to store.
        2. Apply transformation functions to the filtered data.
        3. Check if the transformed values are within the limits.

        Returns:
            transformed_data: list of transformed values
            transformed_dict: dictionary of transformed values
            wrong_values: string of out-of-range values
        """
        # Filter the data.
        transformed_data = [raw_data[i] for i in self.filter]
        # Build a dictionary of raw values. We must not change it because some calculations depend on raw values.
        raw_dict = {self.header[i]: transformed_data[i] for i in range(len(transformed_data))}
        transformed_dict = {}
        # Keep track of out-of-range values.
        wrong_values = []

        for i in range(len(transformed_data)):
            key = self.header[i]
            value = transformed_data[i]
            # If transformation function is defined, apply it. Otherwise, keep the value as is.
            if key in CsvStorage.transformations:
                value = round(CsvStorage.transformations[key](raw_dict, key), CsvStorage.rounding.get(key, 2))
                transformed_data[i] = value
            transformed_dict[key] = value

            if key in CsvStorage.limits:
                if transformed_data[i] < CsvStorage.limits[key][0] or transformed_data[i] > CsvStorage.limits[key][1]:
                    wrong_values.append(f"* {key} = {transformed_data[i]}")

        return transformed_data, transformed_dict, "\n".join(wrong_values)
