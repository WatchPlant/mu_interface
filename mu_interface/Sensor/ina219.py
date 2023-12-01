import os
import sys
import argparse
import time
import socket
from datetime import datetime
from pathlib import Path

import board
import busio
import numpy as np
from adafruit_ina219 import ADCResolution, BusVoltageRange, INA219

from zmq_publisher import ZMQ_Publisher
from mu_interface.Utilities.data2csv import data2csv
from mu_interface.Utilities.utils import get_ip_address, TimeFormat


## Parse arguments.
parser = argparse.ArgumentParser(description="Arguments for the sensor node.")
parser.add_argument('--int', action='store', type=int, default=5,
    help="Time interval between two measurements (in seconds).")
parser.add_argument('--addr', action='store', default='localhost',
    help="Address of the MQTT subscriber. Can be IP, localhost, *.local, etc.")
parser.add_argument('--dir', action='store', default='/home/' + os.getenv('USER') + '/measurements/',
    help="Directory where measurement data is saved.")
args = parser.parse_args()

## Set up ZeroMQ
hostname = socket.gethostname()
ipaddr = get_ip_address()
print(f'IP: {ipaddr}')
ipaddr = [int(x) for x in ipaddr.split('.')]

header = (hostname, 3, False)
pub = ZMQ_Publisher(args.addr)

## Set up I2C sensors. 
i2c_bus = busio.I2C(board.SCL1, board.SDA1)
ina219_solar = INA219(i2c_bus, addr=0x40)
ina219_battery = INA219(i2c_bus, addr=0x41)
# optional : change configuration to use 32 samples averaging for both bus voltage and shunt voltage
ina219_solar.bus_adc_resolution = ADCResolution.ADCRES_12BIT_32S
ina219_solar.shunt_adc_resolution = ADCResolution.ADCRES_12BIT_32S
# optional : change voltage range to 16V
ina219_solar.bus_voltage_range = BusVoltageRange.RANGE_16V
# optional : change configuration to use 32 samples averaging for both bus voltage and shunt voltage
ina219_battery.bus_adc_resolution = ADCResolution.ADCRES_12BIT_32S
ina219_battery.shunt_adc_resolution = ADCResolution.ADCRES_12BIT_32S
# optional : change voltage range to 16V
ina219_battery.bus_voltage_range = BusVoltageRange.RANGE_16V

# Set up csv storing.
file_path = Path(args.dir)
start_time = datetime.now()
print("Measurement started at {}.".format(start_time.strftime(TimeFormat.log)))
print(f"Saving data to: {file_path}")
file_name = f"{hostname}_{start_time.strftime(TimeFormat.file)}.csv"
csv_object = data2csv(file_path, file_name, "energy")
csv_object.fix_ownership()
last_time = datetime.now()

## Measure and display loop
while True:
    # Read data from sensor.
    bus_voltage_solar = round(ina219_solar.bus_voltage, 2)        # voltage on V- (load side)
    current_solar = round(ina219_solar.current, 1)                # current in mA
    bus_voltage_battery = round(ina219_battery.bus_voltage, 2)    # voltage on V- (load side)
    current_battery = round(ina219_battery.current, 1)            # current in mA


    # Publish data over ZMQ.
    payload = np.array([int(datetime.now().timestamp()), bus_voltage_solar, current_solar, bus_voltage_battery, current_battery, *ipaddr])
    pub.publish(header, False, payload)
    
    # Create a new csv file after the specified interval.
    current_time = datetime.now()
    if current_time.hour in {0, 12} and current_time.hour != last_time.hour:
        print("Creating a new csv file.")
        file_name = f"{hostname}_{current_time.strftime(TimeFormat.file)}.csv"
        csv_object = data2csv(file_path, file_name, "energy")
        csv_object.fix_ownership()
        last_time = current_time
    
    # Store data to csv file locally.
    data = payload[:-4].tolist()
    csv_object.write2csv(data)
    
    # TODO: add some statistics to print out

    # Check internal calculations haven't overflowed (doesn't detect ADC overflows)
    if ina219_solar.overflow:
        print("Internal Math Overflow Detected!")
        print("")

    time.sleep(args.int)
