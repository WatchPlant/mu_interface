#!/usr/bin/env python3
import time
import logging
import datetime
import tempfile
from pathlib import Path

from cybres_mu import Cybres_MU

# from Additional_Sensors.rgbtcs34725 import RGB_TCS34725
from fake_zmq_publisher import ZMQ_Publisher
from throttled_zmq_publisher import ZMQ_Publisher_Throttled
from mu_interface.Utilities.data2csv import data2csv
from mu_interface.Utilities.utils import TimeFormat
# from mu_interface.Utilities.HTTP_client import HTTPClient


class Sensor_Node:
    def __init__(self, hostname, port, baudrate, meas_interval, address, file_path, file_prefix):
        self.mu = Cybres_MU(port, baudrate)
        self.pub = ZMQ_Publisher(address)
        self.notify_pub = ZMQ_Publisher_Throttled()
        # self.client = HTTPClient(hostname, hostname)
        self.hostname = hostname
        self.measurment_interval = meas_interval
        self.file_path = file_path
        self.file_prefix = file_prefix
        self.csv_object = None
        self.msg_count = 0
        self.start_time = None
        self.mu_id = 0
        self.mu_mm = 0
        self.mu_settings = {}
        
        # TODO: Refactor and remove this dependency
        self.status_dir = Path("/home/rock/OrangeBox/status/measuring")
        self.status_dir.mkdir(parents=True, exist_ok=True)

        # Add the names of the additional data columns to the list
        # e.g. ['ozon-conc', 'intensity-red', 'intensity-blue']
        # self.rgb = RGB_TCS34725()
        self.additionalSensors = []  # ['light-red', 'light-green', 'light-blue', 'color-temperature', 'light-intensity']

    def check(self):
        """
        Check the communication with the device by requesting a status message.
        """
        response = self.mu.get_initial_status()
        response_lines = response.split("\r\n")
        logging.debug(response_lines)
        try:
            self.mu_settings['OS'], self.mu_settings['CPU_freq'], version = response_lines[2].split(', ')
            self.mu_settings['FW_version'] = version.split(': ')[-1].rstrip('.')
            self.mu_settings['dev_ID'] = response_lines[3].split(': ')[-1].rstrip('.')
            self.mu_settings['meas_cfg'] = response_lines[4].split('=> ')[-1].rstrip('.')
            # TODO: Fill the rest of the settings.
        except (IndexError, ValueError):
            logging.warn("Could not parse the initial status message.")

    def start(self):
        """
        Start the measurements. Continue to publish over MQTT and store to csv.
        """
        
        self.file_path = Path(f"{str(self.file_path)} ({self.mu_settings.get('dev_ID', 'ID NA')})")

        # Measure at set interval.
        response = self.mu.set_measurement_interval(self.measurment_interval)
        logging.debug(response)
        self.mu.start_measurement()

        # Record the starting time and notify the user.
        self.start_time = datetime.datetime.now()
        logging.info("Measurement started at %s.", self.start_time.strftime(TimeFormat.log))
        logging.info("Saving data to: %s", self.file_path)

        # Create the file for storing measurement data.
        file_name = f"{self.file_prefix}_{self.start_time.strftime(TimeFormat.file)}.csv"
        self.csv_object = data2csv(self.file_path, file_name, self.additionalSensors)
        last_time = datetime.datetime.now()
        
        # Create temporary file to signal that measurement is active.
        prefix = f"{self.file_prefix.split('_')[-1]} ({self.mu_settings.get('dev_ID', 'ID NA')})_"
        
        # Measure the average time between measurements.
        time_length = 100
        loop = {"start": time.time(), "duration": [0] * time_length}
        processing = {"start": time.time(), "duration": [0] * time_length}
        time_index = 0

        with tempfile.NamedTemporaryFile(prefix=prefix, dir=self.status_dir):
            while True:
                # Create a new csv file after the specified interval.
                current_time = datetime.datetime.now()
                if current_time.hour in {0, 12} and current_time.hour != last_time.hour:
                    logging.info("Creating a new csv file.")
                    file_name = f"{self.file_prefix}_{current_time.strftime(TimeFormat.file)}.csv"
                    self.csv_object = data2csv(self.file_path, file_name, self.additionalSensors)
                    last_time = current_time

                # Get the next data line.
                next_line = self.mu.get_next()
                loop["duration"][time_index] = time.time() - loop["start"]
                loop["start"] = time.time()
                processing["start"] = time.time()
                header, payload = self.classify_message(next_line)

                # Send data to Edge device via ZMQ if it's valid.
                if header is not None:
                    self.pub.publish(header, self.additionalSensors, payload)

                # Store the data to the csv file.
                if header is not None and header[1] == 1:
                    self.msg_count += 1
                    try:
                        warnings = self.csv_object.write2csv([self.hostname] + payload)
                        #  self.client.add_data(payload, self.additionalSensors)
                        if warnings:
                            self.notify_pub.publish(f"[Warning]: {warnings}", topic="value")
                            
                    except Exception as e:
                        logging.error(
                            "Writing to csv file failed with error:\n%s\n\n\
                            Continuing because this is not a fatal error.",
                            e,
                        )
                        self.notify_pub.publish("[Error]: Writing data to CSV file failed. Fix ASAP!", topic="error")
                    
                # Record the time taken to process the data.
                processing["duration"][time_index] = time.time() - processing["start"]
                time_index += 1
                if time_index == time_length:
                    logging.info("Average loop time: %f", sum(loop["duration"]) / time_length)
                    logging.info("Average processing time: %f", sum(processing["duration"]) / time_length)
                    time_index = 0

                # Print out a status message roughly every 30 mins
                if self.msg_count % 180 == 0 and self.msg_count > 0:
                    td = datetime.datetime.now() - self.start_time
                    hms = (td.seconds // 3600, td.seconds // 60 % 60, td.seconds % 60)
                    duration = f"{td.days} days, {hms[0] :02}:{hms[1] :02}:{hms[2] :02} [HH:MM:SS]"
                    logging.info("I am measuring for %s and I collected %d datapoints.", duration, self.msg_count)

    def classify_message(self, mu_line):
        """
        Determines the message type.

        Args:
            mu_line (str): Complete MU data line

        Returns:
            A tuple containing a header and payload for the MQTT message.
        """
        logging.debug(mu_line)
        try:
            counter = mu_line.count("#")
            if counter == 0:
                # Line is pure data message
                messagetype = 1
                transfromed_data = self.transform_data(mu_line)
                # ID and MM are manually added
                payload = [self.mu_mm, self.mu_id] + transfromed_data

            elif counter == 2:
                # Line is data message/id/measurement mode
                # Every 100 measurements the MU sends also its own
                # ID and measurement mode
                messagetype = 2
                messages = mu_line.split("#")
                mu_id = int(messages[1].split(" ")[1])
                mu_mm = int(messages[2].split(" ")[1])
                # ID and mm get attached at the back of the data array
                payload = [mu_mm, mu_id] + self.transform_data(messages[0])

            elif counter == 4:
                # Line is header
                messagetype = 0
                payload = mu_line
                # ID and MM are saved from the header
                lines = [line.split() for line in mu_line.split("\r\n") if line.startswith("#")]
                lines = {line[0]: line[1] for line in lines}
                self.mu_id = int(lines['#id'])
                self.mu_mm = int(lines['#ta'])
            else:
                logging.warning("Unknown data type: \n%s", mu_line)
                return None, []
        except (ValueError, IndexError, KeyError) as e:
            logging.error("Error while parsing the data: %s", e)
            logging.error("Data: %s", mu_line)
            return None, []

        # Add data from additional external sensors
        if self.additionalSensors and messagetype in {1, 2}:
            # Call here a get_data() method, e.g.:
            # additionalValues = [getOzonValues(), getRGBValues()]
            # Important: len(self.sensors) == len(additionalValues), otherwise
            # it won't work
            additionalValues = []  # self.rgb.getData()#[]
            payload = payload + additionalValues

        header = (self.hostname, messagetype, bool(self.additionalSensors))
        return header, payload

    def transform_data(self, string_data):
        """
        Transform MU data from string to numpy array.

        Args:
            string_data (str): MU data in string format.

        Returns:
            A numpy array containing the MU data
        """
        split_data = string_data.split(" ")
        timestamp = [int(time.mktime(datetime.datetime.now().timetuple()))]
        measurements = [int(elem) for elem in split_data[1:]]
        return timestamp + measurements

    def stop(self):
        """
        Stop the measurement and clean up.
        """
        logging.info("Measurement stopped at %s.", datetime.datetime.now().strftime(TimeFormat.log))
        self.mu.stop_measurement()

    def shutdown(self):
        """
        Restart the MU device and perform final clean up on shutdown.
        """
        self.mu.restart()
        time.sleep(0.5)
        self.close()

    def close(self):
        """
        Perform clean up of the sensor node.
        """
        self.mu.ser.close()
        self.pub.socket.close()
        self.pub.context.term()
