#!/usr/bin/env python3
import json
import time
import logging
import datetime
from pathlib import Path

from cybres_mu import Cybres_MU

# from Additional_Sensors.rgbtcs34725 import RGB_TCS34725
from fake_zmq_publisher import ZMQ_Publisher
from throttled_zmq_publisher import ZMQ_Publisher_Throttled
from mu_interface.Utilities.data2csv import CsvStorage
from mu_interface.Utilities.utils import TimeFormat
from mu_interface.Utilities.HTTP_client import HTTPClient


class Sensor_Node:
    def __init__(self, hostname, port, baudrate, meas_interval, address, file_path, file_prefix):
        self.mu = Cybres_MU(port, baudrate)
        self.pub = ZMQ_Publisher(address)
        self.notify_pub = ZMQ_Publisher_Throttled()
        self.http_client = None
        self.hostname = hostname  # e.g. rockpi, OB-ZAG-0
        self.measurment_interval = meas_interval
        self.file_path = file_path  # e.g. /home/rockpi/measurements/OB-ZAG-0_2/MU/CYB1
        self.file_prefix = file_prefix  # e.g. rockpi_1_ACM0, OB-ZAG-0_2_CYB1
        self.device = file_prefix.split("_")[-1]  # e.g. ACM0, CYB1
        self.csv_object = None
        self.msg_count = 0
        self.start_time = None
        self.mu_id = 0
        self.mu_mm = 0
        self.mu_config = {}

        # TODO: Refactor and remove this dependency
        self.status_dir = Path.home() / "OrangeBox/status/measuring"
        self.status_dir.mkdir(parents=True, exist_ok=True)

        # Add the names of the additional data columns to the list
        # e.g. ['ozon-conc', 'intensity-red', 'intensity-blue']
        # self.rgb = RGB_TCS34725()
        self.additionalSensors = []  # ['light-red', 'light-green', 'light-blue', 'color-temperature', 'light-intensity']

    def check(self):
        """
        Check the communication with the device by requesting a status message.
        """
        response = self.mu.get_system_messages()
        logging.debug(f"Checking system messages:\n{response}")

    def get_config(self, output=False):
        """
        Get the configuration of the MU device.
        """
        response = self.mu.get_initial_status()
        response_lines = response.split("\r\n")
        logging.debug(f"Initial status message:\n{response}")

        try:
            self.mu_config, mu_config_raw = Cybres_MU.parse_status_message(response_lines[1])
            self.mu_config['OS version'], self.mu_config['CPU freq'], version = response_lines[2].split(', ')
            self.mu_config['FW version'] = version.split(': ')[-1].rstrip('.')
            self.mu_config['meas. config'] = response_lines[4].split('=> ')[-1].rstrip('.')
            if output:
                logging.info(f"Blue box configuration:\n{Cybres_MU.print_config_dict(self.mu_config)}")
            logging.debug(json.dumps(mu_config_raw, indent=4))
        except (IndexError, ValueError) as e:
            logging.warning(f"Could not parse the initial status message.\n{e}")

    def configure(self):
        """
        Configure the MU device.
        """
        if self.mu_config.get("Meas. mode") != Cybres_MU.MeasurementMode.CONT_MEAS_FIXED:
            response = self.mu.set_measurement_mode(Cybres_MU.MeasurementMode.CONT_MEAS_FIXED)
            logging.debug(response)

        if self.mu_config.get("Waveform range") != Cybres_MU.WaveformRange.RANGE_1V:
            response = self.mu.set_waveform_range(Cybres_MU.WaveformRange.RANGE_1V)
            logging.debug(response)

        if self.mu_config.get("Waveform amplitude") != 120:
            response = self.mu.set_waveform_amplitude(120)
            logging.debug(response)

        if self.mu_config.get("TIA amplification") != Cybres_MU.TIAAmplification.GAIN_AUTO:
            response = self.mu.set_tia_amplification(Cybres_MU.TIAAmplification.GAIN_AUTO)
            logging.debug(response)

        if self.mu_config.get("Meas. interval") != self.measurment_interval:
            response = self.mu.set_measurement_interval(self.measurment_interval)
            logging.debug(response)

    def start(self):
        """
        Start the measurements. Continue to publish over MQTT and store to csv.
        """
        # Configure the MU device.
        self.get_config()
        self.configure()
        self.get_config(output=True)

        self.file_path = Path(f"{str(self.file_path)} ({self.mu_config.get('ID', 'ID NA')})")

        # Start the HTTP client.
        self.http_client = HTTPClient(self.hostname, timeout=self.measurment_interval / 1000)
        self.http_client.start()

        # Start measurements.
        self.mu.start_measurement()

        # Record the starting time and notify the user.
        self.start_time = datetime.datetime.now()
        logging.info("Measurement started at %s.", self.start_time.strftime(TimeFormat.log))
        logging.info("Saving data to: %s", self.file_path)

        # Create the file for storing measurement data.
        file_name = f"{self.file_prefix}_{self.start_time.strftime(TimeFormat.file)}.csv"
        self.csv_object = CsvStorage(self.file_path, file_name, self.additionalSensors)
        last_time = datetime.datetime.now()

        # Measure the average time between measurements.
        time_length = 100
        loop = {"start": time.time(), "duration": [0] * time_length}
        processing = {"start": time.time(), "duration": [0] * time_length}
        time_index = 0

        while True:
            # Create a new csv file after the specified interval.
            current_time = datetime.datetime.now()
            if current_time.hour in {0, 12} and current_time.hour != last_time.hour:
                logging.info("Creating a new csv file.")
                file_name = f"{self.file_prefix}_{current_time.strftime(TimeFormat.file)}.csv"
                self.csv_object = CsvStorage(self.file_path, file_name, self.additionalSensors)
                last_time = current_time

            # Get the next data line.
            try:
                next_line = self.mu.get_next()
            except TimeoutError:
                logging.error("Caught timeout error while reading from Blue box.")
                self.notify_pub.publish("[Error]: No data received from Blue box.", topic="timeout_error")
                continue

            loop["duration"][time_index] = time.time() - loop["start"]
            loop["start"] = time.time()
            processing["start"] = time.time()
            mu_header, mu_payload = self.classify_message(next_line)

            # Send data to Edge device via ZMQ if it's valid.
            if mu_header is not None:
                self.pub.publish(mu_header, self.additionalSensors, mu_payload)

            # Store the data to the csv file and optionally send it to the cloud.
            if mu_header is not None and mu_header[1] == 1:
                self.msg_count += 1

                # CSV part
                try:
                    timestamp = datetime.datetime.fromtimestamp(mu_payload[3])
                    data_line, data_dict, wrong_values = self.csv_object.transform_data(mu_payload)
                    self.csv_object.write(timestamp.strftime(TimeFormat.data), data_line)

                    if wrong_values:
                        warning = f"[Warning] Unexpected values for:\n{self.device}\n{wrong_values}"
                        self.notify_pub.publish(warning, topic="value_out_of_range")

                except Exception as e:
                    logging.error(f"Writing to csv file failed with error:\n{e}\n\n"
                                  f"Continuing because this is not a fatal error.")
                    self.notify_pub.publish("[Error]: Writing data to CSV file failed. Fix ASAP!", topic="csv_error")

                # Cloud part
                try:
                    fail_rate = self.http_client.send(timestamp, data_dict)
                    if fail_rate is not None and fail_rate > 0.49:
                        warning_type = "[Error]" if fail_rate > 0.89 else "[Warning]"
                        warning_text = f"{warning_type} Sending data to the live web view failed in {fail_rate * 100} % of the time."
                        self.notify_pub.publish(f"{warning_text}\nThis does not affect the experiment, but somebody should take a look.",
                                                topic=warning_type + "-website")
                except RuntimeError as e:
                    logging.error(f"Live web view is unresponsive. Sending data failed:\n{e}\n\n"
                                  f"Continuing because this is not a fatal error.")
                    self.notify_pub.publish("[Error]: Sending data to the live web view failed."
                                            "This does not affect the experiment, but somebody should take a look.",
                                            topic="website_error")
                except Exception as e:
                    logging.error(f"Unhandled exception when sending data to the live web view:\n{e}\n\n"
                                  f"Continuing because this is not a fatal error.")
                    self.notify_pub.publish("[Error]: Sending data to the live web view failed."
                                            "This does not affect the experiment, but somebody should take a look.",
                                            topic="website_error")

            # Record the time taken to process the data.
            processing["duration"][time_index] = time.time() - processing["start"]
            time_index += 1
            if time_index == time_length:
                logging.debug("Average loop time: %f", sum(loop["duration"]) / time_length)
                logging.debug("Average processing time: %f", sum(processing["duration"]) / time_length)
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
                # Line is pure data message.
                messagetype = 1
                timestamp, measurements = self.transform_data(mu_line)
                # ID and MM are manually added.
                payload = [self.hostname, self.mu_mm, self.mu_id, timestamp] + measurements

            elif counter == 2:
                # Line is data message/id/measurement mode.
                # Every 100 measurements the MU sends also its own ID and measurement mode.
                messagetype = 2
                messages = mu_line.split("#")
                mu_id = int(messages[1].split(" ")[1])
                mu_mm = int(messages[2].split(" ")[1])
                timestamp, measurements = self.transform_data(messages[0])
                # ID and MM are manually added.
                payload = [self.hostname, mu_mm, mu_id, timestamp] + measurements

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
        Transform MU data from string to list.

        Args:
            string_data (str): MU data in string format.

        Returns:
            A list containing the MU data.
        """
        split_data = string_data.split(" ")
        timestamp = int(time.mktime(datetime.datetime.now().timetuple()))
        measurements = [int(elem) for elem in split_data[1:]]
        return timestamp, measurements

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

    def close(self):
        """
        Perform clean up of the sensor node.
        """
        self.mu.ser.close()
        self.pub.socket.close()
        self.pub.context.term()
        if self.http_client:
            self.http_client.stop()
