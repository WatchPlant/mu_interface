#!/usr/bin/env python3
import enum
import logging
import re
import time

import serial


class WatchdogCounter:
    def __init__(self, timeout_multiplier, timeout_value):
        self.timeout_multiplier = timeout_multiplier
        self.timeout_value = timeout_value
        self.limit = 0
        self.update_limit(timeout_value)
        self.last_valid = None
        self.last_report = 0

    def check(self, data, target):

        if self.last_valid is None:
            self.last_valid = time.time()

        # Compare received data with target data using regex
        if re.match(target, data):
            self.last_valid = time.time()

        delay = time.time() - self.last_valid
        relative_delay = round(delay / self.timeout_value, 1)

        if delay >= self.limit:
            do_report = False
            if int(relative_delay) != self.last_report:
                do_report = True
                self.last_report = int(relative_delay)
            return False, relative_delay, do_report

        return True, relative_delay, False

    def update_limit(self, timeout_value):
        # When measurment interval is set to 1 second, actual interval is often
        # longer (2-3 seconds), so watchdog constantly triggers. This is an
        # arbitraraliy chosen value to make it more tolerant.
        timeout_multiplier = max(5, self.timeout_multiplier) if timeout_value < 1.5 else self.timeout_multiplier
        self.timeout_value = timeout_value
        self.limit = timeout_multiplier * timeout_value


class Cybres_MU:
    config_dict = {
        "D": "ID",
        "P": "measurement_interval",
        "E": "waveform_range",
        "N": "waveform_amplitude",
        "!": "measurement_mode",
        "$": "tia_amplification",
    }

    class MeasurementMode(enum.Enum):
        EIS_OFF = 0
        IMPEDANCE_SPECTROSCOPE = 1
        SIGNAL_SCOPE = 2
        CONT_MEAS_FIXED = 3
        CONT_MEAS_VARIABLE = 4
        FRP = 5
        CONT_FRP = 6

    class WaveformRange(enum.Enum):
        RANGE_1V = 1
        RANGE_01V = 2
        RANGE_001V = 3

    class TIAAmplification(enum.Enum):
        GAIN_50 = 0
        GAIN_500 = 1
        GAIN_5000 = 2
        GAIN_50000 = 3

    def __init__(self, port_name, baudrate=460800):
        self.timeout = 1

        self.ser = serial.Serial(
            port=None,
            baudrate=baudrate,
            bytesize=serial.EIGHTBITS,
            parity=serial.PARITY_NONE,
            stopbits=serial.STOPBITS_ONE,
            timeout=self.timeout,
            write_timeout=self.timeout,
            xonxoff=False,
            rtscts=True,
            dsrdtr=False,
        )

        self.data_watchdog = WatchdogCounter(3, 10)
        self.frame_watchdog = WatchdogCounter(3, 10)

        self.ser.port = port_name
        self.ser.rts = True
        self.ser.dtr = False
        time.sleep(0.1)
        self.ser.open()
        time.sleep(0.1)

        self.ser.reset_input_buffer()
        self.ser.reset_output_buffer()  # Just in case...

        time.sleep(0.1)
        # self.ser.close()
        # time.sleep(0.1)
        # self.ser.open()
        # time.sleep(0.1)

        self.start_char = "Z"

    # Finds the start of the next data set
    def find_start(self):
        start_found = False
        debug_buffer = ""
        while not start_found:
            # If serial buffer is empty, read will return an empty string after timeout.
            char = self.ser.read(1).decode("ascii")
            debug_buffer += char

            # Check that we are receiving something.
            ok, delay, warn = self.data_watchdog.check(char, r".+")
            if warn:
                logging.error(
                    f"Nothing received from Blue box {delay} times longer than expected."
                )
            ok, delay, warn = self.frame_watchdog.check(char, r"A")
            if warn:
                logging.error(
                    f"Start char not found {delay} times longer than expected."
                )
                logging.debug(f"Current buffer state {debug_buffer}")

            if char == "A":
                start_found = True
                debug_buffer = ""
        self.start_char = char

    # Returns the next complete data set
    def get_next(self):
        line = ""
        end_found = False
        if self.start_char != "A":
            self.find_start()
        while not end_found:
            next_char = self.ser.read(1).decode("ascii")
            ok, delay, warn = self.frame_watchdog.check(next_char, r"Z")
            if warn:
                logging.error(
                    f"End char not found {delay} times longer than expected.",
                )

            if next_char == "Z":
                end_found = True
            else:
                line += next_char
            self.start_char = next_char
        return line[:-1]

    def get_initial_status(self):
        self.ser.write(b",ss*")
        return self._get_response(sleep_time=0.5)

    def get_system_messages(self):
        self.ser.write(b',sy*')
        return self._get_response(sleep_time=0.5)

    def restart(self):
        self.ser.write(b",sr*")

    def start_measurement(self):
        self.ser.write(b",ms*")

    def stop_measurement(self):
        time.sleep(1)
        self._get_response(sleep_time=0.5)
        self.ser.reset_input_buffer()
        self.ser.reset_output_buffer()
        self.ser.write(b",mp*")

    def set_measurement_interval(self, interval):
        self.data_watchdog.update_limit(interval / 1000)
        self.frame_watchdog.update_limit(interval / 1000)

        set_interval = ",mi{:05}*".format(interval)
        self.ser.write(set_interval.encode())
        return self._get_response(sleep_time=0.5)

    def set_waveform_amplitude(self, amplitude):
        if amplitude < 1 or amplitude > 127:
            raise ValueError(f"Impedance Waveform Amplitude must be 1-127. Current: {amplitude}")

        self.ser.write(f",ya{amplitude:03}*".encode())
        return self._get_response(sleep_time=0.5)

    def set_waveform_range(self, range: WaveformRange):
        self.ser.write(f",yy{range.value}*".encode())
        return self._get_response(sleep_time=0.5)

    def set_measurement_mode(self, mode: MeasurementMode):
        self.ser.write(f",yn{mode.value}*".encode())
        return self._get_response(sleep_time=0.5)

    def to_flash(self):
        self.ser.write(b"sf2*")

    def read_all_lines(self):
        self.ser.write(b"f1*")  # f1, mr
        while True:
            line = self.get_next()
            print(line)

    # def read_all(self):
    #     self.ser.write(b'f1*')
    #     counter = 0
    #     while True:
    #         char = self.return_serial()
    #         print(char, end='')
    #         if char == 'A':
    #             counter +=1
    #             print(f"-----------------{counter}---------------------------")

    def _get_response(self, sleep_time=0.1):
        time.sleep(sleep_time)
        response = ""
        while self.ser.in_waiting:
            response += self.ser.read(1).decode("ascii")
        return response

    @staticmethod
    def parse_status_message(msg):
        if msg[0] != "I" or msg[-1] != "Y":
            raise ValueError("Invalid status message format.")
        msg = msg[1:-1]

        # Initialize variables
        raw = {}
        processed = {}
        key = None
        value = ""

        for char in msg:
            if char.isdigit():
                value += char  # If the character is a digit, add it to the value
            else:
                if key is not None and value:
                    raw[key] = int(value)  # Assign the accumulated value to the previous key
                key = char  # The current character is the new key
                value = ""  # Reset the value accumulator

        # Add the last key-value pair
        if key is not None and value:
            raw[key] = int(value)

        for key, value in Cybres_MU.config_dict.items():
            processed[value] = raw[key]

        return processed, raw


def test_mu():
    mu = Cybres_MU("/dev/ttyACM0")
    mu.set_measurement_interval(1000)
    mu.start_measurement()
    time.sleep(180)
    print("Now reading")
    # mu.read_all()


def test_watchdog():
    watchdog = WatchdogCounter(3, 1)
    while True:
        c = input("> ")
        print(watchdog.check(c, r".+"))


if __name__ == "__main__":
    mode = Cybres_MU.MeasurementMode.CONT_MEAS_FIXED
    print(f",yn{mode.value:}*".encode())
