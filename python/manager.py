import signal
import sys

from configparser import ConfigParser
from argparse import ArgumentParser
from threading import Timer

from can import Message

from mixcan import MixCAN
from client_mqtt import MQTTClient
from pycan import Pycan
from logger import setup_logger


class MixCANManager(object):

    SERVICE_NAME = "MixCAN"

    def __init__(self, config):
        self._logger = setup_logger(MixCANManager.SERVICE_NAME,
                                    config["log"]["path"])

        self._should_run = False
        self._current_key = bytes("e179017a-62b0-4996-8a38-e91aa9f1", "UTF-8")
        self._is_sender = config["mixcan"]["is_sender"]

        if self._is_sender == "True":

            try:
                self._cycle_time = int(config["mixcan"]["cycle_time"])
            except ValueError:
                self._logger.error("Error occured while parsing config file.")
                sys.exit(1)

            self._mixcan_timer = Timer(self._cycle_time, self._on_timer)
            self._logger.info("MixCAN configured in sending mode")
        else:
            self._logger.info("MixCAN configured in listening mode")
            self._mixcan_timer = None
            self._cycle_time = None
        
        self._last_key_path = config["key"]["last_key"]

        self._mixcan = MixCAN(self._current_key)
        self._last_frame = ""
        self._last_bf = ""
        
        try:
            self._frame_id = int(config["mixcan"]["frame_id"], 16)
            self._mixcan_id = int(config["mixcan"]["mixcan_id"], 16)
        except ValueError:
            logger.error("Could not parse MixCAN frame ids")
            exit(1)

        self._mqtt = MQTTClient(config["mqtt"]["user"],
                                config["mqtt"]["passwd"],
                                config["mqtt"]["host"],
                                int(config["mqtt"]["port"]),
                                MixCANManager.SERVICE_NAME,
                                self._on_new_key)

        self._pycan = Pycan(config["pycan"]["can"],
                            on_message_callback=self._on_new_can_msg)

    def start(self):
        self._should_run = True

        self._logger.info("Starting mqtt client")
        self._mqtt.connect()

        if self._mixcan_timer:
            self._logger.info("Starting mixcan cycle timer")
            self._mixcan_timer.start()
        
        self._logger.info("Starting pycan")
        self._pycan.start()

    def stop(self):

        self._should_run = False

        if self._mixcan_timer:
            self._logger.info("Stopping cycle timer")
            self._mixcan_timer.cancel()
            self._logger.info("Cycle timer stopped")

        if self._pycan.is_running():
            self._logger.info("Stopping the pycan")
            self._pycan.stop()
            self._logger.info("Pycan stopped")

        if self._mqtt.is_connected():
            self._logger.info("Stopping the mqtt client")
            self._mqtt.stop()
            self._logger.info("Mqtt client stopped")
        
    def _on_timer(self):
        self._logger.debug("Creating MixCAN BloomFilter")

        # Create test frame + mixcan frame
        _data = [0xFF, 0xFF, 0xFF, 0xFF,0xFF, 0xFF]
        _data_as_str = "".join(str(val) for val in _data)

        self._mixcan.insert(_data_as_str)
        _mixcan_data = self._mixcan.to_can()
        
        # Reset MixCAN after it was used
        self._mixcan.reset()

        signal_frame = Message(arbitration_id=self._frame_id,
                               data=_data,
                               is_extended_id=True)
        mixcan_frame = Message(arbitration_id=self._mixcan_id,
                               data=_mixcan_data,
                               is_extended_id=True)

        # Send out signal frame + mixcan frame
        self._logger.debug("Sending signal frame: {}".format(str(_data)))
        self._pycan.can_bus.send(signal_frame)

        self._logger.debug("Sending mixcan frame: {}".format(str(_mixcan_data)))
        self._pycan.can_bus.send(mixcan_frame)

        # Restart cycle timer
        self._mixcan_timer.cancel()
        self._mixcan_timer = Timer(self._cycle_time, self._on_timer)
        self._mixcan_timer.start()

    def _on_new_can_msg(self, msg, *args):

        self._logger.debug("Received new message with can-id {}".format(
                            msg.arbitration_id))

        if msg.arbitration_id == self._frame_id:
            self._logger.debug("Received MixCAN frame: {}".format(msg.data))
            self._last_frame = msg.data
            return
        elif msg.arbitration_id == self._mixcan_id:
            self._logger.debug("Received MixCAN bf: {}".format(msg.data))
            self._last_bf = msg.data
            self._verify_mixcan()
            return
        else:
            return

    def _verify_mixcan(self):
        if not self._last_frame or not self._last_bf:
            self._logger.error("Didn't received last frame and bf.")
            return

        _data = [int(i) for i in self._last_frame]
        _data_as_str = "".join(str(val) for val in _data)

        self._mixcan.insert(_data_as_str)

        bf_as_hex = [hex(i) for i in self._last_bf]
        verified = self._mixcan.verifiy_bf(bf_as_hex)

        if not verified:
            self._mqtt.publish_log("MixCAN BF not verified")
        else:
            self._logger.debug("MixCAN verified successfully.")

        self._mixcan.reset()

    def _on_new_key(self, mqttc, obj, msg):

        self._logger.debug("Received new key: {}".format(msg.payload.decode()))
        self._save_last_key(msg.payload.decode())

    def _save_last_key(self, key):
        self._current_key = key


def signal_handler(signum, frame):
    mixcan_manager.stop()
    sys.exit(0)


if __name__ == "__main__":

    signal.signal(signal.SIGQUIT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    signal.signal(signal.SIGINT, signal_handler)

    parser = ArgumentParser(description="MixCAN Manager.")
    parser.add_argument("-c", type=str, help="Path to config file.")
    args = parser.parse_args()

    config = ConfigParser()
    config.read(args.c)

    global mixcan_manager
    mixcan_manager = MixCANManager(config)
    mixcan_manager.start()
