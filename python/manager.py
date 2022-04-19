import signal
import time
import sys

from configparser import ConfigParser
from argparse import ArgumentParser

from mixcan import MixCAN
from client_mqtt import MQTTClient
from pycan import Pycan

class MixCANManager(object):

    SERVICE_NAME = "MixCAN"

    def __init__(self, config):
        self._should_run = False
        self._current_key = ""
        self._is_verifier = config["mixcan"]["is_verifier"]
        self._last_key_path = config["key"]["last_key"]

        self._mixcan = MixCAN(self._current_key)
        self._last_frame = ""
        self._last_bf = ""

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
        self._mqtt.connect()
        self._pycan.start()

    def stop(self):

        self._should_run = False
        if self._pycan.is_running():
            print("Stopping the pycan")
            self._pycan.stop()
            print("Pycan stopped")

        if self._mqtt.is_connected():
            print("Stopping the mqtt client")
            self._mqtt.stop()
            print("Mqtt client stopped")

    def _on_new_can_msg(self, msg, *args):

        if msg.arbitration_id == MixCAN.FRAME_ID:
            print("Received MixCAN frame: {}".format(msg.data))
            self._last_frame = msg.data
            return
        elif msg.arbitration_id == MixCAN.BF_ID:
            print("Received MixCAN bf: {}".format(msg.data))
            self._last_bf = msg.data
            self._verify_mixcan()
            return
        else:
            return

    def _verify_mixcan():
        if not self._last_frame or not self._last_bf:
            print("Didn't received last frame and bf.")
            return

        self._mixcan.insert(self._last_frame)
        verified = self._mixcan.verifiy_bf(self._last_bf)

        if not verified:
            self._mqtt.publish_log("MixCAN BF not verified")

    def _on_new_key(self, mqttc, obj, msg):

        print("Received new key: {}".format(msg.payload.decode()))
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
