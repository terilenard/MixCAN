import sys
import signal

from threading import Timer
from argparse import ArgumentParser
from configparser import ConfigParser

from can import Message


from pycan import Pycan
from utils import get_key
from mixcan import MixCAN
from utils import write_key
from logger import setup_logger
from client_mqtt import MQTTClient


class MixCANManager(object):

    SERVICE_NAME = "MixCAN"

    def __init__(self, config):
        self._logger = setup_logger(MixCANManager.SERVICE_NAME,
                                    config["log"]["path"])

        self._should_run = False
        self._is_sender = config["mixcan"]["is_sender"]

        if self._is_sender == "True":
            self._logger.info("MixCAN configured in sending mode")

            self._pycan = Pycan(config["pycan"]["can"],
                                on_message_callback=self._on_new_can_msg_sender)
        else:
            self._logger.info("MixCAN configured in listening mode")

            self._pycan = Pycan(config["pycan"]["can"],
                                on_message_callback=self._on_new_can_msg_recv)
        
        self._last_key_path = config["key"]["last_key"]
        self._current_key = get_key(self._last_key_path)

        if not self._current_key:
            self._logger.error("Error reading current key.")
            exit(1)

        self._mixcan = MixCAN(self._current_key)

        self._frame_queue = []
        self._bf_queue = []
        
        try:
            self._frame_id = [int(i,16) for i in (config["mixcan"]["frame_id"]).split(',')]
            self._mixcan_id = [int(i,16) for i in (config["mixcan"]["mixcan_id"]).split(',')]
        except ValueError:
            self._logger.error("Could not parse MixCAN frame ids")
            exit(1)

        self._mqtt = MQTTClient(config["mqtt"]["user"],
                                config["mqtt"]["passwd"],
                                config["mqtt"]["host"],
                                int(config["mqtt"]["port"]),
                                MixCANManager.SERVICE_NAME,
                                self._on_new_key)
    def start(self):
        self._should_run = True

        self._logger.info("Starting mqtt client")
        self._mqtt.connect()
        
        self._logger.info("Starting pycan")
        self._pycan.start()

    def stop(self):

        self._should_run = False

        if self._pycan.is_running():
            self._logger.info("Stopping the pycan")
            self._pycan.stop()
            self._logger.info("Pycan stopped")

        if self._mqtt.is_connected():
            self._logger.info("Stopping the mqtt client")
            self._mqtt.stop()
            self._logger.info("Mqtt client stopped")
        
    def _on_new_can_msg_recv(self, msg, *args):
        self._logger.debug("Received new message with can-id {}".format(
                        msg.arbitration_id))

        if msg.arbitration_id in self._frame_id:
            self._logger.debug("Received MixCAN frame: {}".format(msg.data))
            self._frame_queue.append(msg)
            return
        elif msg.arbitration_id in self._mixcan_id:
            self._logger.debug("Received MixCAN bf: {}".format(msg.data))
            self._bf_queue.append(msg)
            self._verify_mixcan()
            return
        else:
            return

    def _on_new_can_msg_sender(self, msg, *args):

            # Check if the frame is in the frameid array and get the index
            try:
                idx = self._frame_id.index(msg.arbitration_id)
            except:
                return

            # Convert the payload into a string array and insert it in the BF            
            _data = msg.data
            _data_as_str = "".join(str(val) for val in _data)

            self._mixcan.insert(_data_as_str)
            _mixcan_data = self._mixcan.to_can()
            self._mixcan.reset()

            # Construct the mixcan frame
            mixcan_frame = Message(arbitration_id=self._mixcan_id[idx],
                            data=_mixcan_data,
                            is_extended_id=True)

            # Send the mixcan frame                
            self._logger.debug("Sending mixcan frame: {}".format(mixcan_frame.data))
            self._pycan.can_bus.send(mixcan_frame)        

    def _verify_mixcan(self):
        if not self._frame_queue or not self._bf_queue:
            self._logger.error("Didn't receive last frame or last bf.")
            return

        _last_frame = self._frame_queue.pop(0)
        _data = [int(i) for i in _last_frame.data]
        _data_as_str = "".join(str(val) for val in _data)

        self._mixcan.insert(_data_as_str)

        _last_bf = self._bf_queue.pop(0)
        bf_as_hex = [hex(i) for i in _last_bf.data]
        verified = self._mixcan.verifiy_bf(bf_as_hex)

        if not verified:
            self._logger.debug("MixCAN BF not verified.")
            self._mqtt.publish_log("MixCAN BF not verified")
        else:
            self._logger.debug("MixCAN verified successfully.")

        self._mixcan.reset()

    def _on_new_key(self, mqttc, obj, msg):

        self._logger.debug("Received new key: {}".format(msg.payload.decode()))
        self._save_last_key(msg.payload.decode())

    def _save_last_key(self, key):
        
        self._current_key = key
        write_key(self._last_key_path, key)
        

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
