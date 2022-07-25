"""
This work is licensed under the terms of the MIT license.  
For a copy, see <https://opensource.org/licenses/MIT>.

Developed by NISLAB - Network and Information Security Laboratory
at George Emil Palade University of Medicine, Pharmacy, Science and
Technology of Târgu Mureş <https://nislab.umfst.ro/>

Contributors: Teri Lenard, Roland Bolboaca
"""

import sys
import signal

from time import time
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
        self._last_frame = None
        self._last_bf = None

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
        #self._logger.debug("Received new message with can-id {}".format(
        #                msg.arbitration_id))

        if msg.arbitration_id in self._frame_id:
            #self._logger.debug("Received MixCAN frame: {}".format(msg.data))
            self._frame_queue.append(msg)
            return
        elif msg.arbitration_id in self._mixcan_id:
           # self._logger.debug("Received MixCAN bf: {}".format(msg.data))
            self._frame_queue.append(msg)
            self._verify_mixcan()
            return
        else:
            return

    def _on_new_can_msg_sender(self, msg, *args):

            # Check if the frame is in the frameid array and get the index

            if msg.arbitration_id not in self._frame_id:
                # forward to outbus
                self._pycan.out_bus.send(msg)
                return

            # Handle frame and bf
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
            # self._logger.debug("Sending mixcan frame: {}".format(mixcan_frame.data))
            self._pycan.out_bus.send(msg)
            self._pycan.out_bus.send(mixcan_frame)

    def _verify_mixcan(self):
        if not self._frame_queue:
            self._logger.error("Didn't receive last frame or last bf.")
            return

        if self._frame_queue[0].arbitration_id in self._frame_id:
            # Case 1: frame 
            if self._frame_queue[1].arbitration_id in self._frame_id:
                # Case 1.1: frame -> frame 
                self._frame_queue.pop(0) # drop frame
                self._last_frame = self._frame_queue.pop(0) # save the second frame 
                self._logger.debug("Dropping frame")
                return # End function call because we do not have bf
            else:
                # Case 1.2: there is not a frame after the first one
                #           it should be a bf. 
                self._last_frame = self._frame_queue.pop(0) # Save the frame
                self._last_bf = self._frame_queue.pop(0) # Save the bf

        elif self._frame_queue[0].arbitration_id in self._mixcan_id:
            # Case 2: bf this case should drop mismatched bfs that do not have
            #         a leading frame
            self._frame_queue.pop(0) # drop the bf
            # if self._frame_queue[1].arbitration_id in self._mixcan_id:
            #     # Case 2.1: bf -> bf
            return

        _data = [int(i) for i in self._last_frame.data]
        _data_as_str = "".join(str(val) for val in _data)

        self._mixcan.insert(_data_as_str)

        bf_as_hex = [hex(i) for i in self._last_bf.data]
        verified = self._mixcan.verifiy_bf(bf_as_hex)

        if not verified:
            # Verify with old key
            self._mixcan.reset()
            self._mixcan.insert_old_key(_data_as_str)
            verified = self._mixcan.verifiy_bf(bf_as_hex)

            if not verified:
                self._logger.debug("MixCAN BF not verified.")
                self._mixcan.reset()

                can_id = self._last_bf.arbitration_id
                #count = 1
                timestamp = time()

                log = "MixCAN BF not verified CAN ID: {} . Timestamp: {} .\n".format(can_id, timestamp)
                self._mqtt.publish_log(log)
                self._logger.debug("Published log alert: {}".format(log))
                return

        #self._logger.debug("MixCAN verified successfully.")
        self._mixcan.reset()

    def _on_new_key(self, mqttc, obj, msg):

        self._logger.debug("Received new key: {}".format(msg.payload.decode()))
        self._save_last_key(msg.payload.decode())

    def _save_last_key(self, key):

        self._current_key = key
        self._mixcan.set_key(self._current_key.encode())
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
