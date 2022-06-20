"""
This work is licensed under the terms of the MIT license.  
For a copy, see <https://opensource.org/licenses/MIT>.

Developed by NISLAB - Network and Information Security Laboratory
at George Emil Palade University of Medicine, Pharmacy, Science and
Technology of Târgu Mureş <https://nislab.umfst.ro/>

Contributors: Roland Bolboaca, Teri Lenard
"""

import os
import configparser
import argparse
import asyncio

from can.interface import Bus
from can import BufferedReader
from can import Notifier

class Pycan:
    def __init__(self, channel, pipe_path=None, on_message_callback=None):
        self._should_run = False
        self._can_bus = Bus(channel="vcan0", bustype='socketcan')
        self._out_bus = Bus(channel=channel, bustype='socketcan')
        self._on_message_callback = on_message_callback
        self._listener = BufferedReader()
        self._loop = asyncio.get_event_loop()
        self._notifier = Notifier(self._can_bus, [self._listener])#, loop=self._loop)
        
        if pipe_path:
            self._pipeout = self._create_pipe(pipe_path)
        else:
            self._pipeout = None

    @property
    def can_bus(self):
        return self._can_bus

    @property
    def out_bus(self):
        return self._out_bus

    def _create_pipe(self, pipe_path):

        if os.path.exists(pipe_path):
            pipeout = os.open(pipe_path, os.O_WRONLY)
        else:
            os.mkfifo(pipe_path)
            pipeout = os.open(pipe_path, os.O_WRONLY)

        return pipeout

    def send_message_on_pipe(self, msg, *args):

        try:
            sent_msg = ((int(msg.timestamp*1000)).to_bytes(6,'little')) + \
                        (msg.arbitration_id).to_bytes(4,'big') + \
                        (msg.dlc).to_bytes(1,'little') + \
                        msg.data
        except ValueError as ex:
            print(str(ex))
            return

        d = os.write(self._pipeout, sent_msg)

    def listen_and_send(self, callback, *args):
        
        msg = None
        
        while self._should_run:
            try:
                msg = self._listener.get_message(0.5)
            except (BrokenPipeError, IOError) as ex:
                print("Exception occured {}".format(str(ex)))
                self.stop()
                return

            if msg and callback:
                callback(msg, args)
    
    def start(self):
        self._should_run = True
        self.listen_and_send(self._on_message_callback)

    def stop(self):
        self._should_run = False

        if self._pipeout:
            os.close(self._pipeout)
        
        if self._loop:
            self._loop.stop()
            
        if self._notifier:
            self._notifier.remove_listener(self._listener)
            self._notifier.stop(1)

        if self._listener:
            self._listener.stop()
        
        if self._can_bus:
            self._can_bus.shutdown()

    def is_running(self):
        return self._should_run


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("-c")
    args = parser.parse_args()

    config = configparser.RawConfigParser()
    config.optionxform = str
    config.read(args.c)
    config_dict = dict(config.items('CONFIG'))

    global pycan
    pycan = Pycan(config_dict['CAN_CHANNEL_REC'], config_dict['PIPE_PATH'])
    pycan.start()

if __name__ == "__main__":
    global pycan
    try:
        main()
    except Exception:
        pycan.stop()