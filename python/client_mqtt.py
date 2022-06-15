"""
This work is licensed under the terms of the MIT license.  
For a copy, see <https://opensource.org/licenses/MIT>.

Developed by NISLAB - Network and Information Security Laboratory
at George Emil Palade University of Medicine, Pharmacy, Science and
Technology of Târgu Mureş <https://nislab.umfst.ro/>

Contributors: Teri Lenard
"""

import logging

import paho.mqtt.client as mqtt

# Dependencies
# pip3 install paho-mqtt

logger = logging.getLogger(__name__)


class MQTTClient(object):

    def __init__(self, user, password, host, port, service_name="", on_message_callback=None):
        self._inst = mqtt.Client()
        self._inst.username_pw_set(user, password)
        self._inst.on_connect = self._on_connect
        self._inst.on_subscribe = self._on_subscribe
        
        if on_message_callback:
            self._inst.on_message = on_message_callback
        else:
            self._inst.on_message = self._on_message

        self._host = host
        self._port = port
        self._service_name = service_name
        
        self._log_topic = "logging/" + self._service_name
        self._sub_topic = "keymanager_stk/"

    def is_connected(self):
        return self._inst.is_connected()

    def connect(self):
        self._inst.loop_start()
        self._inst.connect(self._host, self._port, 60)

    def stop(self):
        if self._inst.is_connected():
            self._inst.loop_stop(True)
            self._inst.disconnect()

    def _on_connect(self, client, userdata, flags, rc):

        if rc == 0:
            logger.info("Client connected successfully.")
            print("connected")
            self._inst.subscribe(self._sub_topic, 0)

        else:
            logger.error("Client couldn't connect. Received code: {}.".format(rc))
            logger.info("Client tries reconnect...")
            self._inst.reconnect()

    def _on_message(self, mqttc, obj, msg):
        print(msg.topic + " " + str(msg) + " " + str(msg.payload))

    def _on_subscribe(self, mqttc, obj, mid, granted_qos):
        print("Subscribed: " + str(mid) + " " + str(granted_qos))

    def publish_log(self, data):
        if self._inst.is_connected():
            self._inst.publish(self._log_topic, data)
            logger.info("Published: {}".format(str(data)))
            return True
        else:
            logger.error("Client not connected.")
            return False

    def publish(self, data):

        if self._inst.is_connected():
            self._inst.publish("logging", data)
            logger.info("Published: {}".format(str(data)))
            return True
        else:
            logger.error("Client not connected.")
            return False


if __name__ == "__main__":
    '''
    mosquitto_pub -d -h 127.0.0.1 -p 1883 -u "debuguser" -pw "debuguser" -m "secretkey" -t "key_manager/stk/"
    '''

    def on_new_key(mqttc, obj, msg):
        print(msg.payload.decode())

    try:
        client = MQTTClient("debuguser", "debuguser", "127.0.0.1", 1883, 
                            service_name="DebugService", 
                            on_message_callback=on_new_key)
        client.connect()
        
        import time

        while True:
            time.sleep(0.1)
    except Exception:
        client.stop()
