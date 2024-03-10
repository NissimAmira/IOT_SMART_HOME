import sqlite3

import paho.mqtt.client as mqtt


class MQTTManager:
    def __init__(self, broker_address='broker.hivemq.com', port=1883, db_path='db/mqtt_messages.db'):
        self.client = mqtt.Client()
        self.broker_address = broker_address
        self.port = port
        self.client.on_connect = self.on_connect
        self.client.on_message = self.on_message
        self.db_path = db_path

    def on_connect(self, client, userdata, flags, rc):
        print(f"Connected to MQTT Broker with result code {rc}")

    def on_message(self, client, userdata, msg):
        print(f"Received message: '{msg.payload.decode()}' on topic '{msg.topic}'")

    def connect(self):
        self.client.connect(self.broker_address, self.port, 60)
        self.client.loop_start()

    def subscribe(self, topic):
        self.client.subscribe(topic)

    def publish(self, topic, payload):
        self.client.publish(topic, payload)

    def disconnect(self):
        self.client.loop_stop()
        self.client.disconnect()

    def log_message_to_db(self, topic, payload):
        conn = sqlite3.connect(self.db_path)
        c = conn.cursor()
        c.execute('INSERT INTO mqtt_logs (topic, payload) VALUES (?, ?)', (topic, payload))
        conn.commit()
        conn.close()

    def handle_dht_sensor(self, temperature, humidity):
        self.publish("smart_wardrobe/dht/temperature", temperature)
        self.publish("smart_wardrobe/dht/humidity", humidity)
        print(f"Published DHT Sensor Data: Temperature = {temperature}C, Humidity = {humidity}%")
        self.log_message_to_db("smart_wardrobe/dht/humidity", {"Humidity": humidity})
        self.log_message_to_db("smart_wardrobe/dht/temperature", {"temperature": temperature})

    def handle_light_sensor(self, light_level):
        self.publish("smart_wardrobe/light_sensor", light_level)
        print(f"Published Light Sensor Data: Light Level = {light_level}")
        self.log_message_to_db("smart_wardrobe/light_sensor", {"light_level": light_level})

    def handle_door_status(self, status):
        self.publish("smart_wardrobe/door_status", status)
        print(f"Published Door Status: {status}")
        self.log_message_to_db("smart_wardrobe/door_status", {"status": status})

    def handle_manual_light_control(self, status):
        self.publish("smart_wardrobe/manual_light", status)
        print(f"Published Manual Light Control: {status}")
        self.log_message_to_db("smart_wardrobe/manual_light", {"status": status})
