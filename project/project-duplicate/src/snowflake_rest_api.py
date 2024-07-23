import snowflake.connector
import json
import random
import configparser
import time
from datetime import datetime

# Set up logging and config outside the class
import logging

# Setup logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load configuration
config_file = 'config.ini'
config = configparser.ConfigParser()
config.read(config_file)

class SensorDataHandler:
    def __init__(self, config, logger):
        self.logger = logger
        self.conn_params = config['snowflake']
        self.connection = self.connect_to_snowflake()
        self.cursor = self.connection.cursor()
        self.sensor_locations = self.load_sensor_locations()
        self.create_table()

    def connect_to_snowflake(self):
        try:
            connection = snowflake.connector.connect(
                user=self.conn_params['user'],
                password=self.conn_params['password'],
                account=self.conn_params['account'],
                warehouse=self.conn_params['warehouse'],
                database=self.conn_params['database'],
                schema=self.conn_params['schema']
            )
            self.logger.info("Connected to Snowflake")
            return connection
        except snowflake.connector.errors.Error as e:
            self.logger.error(f"Failed to connect to Snowflake: {e}")
            raise

    def execute_query(self, query, params=None, fetch=False):
        try:
            self.cursor.execute(query, params)
            if fetch:
                return self.cursor.fetchall()
        except snowflake.connector.errors.Error as e:
            self.logger.error(f"Failed to execute query: {e}")
            raise

    def create_table(self):
        create_table_query = """
        CREATE TABLE IF NOT EXISTS SensorInfo (
            id VARCHAR(36) PRIMARY KEY,
            observationDateTime TIMESTAMP,
            airTemperature INT,
            ambientNoise INT,
            co2 INT,
            o3 INT,
            pm10 INT
        );
        """
        self.execute_query(create_table_query)
        self.logger.info("Table SensorInfo created or already exists")

    def insert_record(self, id_val, observationDateTime_val, airTemperature_val, ambientNoise_val, co2_val, o3_val, pm10_val):
        insert_query = """
        INSERT INTO SensorInfo (id, observationDateTime, airTemperature, ambientNoise, co2, o3, pm10)
        VALUES (%(id)s, %(observationDateTime)s, %(airTemperature)s, %(ambientNoise)s, %(co2)s, %(o3)s, %(pm10)s)
        """
        params = {
            'id': id_val,
            'observationDateTime': observationDateTime_val,
            'airTemperature': airTemperature_val,
            'ambientNoise': ambientNoise_val,
            'co2': co2_val,
            'o3': o3_val,
            'pm10': pm10_val
        }
        self.execute_query(insert_query, params)
        self.logger.info(f"Inserted record ID {id_val}")

    def upsert_record(self, id_val, observationDateTime_val, airTemperature_val, ambientNoise_val, co2_val, o3_val, pm10_val):
        self.insert_record(id_val, observationDateTime_val, airTemperature_val, ambientNoise_val, co2_val, o3_val, pm10_val)

    def record_exists(self, id_val):
        check_query = """
        SELECT COUNT(1) FROM SensorInfo WHERE id = %(id)s
        """
        params = {'id': id_val}
        result = self.execute_query(check_query, params, fetch=True)
        return result[0][0] > 0

    def load_sensor_locations(self):
        with open("../misc/env_sensor_location.json", "r") as file:
            return json.load(file)

    def generate_sensor_data(self, location_id):
        return {
            "id": location_id,
            "observationDateTime": datetime.now().isoformat(),
            "airTemperature": random.randrange(30, 40, 3),
            "ambientNoise": random.randrange(50, 60, 3),
            "co2": random.randrange(30, 40, 3),
            "o3": random.randrange(30, 40, 3),
            "pm10": random.randrange(100, 130, 3)
        }

    def process_data(self):
        for location in self.sensor_locations:
            json_data = self.generate_sensor_data(location['id'])
            try:
                self.upsert_record(
                    json_data['id'],
                    datetime.fromisoformat(json_data['observationDateTime']).strftime("%Y-%m-%d %H:%M:%S"),
                    json_data['airTemperature'],
                    json_data['ambientNoise'],
                    json_data['co2'],
                    json_data['o3'],
                    json_data['pm10']
                )
            except KeyError as e:
                self.logger.error(f"Missing key in data: {e}")

    def close_connection(self):
        try:
            self.connection.commit()
            self.cursor.close()
            self.connection.close()
            self.logger.info("Snowflake connection closed")
        except snowflake.connector.errors.Error as e:
            self.logger.error(f"Failed to close Snowflake connection: {e}")

    def run(self):
        try:
            while True:
                self.process_data()
                time.sleep(2)  # Wait for 10 seconds before generating data again
        except KeyboardInterrupt:
            self.logger.info("Interrupted by user, shutting down...")
        finally:
            self.close_connection()

if __name__ == "__main__":
    handler = SensorDataHandler(config, logger)
    handler.run()
