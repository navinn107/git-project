import requests
import json
import dateutil.parser as dp
import uuid

class KARIMNAGAR_AQM_INFO:

    def __init__(self, base_url):
        self.base_url = base_url

    def get_data(self):
        """
        Extracts AQM locations from the source API.
        """
        url = self.base_url

        try:
            response = requests.get(url, verify=False)
            response.raise_for_status()

            if response.status_code == 200:
                json_array = response.json()
                self.transform_publish(json_array)
            else:
                print(f"Error: {response.status_code}")

        except requests.exceptions.HTTPError as errh:
            print("An HTTP Error occurred:", errh)
        except requests.exceptions.ConnectionError as errc:
            print("An Error Connecting to the API occurred:", errc)
        except requests.exceptions.Timeout as errt:
            print("A Timeout Error occurred:", errt)
        except requests.exceptions.RequestException as err:
            print("An Unknown Error occurred", err)
        except Exception as oe:
            print('Other errors:', oe)

    def transform_publish(self, list_of_packets):
        """
        Transforms the data as per IUDX vocab and publishes them.
        :param list_of_packets: List of packets containing AQM locations
        :type list_of_packets: list
        """
        transformed_data = []

        for packet in list_of_packets:
            transformed_packet = self.transform_packet(packet)
            transformed_data.append(transformed_packet)

        self.publish_data(transformed_data)

    def transform_packet(self, packet):
        """
        Transforms a single packet to the desired format.
        :param packet: A single packet of sensor data
        :type packet: dict
        :return: Transformed packet
        :rtype: dict
        """
        transformed = {
            "id": packet["id"],
            "airTemperature": packet["airTemperature"]["instValue"],
            "ambientNoise": packet["ambientNoise"]["instValue"],
            "co2": packet["co2"]["instValue"],
            "o3": packet["o3"]["instValue"],
            "pm10": packet["pm10"]["instValue"],
            "observationDateTime": packet["observationDateTime"]
        }
        return transformed

    def publish_data(self, data):
        """
        Placeholder function to publish data.
        :param data: List of transformed data packets
        :type data: list
        """
        print(json.dumps(data, indent=4))
        # Implement the actual publishing logic here (e.g., to a message queue, database, etc.)

# Example usage
if __name__ == "__main__":
    base_url = "http://3.110.255.79/api/env/get-sensor-info"
    karimnagar_aqm = KARIMNAGAR_AQM_INFO(base_url)
    karimnagar_aqm.get_data()
