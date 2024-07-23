import pandas as pd
from datetime import datetime
from datetime import timedelta
import requests
import time
import random

##class for data_generation


def data_generation():
    surr_id = random.randint(1, 3)
    speed = random.randint(20,200)
    date = datetime.today().strftime("%Y-%m-%d")
    time = datetime.now().isoformat()

    return [surr_id, speed, date, time]


if __name__ == '__main__':

    REST_API_URL = 'https://api.powerbi.com/beta/e16ca6c1-813e-4ec3-a7b2-4f6ad40991ab/datasets/1e046f2e-4b13-4a7f-9b5c-fcbf61ab416d/rows?experience=power-bi&key=mWIaLLPJdzTbfjf980UhWg7C0Vv3EgjTb4z8%2FRm%2B2G48pHnsd11F%2BN8Mv36uJoJ1%2FTv3BizhRi%2BYWdf%2B%2Bomxpg%3D%3D'
    while True:
        data_raw = []
        for i in range(1):
            row = data_generation()
            data_raw.append(row)
            print("Raw data - ", data_raw)

        # set the header record
        HEADER = ["surr_id", "speed", "date", "time"]

        data_df = pd.DataFrame(data_raw, columns=HEADER)
        data_json = bytes(data_df.to_json(orient='records'), encoding='utf-8')
        print("JSON dataset", data_json)

        # Post the data on the Power BI API
        req = requests.post(REST_API_URL, data_json)

        print("Data posted in Power BI API")
        time.sleep(2)



#         [
# {
# "surr_id" :98.6,
# "speed" :98.6,
# "date" :"2024-07-21T09:59:07.729Z",
# "time" :"2024-07-21T09:59:07.729Z"
# }
# ]

