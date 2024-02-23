from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd
import requests

from utils import send_to_kafka

API_BASE_URL = "https://odre.opendatasoft.com/api/explore/v2.1"


def get_real_time_data():
    previous_floor_hour = datetime.now(tz=ZoneInfo("Europe/Paris")).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=1)

    params = {
        "order_by": "date_heure ASC",
        "select": "exclude(perimetre), exclude(nature), exclude(date), exclude(heure)",
        "where": f"'{previous_floor_hour - timedelta(hours=1)}' < date_heure and date_heure <= '{previous_floor_hour}'",
    }

    data = requests.get(
        f"{API_BASE_URL}/catalog/datasets/eco2mix-national-tr/records",
        params=params,
    ).json()

    real_time_df = pd.DataFrame(data["results"])

    real_time_df["date_heure"] = (
        pd.to_datetime(real_time_df["date_heure"])
        .dt.tz_convert("Europe/Paris")
        .astype(str)
    )

    return real_time_df.to_dict(orient="records")


if __name__ == "__main__":
    real_time_data = get_real_time_data()
    send_to_kafka(real_time_data)
