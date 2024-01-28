import requests
from zoneinfo import ZoneInfo
from datetime import datetime, timedelta
import pandas as pd

API_BASE_URL = "https://odre.opendatasoft.com/api/explore/v2.1"


def date_to_utc_p1(date_str):
    return datetime.strptime(date_str, "%Y-%m-%d %H:%M:%S").astimezone(
        tz=ZoneInfo("Europe/Paris")
    )


if __name__ == "__main__":
    previous_floor_hour = datetime.now(tz=ZoneInfo("Europe/Paris")).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=1)

    params = {
        "order_by": "date_heure DESC",
        "select": "exclude(perimetre), exclude(nature), exclude(date), exclude(heure)",
        "where": f"'{previous_floor_hour - timedelta(hours=1)}' < date_heure and date_heure <= '{previous_floor_hour}'",
    }

    data = requests.get(
        f"{API_BASE_URL}/catalog/datasets/eco2mix-national-tr/records",
        params=params,
    ).json()

    real_time_df = pd.DataFrame(data["results"])

    real_time_df["date_heure"] = pd.to_datetime(
        real_time_df["date_heure"]
    ).dt.tz_convert("Europe/Paris")

    print(real_time_df[["date_heure", "consommation"]])
