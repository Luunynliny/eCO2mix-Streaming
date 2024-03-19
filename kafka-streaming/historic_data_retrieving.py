from datetime import datetime, timedelta
from zoneinfo import ZoneInfo

import pandas as pd

from utils import send_to_kafka

API_BASE_URL = "https://odre.opendatasoft.com/api/explore/v2.1"


def get_historic_data():
    historic_df = pd.read_csv(
        f"{API_BASE_URL}/catalog/datasets/eco2mix-national-tr/exports/csv",
        delimiter=";",
        usecols=lambda x: x not in ["perimetre", "nature", "date", "heure"],
        parse_dates=["date_heure"],
    )

    historic_df["date_heure"] = historic_df["date_heure"].dt.tz_convert("Europe/Paris")

    previous_floor_hour = datetime.now(tz=ZoneInfo("Europe/Paris")).replace(
        minute=0, second=0, microsecond=0
    ) - timedelta(hours=1)

    historic_df_passed = historic_df[
        historic_df["date_heure"] <= previous_floor_hour
        ].copy()

    historic_df_passed["date_heure"] = pd.to_datetime(historic_df_passed["date_heure"]).dt.strftime(
        '%Y-%m-%dT%H:%M:%S')

    return historic_df_passed.sort_values("date_heure", ascending=True).to_dict(
        orient="records"
    )


if __name__ == "__main__":
    historic_data = get_historic_data()
    send_to_kafka(historic_data)
