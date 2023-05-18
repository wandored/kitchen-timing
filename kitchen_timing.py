'''
kitchen_timing.py is a script that will connect to the SFTP server and download the OrderDetails.csv file for each
'''
import pysftp
import pandas as pd
import json
from datetime import datetime, timedelta

# SFTP credentials

with open("/etc/toast_config.json") as config_file:
    config = json.load(config_file)


class Config:
    HOST = config.get("HOST_NAME")
    USERNAME = config.get("USERNAME")


# Connect to the SFTP server
def connect_to_sftp(export_numbers, today):
    with pysftp.Connection(config.HOST, config.USERNAME, private_key="/home/wandored/.ssh/id_rsa") as sftp:
        # import csv file into pandas dataframe
        for number in export_numbers:
            # check if directory exists
            if not sftp.exists(f"{number}/{today}"):
                print(f"{number}/{today} does not exist")
                continue
            with sftp.cd(f"{number}/{today}"):
                sftp.get("OrderDetails.csv", "OrderDetails.csv")

            # read csv file into pandas dataframe
            df = pd.read_csv(
                "OrderDetails.csv",
                usecols=[
                    "Order #",
                    "Opened",
                    "# of Guests",
                    "Server",
                    "Table",
                    "Service",
                    "Total",
                    "Paid",
                    "Duration (Opened to Paid)",
                ],
            )

            df["Store"] = number
            df["Opened"] = pd.to_datetime(df["Opened"])
            df["Paid"] = pd.to_datetime(df["Paid"])
            df["Duration (Opened to Paid)"] = pd.to_timedelta(df["Duration (Opened to Paid)"])
            avg_duration = df["Duration (Opened to Paid)"].mean()
            # rename columns
            df.rename(columns={"Duration (Opened to Paid)": "Duration"}, inplace=True)
            # get average Duration for all rows where Table is not null
            print(df["Duration"])
            print(avg_duration)


# Retrieve the list of restaurants
def retrieve_restaurants():
    restaurants = []
    with open("./downloads/toast_data/LocationExportNumberMapping.csv", "r") as f:
        restaurants = pd.read_csv(
            f,
            usecols=[
                "Restaurant",
                "Location",
                "Export #",
            ],
        )
    return restaurants


if __name__ == "__main__":
    restaurants = retrieve_restaurants()
    # extract list of Export #s
    export_numbers = restaurants["Export #"].tolist()
    # get yesterdays date
    yesterday = datetime.today() - timedelta(days=1)
    start_date = yesterday.strftime("%Y%m%d")

    connect_to_sftp(export_numbers, start_date)
