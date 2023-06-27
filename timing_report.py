'''
kitchen_timing.py is a script that will connect to the SFTP server and download the OrderDetails.csv file for each restaurant
'''
import pysftp
import pandas as pd
import json
from datetime import datetime, timedelta

# SFTP credentials
with open("toast_config.json") as config_file:
    config = json.load(config_file)

# Get the host and username from the config file needed to access the SFTP server
class Config:
    HOST = config.get("HOST")
    USERNAME = config.get("USERNAME")

# Make the time format more readable (HH:MM:SS)
def formatTime(time):
  if pd.isna(time):
    return "00:00:00"
  else:
    total_sec = time.total_seconds()
    hours = int(total_sec // 3600)
    minutes = int((total_sec % 3600) // 60)
    seconds = round(total_sec % 60)
    # put a 0 in front of the number if it is less than 10
    if hours < 10:
        hours = f"0{hours}"
    if minutes < 10:
        minutes = f"0{minutes}"
    if seconds < 10:
        seconds = f"0{seconds}"
    return f"{hours}:{minutes}:{seconds}"

# Connect to the SFTP server
def connect_to_sftp(export_numbers, today):
    # Df with two columns Store and Average Duration
    avg_duration_df = pd.DataFrame(columns=["date", "export_number", "bar", "dining_room", "handheld", "patio", "online_ordering"])
    with pysftp.Connection(config["HOST"], config["USERNAME"], private_key="C:/Users/jackk/.ssh/id_rsa") as sftp:
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
                    "Revenue Center",
                    "Duration (Opened to Paid)"
                ],
            )

            # Set date
            date = "'" + datetime.strptime(today, "%Y%m%d").strftime("%Y-%m-%d") + "'"

            # Sort the Revenue Center column and get the mean of their durations
            # Look for a "-" in the duration column
            df = df[df["Duration (Opened to Paid)"].str.contains("-") == False]
            df["Duration (Opened to Paid)"] = pd.to_timedelta(df["Duration (Opened to Paid)"])
            df = df.sort_values(by=["Revenue Center"])
            df = df.groupby("Revenue Center").mean()
            # run the formatTime function on the Duration column
            df["Duration (Opened to Paid)"] = df["Duration (Opened to Paid)"].apply(formatTime)

            df = df.T
            
            temp_df = pd.DataFrame(columns=["date", "export_number", "bar", "dining_room", "handheld", "patio", "online_ordering"])
            
            # Add df to the temp_df and fill in blank values with 00:00:00
            temp_df["date"] = [date]
            temp_df["export_number"] = [number]
            try:
                temp_df["bar"] = "'" + df["Bar"][0] + "'"
            except:
                # Else make it null
                temp_df["bar"] = None
            try:
                temp_df["dining_room"] = "'" + df["Dining Room"][0] + "'"
            except:
                temp_df["dining_room"] = None
            try:
                temp_df["handheld"] = "'" + df["Handheld"][0] + "'"
            except:
                temp_df["handheld"] = None
            try:
                temp_df["patio"] = "'" + df["Patio"][0] + "'"
            except:
                temp_df["patio"] = None
            try:
                temp_df["online_ordering"] = "'" + df["Online Ordering"][0] + "'"
            except:
                temp_df["online_ordering"] = None    
                
            # concat the temp_df to the avg_duration_df
            avg_duration_df = pd.concat([avg_duration_df, temp_df], ignore_index=True)

                
    # export avg_duration_df to csv
    return avg_duration_df

# Retrieve the list of restaurants
def retrieve_restaurants():
    restaurants = []
    with open("D:/User_Stuff/Desktop/CentraArchy/kitchen-timing/LocationExportNumberMapping.csv", "r") as f:
        restaurants = pd.read_csv(
            f,
            usecols=[
                "Restaurant",
                "Location",
                "Export #",
            ],
        )
    return restaurants

def run():
    restaurants = retrieve_restaurants()
    # extract list of Export #s
    export_numbers = restaurants["Export #"].tolist()
    # get yesterdays date
    date = datetime.today() - timedelta(days=2)
    start_date = date.strftime("%Y%m%d")

    dayData = connect_to_sftp(export_numbers, start_date)

    return dayData

if __name__ == "__main__":
    data = run()
    print(data)