import csv
import datetime
import logging
import pandas as pd
import requests
from sqlalchemy import create_engine

logging.basicConfig(filename="new_file.log",
                    format="%(asctime)s %(message)s",
                    filemode="a")


# Creating an object
logger = logging.getLogger()

# Setting the threshold of logger to DEBUG
logger.setLevel(logging.DEBUG)


# API data fetching
try:
    api_url = "https://jsonplaceholder.typicode.com/posts"
    response = requests.get(api_url)
    customer_data = response.json()
    logging.info("API data fetch successfully")
except Exception as e:
    logging.error(f"Error occurred during API data fetching {e}")

# json file converted to csv file
if response:
    try:
        data_file = open("data_file.csv", "w", newline="")
        csv_writer = csv.writer(data_file)
        count = 0
        for data in customer_data:
            if count == 0:
                header = data.keys()
                csv_writer.writerow(header)
                count += 1
            csv_writer.writerow(data.values())
        logging.info("json file is converted to csv ")
    except Exception as e:
        logging.error(f"Error during json file converting {e}")
    finally:
        data_file.close()

    # connection to sql
    try:
        conn_string = "postgresql://postgres:admin@localhost/DB"
        db = create_engine(conn_string)
        conn = db.connect()
        logging.info("database connection is done")
    except Exception as e:
        logging.error(f'Error during making connection to database {e}')

    # add created column and export to database
    try:
        df = pd.read_csv("data_file.csv")
        df["created"] = datetime.datetime.today().date()
        df.to_csv("data_file.csv", index=False)
        df.to_sql("customer", conn, if_exists="replace", index=False)
        logging.info("created column is added to csv file and export to database")

    except ExceptionGroup as e:
        logging.error(f"Error during inserting column in csv {e} ")
