from datetime import datetime
import pandas as pd
import mysql.connector
import logging
import configparser

# This is for creating a log file of this script
logging.basicConfig(
    filename="ETL_process.log",
    level=logging.INFO,
    format="%(asctime)s - %(levelname)s - %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)


def log_event(message):
    logging.info(message)


def parse_logfile(logfile):
    """
    Parses a log entry, extracts the necessary fields, and returns a dictionary.

    Parameters:
    logfile (str): A single log entry string.

    Returns:
    dict: A dictionary containing the extracted fields.

    Note: For error handling make sure there is an empty list named 'parsing_errors' before executing the function.
    """
    o = {}

    try:
        first_split = logfile.split(" - - ")
        ip_address = first_split[0].strip()

        second_split = first_split[1].replace("[", "").split("]")
        timestamp = second_split[0].strip()

        http_req, url, _, status_code, response_size = (
            second_split[1].replace('"', "").split()
        )
        query_param = url.split("?")[1] if "?" in url else ""

        o["IP_Address"] = ip_address
        o["Timestamp"] = datetime.strptime(timestamp, "%d/%b/%Y:%H:%M:%S %z")
        o["Request_Method"] = http_req
        o["URL"] = url
        o["Status_Code"] = status_code

        # Some of resopnse sizes are "-" and not int type
        # o["Response_Size"] = response_size if type(response_size) == int else 0
        o["Response_Size"] = 0 if response_size == "-" else response_size
        o["Query_Parameters"] = query_param

    except Exception as e:
        print(e)
        parsing_errors.append({"Log": logfile, "Error": str(e)})

    return o


parsing_errors = []


def main():
    log_event("Starting to read log file.")
    # Logfile directory:
    logs_path = r"data\nginx_logs.txt"

    # Read database configuration
    config = configparser.ConfigParser()
    config.read("config\config.ini")
    db_config = config["mysql"]

    with open(logs_path, "r") as f:

        logs = f.readlines()
        logs = [log.strip() for log in logs]  # This trims and removes '\n'.
        output = [parse_logfile(log) for log in logs]
        df = pd.DataFrame(output)
        log_event(f"Dataframe created. number of records:\t{len(df)}")

    # Log parsing errors if there is/are any error/s
    if parsing_errors:

        with open("parsing_errors.log", "w") as f:
            for error in parsing_errors:
                f.write(f"{error}\n")
        log_event("Some records failed to parse.\nCheck parsing_errors.log")

    # Check and drop duplicates
    log_event(f"There is {len(df[df.duplicated()])} duplicated records.")
    if len(df[df.duplicated()]) != 0:
        df = df.drop_duplicates()
        log_event(f"Duplicated records dropped. number of records:\t{len(df)}")

    # Moving df into mysql

    conn = mysql.connector.connect(
        host=db_config["host"], user=db_config["user"], password=db_config["password"]
    )
    log_event("connected to MySql.")
    cursor = conn.cursor()

    cursor.execute(f"CREATE DATABASE IF NOT EXISTS {db_config['database']}")
    conn.database = db_config["database"]

    log_event(f"Connected to the {db_config['database']}")

    cursor.execute(
        f"""
    CREATE TABLE IF NOT EXISTS {db_config['table']} (
        id INT AUTO_INCREMENT PRIMARY KEY,
        IP_Address VARCHAR(255),
        Timestamp TIMESTAMP,
        Request_Method VARCHAR(10),
        URL TEXT,
        Status_Code INT,
        Response_Size INT,
        Query_Parameters TEXT
    )
    """
    )
    log_event(
        f"Checked if the {db_config['table']} table exists. If not existed the code create one."
    )

    insertion_errors = []

    for _, row in df.iterrows():
        try:
            cursor.execute(
                f"""
    INSERT INTO {db_config["table"]} (IP_Address, Timestamp, Request_Method, URL, Status_Code, Response_Size, Query_Parameters)
    VALUES (%s, %s, %s, %s, %s, %s, %s)
    """,
                (
                    row["IP_Address"],
                    row["Timestamp"],
                    row["Request_Method"],
                    row["URL"],
                    row["Status_Code"],
                    row["Response_Size"],
                    row["Query_Parameters"],
                ),
            )
            conn.commit()
        except Exception as e:
            insertion_errors.append({"Record": row.to_dict(), "Error": str(e)})

    cursor.close()
    conn.close()
    log_event("The dataframe records inserted to db.")

    # Log insertion errors if there is/are any error/s
    if insertion_errors:
        with open("insertion_errors.log", "w") as f:
            for error in insertion_errors:
                f.write(f"{error}\n")
        log_event("Some records failed to insert.\nCheck insertion_errors.log.")


if __name__ == "__main__":
    log_event("Script started running.")
    main()
    log_event("Data insertion is done.")
