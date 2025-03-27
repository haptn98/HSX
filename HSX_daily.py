import os
import re
import pandas as pd
from datetime import date
from xlrd import XLRDError
import urllib
from sqlalchemy import create_engine, text
import time

log_file_path_1 = (
    r"D:\FPA_laptop_PC\FPA_DB\FDA_job\FDA-Data-Management\Log_job_daily\job_log.txt"
)

log_file_path = r"D:\FPA_laptop_PC\FPA_DB\FDA_job\FDA-Data-Management\Infobank\Hose_Module\done_files.log"
list_error_1 = []  # Errors during file processing
list_error_2 = []  # Invalid files (e.g., not Excel)


def write_log(file_path):
    """Write processed file paths to a log file."""
    with open(log_file_path, "a") as log_file:
        log_file.write(file_path + "\n")


def is_file_processed(file_path):
    """Check if a file has already been processed."""
    if os.path.exists(log_file_path):
        with open(log_file_path, "r") as log_file:
            processed_files = log_file.read().splitlines()
        return file_path in processed_files
    return False


def is_valid_excel(file_path):
    """Check if the file is a valid Excel file."""
    return file_path.endswith((".xls", ".xlsx"))  # Accept both .xls and .xlsx files


def process_excel_file(file_path):
    """Process a single Excel file and return its DataFrame."""
    if "TKGD tung phien" in file_path:
        try:
            print(f"Processing file: {file_path}")
            # Read the main data
            if file_path.endswith(".xlsx"):
                df = pd.read_excel(
                    file_path,
                    header=None,
                    skiprows=8,
                    sheet_name="3",
                    engine="openpyxl",
                )
                report_date_df = pd.read_excel(
                    file_path,
                    header=None,
                    skiprows=4,
                    sheet_name="3",
                    engine="openpyxl",
                )
            elif file_path.endswith(".xls"):
                df = pd.read_excel(file_path, header=None, skiprows=8, sheet_name="3")
                report_date_df = pd.read_excel(
                    file_path, header=None, skiprows=4, sheet_name="3"
                )
            else:
                raise ValueError("Unsupported file format")

            # Extract the report date
            report_date = str(report_date_df.iloc[0, 1])
            match = re.search(r"(\d{4}-\d{2}-\d{2})", report_date)
            date_extracted = match.group(1) if match else None

            # Handle missing date_extracted
            if not date_extracted:
                print(
                    "Warning: report_date could not be extracted. Using default date."
                )
                date_extracted = "1900-01-01"  # Fallback date

            # Drop unnecessary columns
            df = df.drop(df.columns[[0, 2]], axis=1)

            # Define column names
            column_names_1 = [
                "ticker",
                "priorDayClose",
                "closePrice",
                "atcChangeStatus",
                "atcChangePoint",
                "atcChangePercent",
                "atcTradingVolume",
                "atcTradingValue",
                "totalVolume",
                "totalValue",
                "listedShares",
                "outstandingShares",
                "adjOutstandingShares",
                "marketCap",
            ]
            df.columns = column_names_1
            print(df)

            # Add the report_date column
            df["report_date"] = date_extracted

            # Create the tranID column
            df["tranID"] = df["ticker"] + date_extracted.replace("-", "")

            # Add the last_update column
            df["last_update"] = date.today().strftime("%Y-%m-%d")  # Convert to string

            # Drop rows with missing tranID
            df = df.dropna(subset=["tranID"])
            df = df[
                [
                    "tranID",
                    "ticker",
                    "priorDayClose",
                    "closePrice",
                    "atcChangeStatus",
                    "atcChangePoint",
                    "atcChangePercent",
                    "atcTradingVolume",
                    "atcTradingValue",
                    "totalVolume",
                    "totalValue",
                    "listedShares",
                    "outstandingShares",
                    "adjOutstandingShares",
                    "marketCap",
                    "report_date",
                ]
            ]

            # Log the processed file
            write_log(file_path)

            return df

        except (
            pd.errors.EmptyDataError,
            XLRDError,
            ValueError,
            TypeError,
            IndexError,
        ) as e:
            print(f"Error processing file '{file_path}': {str(e)}")
            list_error_1.append(file_path)
            return pd.DataFrame()  # Return an empty DataFrame in case of error

    return (
        pd.DataFrame()
    )  # Return an empty DataFrame if the file doesn't match the criteria


def list_folders_in_HSX(folder_path, df1=None):
    """Traverse folders and process Excel files."""
    if df1 is None:
        df1 = pd.DataFrame()

    if os.path.exists(folder_path) and os.path.isdir(folder_path):
        objects = os.listdir(folder_path)
        for obj_name in objects:
            obj_path = os.path.join(folder_path, obj_name)

            if os.path.isdir(obj_path):
                # Recursively process subdirectories
                df1 = list_folders_in_HSX(obj_path, df1)
            elif os.path.isfile(obj_path):
                # Check if the file has already been processed
                if is_file_processed(obj_path):
                    continue

                # Check if the file is a valid Excel file
                if not is_valid_excel(obj_path):
                    print(f"'{obj_path}' is not a valid Excel file.")
                    list_error_2.append(obj_path)
                    continue

                # Process the Excel file and append its data to df1
                df = process_excel_file(obj_path)
                if not df.empty:
                    df1 = pd.concat([df1, df], ignore_index=True)

    else:
        print(f"Path '{folder_path}' does not exist or is not a directory.")
    return df1


# Specify the folder path
folder_path = r"D:\OneDrive - fpts.com.vn\INFOBANK\Thongtintt\KQGD Hose\2025"
today = date.today()
if today.weekday() < 5:
    df1 = list_folders_in_HSX(folder_path)

    df1["report_date"] = pd.to_datetime(df1["report_date"])
    df1["last_update"] = date.today()
    df1["priorDayClose"] = df1["priorDayClose"] * 1000
    df1["closePrice"] = df1["closePrice"] * 1000
    df1["atcChangePoint"] = df1["atcChangePoint"] * 1000
    df1["atcTradingValue"] = df1["atcTradingValue"] * 1000
    df1["totalValue"] = df1["totalValue"] * 1000
    df1["marketCap"] = df1["marketCap"] * 10**9

    connection_string = urllib.parse.quote_plus(
        "DRIVER={ODBC Driver 17 for SQL Server};"
        "SERVER=10.26.2.193;"
        "DATABASE=INFOBANK_FPA;"
        "UID=sa;"
        "PWD=123@abc;"
    )

    # Tạo engine kết nối
    engine = create_engine(f"mssql+pyodbc:///?odbc_connect={connection_string}")
    if df1.empty:
        print("Empty dataframe.")
        with open(log_file_path_1, "a", encoding="utf-8") as log_file:
            # Write the log message with the current date
            log_file.write(f"Ngày {date.today()}: Job Infobank HSX bị lỗi: Data empty.\n")
    else:
        start_time = time.time()
        with engine.connect() as conn:
            try:
                df1.to_sql(
                    name="SessionPreclose_stock",
                    schema="HOSE",
                    con=engine,
                    if_exists="append",
                    index=False,
                )
                conn.commit()
                print("Push data done")
                with open(log_file_path_1, "a", encoding="utf-8") as log_file:
                    # Write the log message with the current date
                    log_file.write(
                        f"Ngày {date.today()}: Job Infobank HSX chạy thành công.\n"
                    )
            except Exception as e:
                print("Error in SQL_fast:", e)
                with open(log_file_path_1, "a", encoding="utf-8") as log_file:
                    # Write the log message with the current date
                    log_file.write(f"Ngày {date.today()}: Job Infobank HSX bị lỗi.\n")
            finally:
                end_time = time.time()
                print(f"Operation time: {end_time - start_time} seconds")
else:
    print('Today is weekend')
print("All done")
