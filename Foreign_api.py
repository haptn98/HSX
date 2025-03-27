import pandas as pd
import numpy as np
import os
from datetime import date
from datetime import datetime
from tqdm import tqdm
import warnings
import time

warnings.filterwarnings("ignore")


###########################################################################################################
class foreign:
    def stock(data, x):
        """format Foreign_trading từ 2007 - 2023 chia theo format

        Args:
            data: DataFrame raw sau khi pd.read

            x: Date dữ liệu trong tên của folder chứa dữ liệu (VD: folder dữ liệu HOSE 16-08-2023)

        Returns:
            datab: DataFrame đã qua xử lý
        """
        if "Stt\nNo." in data.iloc[:, 0].tolist():
            row = data[
                data.iloc[:, 1].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data.columns = data.iloc[row - 2]
            data = data[row:]
            datab = data.set_axis(
                [
                    "ftID",
                    "ticker",
                    "totalRoom",
                    "currentRoom",
                    "foreignOwnedRatio",
                    "stateOwnedRatio",
                    "omPreOpenBuyingVolume",
                    "omContBuyingVolume",
                    "omPreCloseBuyingVolume",
                    "omBuyingValue",
                    "omPreOpenSellingVolume",
                    "omContSellingVolume",
                    "omPreCloseSellingVolume",
                    "omSellingValue",
                    "ptBuyingVolume",
                    "ptBuyingValue",
                    "ptSellingVolume",
                    "ptSellingValue",
                ],
                axis=1,
            )
            datab.dropna(how="any", subset="ftID", inplace=True)
            datab.drop(datab[datab["ticker"] == 0].index, inplace=True)
            datab = datab[datab["ticker"].str.len() < 10]
            datab["report_date"] = (
                x[len(x) - 4 : len(x)]
                + "-"
                + x[len(x) - 7 : len(x) - 5]
                + "-"
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["ftID"] = (
                datab["ticker"]
                + x[len(x) - 4 : len(x)]
                + x[len(x) - 7 : len(x) - 5]
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["last_update"] = str(date.today())
        elif "Mã CK\nStock code" in list(data.iloc[:, 0]):
            row = data[
                data.iloc[:, 0].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data.columns = data.iloc[row - 2]
            data = data[row:]
            datab = data.set_axis(
                [
                    "ticker",
                    "totalRoom",
                    "currentRoom",
                    "foreignOwnedRatio",
                    "stateOwnedRatio",
                    "omPreOpenBuyingVolume",
                    "omContBuyingVolume",
                    "omPreCloseBuyingVolume",
                    "omBuyingValue",
                    "omPreOpenSellingVolume",
                    "omContSellingVolume",
                    "omPreCloseSellingVolume",
                    "omSellingValue",
                    "ptBuyingVolume",
                    "ptBuyingValue",
                    "ptSellingVolume",
                    "ptSellingValue",
                ],
                axis=1,
            )
            datab.dropna(how="any", subset="ticker", inplace=True)
            datab.drop(datab[datab["ticker"] == 0].index, inplace=True)
            datab = datab[datab["ticker"].str.len() < 10]
            datab["ftID"] = (
                datab["ticker"]
                + x[len(x) - 4 : len(x)]
                + x[len(x) - 7 : len(x) - 5]
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["report_date"] = (
                x[len(x) - 4 : len(x)]
                + "-"
                + x[len(x) - 7 : len(x) - 5]
                + "-"
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["last_update"] = str(date.today())
        elif ("Mã CK\n(Securities)" in data.iloc[:, 0].tolist()) == True:
            row = data[
                data.iloc[:, 0].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data.columns = data.iloc[row - 2]
            data = data[row:]
            data = data.iloc[:, 0:13]
            datab = data.set_axis(
                [
                    "ticker",
                    "totalRoom",
                    "currentRoom",
                    "foreignOwnedRatio",
                    "stateOwnedRatio",
                    "omPreOpenBuyingVolume",
                    "omContBuyingVolume",
                    "omPreCloseBuyingVolume",
                    "omPreOpenSellingVolume",
                    "omContSellingVolume",
                    "omPreCloseSellingVolume",
                    "ptBuyingVolume",
                    "ptSellingVolume",
                ],
                axis=1,
            )
            datab = datab[datab["ticker"].str.len() < 10]
            datab.drop(datab[datab["ticker"].str.contains("Sum -")].index, inplace=True)
            datab.drop(datab[datab["ticker"].str.contains("Tổng")].index, inplace=True)
            datab["omBuyingValue"] = ""
            datab["omSellingValue"] = ""
            datab["ptBuyingValue"] = ""
            datab["ptSellingValue"] = ""
            datab["ftID"] = (
                datab["ticker"]
                + x[len(x) - 4 : len(x)]
                + x[len(x) - 7 : len(x) - 5]
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["report_date"] = (
                x[len(x) - 4 : len(x)]
                + "-"
                + x[len(x) - 7 : len(x) - 5]
                + "-"
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["last_update"] = str(date.today())
            cols = [
                "ftID",
                "ticker",
                "totalRoom",
                "currentRoom",
                "foreignOwnedRatio",
                "stateOwnedRatio",
                "omPreOpenBuyingVolume",
                "omContBuyingVolume",
                "omPreCloseBuyingVolume",
                "omBuyingValue",
                "omPreOpenSellingVolume",
                "omContSellingVolume",
                "omPreCloseSellingVolume",
                "omSellingValue",
                "ptBuyingVolume",
                "ptBuyingValue",
                "ptSellingVolume",
                "ptSellingValue",
                "report_date",
                "last_update",
            ]
            datab = datab[cols]
        elif ("CK" in data.iloc[:, 0].tolist()) == True:
            row = data[
                data.iloc[:, 0].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data.columns = data.iloc[row - 2]
            data = data[row:]
            data = data.iloc[:, 0:13]
            datab = data.set_axis(
                [
                    "ticker",
                    "totalRoom",
                    "currentRoom",
                    "foreignOwnedRatio",
                    "stateOwnedRatio",
                    "omPreOpenBuyingVolume",
                    "omContBuyingVolume",
                    "omPreCloseBuyingVolume",
                    "omPreOpenSellingVolume",
                    "omContSellingVolume",
                    "omPreCloseSellingVolume",
                    "ptBuyingVolume",
                    "ptSellingVolume",
                ],
                axis=1,
            )
            datab = datab[datab["ticker"].str.len() < 10]
            datab.drop(datab[datab["ticker"].str.contains("Tổng")].index, inplace=True)
            datab["omBuyingValue"] = ""
            datab["omSellingValue"] = ""
            datab["ptBuyingValue"] = ""
            datab["ptSellingValue"] = ""
            datab["ftID"] = (
                datab["ticker"]
                + x[len(x) - 4 : len(x)]
                + x[len(x) - 7 : len(x) - 5]
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["report_date"] = (
                x[len(x) - 4 : len(x)]
                + "-"
                + x[len(x) - 7 : len(x) - 5]
                + "-"
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["last_update"] = str(date.today())
            cols = [
                "ftID",
                "ticker",
                "totalRoom",
                "currentRoom",
                "foreignOwnedRatio",
                "stateOwnedRatio",
                "omPreOpenBuyingVolume",
                "omContBuyingVolume",
                "omPreCloseBuyingVolume",
                "omBuyingValue",
                "omPreOpenSellingVolume",
                "omContSellingVolume",
                "omPreCloseSellingVolume",
                "omSellingValue",
                "ptBuyingVolume",
                "ptBuyingValue",
                "ptSellingVolume",
                "ptSellingValue",
                "report_date",
                "last_update",
            ]
            datab = datab[cols]
        elif ("CK (Securities)" in data.iloc[:, 0].tolist()) == True:
            row = data[
                data.iloc[:, 0].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data.columns = data.iloc[row - 2]
            data = data[row:]
            data = data.iloc[:, 0:13]
            datab = data.set_axis(
                [
                    "ticker",
                    "totalRoom",
                    "currentRoom",
                    "foreignOwnedRatio",
                    "stateOwnedRatio",
                    "omPreOpenBuyingVolume",
                    "omContBuyingVolume",
                    "omPreCloseBuyingVolume",
                    "omPreOpenSellingVolume",
                    "omContSellingVolume",
                    "omPreCloseSellingVolume",
                    "ptBuyingVolume",
                    "ptSellingVolume",
                ],
                axis=1,
            )
            datab = datab[datab["ticker"].str.len() < 10]
            datab.drop(datab[datab["ticker"].str.contains("Tổng")].index, inplace=True)
            datab["omBuyingValue"] = ""
            datab["omSellingValue"] = ""
            datab["ptBuyingValue"] = ""
            datab["ptSellingValue"] = ""
            datab["report_date"] = (
                x[len(x) - 4 : len(x)]
                + "-"
                + x[len(x) - 7 : len(x) - 5]
                + "-"
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["ftID"] = (
                datab["ticker"]
                + x[len(x) - 4 : len(x)]
                + x[len(x) - 7 : len(x) - 5]
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["last_update"] = str(date.today())
            cols = [
                "ftID",
                "ticker",
                "totalRoom",
                "currentRoom",
                "foreignOwnedRatio",
                "stateOwnedRatio",
                "omPreOpenBuyingVolume",
                "omContBuyingVolume",
                "omPreCloseBuyingVolume",
                "omBuyingValue",
                "omPreOpenSellingVolume",
                "omContSellingVolume",
                "omPreCloseSellingVolume",
                "omSellingValue",
                "ptBuyingVolume",
                "ptBuyingValue",
                "ptSellingVolume",
                "ptSellingValue",
                "report_date",
                "last_update",
            ]
            datab = datab[cols]
        datab["foreignOwnedRatio"] = datab["foreignOwnedRatio"].transform(
            lambda x: x.replace("0,00", "0")
        )
        datab["omBuyingValue"] = datab["omBuyingValue"] * 1000
        datab["omSellingValue"] = datab["omSellingValue"] * 1000
        datab["ptBuyingValue"] = datab["ptBuyingValue"] * 1000
        datab["ptSellingValue"] = datab["ptSellingValue"] * 1000
        return datab
