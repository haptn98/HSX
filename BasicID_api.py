import pandas as pd
import numpy as np
import os
from datetime import date
from datetime import datetime
from tqdm import tqdm
import warnings

warnings.filterwarnings("ignore")


class BasicIndicator:
    def stock(data, x):
        """format basic_idicators từ 2007 - 2023 chia theo số cột dữ liệu

        Args:
            data: DataFrame raw sau khi pd.read

            x: Date dữ liệu trong tên của folder chứa dữ liệu (VD: folder dữ liệu HOSE 16-08-2023)

        Returns:
            datab: DataFrame đã qua xử lý
        """
        if data.shape[1] == 18:
            """ format 18 cột 
            """
            row = data[
                data.iloc[:, 1].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data = data[row:]
            datab = data.set_axis(
                [
                    "biID",
                    "ticker",
                    "averageOutstandingShares",
                    "primaryEPS",
                    "notes",
                    "adjRatio1",
                    "notes1",
                    "adjRatio2",
                    "notes2",
                    "adjustedEPS",
                    "marketPrice",
                    "PE",
                    "dividend",
                    "divPerMarketPrice",
                    "listedShares",
                    "outstandingShares",
                    "priorDayClose",
                    "OS",
                ],
                axis=1,
            )
            datab.drop(columns=["priorDayClose"], inplace=True)
            datab.drop(columns=["OS"], inplace=True)
            datab = datab.drop(datab[datab["biID"].isnull()].index)
            datab = datab.drop(datab[datab["ticker"].isnull()].index)
            datab.drop(datab[datab["ticker"].str.len() != 3].index, inplace=True)
            datab["recordHigh52Week"] = np.nan
            datab["recordLow52Week"] = np.nan
            datab["ROA"] = np.nan
            datab["ROE"] = np.nan
            datab["changeOutstandingSharesPercent"] = np.nan
            datab["adjustedOutstandingShares"] = np.nan
            datab["turnoverRatio"] = np.nan
            #HaPTN: Xem lại cách lấy reprt_date
            datab["report_date"] = (
                x[len(x) - 4: len(x)]
                + "-"
                + x[len(x) - 7: len(x) - 5]
                + "-"
                + x[len(x) - 10: len(x) - 8]
            )
            datab["biID"] = (
                datab["ticker"]
                + x[len(x) - 4: len(x)]
                + x[len(x) - 7: len(x) - 5]
                + x[len(x) - 10: len(x) - 8]
            )
            datab["last_update"] = str(date.today())
            cols = [
                "biID",
                "ticker",
                "recordHigh52Week",
                "recordLow52Week",
                "averageOutstandingShares",
                "primaryEPS",
                "notes",
                "adjRatio1",
                "notes1",
                "adjRatio2",
                "notes2",
                "adjustedEPS",
                "marketPrice",
                "PE",
                "dividend",
                "divPerMarketPrice",
                "ROA",
                "ROE",
                "listedShares",
                "outstandingShares",
                "changeOutstandingSharesPercent",
                "adjustedOutstandingShares",
                "turnoverRatio",
                "report_date",
                "last_update",
            ]
            datab = datab[cols]
            datab["recordHigh52Week"] = datab["recordHigh52Week"] * 1000
            datab["recordLow52Week"] = datab["recordLow52Week"] * 1000
            datab["marketPrice"] = datab["marketPrice"] * 1000
            datab["dividend"] = datab["dividend"] * 10000
            return datab

        elif data.shape[1] == 20:
            """ format 20 cột 
            """
            row = data[
                data.iloc[:, 1].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data = data[row:]
            datab = data.set_axis(
                [
                    "biID",
                    "ticker",
                    "recordHigh52Week",
                    "recordLow52Week",
                    "averageOutstandingShares",
                    "primaryEPS",
                    "notes",
                    "adjRatio1",
                    "notes1",
                    "adjRatio2",
                    "notes2",
                    "adjustedEPS",
                    "marketPrice",
                    "PE",
                    "dividend",
                    "divPerMarketPrice",
                    "listedShares",
                    "outstandingShares",
                    "priorDayClose",
                    "OS",
                ],
                axis=1,
            )
            datab.drop(columns=["priorDayClose"], inplace=True)
            datab.drop(columns=["OS"], inplace=True)
            datab = datab.drop(datab[datab["biID"].isnull()].index)
            datab = datab.drop(datab[datab["ticker"].isnull()].index)
            datab.drop(datab[datab["ticker"].str.len() != 3].index, inplace=True)
            datab["ROA"] = np.nan
            datab["ROE"] = np.nan
            datab["changeOutstandingSharesPercent"] = np.nan
            datab["adjustedOutstandingShares"] = np.nan
            datab["turnoverRatio"] = np.nan
            datab["report_date"] = (
                x[len(x) - 4 : len(x)]
                + "-"
                + x[len(x) - 7 : len(x) - 5]
                + "-"
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["biID"] = (
                datab["ticker"]
                + x[len(x) - 4 : len(x)]
                + x[len(x) - 7 : len(x) - 5]
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["last_update"] = str(date.today())
            cols = [
                "biID",
                "ticker",
                "recordHigh52Week",
                "recordLow52Week",
                "averageOutstandingShares",
                "primaryEPS",
                "notes",
                "adjRatio1",
                "notes1",
                "adjRatio2",
                "notes2",
                "adjustedEPS",
                "marketPrice",
                "PE",
                "dividend",
                "divPerMarketPrice",
                "ROA",
                "ROE",
                "listedShares",
                "outstandingShares",
                "changeOutstandingSharesPercent",
                "adjustedOutstandingShares",
                "turnoverRatio",
                "report_date",
                "last_update",
            ]
            datab = datab[cols]
            datab["recordHigh52Week"] = datab["recordHigh52Week"] * 1000
            datab["recordLow52Week"] = datab["recordLow52Week"] * 1000
            datab["marketPrice"] = datab["marketPrice"] * 1000
            datab["dividend"] = datab["dividend"] * 10000
            return datab

        elif data.shape[1] == 21:
            """ format 21 cột 
            """
            row = data[
                data.iloc[:, 1].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data = data[row:]
            datab = data.set_axis(
                [
                    "biID",
                    "ticker",
                    "recordHigh52Week",
                    "recordLow52Week",
                    "averageOutstandingShares",
                    "primaryEPS",
                    "notes",
                    "adjRatio1",
                    "notes1",
                    "adjRatio2",
                    "notes2",
                    "adjustedEPS",
                    "marketPrice",
                    "PE",
                    "dividend",
                    "divPerMarketPrice",
                    "listedShares",
                    "outstandingShares",
                    "adjustedOutstandingShares",
                    "priorDayClose",
                    "OS",
                ],
                axis=1,
            )
            datab.drop(columns=["priorDayClose"], inplace=True)
            datab.drop(columns=["OS"], inplace=True)
            datab = datab.drop(datab[datab["biID"].isnull()].index)
            datab = datab.drop(datab[datab["ticker"].isnull()].index)
            datab.drop(datab[datab["ticker"].str.len() != 3].index, inplace=True)
            datab["ROA"] = np.nan
            datab["ROE"] = np.nan
            datab["changeOutstandingSharesPercent"] = np.nan
            datab["turnoverRatio"] = np.nan
            datab["report_date"] = (
                x[len(x) - 4 : len(x)]
                + "-"
                + x[len(x) - 7 : len(x) - 5]
                + "-"
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["biID"] = (
                datab["ticker"]
                + x[len(x) - 4 : len(x)]
                + x[len(x) - 7 : len(x) - 5]
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["last_update"] = str(date.today())
            cols = [
                "biID",
                "ticker",
                "recordHigh52Week",
                "recordLow52Week",
                "averageOutstandingShares",
                "primaryEPS",
                "notes",
                "adjRatio1",
                "notes1",
                "adjRatio2",
                "notes2",
                "adjustedEPS",
                "marketPrice",
                "PE",
                "dividend",
                "divPerMarketPrice",
                "ROA",
                "ROE",
                "listedShares",
                "outstandingShares",
                "changeOutstandingSharesPercent",
                "adjustedOutstandingShares",
                "turnoverRatio",
                "report_date",
                "last_update",
            ]
            datab = datab[cols]
            datab["recordHigh52Week"] = datab["recordHigh52Week"] * 1000
            datab["recordLow52Week"] = datab["recordLow52Week"] * 1000
            datab["marketPrice"] = datab["marketPrice"] * 1000
            datab["dividend"] = datab["dividend"] * 10000
            return datab

        elif data.shape[1] == 22:
            """ format 22 cột 
            """
            row = data[
                data.iloc[:, 1].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data = data[row:]
            datab = data.set_axis(
                [
                    "biID",
                    "ticker",
                    "recordHigh52Week",
                    "recordLow52Week",
                    "averageOutstandingShares",
                    "primaryEPS",
                    "notes",
                    "adjRatio1",
                    "notes1",
                    "adjRatio2",
                    "notes2",
                    "adjustedEPS",
                    "marketPrice",
                    "PE",
                    "dividend",
                    "divPerMarketPrice",
                    "listedShares",
                    "outstandingShares",
                    "changeOutstandingSharesPercent",
                    "adjustedOutstandingShares",
                    "priorDayClose",
                    "OS",
                ],
                axis=1,
            )
            datab.drop(columns=["priorDayClose"], inplace=True)
            datab.drop(columns=["OS"], inplace=True)
            datab = datab.drop(datab[datab["biID"].isnull()].index)
            datab = datab.drop(datab[datab["ticker"].isnull()].index)
            datab.drop(datab[datab["ticker"].str.len() != 3].index, inplace=True)
            datab["ROA"] = np.nan
            datab["ROE"] = np.nan
            datab["turnoverRatio"] = np.nan
            datab["report_date"] = (
                x[len(x) - 4 : len(x)]
                + "-"
                + x[len(x) - 7 : len(x) - 5]
                + "-"
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["biID"] = (
                datab["ticker"]
                + x[len(x) - 4 : len(x)]
                + x[len(x) - 7 : len(x) - 5]
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["last_update"] = str(date.today())
            cols = [
                "biID",
                "ticker",
                "recordHigh52Week",
                "recordLow52Week",
                "averageOutstandingShares",
                "primaryEPS",
                "notes",
                "adjRatio1",
                "notes1",
                "adjRatio2",
                "notes2",
                "adjustedEPS",
                "marketPrice",
                "PE",
                "dividend",
                "divPerMarketPrice",
                "ROA",
                "ROE",
                "listedShares",
                "outstandingShares",
                "changeOutstandingSharesPercent",
                "adjustedOutstandingShares",
                "turnoverRatio",
                "report_date",
                "last_update",
            ]
            datab = datab[cols]
            datab["recordHigh52Week"] = datab["recordHigh52Week"] * 1000
            datab["recordLow52Week"] = datab["recordLow52Week"] * 1000
            datab["marketPrice"] = datab["marketPrice"] * 1000
            datab["dividend"] = datab["dividend"] * 10000
            return datab

        elif data.shape[1] == 24:
            """ format 24 cột 
            """
            row = data[
                data.iloc[:, 1].str.startswith(("A", "B"), na=False) == True
            ].index.values.astype(int)[0]
            data = data[row:]
            datab = data.set_axis(
                [
                    "biID",
                    "ticker",
                    "priorDayClose",
                    "recordHigh52Week",
                    "recordLow52Week",
                    "averageOutstandingShares",
                    "primaryEPS",
                    "notes",
                    "adjRatio1",
                    "notes1",
                    "adjRatio2",
                    "notes2",
                    "adjustedEPS",
                    "marketPrice",
                    "PE",
                    "dividend",
                    "divPerMarketPrice",
                    "ROA",
                    "ROE",
                    "listedShares",
                    "outstandingShares",
                    "changeOutstandingSharesPercent",
                    "adjustedOutstandingShares",
                    "turnoverRatio",
                ],
                axis=1,
            )
            datab.drop(columns=["priorDayClose"], inplace=True)
            datab = datab.drop(datab[datab["biID"].isnull()].index)
            datab = datab.drop(datab[datab["ticker"].isnull()].index)
            datab.drop(datab[datab["ticker"].str.len() != 3].index, inplace=True)
            datab["report_date"] = (
                x[len(x) - 4 : len(x)]
                + "-"
                + x[len(x) - 7 : len(x) - 5]
                + "-"
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["biID"] = (
                datab["ticker"]
                + x[len(x) - 4 : len(x)]
                + x[len(x) - 7 : len(x) - 5]
                + x[len(x) - 10 : len(x) - 8]
            )
            datab["last_update"] = str(date.today())
            datab["recordHigh52Week"] = datab["recordHigh52Week"] * 1000
            datab["recordLow52Week"] = datab["recordLow52Week"] * 1000
            datab["marketPrice"] = datab["marketPrice"] * 1000
            datab["dividend"] = datab["dividend"] * 10000
            return datab
