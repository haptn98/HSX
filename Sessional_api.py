import pandas as pd
import numpy as np
import os
from datetime import date
from datetime import datetime
from tqdm import tqdm
import warnings
import time

warnings.filterwarnings("ignore")

############################################################################################


class Sessional:
    def open(data, x):
        """format Session.open từ 2007 - 2023

        Args:
            data: DataFrame raw sau khi pd.read

            x: Date dữ liệu trong tên của folder chứa dữ liệu (VD: folder dữ liệu HOSE 16-08-2023)

        Returns:
            datab: DataFrame đã qua xử lý
        """
        row = data[
            data.iloc[:, 1].str.startswith(("A", "B"), na=False) == True
        ].index.values.astype(int)[0]
        data.columns = data.iloc[row - 2]
        data = data[row:]
        if data.shape[1] == 10:
            data = data.set_axis(
                [
                    "poID",
                    "ticker",
                    "drop",
                    "priorDayClose",
                    "openPrice",
                    "atoChangeStatus",
                    "atoChangePoint",
                    "atoChangePercent",
                    "atoVolume",
                    "atoValue",
                ],
                axis=1,
            )
            data.drop(columns={"drop"}, inplace=True)
        elif data.shape[1] == 9:
            data = data.set_axis(
                [
                    "poID",
                    "ticker",
                    "priorDayClose",
                    "openPrice",
                    "atoChangeStatus",
                    "atoChangePoint",
                    "atoChangePercent",
                    "atoVolume",
                    "atoValue",
                ],
                axis=1,
            )
        data = data[data["ticker"].map(lambda x: type(x) == str)]
        data.drop(columns={"priorDayClose"}, inplace=True)
        data.replace(
            {"atoChangeStatus": {3: "-", 4: "+", 7: "--", 8: "++", "<": "o"}},
            inplace=True,
        )
        data["report_date"] = (
            x[len(x) - 4 : len(x)]
            + "-"
            + x[len(x) - 7 : len(x) - 5]
            + "-"
            + x[len(x) - 10 : len(x) - 8]
        )
        data["poID"] = (
            data["ticker"]
            + x[len(x) - 4 : len(x)]
            + x[len(x) - 7 : len(x) - 5]
            + x[len(x) - 10 : len(x) - 8]
        )
        data["last_update"] = str(date.today())
        data["openPrice"] = data["openPrice"] * 1000
        data["atoChangePoint"] = data["atoChangePoint"] * 1000
        data["atoValue"] = data["atoValue"] * 1000
        return data

    def continuous(data, x):
        """format Session.continuous từ 2007 - 2023

        Args:
            data: DataFrame raw sau khi pd.read

            x: Date dữ liệu trong tên của folder chứa dữ liệu (VD: folder dữ liệu HOSE 16-08-2023)

        Returns:
            datab: DataFrame đã qua xử lý
        """
        row = data[
            data.iloc[:, 1].str.startswith(("A", "B"), na=False) == True
        ].index.values.astype(int)[0]
        data.columns = data.iloc[row - 2]
        data = data[row:]
        if data.shape[1] == 13:
            data = data.set_axis(
                [
                    "conID",
                    "ticker",
                    "drop1",
                    "priorDayClose",
                    "highPrice",
                    "averagePrice",
                    "lowPrice",
                    "omPrice",
                    "drop2",
                    "drop3",
                    "drop4",
                    "omVolume",
                    "omValue",
                ],
                axis=1,
            )
            data.drop(columns={"drop1", "drop2", "drop3", "drop4"}, inplace=True)
        elif data.shape[1] == 10:
            data = data.set_axis(
                [
                    "conID",
                    "ticker",
                    "drop",
                    "priorDayClose",
                    "highPrice",
                    "averagePrice",
                    "lowPrice",
                    "omPrice",
                    "omVolume",
                    "omValue",
                ],
                axis=1,
            )
            data.drop(columns={"drop"}, inplace=True)
        elif data.shape[1] == 9:
            data = data.set_axis(
                [
                    "conID",
                    "ticker",
                    "priorDayClose",
                    "highPrice",
                    "averagePrice",
                    "lowPrice",
                    "omPrice",
                    "omVolume",
                    "omValue",
                ],
                axis=1,
            )
        data = data[data["ticker"].map(lambda x: type(x) == str)]
        data.drop(columns={"priorDayClose"}, inplace=True)
        data["report_date"] = (
            x[len(x) - 4 : len(x)]
            + "-"
            + x[len(x) - 7 : len(x) - 5]
            + "-"
            + x[len(x) - 10 : len(x) - 8]
        )
        data["conID"] = (
            data["ticker"]
            + x[len(x) - 4 : len(x)]
            + x[len(x) - 7 : len(x) - 5]
            + x[len(x) - 10 : len(x) - 8]
        )
        data["last_update"] = str(date.today())
        data["highPrice"] = data["highPrice"] * 1000
        data["averagePrice"] = data["averagePrice"] * 1000
        data["lowPrice"] = data["lowPrice"] * 1000
        data["omPrice"] = data["omPrice"] * 1000
        data["omValue"] = data["omValue"] * 1000
        return data

    # def close(data,x):
    #     """ format Session.close từ 2007 - 2023

    #     Args:
    #         data: DataFrame raw sau khi pd.read

    #         x: Date dữ liệu trong tên của folder chứa dữ liệu (VD: folder dữ liệu HOSE 16-08-2023)

    #     Returns:
    #         datab: DataFrame đã qua xử lý
    #     """
    #     # if (x[len(x)-10:len(x)-8] + '.' + x[len(x)-7:len(x)-5] + '.' + x[len(x)-4:len(x)]) == '03.06.2009':
    #     #     """Dành cho dữ liệu ngày 03.06.2009"""
    #     #     row_num = data[data.iloc[:,1]=='Maõ CK'].index.values.astype(int)[0]
    #     #     data.columns = data.iloc[row_num]
    #     #     data = data[row_num+1:]
    #     #     if data.shape[1] == 16:
    #     #         data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice','drop1',
    #     #                         'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
    #     #                         'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
    #     #                         'marketCap'], axis=1)
    #     #         data.drop(columns={'drop','drop1'}, inplace=True)
    #     #         data['adjOutstandingShares'] = ''
    #     #     elif data.shape[1] == 17:
    #     #         data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice','drop1',
    #     #                         'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
    #     #                         'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
    #     #                         'adjOutstandingShares','marketCap'], axis=1)
    #     #         data.drop(columns={'drop','drop1'}, inplace=True)
    #     #     cols = ['pcID','ticker','priorDayClose','closePrice',
    #     #             'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
    #     #             'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
    #     #             'adjOutstandingShares','marketCap']
    #     #     data = data[cols]
    #     #     data = data[data['ticker'].map(lambda x : type(x) == str)]
    #     #     data.replace({'atcChangeStatus':{3:'-',4:'+',7:'--',8:'++','<':'o'}},inplace=True)
    #     #     data['report_date'] = x[len(x)-4:len(x)] + '-' + x[len(x)-7:len(x)-5] + '-' + x[len(x)-10:len(x)-8]
    #     #     data['pcID'] = data['ticker'] + x[len(x)-4:len(x)] + x[len(x)-7:len(x)-5] + x[len(x)-10:len(x)-8]
    #     #     data['last_update'] = str(date.today())
    #     #     return data
    #     # else:
    #     #     """Dành cho phần còn lại"""
    #     #     row = data[data.iloc[:,1].str.startswith(('A','B'), na=False)==True].index.values.astype(int)[0]
    #     #     data.columns = data.iloc[row-2]
    #     #     data = data[row:]
    #     #     if (data.shape[1] == 16) & (((data.columns)[5] == float(np.nan))==False):
    #     #         data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice','drop1',
    #     #                     'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
    #     #                     'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
    #     #                     'marketCap'], axis=1)
    #     #         data.drop(columns={'drop','drop1'}, inplace=True)
    #     #         data['adjOutstandingShares'] = ''
    #     #     if (data.shape[1] == 16) & (((data.columns)[5] == float(np.nan))==False):
    #     #         data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice',
    #     #                     'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
    #     #                     'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
    #     #                     'adjOutstandingShares','marketCap'], axis=1)
    #     #         data.drop(columns={'drop'}, inplace=True)
    #     #     elif data.shape[1] == 15:
    #     #         data = data.set_axis(['pcID','ticker','priorDayClose','closePrice',
    #     #                     'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
    #     #                     'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
    #     #                     'adjOutstandingShares','marketCap'], axis=1)
    #     #     elif data.shape[1] == 17:
    #     #         data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice','drop1',
    #     #                         'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
    #     #                         'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
    #     #                         'adjOutstandingShares','marketCap'], axis=1)
    #     #         data.drop(columns={'drop','drop1'}, inplace=True)
    #     row = data[data.iloc[:,1].str.startswith(('A','B'), na=False)==True].index.values.astype(int)[0]
    #     data.columns = data.iloc[row-2]
    #     data = data[row:]
    #     num = 0
    #     for col in data.columns:
    #         if isinstance(col, float) and np.isnan(col):
    #             data.rename(columns={col: 'col' + str(num)}, inplace=True)
    #             num += 1
    #     if ('col0' in data.columns.to_list()) & ('col1' in data.columns.to_list()):
    #         data.rename(columns={'Stt\nNo.':'pcID','Mã CK\nStock code':'ticker',"Giá đóng cửa hôm trước\nPrior day's close":'priorDayClose',
    #                             'Đóng cửa \nToday close':'closePrice','Thay đổi\nChange':'atcChangeStatus','KLGD\nTrading volume':'atcTradingVolume',
    #                             'GTGD (ng.đ) \nTrading value (VND 1,000)':'atcTradingValue','Tổng KLGD \nTotal volume':'totalVolume',
    #                             'Tổng GTGD (ng.đ)\nTotal value (VND 1,000)':'totalValue','KL niêm yết\nNo. of listed shares':'listedShares','KL đang lưu hành \nOutstanding shares':'outstandingShares',
    #                             'KL đang lưu hành điều chỉnh \nAdjusted outstanding shares':'adjOutstandingShares','Giá trị vốn hóa thị trường (tỷ đ)\nMarket cap. (VNDbil.)':'marketCap',
    #                             'Thay đổi (*)\nChange':'atcChangeStatus','Giá đóng cửa hôm trước\nPrior closing price':'priorDayClose','KL niêm yết\nCurrent listing shares':'listedShares',
    #                             'col0':'atcChangePoint','col1':'atcChangePercent',},inplace=True)
    #     elif ('col0' in data.columns.to_list()) & ('col1' in data.columns.to_list()) & ('col2' in data.columns.to_list()):
    #         data.drop(columns='col0',inplace=True)
    #         data.rename(columns={'Stt\nNo.':'pcID','Mã CK\nStock code':'ticker',"Giá đóng cửa hôm trước\nPrior day's close":'priorDayClose',
    #                             'Đóng cửa \nToday close':'closePrice','Thay đổi\nChange':'atcChangeStatus','KLGD\nTrading volume':'atcTradingVolume',
    #                             'GTGD (ng.đ) \nTrading value (VND 1,000)':'atcTradingValue','Tổng KLGD \nTotal volume':'totalVolume',
    #                             'Tổng GTGD (ng.đ)\nTotal value (VND 1,000)':'totalValue','KL niêm yết\nNo. of listed shares':'listedShares','KL đang lưu hành \nOutstanding shares':'outstandingShares',
    #                             'KL đang lưu hành điều chỉnh \nAdjusted outstanding shares':'adjOutstandingShares','Giá trị vốn hóa thị trường (tỷ đ)\nMarket cap. (VNDbil.)':'marketCap',
    #                             'Thay đổi (*)\nChange':'atcChangeStatus','Giá đóng cửa hôm trước\nPrior closing price':'priorDayClose','KL niêm yết\nCurrent listing shares':'listedShares',
    #                             'col1':'atcChangePoint','col2':'atcChangePercent',},inplace=True)
    #     data = data[data['ticker'].map(lambda x : type(x) == str)]
    #     data.replace({'atcChangeStatus':{3:'-',4:'+',7:'--',8:'++','<':'o'}},inplace=True)
    #     data['report_date'] = x[len(x)-4:len(x)] + '-' + x[len(x)-7:len(x)-5] + '-' + x[len(x)-10:len(x)-8]
    #     data['pcID'] = data['ticker'] + x[len(x)-4:len(x)] + x[len(x)-7:len(x)-5] + x[len(x)-10:len(x)-8]
    #     data['last_update'] = str(date.today())
    #     data['priorDayClose'] = data['priorDayClose'] *1000
    #     data['closePrice'] = data['closePrice'] *1000
    #     data['atcChangePoint'] = data['atcChangePoint'] *1000
    #     data['atcTradingValue'] = data['atcTradingValue'] *1000
    #     data['totalValue'] = data['totalValue'] *1000
    #     data['marketCap'] = data['marketCap'] *10**9
    #     return data

    def close(data, x):
        """format Session.close từ 2007 - 2023

        Args:
            data: DataFrame raw sau khi pd.read

            x: Date dữ liệu trong tên của folder chứa dữ liệu (VD: folder dữ liệu HOSE 16-08-2023)

        Returns:
            datab: DataFrame đã qua xử lý
        """
        # if (x[len(x)-10:len(x)-8] + '.' + x[len(x)-7:len(x)-5] + '.' + x[len(x)-4:len(x)]) == '03.06.2009':
        #     """Dành cho dữ liệu ngày 03.06.2009"""
        #     row_num = data[data.iloc[:,1]=='Maõ CK'].index.values.astype(int)[0]
        #     data.columns = data.iloc[row_num]
        #     data = data[row_num+1:]
        #     if data.shape[1] == 16:
        #         data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice','drop1',
        #                         'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
        #                         'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
        #                         'marketCap'], axis=1)
        #         data.drop(columns={'drop','drop1'}, inplace=True)
        #         data['adjOutstandingShares'] = '
        #     elif data.shape[1] == 17:
        #         data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice','drop1',
        #                         'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
        #                         'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
        #                         'adjOutstandingShares','marketCap'], axis=1)
        #         data.drop(columns={'drop','drop1'}, inplace=True)
        #     cols = ['pcID','ticker','priorDayClose','closePrice',
        #             'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
        #             'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
        #             'adjOutstandingShares','marketCap']
        #     data = data[cols]
        #     data = data[data['ticker'].map(lambda x : type(x) == str)]
        #     data.replace({'atcChangeStatus':{3:'-',4:'+',7:'--',8:'++','<':'o'}},inplace=True)
        #     data['report_date'] = x
        #     data['pcID'] = data['ticker'] + x.replace('-',')
        #     data['last_update'] = str(date.today())
        #     return data
        # else:
        # """Dành cho phần còn lại"""
        row = data[
            data.iloc[:, 1].str.startswith(("A", "B"), na=False) == True
        ].index.values.astype(int)[0]
        data.columns = data.iloc[row - 2]
        data = data[row:]
        num = 0
        for col in data.columns:
            if isinstance(col, float) and np.isnan(col):
                data.rename(columns={col: "col" + str(num)}, inplace=True)
                num += 1
        if (
            ("col0" in data.columns.to_list())
            & ("col1" in data.columns.to_list())
            & ("col2" not in data.columns.to_list())
        ):
            data.rename(
                columns={
                    "Stt\nNo.": "pcID",
                    "Mã CK\nStock code": "ticker",
                    "Giá đóng cửa hôm trước\nPrior day's close": "priorDayClose",
                    "Đóng cửa \nToday close": "closePrice",
                    "Thay đổi\nChange": "atcChangeStatus",
                    "KLGD\nTrading volume": "atcTradingVolume",
                    "GTGD (ng.đ) \nTrading value (VND 1,000)": "atcTradingValue",
                    "Tổng KLGD \nTotal volume": "totalVolume",
                    "Tổng GTGD (ng.đ)\nTotal value (VND 1,000)": "totalValue",
                    "KL niêm yết\nNo. of listed shares": "listedShares",
                    "KL đang lưu hành \nOutstanding shares": "outstandingShares",
                    "KL đang lưu hành điều chỉnh \nAdjusted outstanding shares": "adjOutstandingShares",
                    "Giá trị vốn hóa thị trường (tỷ đ)\nMarket cap. (VNDbil.)": "marketCap",
                    "Thay đổi (*)\nChange": "atcChangeStatus",
                    "Giá đóng cửa hôm trước\nPrior closing price": "priorDayClose",
                    "KL niêm yết\nCurrent listing shares": "listedShares",
                    "col0": "atcChangePoint",
                    "col1": "atcChangePercent",
                },
                inplace=True,
            )
        elif (
            ("col0" in data.columns.to_list())
            & ("col1" in data.columns.to_list())
            & ("col2" in data.columns.to_list())
        ):
            data.drop(columns="col0", inplace=True)
            data.rename(
                columns={
                    "Stt\nNo.": "pcID",
                    "Mã CK\nStock code": "ticker",
                    "Giá đóng cửa hôm trước\nPrior day's close": "priorDayClose",
                    "Đóng cửa \nToday close": "closePrice",
                    "Thay đổi\nChange": "atcChangeStatus",
                    "KLGD\nTrading volume": "atcTradingVolume",
                    "GTGD (ng.đ) \nTrading value (VND 1,000)": "atcTradingValue",
                    "Tổng KLGD \nTotal volume": "totalVolume",
                    "Tổng GTGD (ng.đ)\nTotal value (VND 1,000)": "totalValue",
                    "KL niêm yết\nNo. of listed shares": "listedShares",
                    "KL đang lưu hành \nOutstanding shares": "outstandingShares",
                    "KL đang lưu hành điều chỉnh \nAdjusted outstanding shares": "adjOutstandingShares",
                    "Giá trị vốn hóa thị trường (tỷ đ)\nMarket cap. (VNDbil.)": "marketCap",
                    "Thay đổi (*)\nChange": "atcChangeStatus",
                    "Giá đóng cửa hôm trước\nPrior closing price": "priorDayClose",
                    "KL niêm yết\nCurrent listing shares": "listedShares",
                    "col1": "atcChangePoint",
                    "col2": "atcChangePercent",
                },
                inplace=True,
            )
            # if (data.shape[1] == 16) & (((data.columns)[5] == float(np.nan))==False):
        #     data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice','drop1',
        #                 'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
        #                 'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
        #                 'marketCap'], axis=1)
        #     data.drop(columns={'drop','drop1'}, inplace=True)
        #     data['adjOutstandingShares'] = '
        # if (data.shape[1] == 16) & (((data.columns)[5] == float(np.nan))==False):
        #     data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice',
        #                 'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
        #                 'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
        #                 'adjOutstandingShares','marketCap'], axis=1)
        #     data.drop(columns={'drop'}, inplace=True)
        # elif data.shape[1] == 15:
        #     data = data.set_axis(['pcID','ticker','priorDayClose','closePrice',
        #                 'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
        #                 'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
        #                 'adjOutstandingShares','marketCap'], axis=1)
        # elif data.shape[1] == 17:
        #     data = data.set_axis(['pcID','ticker','drop','priorDayClose','closePrice','drop1',
        #                     'atcChangeStatus','atcChangePoint','atcChangePercent','atcTradingVolume',
        #                     'atcTradingValue','totalVolume','totalValue','listedShares','outstandingShares',
        #                     'adjOutstandingShares','marketCap'], axis=1)
        #     data.drop(columns={'drop','drop1'}, inplace=True)
        data = data[data["ticker"].map(lambda x: type(x) == str)]
        data.replace(
            {"atcChangeStatus": {3: "-", 4: "+", 7: "--", 8: "++", "<": "o"}},
            inplace=True,
        )
        data["report_date"] = x
        data["pcID"] = data["ticker"] + x.replace("-", "")
        data["last_update"] = str(date.today())
        data["priorDayClose"] = data["priorDayClose"] * 1000
        data["closePrice"] = data["closePrice"] * 1000
        data["atcChangePoint"] = data["atcChangePoint"] * 1000
        data["atcTradingValue"] = data["atcTradingValue"] * 1000
        data["totalValue"] = data["totalValue"] * 1000
        data["marketCap"] = data["marketCap"] * 10**9
        return data

    def putthrough(data, x):
        """format Session.putthough từ 2007 - 2023

        Args:
            data: DataFrame raw sau khi pd.read

            x: Date dữ liệu trong tên của folder chứa dữ liệu (VD: folder dữ liệu HOSE 16-08-2023)

        Returns:
            datab: DataFrame đã qua xử lý
        """
        try:
            row_num = data[data.iloc[:, 0] == "Stt\nNo."].index.values.astype(int)[0]
            data.columns = data.iloc[row_num]
            data = data[row_num + 1 :]
        except:
            try:
                row_num = data[data.iloc[:, 1] == "Mã CK\nSymbol"].index.values.astype(
                    int
                )[0]
                data.columns = data.iloc[row_num]
                data = data[row_num + 1 :].iloc[:, 0:4]
            except:
                pass
        if data.shape[1] == 5:
            data = data.set_axis(
                ["ptID", "ticker", "drop", "ptTradingVolume", "ptTradingValue"], axis=1
            )
            data.drop(columns={"drop"}, inplace=True)
        elif data.shape[1] == 4:
            data = data.set_axis(
                ["ptID", "ticker", "ptTradingVolume", "ptTradingValue"], axis=1
            )
        data = data[data["ticker"].map(lambda x: type(x) == str)]
        data["report_date"] = (
            x[len(x) - 4 : len(x)]
            + "-"
            + x[len(x) - 7 : len(x) - 5]
            + "-"
            + x[len(x) - 10 : len(x) - 8]
        )
        data["ptID"] = (
            data["ticker"]
            + x[len(x) - 4 : len(x)]
            + x[len(x) - 7 : len(x) - 5]
            + x[len(x) - 10 : len(x) - 8]
        )
        data["last_update"] = str(date.today())
        data["ptTradingValue"] = data["ptTradingValue"] * 1000
        return data
