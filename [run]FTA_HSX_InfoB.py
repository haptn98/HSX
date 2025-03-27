from BasicID_api import BasicIndicator as basic
from Sessional_api import Sessional as se
from Foreign_api import foreign as fg
from HSX_Transform_api import edit_data
from OrderPlacement_api import orderP
from sqlserver_api import syntax
from Log_write_api import parent_dir, folder_log, log
from environs import Env
from datetime import date
from datetime import datetime
import logging
import warnings
import pandas as pd
import numpy as np
import sys
import os

warnings.filterwarnings("ignore")

import time

start_time = time.time()

env = Env()
env.read_env()

"""
Trong trường hợp chạy lại toàn bộ dữ liệu cần phải kiểm tra dữ liệu ngày 

    ("3015-03-02","2015-03-02")
    ("2915-09-07","2015-09-07")

"""

try:
    file_path = folder_log + "/" + "{}_systemcheck.txt".format(str(date.today()))

    sys.stdout = open(file_path, "w", encoding="utf-8")
    print(sys.stdout)

    # original_stdout = sys.stdout

    # with open(file_path, "w",encoding='utf-8') as f:
    #     sys.stdout = f
    #     sys.stdout = original_stdout
    #     print(f)

    with open(file_path, "r") as a:
        contents = a.read()
        print(contents)

    path = env("datasource")
    print("datasource: ", path)
    print("parent_dir: ", parent_dir)
    print("folder_log:", folder_log)

    """##################################################################################################################"""

    def check_number_datatype(
        python_data, table_database, table_name, table_schema=None
    ):
        """Kết hợp 3 function exec_spcolumn, numberdata, push_test
                - Lấy thông tin kiểu dữ liệu của bảng

                - Kiểm tra kiểu dữ liệu

                - Đẩy dữ liệu đã kiểm tra lên SQL Server

        Args:
            python_data (variable): Variable chứa DataFrame đã format

            table_database (str): Tên database

            table_schema (str, None): Tên schema tương ứng. Defaults to None.

            table_name (str): Tên bảng SQL

        """
        if table_schema == None:
            table_full_name = table_name
        else:
            table_full_name = table_database + "." + table_schema + "." + table_name
        print("Check datatype: START")
        data_type = syntax.exec_spcolumn(table_name, table_schema)
        data_type = data_type[["COLUMN_NAME", "TYPE_NAME"]]
        data_type = data_type[data_type["TYPE_NAME"] != "object"]
        data_type.reset_index(drop=True, inplace=True)
        dataloi = 0
        for i in range(data_type.shape[0]):
            checkdata = edit_data.numberdata(
                python_data, data_type["COLUMN_NAME"][i], table_full_name
            )
            dataloi += len(checkdata)
            if len(checkdata) != 0:
                print("error %s: " % str(data_type["COLUMN_NAME"][i]), checkdata)

        if dataloi == 0:
            python_data = python_data.fillna(value=np.nan).replace([np.nan], [None])
            print("Check Datatype: DONE")
            syntax.push_test(table_full_name, python_data, None)
        else:
            pass

    """##################################################################################################################"""
    Basic_part1 = edit_data.process(
        elemental_start="Chi so tai chinh cua CP",
        elemental_end="",
        how="in",
        function=basic.stock,
        name_sheet="1",
        data_source=path,
        phase="Basic_indicator",
    )
    Basic_part2 = edit_data.process(
        elemental_start="TK Chi so co ban cua CP (Basic idicators)",
        elemental_end="",
        how="in",
        function=basic.stock,
        name_sheet="1",
        data_source=path,
        phase="Basic_indicator",
    )
    Basic = pd.concat([Basic_part1, Basic_part2])
    Basic.drop_duplicates(subset="biID", inplace=True)
    try:
        """Sửa lỗi vặt trong data không măng tính chất lập lại
        """

        Basic = Basic[Basic["report_date"] != "hinh-u -di"]
        Basic["ROE"] = Basic["ROE"].replace("10.44%", "0.1044")
        Basic["report_date"] = Basic["report_date"].replace("2011--7-\\2", "2011-07-29")
        Basic["report_date"] = Basic["report_date"].replace("3015-03-02", "2015-03-02")
        Basic["report_date"] = Basic["report_date"].replace("2915-09-07", "2015-09-07")

        Basic["biID"] = Basic["biID"].transform(
            lambda x: x.replace("2011-7\\2", "20110729")
        )
        Basic["biID"] = Basic["biID"].transform(
            lambda x: x.replace("30150302", "20150302")
        )
        Basic["biID"] = Basic["biID"].transform(
            lambda x: x.replace("29150907", "20150907")
        )

        Basic["adjustedOutstandingShares"] = Basic["adjustedOutstandingShares"].replace(
            "314893882(n)", "314893882"
        )
    except:
        pass

    Basic.drop_duplicates(subset="biID", keep="last", inplace=True)
    print("{} row: ".format("Basic_indicator"), Basic.shape[0])
    check_number_datatype(Basic, "INFOBANK_FPA", "BasicIndicator_stock", "HOSE")
    print(
        "**************************************************************************************************"
    )

    """#################################################################################################################"""
    sess_open1 = edit_data.process(
        elemental_start="TKGD tung phien (Sessions)",
        elemental_end="",
        how="in",
        function=se.open,
        name_sheet="1",
        data_source=path,
        phase="Session_open",
    )
    sess_open2 = edit_data.process(
        elemental_start="OM",
        elemental_end="d1.xls",
        how="start_end",
        function=se.open,
        name_sheet="2",
        data_source=path,
        phase="Session_open",
    )
    sess_open = pd.concat([sess_open2, sess_open1])
    try:
        """Sửa lỗi vặt trong data không măng tính chất lập lại
        """

        sess_open = sess_open[sess_open["report_date"] != "hinh-u -di"]
        sess_open["report_date"] = sess_open["report_date"].replace(
            "2011--7-\\2", "2011-07-29"
        )
        sess_open["tranID"] = sess_open["tranID"].transform(
            lambda x: x.replace("2011-7\2", "20110729")
        )
        sess_open = sess_open[
            ~sess_open["report_date"].isin("3015-03-02", "2915-09-07")
        ]
    except:
        pass
    sess_open.drop_duplicates(subset="poID", keep="last", inplace=True)
    print("{} row: ".format("Session_open"), sess_open.shape[0])
    check_number_datatype(sess_open, "INFOBANK_FPA", "SessionPreopen_stock", "HOSE")
    print(
        "**************************************************************************************************"
    )

    """-----------------------------------------------------------------------------------------------------------------"""

    sess_continuoun1 = edit_data.process(
        elemental_start="TKGD tung phien (Sessions)",
        elemental_end="",
        how="in",
        function=se.continuous,
        name_sheet="2",
        data_source=path,
        phase="Session_continuoun",
    )
    sess_continuoun2 = edit_data.process(
        elemental_start="OM",
        elemental_end="dcont.xls",
        how="start_end",
        function=se.continuous,
        name_sheet="2",
        data_source=path,
        phase="Session_continuoun",
    )
    sess_continuoun = pd.concat([sess_continuoun2, sess_continuoun1])
    try:
        """Sửa lỗi vặt trong data không măng tính chất lập lại
        """

        sess_continuoun = sess_continuoun[
            sess_continuoun["report_date"] != "hinh-u -di"
        ]
        sess_continuoun["report_date"] = sess_continuoun["report_date"].replace(
            "2011--7-\\2", "2011-07-29"
        )
        sess_continuoun["tranID"] = sess_continuoun["tranID"].transform(
            lambda x: x.replace("2011-7\2", "20110729")
        )
        sess_continuoun = sess_continuoun[
            ~sess_continuoun["report_date"].isin("3015-03-02", "2915-09-07")
        ]
    except:
        pass
    sess_continuoun.drop_duplicates(subset="conID", keep="last", inplace=True)
    print("{} row: ".format("Session_continuoun"), sess_continuoun.shape[0])
    check_number_datatype(
        sess_continuoun, "INFOBANK_FPA", "SessionContinuous_stock", "HOSE"
    )
    print(
        "**************************************************************************************************"
    )

    """-----------------------------------------------------------------------------------------------------------------"""

    # sess_close1 = edit_data.process(elemental_start='TKGD tung phien (Sessions)',
    #                                 elemental_end='',
    #                                 how='in',
    #                                 function=se.close,
    #                                 name_sheet='3',
    #                                 data_source=path,
    #                                 phase='Session_close')
    # sess_close2 = edit_data.process(elemental_start='OM',

    #                                 elemental_end='d3.xls',
    #                                 how='start_end',
    #                                 function=se.close,
    #                                 name_sheet="2",
    #                                 data_source=path,
    #                                 phase='Session_close')
    # sess_close = pd.concat([sess_close2,sess_close1])
    # try:
    #     """Sửa lỗi vặt trong data không măng tính chất lập lại
    #     """

    #     sess_close = sess_close[sess_close['report_date']!='hinh-u -di']
    #     sess_close['adjOutstandingShares'] = sess_close['adjOutstandingShares'].replace('',np.nan)
    #     sess_close['report_date'] = sess_close['report_date'].replace("2011--7-\\2","2011-07-29")
    #     sess_close['tranID'] = sess_close['tranID'].transform(lambda x:x.replace("2011-7\2","20110729"))
    #     sess_close = sess_close[~sess_close['report_date'].isin("3015-03-02","2915-09-07")]
    # except:
    #     pass
    # sess_close.drop_duplicates(subset='pcID',keep='last',inplace=True)
    # print('{} row: '.format('Session_close'), sess_close.shape[0])
    # check_number_datatype(sess_close,'preclose','InfoB_Session')
    # print('**************************************************************************************************')

    """-----------------------------------------------------------------------------------------------------------------"""

    sess_putthrough1 = edit_data.process(
        elemental_start="TKGD tung phien (Sessions)",
        elemental_end="",
        how="in",
        function=se.putthrough,
        name_sheet="4",
        data_source=path,
        phase="Session_putthrough",
    )
    sess_putthrough2 = edit_data.process(
        elemental_start="PT",
        elemental_end="",
        how="in",
        function=se.putthrough,
        name_sheet="PT",
        data_source=path,
        phase="Session_putthrough",
    )
    sess_putthrough = pd.concat([sess_putthrough1, sess_putthrough2])
    try:
        """Sửa lỗi vặt trong data không măng tính chất lập lại
        """

        sess_putthrough = sess_putthrough[
            sess_putthrough["report_date"] != "hinh-u -di"
        ]
        sess_putthrough["report_date"] = sess_putthrough["report_date"].replace(
            "2011--7-\\2", "2011-07-29"
        )
        sess_putthrough["tranID"] = sess_putthrough["tranID"].transform(
            lambda x: x.replace("2011-7\2", "20110729")
        )
        sess_putthrough = sess_putthrough[
            ~sess_putthrough["report_date"].isin("3015-03-02", "2915-09-07")
        ]
    except:
        pass
    sess_putthrough.drop_duplicates(subset="ptID", keep="last", inplace=True)
    print("{} row: ".format("Session_putthrough"), sess_putthrough.shape[0])
    check_number_datatype(
        sess_putthrough, "INFOBANK_FPA", "SessionPutthrough_stock", "HOSE"
    )
    print(
        "**************************************************************************************************"
    )

    """#################################################################################################################"""
    NDTNN = pd.DataFrame()

    file_name = [
        "NDTNN (Foreign Trading)",
        "NDTNN (Foreign Trading)(REVISED)",
        "NDTNN (Foreign Trading) file sua 2",
        "NDTNN (Foreign Trading) revised",
        "TKGD NDTNN (Foreign Trading)",
        "TKGD NDTNN (Foreign Trading) - REVISED",
        "TKGD NDTNN (Foreign Trading) revised",
        "GDNDTNN",
        "gd cua nha DTNN",
    ]

    TKGD = [
        "TKGD NDTNN (Foreign Trading)",
        "TKGD NDTNN (Foreign Trading) - REVISED",
        "TKGD NDTNN (Foreign Trading) revised",
    ]

    for i in file_name:
        print("file name %s" % i)
        if i not in TKGD:
            df = edit_data.process(
                elemental_start=i,
                elemental_end="TKGD",
                function=fg.stock,
                how="not_in",
                name_sheet="5",
                data_source=path,
                phase="foreign Trading",
            )
            NDTNN = pd.concat([NDTNN, df])
        elif i in TKGD:
            df = edit_data.process(
                elemental_start=i,
                elemental_end="",
                function=fg.stock,
                how="in",
                name_sheet="2",
                data_source=path,
                phase="foreign Trading",
            )
            NDTNN = pd.concat([NDTNN, df])
        else:
            print("file loi %s" % i)
            pass
    print("{} row: ".format("foreign Trading"), NDTNN.shape[0])

    try:
        """Sửa lỗi vặt trong data không măng tính chất lập lại
        """

        NDTNN["currentRoom"] = (
            NDTNN["currentRoom"]
            .replace("(**)", np.nan)
            .replace("(-)", np.nan)
            .replace("-", np.nan)
        )
        NDTNN["foreignOwnedRatio"] = (
            NDTNN["foreignOwnedRatio"].replace("(-)", np.nan).replace("-", np.nan)
        )
        NDTNN["omBuyingValue"] = NDTNN["omBuyingValue"].replace("", np.nan)
        NDTNN["omSellingValue"] = NDTNN["omSellingValue"].replace("", np.nan)
        NDTNN["ptBuyingValue"] = NDTNN["ptBuyingValue"].replace("", np.nan)
        NDTNN["ptSellingValue"] = NDTNN["ptSellingValue"].replace("", np.nan)
        NDTNN["report_date"] = NDTNN["report_date"].replace("2011--7-\\2", "2011-07-29")
        NDTNN["ftID"] = NDTNN["ftID"].transform(
            lambda x: x.replace("2011-7\\2", "20110729")
        )
        NDTNN["ftID"] = NDTNN["ftID"].transform(
            lambda x: x.replace("2011-7\2", "20110729")
        )
        NDTNN = NDTNN[~NDTNN["report_date"].isin("3015-03-02", "2915-09-07")]
    except:
        pass
    NDTNN.drop_duplicates(subset="ftID", keep="last", inplace=True)
    check_number_datatype(NDTNN, "INFOBANK_FPA", "ForeignTrading_stock", "HOSE")
    print(
        "**************************************************************************************************"
    )

    """#################################################################################################################"""

    order_p = edit_data.process(
        elemental_start="Cung cau hang ngay",
        elemental_end="",
        function=orderP.stock,
        how="in",
        name_sheet="Chi tiet (Details)",
        data_source=path,
        phase="Order_P",
    )
    order_p.drop_duplicates(subset="opID", inplace=True)
    order_p = order_p[~order_p["report_date"].isin(["3015-03-02", "2915-09-07"])]
    print("{} row: ".format("Order_P"), order_p.shape[0])
    check_number_datatype(order_p, "INFOBANK_FPA", "OrderPlacement_stock", "HOSE")
    print(
        "**************************************************************************************************"
    )

    print(
        "Process data InfoBank HSX: DONE--- %s seconds ---" % (time.time() - start_time)
    )

except Exception as Argument:
    f = open(file_path, "a", encoding="utf-8")
    f.write("SysError: " + str(Argument))

print("InfoBank_HSX: DONE")
