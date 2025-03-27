import pandas as pd
import numpy as np
import os
from datetime import date
from datetime import datetime
from environs import Env
from tqdm import tqdm
import warnings
import time
from Log_write_api import log

env = Env()
env.read_env()

folder_log = env("folder_log")


class edit_data:
    def process(
        elemental_start, elemental_end, function, how, name_sheet, data_source, phase
    ):
        """Function Xử lý dữ liệu theo format định sẵn

        Args:
            elemental_start (str): element trong tên của file

            elemental_end (str): element trong tên của file

            function (variable): function format tương ứng

            how (str): one|in|not in|start_end

                            one: 1 elemental

                            in: elemental_start in i and elemental_end in i

                            not in: elemental_start in i and elemental_end not in i

                            start_end: i.startswith(elemental_start) and

                                       i.endswith(elemental_end)

            name_sheet (str): Tên sheet trên excel file và phải dùng tên không dùng vị trí sheet

            data_source (variable): biến chứa đường dẫn đến folder chứa dữ liệu

            phase (str): Tên giai đoạn đang xử lý (basic idicators, session open ...)

        Returns:
            data: DataFrame đã xử lý theo format.
        """
        print("process {} data: START".format(phase))
        # checklist = pd.read_txt()
        start_time = time.time()

        folder = []
        name = []
        sheet = []
        full_name = []
        file_list = pd.DataFrame()

        print(data_source)

        for root, dirs, files in os.walk(data_source):
            try:
                for i in files:
                    if (elemental_start in i and elemental_end == "") and how == "one":
                        FK = os.path.join(root, i)
                        dataFK = pd.read_excel(FK, None)
                        sheet_list = list(dataFK.keys())
                        for y in sheet_list:
                            full = os.path.join(i, y)
                            folder.append(root)
                            name.append(i)
                            sheet.append(y)
                            full_name.append(full)
                    elif (elemental_start in i and elemental_end in i) and how == "in":
                        FK = os.path.join(root, i)
                        dataFK = pd.read_excel(FK, None)
                        sheet_list = list(dataFK.keys())
                        for y in sheet_list:
                            full = os.path.join(i, y)
                            folder.append(root)
                            name.append(i)
                            sheet.append(y)
                            full_name.append(full)
                    elif (
                        elemental_start in i and elemental_end not in i
                    ) and how == "not_in":
                        FK = os.path.join(root, i)
                        dataFK = pd.read_excel(FK, None)
                        sheet_list = list(dataFK.keys())
                        for y in sheet_list:
                            full = os.path.join(i, y)
                            folder.append(root)
                            name.append(i)
                            sheet.append(y)
                            full_name.append(full)
                    elif (
                        i.startswith(elemental_start)
                        and i.endswith(elemental_end)
                        and how == "start_end"
                    ):
                        FK = os.path.join(root, i)
                        dataFK = pd.read_excel(FK, None)
                        sheet_list = list(dataFK.keys())
                        for y in sheet_list:
                            sheet_name = os.path.join(i, y)
                            full = os.path.join(i, y)
                            folder.append(root)
                            name.append(i)
                            sheet.append(y)
                            full_name.append(full)
            except:
                print("file error: ", i)

        file_list_all = pd.DataFrame(
            list(zip(folder, name, sheet, full_name)),
            columns=["folder", "name", "sheet", "full_name"],
        )
        file_list_all["sheet"] = file_list_all["sheet"].astype("str")

        try:
            file_list_check = list(
                file_list_all[file_list_all["sheet"] == name_sheet]["full_name"]
            )

            log_hist = os.path.join(folder_log, "log_fileprocess.csv")
            history_file = pd.read_csv(r"%s" % log_hist, header=None)
            history_file = history_file.set_axis(["filename"], axis=1)
            history_list = list(history_file["filename"])

            trash_log = os.path.join(folder_log, "log_filetrash.csv")
            trash_file = pd.read_csv(r"%s" % trash_log, header=None)
            trash_file = trash_file.set_axis(["trashname"], axis=1)
            trash_file = trash_file[~trash_file["trashname"].isin(file_list_check)]
            trash_file.to_csv(trash_log, index=False)

            trash_list = list(trash_file["trashname"])

            trash_log = os.path.join(folder_log, "log_filetrash.csv")
            trash_file = pd.read_csv(r"%s" % trash_log, header=None)
            trash_file = trash_file.set_axis(["trashname"], axis=1)

            file_list = file_list_all[
                (file_list_all["sheet"] == name_sheet)
                & (~file_list_all["full_name"].isin(history_list))
                & (~file_list_all["full_name"].isin(trash_list))
            ]
            file_list.drop_duplicates(inplace=True)
            file_list.reset_index(drop=True, inplace=True)
            file_list_run = file_list[["full_name"]]
            # file_list_run = list(file_list['full_name'])
            print("Update:", file_list.shape[0])

            file_list_run.to_csv(os.path.join(folder_log, "full_name.csv"))

            data = pd.DataFrame()
            check_fileprocess = []

            for i in tqdm(range(file_list.shape[0]), position=0):
                try:
                    FT = os.path.join(file_list["folder"][i], file_list["name"][i])
                    df = pd.read_excel(FT, sheet_name=name_sheet)
                    dfb = function(df, file_list["folder"][i])
                    data = pd.concat([data, dfb])
                    log.run_csv([str(os.path.join(file_list["name"][i], name_sheet))])
                    check_fileprocess.append(
                        str(os.path.join(file_list["name"][i], name_sheet))
                    )
                except:
                    print(FT)
                    pass

            log_hist = os.path.join(folder_log, "log_fileprocess.csv")
            history_file = pd.read_csv(r"%s" % log_hist, header=None)
            history_file = history_file.set_axis(["filename"], axis=1)
            history_list = list(history_file["filename"])

            trashlist = list(
                file_list_all[
                    (~file_list_all["full_name"].isin(history_list))
                    & (~file_list_all["full_name"].isin(trash_list))
                ]["full_name"]
            )

            for i in trashlist:
                log.run_csv_trash([i], "a")

            print(
                "process {} data: DONE --- {} seconds --- ".format(
                    phase, (time.time() - start_time)
                )
            )
            print("Trash_file: ", len(trashlist))
            print(
                "----------------------------------------------------------------------------------"
            )
            return data

        except:
            file_list = file_list_all[["folder", "name"]]
            file_list.drop_duplicates(inplace=True)
            file_list.reset_index(drop=True, inplace=True)
            print("Create:", file_list.shape[0])

            data = pd.DataFrame()
            check_fileprocess = []

            for i in tqdm(range(file_list.shape[0]), position=0):
                try:
                    FT = os.path.join(file_list["folder"][i], file_list["name"][i])
                    df = pd.read_excel(FT, sheet_name=name_sheet)
                    dfb = function(df, file_list["folder"][i])
                    data = pd.concat([data, dfb])
                    log.run_csv([str(os.path.join(file_list["name"][i], name_sheet))])
                    check_fileprocess.append(
                        str(os.path.join(file_list["name"][i], name_sheet))
                    )
                except:
                    print(FT)
                    pass

            trashlist = list(
                file_list_all[~file_list_all["full_name"].isin(check_fileprocess)][
                    "full_name"
                ]
            )

            for i in trashlist:
                log.run_csv_trash([i], "a")

            print(
                "process {} data: DONE --- {} seconds --- ".format(
                    phase, (time.time() - start_time)
                )
            )
            print("Trash_file: ", len(trashlist))
            print(
                "----------------------------------------------------------------------------------"
            )
            return data

    def numberdata(data_name, column_name, sql_table_name=None):
        """Kiểm tra dữ liệu str lần trong cột dữ liệu số

        Args:
            data_name (variable): Biến đại diện DataFrame

            column_name (str): Tên cột

            sql_table_name (str, None): Tên bảng trong SQL server. Defaults to None.

        Returns:
            set(set_error): list dữ liệu lỗi
        """
        set_error = list()
        for i in tqdm(range(data_name.shape[0])):
            if str(data_name[column_name].iloc[i]) != "nan":
                if type(data_name[column_name].iloc[i]) in [float, int]:
                    pass
                elif type(data_name[column_name].iloc[i]) not in [float, int]:
                    try:
                        type(float(data_name[column_name].iloc[i]))
                    except:
                        pass
                        try:
                            type(int(data_name[column_name].iloc[i]))
                        except:
                            set_error.append(data_name[column_name].iloc[i])
                            pass
        # log.datatype(sql_table_name+':'+column_name+':'+str(set(set_error)))
        return set(set_error)
