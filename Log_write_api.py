import pandas as pd 
import numpy as np 
import os
from datetime import date
from datetime import datetime
from tqdm import tqdm
import warnings
import time
from environs import Env
import pathlib
import csv

env  = Env()
env.read_env()

folder_log = env('folder_log')

""" Tạo tự động 2 folder log và csv lưu dữ liệu """

p = pathlib.Path(__file__)
parent_dir =  pathlib.PurePath(p).parents[0]
print('root_folder:',parent_dir)



folder_log = os.path.join(parent_dir,'log_HOSE')

try: 
    os.mkdir(folder_log)
    print(parent_dir)
    print(folder_log)
    print('Create Folder: DONE')
except OSError as error: 
    print(error)
    pass


"""Tự động cập nhất vị trí folder lịch sử """

dirc_log = '"' + folder_log + '"'

with open(str(parent_dir) + "\.env",'r',encoding='utf-8') as data:
    df = data.readlines()

df[6] = "folder_log={}\n".format(dirc_log)

with open(str(parent_dir) + "\.env",'w') as data:
    data.writelines(df)

#####################################################################################################################

log_daycheck = "log_dayprocess.txt"

log_runcheck = "log_fileprocess.txt"

log_errorlis = "log_SyStoSQL_error.txt" # Hiện chỉ cần sử dụng file này

log_datatype = "log_datatypecolumn.txt"

log_runcheck_csv = "log_fileprocess.csv"

log_trash_csv = "log_filetrash.csv"

class log:
    """ 
        Tạo tự động và function ghi dữ liệu lịch sử 

    """
    
    def daily(message):
        """_summary_

        Args:
            message (str): kết quả muốn ghi
        """
        timestamp_format = '%Y-%h-%d %H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
        now = datetime.now() # get current timestamp
        timestamp = now.strftime(timestamp_format)
        with open(os.path.join(folder_log,(str(date.today())+'_'+log_daycheck)),"a",encoding='utf-8') as f:
            f.write(message + '\n')

    def error(message):
        """_summary_

        Args:
            message (str): kết quả muốn ghi
        """
        timestamp_format = '%Y-%h-%d %H:%M:%S' # Year-Monthname-Day-Hour-Minute-Second
        now = datetime.now() # get current timestamp
        timestamp = now.strftime(timestamp_format)
        with open(os.path.join(folder_log,(str(date.today())+'_'+log_errorlis)),"a",encoding='utf-8') as f:
            f.write(message + '\n')

    def run(message):
        """_summary_

        Args:
            message (str): kết quả muốn ghi
        """
        with open(os.path.join(folder_log,log_runcheck),"a",encoding='utf-8') as f:
            f.write(message + '\n')
        
    def datatype(message):
        """_summary_

        Args:
            message (str): kết quả muốn ghi
        """
        with open(os.path.join(folder_log,log_datatype),"w",encoding='utf-8') as f:
            f.write(message + '\n')
            
    def run_csv(message):
        """_summary_

        Args:
            message (str): kết quả muốn ghi
        """
        with open(os.path.join(folder_log,log_runcheck_csv),'a',encoding='utf-8',newline='') as f:
            writer = csv.writer(f)
            writer.writerow(message)
            
    def run_csv_trash(message,style):
        """_summary_

        Args:
            message (str): kết quả muốn ghi
            style (str): kiểu ghi ('a':append
                                   'w': over write)
        """
        with open(os.path.join(folder_log,log_trash_csv),style,encoding='utf-8',newline='') as f:
            writer = csv.writer(f)
            writer.writerow(message)