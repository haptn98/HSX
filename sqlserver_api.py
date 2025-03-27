import time
import pyodbc
import pandas as pd
from tqdm import tqdm
from fast_to_sql import fast_to_sql as fts
from environs import Env
from Log_write_api import log

env = Env()
env.read_env()

"""
Kết nối SQL server
"""

cnxnstr = ("Driver={};\n"
            "Server={};\n"
            "Database={};\n"
            "UID={};\n"
            "PWD={};".format(env('Driver'),env('Server'),env('Database'),env('UID'),env('PWU')))

sqlcnxn = pyodbc.connect(cnxnstr)

sqlcursor = sqlcnxn.cursor()

class syntax:
    def truncate(sql_table_name): #truncate bảng dữ liệu
        """ Truncata table: SQL query, Xóa dữ liệu trong SQL server

        Args:
            sql_table_name (str): Tên bảng trong SQL server

        Returns:
            text: Truncate done => báo hoàn thành
        """
        truncate = "truncate table " + sql_table_name
        sqlcursor.execute(truncate)
        sqlcnxn.commit()
        return print('truncate done')

    def push_test(sql_table_name,python_data,columnID):
        """ Đối chiếu dữ liệu trong SQL server và dữ liệu đã xử lý => xác định dữ liệu chênh lệch
        
            Đẩy dữ liệu mới lên SQL server
            
        Args:
            sql_table_name (str): tên bảng SQL server 
            
            python_data (variable): Tên Dataframe
            
            columnID (str,None): tên cột

        Returns:
            print('Push Data to SQL: Done'): Thông báo hoàn thành  
        
        VD:
            select_sql = "select " + "{}".format(columnID) + " " +\
                          "from " + "{}".format(sql_table_name) + " " +\
                          "where report_date < '2023-05-01'"
        """
        start_time = time.time()

        if columnID != None:
            select_sql = "select " + "{}".format(columnID) + " " +\
                         "from " + "{}".format(sql_table_name)
            check_df = pd.read_sql(select_sql, sqlcnxn)
            pushlist = list(set(python_data[columnID].tolist()).difference(check_df[columnID].tolist()))
            python_data = python_data[python_data[columnID].isin(pushlist)]
        else:
            pass

        cols = "`,`".join([str(i) for i in python_data.columns.tolist()])
        for i,row in tqdm(python_data.iterrows()):
            try:
                sql = "INSERT INTO " + sql_table_name + " VALUES (" + "?,"*(len(row)-1) + "?)"
                sqlcursor.execute(sql, tuple(row))
                sqlcnxn.commit()
            except:
                print(tuple(row))
                log.error('{}:{}'.format(sql_table_name,str(tuple(row))))
                pass
        return print('Push Data to SQL: Done')
    
    def push(sql_table_name,python_data):
        """ Đẩy dữ liệu lên SQL theo từng dòng 
        -----------
        Variables:
            sql_table_name: str tên bảng trong SQL_server
        
            python_data: Biến dữ liệu trong python
        
        -----------
        Return:
            "PUSH DATA: DONE--- %s seconds ---": thời gian chạy của chương trình
        """
        for i,row in tqdm(python_data.iterrows()):
            try:
                sql = "INSERT INTO " + sql_table_name + " VALUES (" + "?,"*(len(row)-1) + "?)"
                sqlcursor.execute(sql, tuple(row))
                sqlcnxn.commit()
            except Exception as Argument:
                print(tuple(row))
                print('Error: ',Argument)
                pass
        return print('Push data Done') 

    def push_fast(sql_table_name,python_data):
        """ Đẩy dữ liệu lên SQL với tốc độ nhanh

        Args:
            sql_table_name (str): tên bảng SQL server 
            
            python_data (variable): tên Dataframe

        Returns:
            print('Push Data to SQL: Done'): Thông báo hoàn thành
        """        
        start_time = time.time()
        create_statement = fts.fast_to_sql(python_data,sql_table_name,sqlcnxn,if_exists="replace")
        sqlcnxn.commit()
        return print('Push Data to SQL: Done')
    
    def exec_spcolumn(table_name=None ,table_owner=None):
        """ Đại diện câu lệnh EXEC sp_cloumns nhằm lấy dữ liệu bảng trong SQL Server 

        Args:
            table_name (str, None): Tên bảng SQL không có schema. Defaults to None.
            
            table_owner (str, None): Tên schema. Defaults to None.

        Returns:
            df: DataFrame chứa thông tin bảng
        """
        sql = """EXEC sp_columns @table_name = {}, @table_owner = {}""".format(table_name,table_owner)
        df = pd.read_sql(sql,sqlcnxn)
        df['TYPE_NAME'] = df['TYPE_NAME'].replace('varchar','object').replace('nvarchar','object')\
                                        .replace('date','object').replace('bigint','int')
        return df
    
    def SQL_select_to_pd(sqlquery):
        select_sql = sqlquery
        df = pd.read_sql(select_sql,sqlcnxn)
        return df
    
    def SQL_close():
        sqlcnxn.close()
        return print('Connect Close')