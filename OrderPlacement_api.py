import pandas as pd 
import numpy as np 
import os
from datetime import date
from datetime import datetime
from tqdm import tqdm
import warnings
warnings.filterwarnings('ignore')

"""#######################################################################################"""
class orderP:
    def stock(data,x):
        """ format Order_placement từ 2007 - 2023
        
        Args:
            data: DataFrame raw sau khi pd.read
            
            x: Date dữ liệu trong tên của folder chứa dữ liệu (VD: folder dữ liệu HOSE 16-08-2023)

        Returns:
            datab: DataFrame đã qua xử lý
        """
        row = data[data.iloc[:,0].str.startswith(('A','B'), na=False)==True].index.values.astype(int)[0]
        data = data[row:]
        datab = data.set_axis(['ticker', 'numBuyingOrders', 'buyingVolume', 'numSellingOrders', 
                            'sellingVolume', 'tradingVolume', 'netVolume'], axis=1)
        try:
            datab.drop(datab[datab['ticker'].str.contains('Tổng')].index,inplace=True)
        except:
            pass
            try:
                datab.drop(datab[datab['ticker']==0].index,inplace=True)
            except:  # noqa: E722
                pass
        datab['report_date'] = x[len(x)-4:len(x)] + '-' + x[len(x)-7:len(x)-5] + '-' + x[len(x)-10:len(x)-8]
        datab['opID'] = datab['ticker'] + x[len(x)-4:len(x)] + x[len(x)-7:len(x)-5] + x[len(x)-10:len(x)-8]
        datab['last_update'] = str(date.today()) 
        cols = ['opID', 'ticker', 'numBuyingOrders', 'buyingVolume', 'numSellingOrders', 'sellingVolume',
                'tradingVolume', 'netVolume', 'report_date', 'last_update']
        datab = datab[cols]       
        return datab