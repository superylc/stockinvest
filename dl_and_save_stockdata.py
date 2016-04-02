# -*- coding: utf-8 -*-
"""
Created on Sun Jun 07 10:52:43 2015

@author: 123
"""

from stock_invest.dldata import *

import random
import datetime
import sys

conn = DbConn()
# print conn.conn, conn.cur

curdate = datetime.date.today()
timedel = datetime.timedelta(-15)
start_date = curdate + timedel

# st_date is string format like '2015-04-01'
st_date = start_date.strftime("%Y-%m-%d")

'''
data1 = get_one_stockdata('000540','2014-07-01')
print data1
save_mysql_daydata_stock('000540', data1, conn)
'''

#stock_list = ['000540','601933','000603']
df_stock_list = ts.get_stock_basics()
# 保存股票基本数据
save_rtn = save_mysql_stock_basics(conn)

stock_list = list(df_stock_list.index)
#stock_list = random.sample(stock_list, 3)

#--debug
#print conn.conn

logfile = 'stockdl.txt'

#stock_list = ['000540','601933','000603']
# fail_stock_list = save_mysql_daydata_stock_all(stock_list, st_date, conn)
fail_stock_list = save_mysql_daydata_stock_all_multithread(stock_list, st_date, conn, logfile)
print "failed stock:%s " % fail_stock_list

del conn

# clear logfile

reserve_last_lines(logfile, 20000)