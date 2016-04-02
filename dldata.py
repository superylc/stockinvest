# -*- coding: utf-8 -*-
"""
Created on Sat Jun 06 18:49:10 2015

@author: 123
"""
import sys
import MySQLdb
import pickle
import datetime
import tushare as ts
import numpy as np
import pandas as pd
import Queue
import threading
import traceback
import datetime
from pandas import DataFrame, Series
from time import ctime


'''
try:
    conn_target=MySQLdb.connect(host='localhost',user='root',passwd='YLCyang#1118', \
                            db='stockdata',port=3306, charset='utf8')
except MySQLdb.Error,e:
    print "Mysql Error %d: %s" % (e.args[0], e.args[1])
    sys.exit()

conn_cur = conn_target.cursor()
'''


def get_dbconn():
    try:
        conn_target=MySQLdb.connect(host='localhost',user='root',passwd='YLCyang#1118', \
                                db='stockdata',port=3306, charset='utf8')
    except MySQLdb.Error,e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        sys.exit()
    return conn_target
    

# 仅提供连接数据库连接通过,DbConn.conn, DbConn.cur使用
class DbConn():
    def __init__(self):
        self.conn = get_dbconn()
        self.cur = self.conn.cursor()
    
    def __del__(self):
        self.cur.close()
        self.conn.close()

# return a list
def get_stock_data(stock_list, start_date):
    stock_data_list = []
    for stockcode in stock_list:
        stock_data_list.append({})
        stock_data_list[-1]['code'] = stockcode
        stock_data_list[-1]['data']=ts.get_h_data(code = stockcode, start = start_date).sort(ascending = True)
    return stock_data_list

def get_stocknames_by_codelist(codelist):
    # 先获取所有股票列表名称
    df_stock_list = ts.get_stock_basics()['name']
    # 根据codelist重新筛选并返回
    return df_stock_list.reindex(codelist
    )

def get_one_stockdata(stockcode, start_date, pause = 0):
    stockdata = ts.get_h_data(code = stockcode, start = start_date, pause = pause)
    stockdata.sort(ascending = True, inplace = True)
    return stockdata

 
# 保存单个板块的数据到mysql的daydata_stats_1表中：
def save_mysql_daystats1(bankuai_str, df_daystats, dbconn):
    # 设置删除该板块记录的sql
    sql_del = "delete from daydata_stats_1 where bankuai = '" + bankuai_str + "'"
    # 生成mysql表的字段名
    #--debug
    #print bankuai_str
    #print df_daystats
    insert_field_list = ["bankuai"]
    insert_field_list.append(df_daystats.index.name)
    insert_field_list.extend(df_daystats.columns.values)
    #--debug
    #print len(insert_field_list)
    #print insert_field_list
    #需要插入数据库的数据
    insert_values = []
    for k, vrow in df_daystats.iterrows():
        insert_values.append([])
        insert_values[-1].append(bankuai_str)
        insert_values[-1].append(k)
        insert_values[-1].extend(vrow.tolist())
    
    #print len(insert_values[0])
    #print insert_values[0]
    # 字段个数为
    sqlstr = "insert into daydata_stats_1(" + ",".join(insert_field_list) + ") " + \
                "values(" + ",".join(["%s"] * len(insert_field_list)) + ") " + \
                "on duplicate key update " + ",".join([s + "=values(" + s + ")" for s in insert_field_list])
    #--debug
    #print sqlstr
    #sys.exit()    
    try:
        #删除板块所有记录后插入
        dbconn.cur.execute(sql_del)
        dbconn.cur.executemany(sqlstr, insert_values)
        dbconn.conn.commit()
    except Exception, e:
        dbconn.conn.rollback()
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        print e
        sys.exit()
    
# 从daydata_stats_1中读出板块所需数据, field_list可能需要根据情况变化
def load_mysql_daystats1(bankuai_str, dbconn):
    field_list = ['code', 'name', 'rate_eachday', 'rate_eachday_mean', 'rate_eachday_mean_minus', 'rate_eachday_mean_per', \
                    'acum_amount', 'acum_amount_mean', 'acum_amount_mean_minus', 'acum_amount_mean_per', 'avg_volt', \
                    'avg_volt_mean', 'avg_volt_mean_minus', 'avg_volt_mean_per', 'sum_volt', 'sum_volt_mean', 'sum_volt_mean_minus', 'sum_volt_mean_per']

    sqlstr = "select " + ",".join(field_list) + " from daydata_stats_1 where bankuai = '" + bankuai_str + "'"
    #print sqlstr
    try:
        dbconn.cur.execute(sqlstr)
    except Exception, e:
        print "Mysql Error %d: %s" % (e.args[0], e.args[1])
        sys.exit()
    sql_result = []
    for row in dbconn.cur.fetchall():
        sql_result.append(row)
    print sql_result
    #test_r = [(u'300318', u'\u535a\u6656\u521b\u65b0', 0.0102981365268529, 0.00426008172825317, 0.0060380547986, 1.4173565635, 10166649019.0, 31996145873.9583, -21829496855.0, -0.682253948365, 0.0112045603818525, 0.00466460971859635, 0.00653995066326, 1.4020359811, 1.36695636658601, 1.00584199962164, 0.361114366964, 0.359016989846), (u'600750', u'XD\u6c5f\u4e2d\u836f', 0.00442300146454122, 0.00426008172825317, 0.000162919736288, 0.0382433358514, 35632993090.0, 31996145873.9583, 3636847216.04, 0.113665165497, 0.00476565200447357, 0.00466460971859635, 0.000101042285877, 0.0216614662261, 1.1008656130334, 1.00584199962164, 0.0950236134118, 0.0944717097193)]
    
    df_result = pd.DataFrame(data = sql_result, columns = field_list)
    #print df_result
    df_result.set_index("code", inplace = True)
    return df_result

# 清空单个平台数据
def truncate_mysql_daydata_stock(stockcode, dbconn):
    try:
        rslt = dbconn.cur.execute("delete from stock_day_data where code = '" + stockcode + "'")
    except Exception, e:
        dbconn.conn.rollback()
        print "code: %s, error while deleting data " % stockcode
        print e
        return False
    else:
        dbconn.conn.commit()
        print "data deleted.\ntable: stock_day_data , code: " + stockcode + ", rows: " + str(rslt)
        return True

# 保存单只股票数据，保存成功返回True, 失败返回False. dbconn 是DbConn的实例
def save_mysql_daydata_stock(stockcode, stockdata, dbconn):
    insert_values = []
    for idx, row in stockdata.iterrows():
        # insert one row
        insert_values.append([])
        # get code and date
        insert_values[-1].append(stockcode)
        insert_values[-1].append(idx)
        # get values
        for v in row.values:
            insert_values[-1].append(v)
    
    insert_err_flg = 0
    try:
        dbconn.cur.executemany("insert into stock_day_data(code, date, open, high, close, low, volume, amount) \
                            values(%s,%s,%s,%s,%s,%s,%s,%s) \
                            on duplicate key update code=values(code), date=values(date), \
                            open=values(open), high=values(high), close=values(close), \
                            low=values(low), volume=values(volume),amount=values(amount)", insert_values)
    except Exception, e:
        insert_err_flg = 1
        print "code: %s, error while inserting data " % stockcode
        print e
    if insert_err_flg == 0:
        dbconn.conn.commit()
        return True
    else:
        dbconn.conn.rollback()
        print "rollbacked!"
        return False

#获取单只股票数据
def load_mysql_daydata_stock(stockcode, dbconn, start_date = '1970-01-01', end_date = '9999-12-31'):
    col_names = ['open','high','close','low','volume','amount']
    idx_name = 'date'
    
    idx_list = []
    data_list = []
    sqlstr = "select " + idx_name + "," + ",".join(col_names) + " from stock_day_data where code = '" \
                + MySQLdb.escape_string(stockcode) + "' and date >= '" + start_date + \
                "' and date <= '" + end_date + "'"
    #print sqlstr
    dbconn.cur.execute(sqlstr)
    for row in dbconn.cur.fetchall():
        idx_list.append(row[0])
        data_list.append(row[1:])
    # 将索引变成pd.Index对象
    idx_index = pd.DatetimeIndex(data = idx_list, name = idx_name)
    
    #如果没有去除数据，data_list 置为空
    if len(data_list) == 0:
        data_list = None
    df_data = DataFrame(data = data_list, index = idx_index, columns = col_names)
    return df_data
    
    
#获取多只股票数据
def load_mysql_daydata_stock_all(stock_list, dbconn, start_date = '1970-01-01', end_date = '9999-12-31'):
    df_stocks = []
    for stockcode in stock_list:
        df_onestock = load_mysql_daydata_stock(stockcode, dbconn, start_date, end_date)
        df_stocks.append({})
        df_stocks[-1]['code'] = stockcode
        df_stocks[-1]['data'] = df_onestock
    return df_stocks
    
# 保存多只股票数据， mode = all会先删除该股票所有数据， mode = part 则不会
def save_mysql_daydata_stock_all(stock_list, start_date, dbconn, pause = 0, mode = 'part'):
    # stock_list must be list type
    # 保存未完成的平台列表
    unfinished_stock = stock_list[:]
    stock_cnt = len(stock_list)
    cur_stock_id = 0
    time_st = datetime.datetime.now()
    for stockcode in stock_list:
        cur_stock_id += 1
        timeused = datetime.datetime.now() - time_st
        print "code: %s, getting data [%d/%d] - time used : " % (stockcode, cur_stock_id, stock_cnt) , timeused
        err_flg = 0
        try:
            # 获取数据失败后标记错误
            stockdata = get_one_stockdata(stockcode, start_date, pause)
        except Exception, e:
            print "code: %s, error while downloading data " % stockcode
            print e
            err_flg = 1
        #--debug
        '''
        if stockcode == '000540':
            stockdata['ccc'] = 5
        '''
        print ""
        
        # 正确获取数据则插入数据库
        if err_flg == 0:
            # mode = all 则先清空该平台数据
            if mode == 'all':
                truncate_mysql_daydata_stock(stockcode, dbconn)
            # exception will report inside below function if insert failed
            save_success = save_mysql_daydata_stock(stockcode, stockdata, dbconn)
            # 插入失败后标记错误
            if not save_success:
                err_flg = 1
        
        # 获取和插入都成功则删除平台code
        if err_flg == 0:
            del unfinished_stock[unfinished_stock.index(stockcode)]
    return unfinished_stock


def save_log(filename, msg):
    f = open(filename, 'ab')
    f.write('[' + datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S') + ']\n')
    f.write(msg + '\n')
    f.close()


def reserve_last_lines(filename, last_n_lines):
    f1 = open(filename, 'r+')
    lines = f1.readlines()
    lines = lines[-last_n_lines:]
    f1.seek(0)
    f1.truncate()
    for line in lines:
        f1.write(line)
    f1.close()
    return True


class DownThread(threading.Thread):
    #cur_stock_id must be a list : [5] means the 5th stock
    def __init__(self, stock_que, thread_name, lock, cur_stock_id, unfinished_stock, time_st, stock_cnt, start_date, pause, mode, logfile): 
        super(DownThread, self).__init__(name = thread_name)
        # threading.Thread.__init__(self) 
        # self.__stockname = None
        self.__queue = stock_que
        self.__lock = lock
        # value type
        self.__time_st = time_st
        self.__stock_cnt = stock_cnt
        self.__start_date = start_date
        self.__pause = pause
        self.__mode = mode
        self.__logfile = logfile

        # list type
        self.__unfinished_stock = unfinished_stock
        # list type for reference
        self.__cur_stock_id = cur_stock_id
        
        # new connection for each thread
        self.__dbconn = DbConn()

    '''
    # function must be called manully; won't be called if sub thread is killed by main thread
    def __del__(self):
        # super(DownThread, self).__del__() #no __del__ method for Thread
        #del self.__dbconn
        save_log("thread_del.txt", self.getName() + " delete")
    '''

    def run(self):
        # global time_st, stock_cnt, start_date, pause, mode, dbconn

        while True:
                stockcode = self.__queue.get()
                try:
                    self.__lock.acquire()
                    self.__cur_stock_id[0] = self.__cur_stock_id[0] + 1
                    curstock_id = self.__cur_stock_id[0]
                    self.__lock.release()
                    timeused = datetime.datetime.now() - self.__time_st
                    #print "code: %s, getting data [%d/%d] - time used : " % (stockcode, curstock_id, self.__stock_cnt) , timeused
                    save_log(self.__logfile, "code: %s, getting data [%d/%d] - time used : %s" % (stockcode, curstock_id, self.__stock_cnt, timeused))
                    err_flg = 0
                    try:
                        # 获取数据失败后标记错误
                        stockdata = get_one_stockdata(stockcode, self.__start_date, self.__pause)
                    except Exception, e:
                        #print "code: %s, error while downloading data " % stockcode
                        #print e
                        save_log(self.__logfile, "code: %s, error while downloading data " % stockcode)
                        save_log(self.__logfile, str(e))
                        err_flg = 1

                    #--debug
                    # if stockcode == '000540':
                    #     stockdata['ccc'] = 5

                    
                    # 正确获取数据则插入数据库
                    if err_flg == 0:
                        # mode = all 则先清空该平台数据
                        if self.__mode == 'all':
                            truncate_mysql_daydata_stock(stockcode, self.__dbconn)
                        # exception will report inside below function if insert failed
                        save_success = save_mysql_daydata_stock(stockcode, stockdata, self.__dbconn)
                        # 插入失败后标记错误
                        if not save_success:
                            err_flg = 1
                    
                    # 获取和插入都成功则删除平台code
                    if err_flg == 0:
                        self.__lock.acquire()
                        del self.__unfinished_stock[self.__unfinished_stock.index(stockcode)]
                        self.__lock.release()
                        save_log(self.__logfile, "code: %s, succeed to download and save data." % stockcode)
                except Exception, e:
                    #print e
                    #print traceback.print_exc()
                    save_log(self.__logfile, str(e))
                    save_log(self.__logfile, str(traceback.print_exc()))
                finally:
                    self.__queue.task_done()


# 保存多只股票数据， mode = all会先删除该股票所有数据， mode = part 则不会
def save_mysql_daydata_stock_all_multithread(stock_list, start_date, dbconn, logfile, pause = 0, mode = 'part'):
    # print 'muli test'
    # print dbconn.conn
    # stock_list must be list type
    # 保存未完成的平台列表
    #--debug: test 10 stocks
    # stock_list = stock_list[:10]

    thread_num = 20
    #logfile = 'stockdl.txt'

    unfinished_stock = stock_list[:]
    stock_cnt = len(stock_list)

    stock_que = Queue.Queue()
    for stock in stock_list:
        stock_que.put(stock)
    lock = threading.Lock()

    # record how many stocks processed
    cur_stock_id = [0]
    time_st = datetime.datetime.now()

    for i in range(thread_num):
        t = DownThread(stock_que, "th" + str(i), lock, cur_stock_id, unfinished_stock, time_st, stock_cnt, start_date, pause, mode, logfile)
        # del t
        t.setDaemon(True)
        t.start()

    stock_que.join()
    '''
    for stockcode in stock_list:
        cur_stock_id += 1
        timeused = datetime.datetime.now() - time_st
        print "code: %s, getting data [%d/%d] - time used : " % (stockcode, cur_stock_id, stock_cnt) , timeused
        err_flg = 0
        try:
            # 获取数据失败后标记错误
            stockdata = get_one_stockdata(stockcode, start_date, pause)
        except Exception, e:
            print "code: %s, error while downloading data " % stockcode
            print e
            err_flg = 1
        #--debug
        # if stockcode == '000540':
        #     stockdata['ccc'] = 5

        print ""
        
        # 正确获取数据则插入数据库
        if err_flg == 0:
            # mode = all 则先清空该平台数据
            if mode == 'all':
                truncate_mysql_daydata_stock(stockcode, dbconn)
            # exception will report inside below function if insert failed
            save_success = save_mysql_daydata_stock(stockcode, stockdata, dbconn)
            # 插入失败后标记错误
            if not save_success:
                err_flg = 1
        
        # 获取和插入都成功则删除平台code
        if err_flg == 0:
            del unfinished_stock[unfinished_stock.index(stockcode)]
    '''
    return unfinished_stock

    
# 获取和保存前复权数据到pickle -> stockdata
def save_pickle_qfqstockdata(start_date):
    df_stock_list = ts.get_stock_basics()
    stock_codes = df_stock_list.index
    stock_data_list = get_stock_data(stock_codes, start_date)
    
    f_stock = open('qfqstocks.pkl','wb')
    pickle.dump(stock_data_list, f_stock)
    f_stock.close()
    return f_stock.name    


def save_daydata_csv(df_data, filename, filedir = ""):
    #filename = "daydata"
    fileno = 1
    save_suc_flg = 0
    while (not save_suc_flg) and (fileno <= 10):
        fileaddr = filedir + filename + str(fileno) + ".csv"
        try:
            df_data.to_csv( fileaddr, encoding = 'utf-8')
            print "file saved to %s" % fileaddr
            save_suc_flg = 1
        except Exception, e:
            print "failed to save file:%s" % fileaddr
            print e
            fileno += 1
    
    return save_suc_flg,  fileaddr
    
    
def save_daydata_pickle(df_data, filename, filedir = ""):
    #filename = "daydata"
    fileno = 1
    save_suc_flg = 0
    while (not save_suc_flg) and (fileno <= 10):
        fileaddr = filedir + filename + str(fileno) + ".pkl"
        try:
            df_data.to_pickle( fileaddr)
            print "file saved to %s" % fileaddr
            save_suc_flg = 1
        except Exception, e:
            print "failed to save file:%s" % fileaddr
            print e
            fileno += 1
    return save_suc_flg,  fileaddr

def load_daydata_pickle(filepath = ""):
    f = open(filepath, 'rb')
    data = pickle.load(f)
    f.close()
    return data

def save_mysql_stock_basics(dbconn):
    insert_err_flg = 0
    err_msg = []    # output variables for debug when face error
    df_stock_list = ts.get_stock_basics()
    field_list = ['code','name','industry','area','pe','outstanding','totals','totalassets','liquidassets','fixedassets','reserved','reservedpershare','esp','bvps','pb','timetomarket']
    # %s * 字段数量
    str_snum = ['%s'] * 16
    #插入数据表的list
    content_list = []
    for row in df_stock_list.iterrows():
        if not(type(row[1]['industry']) == float and np.isnan(row[1]['industry'])):
            value_list = []
            value_list.append(row[0])
            value_list.extend(row[1].values)
            # 处理日期
            datestr = str(value_list[-1])
            datestr = "-".join([datestr[:4], datestr[4:6], datestr[6:8]])
            value_list[-1] = datestr
            content_list.append(value_list)
            #--debug
            #print datestr
            #print value_list
            #break
    sql_str = "insert into stock_basics(" + ",".join(field_list) + ") values(" + ",".join(str_snum) + ")"
    #--debug
    #print sql_str
    #content_list = content_list[0:1730]
    #print content_list
    try:
        dbconn.cur.execute("truncate stock_basics")
        dbconn.cur.executemany(sql_str, content_list)
    except Exception, e:
        insert_err_flg = 1
        print "code: %s, error while inserting stock basic data "
        print e
        err_msg.append(False)
        err_msg.append(sql_str)
        err_msg.append(content_list)
    if insert_err_flg == 0:
        dbconn.conn.commit()
        print "total %d stocks basic data saved." % len(content_list)
        return True
    else:
        dbconn.conn.rollback()
        print "rollbacked!"
        return err_msg

    
    
'''
a = 1
b = 2

pickle.dump(a, f_stock)
pickle.dump(b, f_stock)
'''


#module test code
if __name__ == "__main__":
    #stockdata = save_pickle_qfqstockdata('2014-07-01')
    #print stockdata
    #save_mysql_daystats1()

    conn = DbConn()
    #truncate_mysql_daydata_stock('300168', conn)
    save_mysql_daydata_stock_all(['300168'],'1980-01-01' ,conn, mode = 'all')
    #save_mysql_stock_basics(conn)
    '''
    print conn.conn, conn.cur
    df_r = load_mysql_daystats1(u"con_婴童概念",conn)
    print df_r
    
    st_date = '2014-07-01'

    data1 = get_one_stockdata('000540','2014-07-01')
    print data1

    save_mysql_daydata_stock('000540', data1, conn)


    #stock_list = ['000540','601933','000603']
    df_stock_list = ts.get_stock_basics()
    stock_list = list(df_stock_list.index)
    
    #stock_list = ['000540','601933','000603']
    fail_stock_list = save_mysql_daydata_stock_all(stock_list, st_date, conn)
    print fail_stock_list

    
    df1 = load_mysql_daydata_stock('000540', conn)
    print df1
    
    df2 = load_mysql_daydata_stock_all(['000540','601933','000603'], conn, '2015-01-07', '2015-05-31')
    print df2
    '''
    del conn
