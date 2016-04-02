# -*- coding: utf-8 -*-
"""
Created on Sat Jun 06 10:38:22 2015

@author: 123
"""
import numpy as np
import pandas as pd
from pandas import DataFrame, Series
import tushare as ts
import random
import time
# 获取数据的模块
import stock_invest.dldata as dld

# 求序列每日波动，返回一个每日涨幅的list
def cal_volatility(ser):
    vol = []
    vol.append(0)
    for i in range(1,len(ser)):
        vol.append(float(ser[i]-ser[i-1])/ser[i-1])
    return vol
    
'''
# 求序列之和，主要用于求cal_volatility返回的波动率之和
def sum_ser(v_list):
    return np.array(v_list).sum()
'''

# 处理序列并求序列均值
def avg_method(ser, cal_func):
    ser_result = cal_func(ser)
    return np.mean(ser_result)

# 复利计算序列的收益率
def rate_each(start_value, end_value, date_len):
    if date_len < 2 or start_value < 0 or end_value < 0:
        return None
    else:
        return (1 + float(end_value - start_value)/start_value)**(1.0 / (date_len - 1)) - 1

# 去除头尾的nan
def cutht_ser(ser, copy=True):
    ser_tmp = ser.copy() if copy == True else ser
    #--debug
    #print "len ser ", len(ser_tmp)
    #print ser_tmp
    # 空series直接返回
    if len(ser_tmp) == 0:
        return ser_tmp
    idx_b = 0
    idx_e = len(ser_tmp)-1
    while(idx_b < len(ser_tmp) and (np.isnan(ser_tmp[idx_b]))):
        #--debug
        #print "idx_b", idx_b
        idx_b += 1
    while(idx_e >= 0 and (np.isnan(ser_tmp[idx_e]))):
        idx_e -= 1
    # 开始序号小于等于结束序号，则返回； 否则说明全部数据都是nan，返回空序列
    if idx_b <= idx_e:
        ser_tmp = ser_tmp[idx_b : idx_e + 1]
    else:
        ser_tmp = ser_tmp.reindex([])
    return ser_tmp

# 按平均值填充时间序列中的空值
def fill_avg_value(ser, copy=True):
    ser_tmp = ser.copy() if copy == True else ser
    idx_b, idx_e = 0,0
    idx_cur = 0
    # 1:searching start point    2:searching end point 
    s_status = 1
    while(idx_cur < len(ser_tmp)):
        if s_status == 1:
            if np.isnan(ser_tmp[idx_cur]):
                idx_b = idx_cur
                s_status = 2
        elif s_status == 2:
            if not np.isnan(ser_tmp[idx_cur]):
                idx_e = idx_cur
                # start to fill gap
                b_value = ser_tmp[idx_b - 1]
                e_value = ser_tmp[idx_e]
                gap_nums = np.linspace(b_value, e_value, idx_e - idx_b + 2)
                #print gap_nums
                gap_nums = gap_nums[1 : -1]
                #print gap_nums
                for i in range(len(gap_nums)):
                    ser_tmp[idx_b + i] = gap_nums[i]
                #--debug
                #print idx_b, idx_e, "filling gap"
                s_status = 1
        idx_cur += 1
    return ser_tmp


# 对时间序列数据先进行填充，再求日均涨幅，复利收益率
def cal_volatility_stock(df_stock, idx_all):
    # 长度为0返回nan，否则计算值
    if len(df_stock) == 0:
        avg_volt, rate_eachday, start, end, datalen, sum_volt = np.nan, np.nan, np.nan, np.nan, np.nan, np.nan
    else:
        # 取每日的收盘价，并加上缺失日期
        ser_close = df_stock['close'].reindex(idx_all).apply(float)
        # 去掉头尾缺失日期，对中间停牌数据进行均值填充
        ser_deal = cutht_ser(ser_close)
        ser_deal = fill_avg_value(ser_deal)
        # 计算日均涨幅和复利日收益率、累计涨跌幅
        avg_volt = avg_method(ser_deal, cal_volatility)
        rate_eachday = rate_each(ser_deal[0], ser_deal[-1], len(ser_deal))
        start = ser_deal.index[0]
        end  = ser_deal.index[-1]
        datalen  = len(ser_deal)
        sum_volt = sum(cal_volatility(ser_deal))
    # 返回日均涨幅和复利日收益率， 开始日期，结束日期，序列长度
    return avg_volt, rate_eachday, start, end, datalen, sum_volt


# return all_date index
def all_date_list(stock_data_list):
    idx_all = stock_data_list[0]['data'].index
    for row in stock_data_list:
        data = row['data']
        idx_all = idx_all.join(data.index, how='outer')
    return idx_all


# 提取的stock_data_list包含'code'域，本函数将'code'域去除
def get_only_data(stock_data_list):
    df_data = []
    for item in stock_data_list:
        df_data.append(item['data'].copy())
    return df_data

# 将ts.get_industry_classified()等获取的数据进行转换，转换后可以用户连接股票数据, str_prefix为unicode
def convert_cls_stockdata(cls_stockdata, cls_fieldname = "c_name", prefix = ""):
    cls_stockdata_inner = cls_stockdata.copy()
    
    # 类别中的空值改为'unknown'
    for i in range(len(cls_stockdata_inner)):
        v = cls_stockdata_inner[cls_fieldname][i]
        if not (isinstance(v, str) or isinstance(v, unicode)):
            if np.isnan(cls_stockdata_inner[cls_fieldname][i]):
                cls_stockdata_inner[cls_fieldname][i] = 'unknown'
        
    # 获取分类名称
    clsnames = cls_stockdata_inner[cls_fieldname].unique()

    # 获取唯一的股票code
    codesuniq = cls_stockdata_inner['code'].unique()
    # 将唯一的股票code转成DataFrame
    df_uniqcode = DataFrame(data=codesuniq, columns=['code'])
    
    for cname in clsnames:
        # 获取原始数据中类别为cname的数据
        clslist = cls_stockdata_inner[cls_stockdata_inner[cls_fieldname] == cname]
        # 将提取出来的数据只保留code，将标题设置为类别名称，设置标志1
        if len(clslist) == 0:
            clsflags = None
        else:
            clsflags = zip(clslist['code'].values, [1]*len(clslist))
        # --debug
        #print prefix, cname
        clsjoin = DataFrame(data= clsflags, columns = ['code', prefix + "_" + cname])
        # 连接数据，则数据该分类的股票会被标记为1
        df_uniqcode = pd.merge(df_uniqcode, clsjoin, how='left', on = 'code', suffixes = ['',''])
        #print clslist
        #print clsjoin
    return df_uniqcode


def convert_timetomarket(str_time2m):
    try:
        time2m = time.strptime(str(str_time2m), "%Y%m%d")
    except Exception, e:
        print e
        time2m = time.strptime("19700101", "%Y%m%d")
    # 转换成pandas的datetime类型
    rtn_date = pd.datetime(time2m.tm_year, time2m.tm_mon, time2m.tm_mday)
    return rtn_date

#module test code
if __name__ == "__main__":

    #cls_orig = ts.get_industry_classified()
    cls_orig = ts.get_concept_classified()
    cls_trans = convert_cls_stockdata(cls_orig)
    print cls_trans
    cls_trans.to_csv('a.csv', encoding = 'utf-8')
    df_stock_list = ts.get_stock_basics()
    
    # random pick 50 stocks
    
    rand_stocks = random.sample(df_stock_list.index,50)
    rand_stock_ex = rand_stocks[:1]
    st_date = '2014-07-01'
            
    #stock_data_list = []
    stock_data_list = dld.get_stock_data(rand_stock_ex, st_date)
    
    idx_all = all_date_list(stock_data_list)
    
    stock_stats = []
    for stock_data in stock_data_list:
        stock_stats.append({})
        stock_stats[-1]['code'] = stock_data['code']
        stock_stats[-1]['avg_volt'], \
        stock_stats[-1]['rate_eachday'], \
        stock_stats[-1]['start'], \
        stock_stats[-1]['end'], \
        stock_stats[-1]['days_num']  = cal_volatility_stock(stock_data['data'], idx_all)
    print DataFrame(stock_stats)
    
    for row in stock_stats:
        print row['code'], df_stock_list.ix[row['code']]['name']
    
    
    dft = stock_data_list[0]['data'].reindex(idx_all)
    ser = dft['close'].apply(float)
    ser_deal = cutht_ser(ser)
    ser_deal = fill_avg_value(ser_deal)
    
    sert = Series([np.nan])
    print sert    
    print cutht_ser(sert)
    ''' no need, move to dldata
    # return a list
    def get_stock_data(stock_list, start_date):
        stock_data_list = []
        for stockcode in stock_list:
            stock_data_list.append({})
            stock_data_list[-1]['code'] = stockcode
            stock_data_list[-1]['data']=ts.get_h_data(code = stockcode, start = start_date).sort(ascending = True)
        return stock_data_list
    '''
    
    '''
    # no need
    i=0
    idx_interval = []
    idx_interval.append(i)
    while(i<len(df_stock_list)):
        i += 100
        idx_interval.append(i)
    idx_interval[-1] = len(df_stock_list) - 1
    idx_interval
    
    rand_idx = []
    for i in range(len(idx_interval)-1):
        rand_idx.append(random.randint(idx_interval[i],idx_interval[i+1] - 1))
    '''
    
    '''
    # no need
    #--debug need to change to rand_stocks
    for stockcode in rand_stock_ex:
        stock_data_list.append({})
        stock_data_list[-1]['code'] = stockcode
        stock_data_list[-1]['data']=ts.get_h_data(code = stockcode, start = st_date)
        #stock_data_list.append(ts.get_h_data(code = stockcode, start = st_date))
    
    '''
    
    '''
    # no need
    idx_all = stock_data_list[0]['data'].index
    for row in stock_data_list:
        data = row['data']
        idx_all = idx_all.join(data.index, how='outer')
    '''
    
    '''
    # no need
    idx_b = 0
    idx_e = len(ser)-1
    while((np.isnan(ser[idx_b])) and idx_b < len(ser)):
        idx_b += 1
    while((np.isnan(ser[idx_e])) and idx_e >= 0):
        idx_e -= 1
    ser_cutht = ser[idx_b : idx_e + 1]
    '''
    

    
    #avg_volt, rate_eachday = cal_volatility_stock(stock_data_list[0]['data'], idx_all)
    #print serc[np.isnan(serc)]
    

        