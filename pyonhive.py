#/usr/bin/env python
# -*- coding: UTF-8 -*-

''' this python file is going to excute the hive command'''
import os
import math
import random
import numpy as np
import pandas as pd

succ_rate_target = 0.98

method = {1:'CONNETC',
2:'HEAD',
3:'PUT',
4:'DELETE',
5:'POST',
6:'GET',
7:'TRACE',
8:'OPTIONS',
9:'PROPFIND',
10:'PROPPATCH',
11:'MKCOL',
12:'COPY',
13:'MOVE',
14:'LOCK',
15:'UNLOCK',
16:'PATCH',
17:'BPROPFIND',
18:'BPROPPATCH',
19:'BCOPY',
20:'BDELETE',
21:'BMOVE',
22:'NOTIFY',
23:'POLL',
24:'SEARCH',
25:'SUBSCRIBE',
26:'UNSUBSCRIBE',
27:'ORDERPATCH',
28:'ACL',
29:'UPDATE',
30:'MERGE'}

    
def clean_result_file(origin_file,dest_file):
    '''处理hive产生的结果，变成pandas可以读取的格式'''
    f1=open(origin_file)
    f2=open(dest_file,"w")
    line_length = 0
    
    while (line_length < 100):
        #skip the blank lines
        columnnames = f1.readline()
        line_length = len(columnnames)
    head10 = columnnames[0:10]
    seperate_count = columnnames.count(',')    
    print(columnnames, seperate_count)
    lines=0
    
    f2.write(columnnames)

    while True:
        try:
            line = f1.readline()
            lines = lines +1
            if not line :
                break
                #文件尾巴，跳出循环
            if line.count(',') != seperate_count:
                print (lines,line.count(','))
                print (line)
            elif len(line)>10 and line.find(head10) == -1 :
                f2.write(line)
        except:
            print("error")
            #读到异常的行
       
    f1.close()
    f2.close()
    
    
def ip_dec2dot(ip_dec):
    if  not np.isnan(ip_dec):
        #print(ip_dec,type(ip_dec)
        ip_dec = long(math.floor(ip_dec))
        ip_num1 = ip_dec // 256**3
        ip_num2 = (ip_dec % 256**3) // 256**2
        ip_num3 = (ip_dec % 256**2) // 256
        ip_num4 = ip_dec % 256
        ip_dot = "%s.%s.%s.%s" % (ip_num1,ip_num2,ip_num3,ip_num4)
        #print(ip_dot)
        return(ip_dot)
    else:
        return('none')
    
def ip_dot2dec(ip_dot):
    ip_num = ip_dot.split(".")
    ip_dec = 0
    for i in range(4):
        ip_dec =ip_dec * 256 + int(ip_num[i])
    #print(ip_dec)
    return(str(ip_dec))
    

def construct_groupby(groupby_col):
    return('group by %s' % groupby_col)

def conturct_sel_content(groupby_col, sum_col):
    return '%s %s' % (groupby_col, sum_col) 

def constuct_con(slicetime, cityname, other_cond=''):
    time_con =  "slicetime = '%s' " % (slicetime)
    city_con = "cityname = '%s'" % cityname
    if other_cond == '':
        cond = "%s and %s" % (city_con , time_con)
    else:
        cond = "%s and %s and %s" % (city_con , time_con, other_cond)
    print (cond)
    return cond

'''暂时不用
def hql_createtable(desttable, sourcetable, content='*', cond='', grouby ='' , limit=0):
    if limit<>0:
        limit_con = "limit %d" %limit 
        hql = "create table %s as select  %s from  %s  where  %s  and  %s %s; \n" % (desttable, content, sourcetable, city_con, time_con, limit_con)
    else:
        hql = "create table %s as select  %s from  %s  where  %s  and  %s ; \n" % (desttable, content, sourcetable, city_con, time_con)

    print (hql)
    return(hql)'''


def hql_selecttable(sourcetable, content='*', cond='', groupby='', orderby='', limit=0):
    if limit<>0:
        limit_con = "limit %d" %limit 
        hql = "select  %s  from  %s  where  %s %s %s %s; \n" % (content, sourcetable, cond, groupby, orderby, limit_con)
    else:
        hql = "select  %s  from  %s  where  %s %s %s; \n" % (content, sourcetable, cond, groupby, orderby )

    print (hql)
    return(hql)


def write_hql(hql, hql_filename):
    f=open(hql_filename,"w")
    try:
        f.write(hql)
    except:
        print("write error")
    finally:
        f.close()

def exec_hive(hql_file, result_file, op='a'):
    tmp_result = 'mytempfile1'
    tmp_merge = 'mytempfile2'
    cmd_del = 'rm %s' % tmp_result
    os.system(cmd_del)
    cmd_del = 'rm %s' % tmp_merge
    os.system(cmd_del)

    cmd_hive = 'beeline -u "jdbc:hive2://master03.cluster-b.gdyd.com:10000/default;principal=hive/_HOST@GDSAI.COM?tez.queue.name=xingneng_exam2017_001"  --outputformat=csv2 --silent=true -f %s >%s' % (hql_file, tmp_result)
    os.system(cmd_hive)

    if op == 'a':
        cmd_cat = 'cat %s %s > %s' % (result_file, tmp_result, tmp_merge)
        os.system(cmd_cat)
        cmd_del = 'rm %s' % result_file
        os.system(cmd_del)
        cmd_rename = 'mv %s %s' % (tmp_merge, result_file)
        os.system(cmd_rename)
    elif op == 'o':
        cmd_del = 'rm %s' % result_file
        os.system(cmd_del)
        cmd_rename = 'mv %s %s' % (tmp_result, result_file)
        os.system(cmd_rename)

'''暂时不用
def analyze_apptype_host(cities,times,apptype,appsubtype,hql_filename,result_filename):
    cmd_del = 'rm %s' % result_filename
    os.system(cmd_del)
    
    #paramenter for this inquirey
    sourcetable = "mlte_s1u_http"
    
    sum_col = ', COUNT(*) attemp, \
    SUM(if((HTTP_WAP_Affair_Status BETWEEN 1 and 399) and HTTP_1STACK_1STREQ_DELAY>0,1,0)) response, \
    SUM(if((HTTP_WAP_Affair_Status BETWEEN 1 and 399) and HTTP_1STACK_1STREQ_DELAY>0,1,0))/COUNT(*) resp_rate, \
    sum(dl_data) dl_vol, \
    sum(ul_data) ul_vol'

    groupby_col = 'cityname, slicetime, app_type, APP_SUB_TYPE, TRANSACTION_TYPE, host'
    groupby = construct_groupby(groupby_col)

    sel_content = conturct_sel_content(groupby_col, sum_col)

    other_cond = ' app_type=%s and  APP_SUB_TYPE=%s ' % (apptype,appsubtype)

    orderby = 'order by attemp desc'
 
    
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, sel_content, cond, groupby, orderby)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')

            
            
def analyze_apptype_uri(cities,times,apptype,appsubtype,hql_filename,result_filename):
    cmd_del = 'rm %s' % result_filename
    os.system(cmd_del)
    
    #paramenter for this inquirey
    sourcetable = "mlte_s1u_http"
    
    sum_col = ', COUNT(*) attemp, \
    SUM(if((HTTP_WAP_Affair_Status BETWEEN 1 and 399) and HTTP_1STACK_1STREQ_DELAY>0,1,0)) response, \
    SUM(if((HTTP_WAP_Affair_Status BETWEEN 1 and 399) and HTTP_1STACK_1STREQ_DELAY>0,1,0))/COUNT(*) resp_rate, \
    sum(dl_data) dl_vol, \
    sum(ul_data) ul_vol'
    #SUM(http_lastpacket_1streq_delay)/1000  time, \
    #SUM(dl_data)/SUM(http_lastpacket_1streq_delay)*8*1000/1024 speed'

    groupby_col = 'cityname, slicetime, app_type, APP_SUB_TYPE, TRANSACTION_TYPE, uri'
    groupby = construct_groupby(groupby_col)

    sel_content = conturct_sel_content(groupby_col, sum_col)

    other_cond = ' app_type=%s and  APP_SUB_TYPE=%s ' % (apptype,appsubtype)

    orderby = 'order by attemp desc'
    
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, sel_content, cond, groupby, orderby)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')


def analyze_speed_apptype_cell(cities,times,apptype,appsubtype,hql_filename='tmp_hql',result_filename='tmp_result',percentile='n'):
    #calculate speed every cell for specific app type
    cmd_del = 'rm %s' % result_filename
    os.system(cmd_del)
    
    #paramenter for this inquirey
    sourcetable = "mlte_s1u_http"
    
    sum_col = ', COUNT(*) attemp, \
    sum(dl_data) dl_vol, \
    sum(ul_data) ul_vol,\
    SUM(http_lastpacket_1streq_delay)/1000  time, \
    SUM(dl_data)/SUM(http_lastpacket_1streq_delay)*8*1000/1024 speed'
    if (percentile=='y' or percentil=='Y'):
        sum_col = sum_col + ', percentile_approx(dl_data/http_lastpacket_1streq_delay*8*1000/1024, array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9)) percentile '

    groupby_col = 'cityname, slicetime, app_type, APP_SUB_TYPE, TRANSACTION_TYPE, cell_id'
    groupby = construct_groupby(groupby_col)

    sel_content = conturct_sel_content(groupby_col, sum_col)

    other_cond = ' app_type=%s and  APP_SUB_TYPE=%s and dl_data >500000' % (apptype,appsubtype)

    orderby = 'order by attemp desc'
    
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, sel_content, cond, groupby, orderby)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')




    
def analyze_speed_ip_host(cities,times,ip_dec,apptype=0,appsubtype=0,hql_filename='tmp_hql',result_filename='tmp_result',percentile='n'):
    #specify the ip, caculate speed of all the apptype and host 
    cmd_del = 'rm %s' % result_filename
    os.system(cmd_del)
    
    #paramenter for this inquirey
    sourcetable = "mlte_s1u_http"
    
    sum_col = ', COUNT(*) attemp, \
    sum(dl_data) dl_vol, \
    sum(ul_data) ul_vol,\
    SUM(http_lastpacket_1streq_delay)/1000  time, \
    SUM(dl_data)/SUM(http_lastpacket_1streq_delay)*8*1000/1024 speed'
    if (percentile=='y' or percentile=='Y'):
        sum_col = sum_col + ', percentile_approx(dl_data/http_lastpacket_1streq_delay*8*1000/1024, array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9)) percentile '

   
    groupby_col = 'cityname, slicetime, app_server_ip_ipv4, app_type, APP_SUB_TYPE, host, TRANSACTION_TYPE '
    groupby = construct_groupby(groupby_col)

    sel_content = conturct_sel_content(groupby_col, sum_col)

    if apptype == 0:
        other_cond = " dl_data >500000 and app_server_ip_ipv4 = '%s'" %  ip_dec
    elif appsubtype == 0:
        other_cond = " app_type=%s and dl_data >500000 and app_server_ip_ipv4 = '%s'" % (apptype ,ip_dec)
    else:
        other_cond = " app_type=%s and  APP_SUB_TYPE=%s and dl_data >500000 and app_server_ip_ipv4 = '%s'" % (apptype,appsubtype,ip_dec)

    orderby = 'order by attemp desc'
    
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, sel_content, cond, groupby, orderby)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')
            
'''            

def analyze_dl_speed_apptype(cities,times,apptype=0,appsubtype=0,result_dir='dl_speed',exclude_zerowin='y',percentile='n'):
    #paramenter for this inquirey
    hql_filename = result_dir + 'dl_speed.hql'
    result_filename = result_dir + 's10s11xdr.csv'
    sourcetable = "mlte_s1u_http"
    
    sum_col = ', COUNT(*) attemp, \
    sum(dl_data) dl_vol, \
    sum(ul_data) ul_vol,\
    SUM(http_lastpacket_1streq_delay)/1000  time, \
    SUM(dl_data)/SUM(http_lastpacket_1streq_delay)*8*1000/1024 speed'
    if (percentile=='y' or percentile=='Y'):
        sum_col = sum_col + ', percentile_approx(dl_data/http_lastpacket_1streq_delay*8*1000/1024, array(0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9)) percentile '
 

    if apptype == 0:
        #all the app_type , no app_sub_type
        other_cond = ' dl_data >500000' 
        groupby_col = 'cityname, slicetime, app_type, TRANSACTION_TYPE '
        
    elif appsubtype == 0:
        #specify the app_type , all the app_sub_type
        other_cond = ' app_type=%s and dl_data >500000' % apptype
        groupby_col = 'cityname, slicetime, app_type, APP_SUB_TYPE, TRANSACTION_TYPE '
        
    else:
        other_cond = ' app_type=%s and  APP_SUB_TYPE=%s and dl_data >500000' % (apptype,appsubtype)
        groupby_col = 'cityname, slicetime, app_type, APP_SUB_TYPE, TRANSACTION_TYPE '

    if exclude_zerowin == 'y' or exclude_zerowin == 'Y':
        other_cond = other_cond + ' and ulzerowindowtimes = 0 '
        
    orderby = 'order by attemp desc'
    groupby = construct_groupby(groupby_col)

    sel_content = conturct_sel_content(groupby_col, sum_col)
    
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, sel_content, cond, groupby, orderby)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')
            
            
            
def inquire_ip2domain(cities,times,ip,hql_filename='tmp_dns_hql',result_filename='domainname.csv'):
    '''inquire domain name for specific ip'''
    cmd_del = 'rm %s' % result_filename
    os.system(cmd_del)
    
    #paramenter for this inquirey
    sourcetable = 'mlte_s1u_dns'
    sel_content = 'cityname, slicetime,apn,request_query_domain,query_result_ip'
    other_cond = 'query_result_ip like "%s"' % ip 
 
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, sel_content, cond)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')
            
def inquire_domain2ip(cities,times,domain,hql_filename='tmp_dns_hql',result_filename='ip.csv'):
    '''inquire ip for specific domain name'''
    cmd_del = 'rm %s' % result_filename
    os.system(cmd_del)
    
    #paramenter for this inquirey
    sourcetable = 'mlte_s1u_dns'
    sel_content = 'cityname, slicetime,apn,request_query_domain,query_result_ip'
    other_cond = 'request_query_domain like "%s"' % domain 
 
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, sel_content, cond)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')
            

def collect_domainname(cities,times,hql_filename='tmp_dns_hql',result_filename='domainname.csv'):
    '''collect domain name form dns table'''
    cmd_del = 'rm %s' % result_filename
    os.system(cmd_del)
    
    #paramenter for this inquirey
    sourcetable = 'mlte_s1u_dns'
    sel_content = 'cityname, slicetime,apn,request_query_domain,query_result_ip'
 
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname)
            hql = hql_selecttable(sourcetable, sel_content, cond)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')


def collect_http_xdr(cities,times,other_cond='',content='*',hql_filename='httpxdr_hql',result_filename='httpxrd_result',xdrlimit=20000):
    '''collect http xdr''' 
    cmd_del = 'rm %s' % result_filename
    os.system(cmd_del)
   
    #paramenter for this inquirey
    sourcetable = "mlte_s1u_http"

   
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, content=content, cond=cond, groupby='', orderby='', limit=xdrlimit)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')


def collect_s10s11_xdr(cities,times,other_cond='',content='*',hql_filename='s10s11_hql',result_filename='s10s11xdr_result',xdrlimit=20000):
    '''collect http xdr''' 
    cmd_del = 'rm %s' % result_filename
    os.system(cmd_del)
   
    #paramenter for this inquirey
    sourcetable = "mlte_s10s11"

   
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, content=content, cond=cond, groupby='', orderby='', limit=xdrlimit)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')

    
    
def df4analysis(df):
    '''add extra column for rate analysis'''
    df['ip']=df['app_server_ip_ipv4'].map(ip_dec2dot)
    df['http_method'] = df['transaction_type'].map(method.get)
    #print(df['ip'].head(100))

    #default = unsuccessful
    df['respones_success'] = 0
    #find the successful xdr
    df.loc[ (df['http_wap_affair_status']>0) & (df['http_wap_affair_status'] <399) & (df['http_1stack_1streq_delay']>0) ,['respones_success']] =1

    df['dl_data_log'] = np.exp2(np.clip(np.floor(np.log2(df['dl_data']/500000)),0,None))*500000
    #normal speed expression
    df['speed'] = df['dl_data'] * 8 / df['http_lastpacket_1streq_delay']
    df['speed_log'] = np.exp2(np.clip(np.floor(np.log2(df['speed']/10)),0,None))*10
    #special speed expression, denumerator is last ack
    df['speed_ack'] = df['dl_data'] * 8 / df['http_lastack_delay']
    df['speed_ack_log'] = np.exp2(np.clip(np.floor(np.log2(df['speed_ack']/10)),0,None))*10
    return df
    

def dice4responserate(df,rows,title,savefile='response_time.csv'):
    global succ_rate_target
    grouped_data=df.groupby(rows)
    attempt = grouped_data['respones_success'].count()
    success = grouped_data['respones_success'].sum()
    #print(attempt)
    #print(success)
    attempt_total = attempt.sum()
    success_total = success.sum()
    attempt_portion = attempt / attempt_total
    
    newdf=pd.DataFrame()
    newdf['attempt'] = attempt
    newdf['success'] = success
    newdf['succ_rate'] = success / attempt
    succ_rate_factor = (success / attempt - succ_rate_target) * attempt_portion
    newdf['succ_rate_factor'] = succ_rate_factor

    
    #newdf = newdf.sort_values(by='factor')
    #print(newdf)
    newdf.to_csv(savefile+'.csv')
    
def dice4responsetime(df,rows,title,savefile='response_time.csv',sample_num=0, factor_above=5):
    '''delete all xdr of which response time <=0, becuase otherwise it would mislead.'''
    df = df[df['http_1stack_1streq_delay']>0]
    
    grouped_data=df.groupby(rows)
    attempt = grouped_data['respones_success'].count()
    attempt_total = attempt.sum()
    attempt_portion = attempt / attempt_total
    respose_time_average = df['http_1stack_1streq_delay'].sum() / attempt_total
    
    newdf=pd.DataFrame()
    newdf['attempt'] = attempt
    newdf['resp_time_mean'] = grouped_data['http_1stack_1streq_delay'].mean()
    newdf['resp_time_median'] = grouped_data['http_1stack_1streq_delay'].median()
    newdf['resp_time_factor'] = (newdf['resp_time_mean'] - respose_time_average) * attempt_portion
    
    #newdf = newdf.sort_values(by='factor')
    #print(newdf)
    newdf.to_csv(savefile+'.csv')
    
    if not sample_num == 0:
        #select the worst case
        worst_df = newdf[newdf['resp_time_factor'] > factor_above]
        #change dimension back from index back to normal column
        worst_df = worst_df.reset_index()
        
        for index, wrostdf_row in worst_df.iterrows():
            smple_df = df
            #filter all xdr meet the citeria with one case.
            for rowname in rows:
                row_content = wrostdf_row[rowname]
                print('rowname',rowname,'content',row_content)
                smple_df = smple_df[smple_df.loc[:,rowname]==row_content]
                
            if len(smple_df) > sample_num:
                smple_df = smple_df.sample(n=sample_num)
            samplefile = savefile + '_sample' + str(index) + '.csv'
            smple_df.to_csv(samplefile)


            
def dice4speed(df,rows,title,savefile='csvfile.csv',picfile='savefile.jpg', sample_num=100, factor_below=-10,inclu_zero_win=False):
    #if factor_below == 0 , no sample file will save.
    
    print('dice4speed',rows,title,savefile,picfile, sample_num, factor_below,inclu_zero_win)
    
    #del all the xdr whose response time = 0  and dl_data < 500k
    df = df[df['http_lastpacket_1streq_delay']>0]
    df = df[df['dl_data']> 500000]
    
    #get rid of the xdrs with 0 win , if inclu_zero_win == False 
    if not inclu_zero_win:
        df = df[(df['ulzerowindowtimes']==0) & (df['ulzerowindowtotaltime']==0)]
        
    grouped_data=df.groupby(rows)
    attempt = grouped_data['respones_success'].count()
    mysum = grouped_data.sum()
    sum_speed = mysum['dl_data'] * 8 / mysum['http_lastpacket_1streq_delay']
    dl_data_sum = mysum['dl_data']
    time_sum = mysum['http_lastpacket_1streq_delay'] 
    dl_data_total = dl_data_sum.sum()
    dl_data_portion = dl_data_sum / dl_data_total
    time_total = time_sum.sum()
    time_portion = time_sum / time_total
    speed_average = dl_data_total * 8 / time_total
        
    newdf=pd.DataFrame()
    newdf['attempt'] = attempt
    newdf['speed.1'] = grouped_data['speed'].quantile(.1)
    newdf['speed.25'] = grouped_data['speed'].quantile(.25)
    newdf['speed.5'] = grouped_data['speed'].quantile(.5)
    newdf['speed.75'] = grouped_data['speed'].quantile(.75)
    newdf['speed.9'] = grouped_data['speed'].quantile(.9)
    newdf['speed_normal_kbps'] = sum_speed
    newdf['speed_ack_normal_kbps'] = mysum['dl_data'] * 8 / mysum['http_lastack_delay']
    newdf['dl_data_kbyte'] = dl_data_sum / 1000
    newdf['dl_time_sec'] = time_sum / 1000
    newdf['dl_data_portion'] = dl_data_portion
    newdf['time_portion'] = time_portion
    newdf['speed_factor'] = (sum_speed - speed_average ) * time_portion
    newdf['dl_data_per_session'] = dl_data_sum / 1000 / attempt
    
    newdf.to_csv(savefile+'.csv')
    
    if not sample_num == 0:
        #select the worst case
        worst_df = newdf[newdf['speed_factor'] < factor_below]
        #change dimension back from index back to normal column
        worst_df = worst_df.reset_index()
        
        for index, wrostdf_row in worst_df.iterrows():
            smple_df = df
            #filter all xdr meet the citeria with one case.
            for rowname in rows:
                row_content = wrostdf_row[rowname]
                print('rowname',rowname,'content',row_content)
                smple_df = smple_df[smple_df.loc[:,rowname]==row_content]
                
            if len(smple_df) > sample_num:
                smple_df = smple_df.sample(n=sample_num)
            samplefile = savefile + '_sample' + str(index) + '.csv'
            smple_df.to_csv(samplefile)
            
            
def histo_data_speed(df,title,savefile='csvfile.csv',picfile='savefile.jpg',inclu_zero_win=False):
    #del all the xdr whose response time = 0  and dl_data < 500k
    #if factor_below == 0 , no sample file will save.
    print('histo_data_speed',title,savefile,picfile)
    
    df = df[df['http_lastpacket_1streq_delay']>0]
    df = df[df['dl_data']> 500000]

    #get rid of the xdrs with 0 win , if inclu_zero_win == False 
    if not inclu_zero_win:
        df = df[(df['ulzerowindowtimes']==0) & (df['ulzerowindowtotaltime']==0)]
        
    #normal speed
    newdf=pd.crosstab(df['dl_data_log'],df['speed_log'], margins=True)
    print(newdf)
    newdf.to_csv(savefile+'.csv')

    #special speed with ack
    newdf=pd.crosstab(df['dl_data_log'],df['speed_ack_log'], margins=True)
    print(newdf)
    newdf.to_csv(savefile+'_ack.csv')    
    
def http_analysis(app_type,app_sub_type,cities,times,result_dir,xdrlimit=30000,sample_num=1000,resp_time_factor_above=5,speed_factor_below=-10):
    print('http_analysis',app_type,app_sub_type,cities,times,result_dir,xdrlimit,sample_num,resp_time_factor_above,speed_factor_below)
    #xdrlimit=0, mean no xdr limit
    #sample_num=0, mean no sample xdr at all
    hql_filename = result_dir + 'httpxdr.hql'
    result_filename = result_dir + 'httpxdr.csv'
    origin_file = result_dir + 'httpxdr.csv'
    dest_file = result_dir + 'httpxdr-adjust.csv'

    other_cond = " app_type=%s and  APP_SUB_TYPE=%s " % (app_type,app_sub_type)
    content = ' report_time, imei, sgw_ggsn_ip_add, enb_sgsn_ip_add, cell_id, apn, app_type_code, procedure_starttime, procedure_endtime, protocol_type, app_type, app_sub_type, app_content, app_status, user_ipv4, app_server_ip_ipv4, user_ipv6, app_server_ip_ipv6, app_server_port, ul_data, dl_data, ul_ip_packet, dl_ip_packet, down_tcp_disorder_num, down_tcp_retrans_num, tcp_creactlink_response_delay, tcp_creactlink_confirm_delay, tcp_1strequest_delay, tcp_1streuest_response_delay,  tcp_window_size, session_finish_indicator,  dlrtttotaldelay /dlrtttimes as dl_rtt , dlzerowindowtimes, dlzerowindowtotaltime, ulzerowindowtimes, ulzerowindowtotaltime, sessionresetindication, sessionresetdirection, down_tcp_lost_num, transaction_type, http_wap_affair_status, http_1stack_1streq_delay, http_lastpacket_1streq_delay, http_lastack_delay, host, uri, xonlinehost, refer_uri, user_agent, dl_rate_data, http_host, http_x_online_host, cityname, slicetime'
    #collect_http_xdr(cities,times,other_cond,content,hql_filename,result_filename,xdrlimit=xdrlimit)

    #use only at the first time 
    
    #clean_result_file(origin_file,dest_file)
    
    #get xdr
    df = pd.read_csv(dest_file,sep=',',header=0)
    print(df.columns)
    df_analysis = df4analysis(df)
    
    #rate_analysis
    dice4responserate(df_analysis,['slicetime','http_method'],'http method',savefile=result_dir+'rate_by_method')
    dice4responserate(df_analysis,['slicetime','http_method','http_wap_affair_status'],'http method and error code',savefile=result_dir+'rate_by_method_errcode')
    dice4responserate(df_analysis,['slicetime','cityname','http_method'],'city and http method',savefile=result_dir+'rate_by_city_method')
    dice4responserate(df_analysis,['slicetime','sgw_ggsn_ip_add','http_method'],'sgw and http method',savefile=result_dir+'rate_by_sgw_method')
    dice4responserate(df_analysis,['slicetime','sgw_ggsn_ip_add','cityname','http_method'],'sgw ,city and http method',savefile=result_dir+'rate_by_sgw_city_method')
    dice4responserate(df_analysis,['slicetime','host','http_method'],'host and http method',savefile=result_dir+'rate_by_host_method')
    dice4responserate(df_analysis,['slicetime','ip','http_method'],'ip and http method',savefile=result_dir+'rate_by_ip_method')

    #respose_time_analysis
    dice4responsetime(df_analysis,['slicetime','http_method'],'http method',savefile=result_dir+'resp_time_by_method',sample_num=sample_num)
    dice4responsetime(df_analysis,['slicetime','cityname','http_method'],'city and http method',savefile=result_dir+'resp_time_by_city_method',sample_num=sample_num)
    dice4responsetime(df_analysis,['slicetime','sgw_ggsn_ip_add','http_method'],'sgw and http method',savefile=result_dir+'resp_time_by_sgw_method',sample_num=sample_num)
    dice4responsetime(df_analysis,['slicetime','sgw_ggsn_ip_add','cityname','http_method'],'sgw, city and http method',savefile=result_dir+'resp_time_by_sgw_city_method',sample_num=sample_num)
    dice4responsetime(df_analysis,['slicetime','host','http_method'],'host and http method',savefile=result_dir+'resp_time_by_host_method',sample_num=sample_num,factor_above=resp_time_factor_above)
    #dice4responsetime(df_analysis,['host','uri','http_method'],'host, uri and http method',savefile=result_dir+'resp_time_by_host_uri_method')
    dice4responsetime(df_analysis,['slicetime','ip','http_method'],'ip and http method',savefile=result_dir+'resp_time_by_ip_method',sample_num=sample_num,factor_above=resp_time_factor_above) 

    #speed_analysis_no0win
    dice4speed(df_analysis,['slicetime','http_method'],'http method',savefile=result_dir+'speed_by_method_no0win', sample_num=0,inclu_zero_win=False)
    dice4speed(df_analysis,['slicetime','cityname','http_method'],'city and http method',savefile=result_dir+'speed_by_city_method_no0win',sample_num=sample_num,inclu_zero_win=False)
    dice4speed(df_analysis,['slicetime','host','http_method'],'host and http method',savefile=result_dir+'speed_by_host_method_no0win', sample_num=sample_num,inclu_zero_win=False)
    dice4speed(df_analysis,['slicetime','ip','http_method'],'ip and http method',savefile=result_dir+'speed_by_ip_method_no0win', sample_num=sample_num,inclu_zero_win=False)
    dice4speed(df_analysis,['slicetime','host','ip','http_method'],'host, ip and http method',savefile=result_dir+'speed_by_host_ip_method_no0win',sample_num=sample_num,factor_below=speed_factor_below,inclu_zero_win=False) 

    histo_data_speed(df_analysis,'data_log and speed_log',savefile=result_dir+'histo_by_datalog_and_speedlog_no0win',picfile='savefile.jpg',inclu_zero_win=False)
    
    #speed_analysis
    dice4speed(df_analysis,['slicetime','http_method'],'http method',savefile=result_dir+'speed_by_method', sample_num=0,inclu_zero_win=True)
    dice4speed(df_analysis,['slicetime','cityname','http_method'],'city and http method',savefile=result_dir+'speed_by_city_method',sample_num=0,inclu_zero_win=True)
    dice4speed(df_analysis,['slicetime','host','http_method'],'host and http method',savefile=result_dir+'speed_by_host_method', sample_num=0,inclu_zero_win=True)
    dice4speed(df_analysis,['slicetime','ip','http_method'],'ip and http method',savefile=result_dir+'speed_by_ip_method', sample_num=0,inclu_zero_win=True)
    dice4speed(df_analysis,['slicetime','host','ip','http_method'],'host, ip and http method',savefile=result_dir+'speed_by_host_ip_method',sample_num=sample_num,factor_below=speed_factor_below,inclu_zero_win=True) 

    histo_data_speed(df_analysis,'data_log and speed_log',savefile=result_dir+'histo_by_datalog_and_speedlog',picfile='savefile.jpg',inclu_zero_win=True)
 
    
def ambr_analysis(cities,times,result_dir,sample_num=100):
    hql_filename = result_dir + 's10s11xdr.hql'
    result_filename = result_dir + 's10s11xdr.csv'
    origin_file = result_dir + 's10s11xdr.csv'
    dest_file = result_dir + 's10s11xdr-adjust.csv'
    total_imsis_file = result_dir +'total_imsis.csv'
    restrict_imsis_file = result_dir +'restrict_imsis.csv'
    
    other_cond = " APN_AMBR_DL > 0 and  APN_AMBR_UL > 0 " 
    content = " rat,imsi,imei,procedure_type,procedure_starttime,procedure_endtime,procedure_status,failure_cause,request_cause,user_ipv4,user_ipv6,mme_address,sgw_old_mme_address,cell_id,qci_0,apn_ambr_ul,apn_ambr_dl, cityname, slicetime "
 
    #xdrlimit=0, collect all xdr
    collect_s10s11_xdr(cities,times,other_cond,content,hql_filename,result_filename,xdrlimit=0)

    #use only at the first time
    clean_result_file(origin_file,dest_file)
    
    
    #get xdr
    df = pd.read_csv(dest_file,sep=',',header=0)
    print(df.columns)
    #df_analysis = df4analysis(df)
    #s10s11 doesnt need to add any new column
    
    df = df[(df['apn_ambr_ul'] >0) | (df['apn_ambr_dl'] >0)]
    #filter out the xdr with AMBR, in practice It is useless

    #df.drop_duplicates(['IMSI'],inplace=True)
    #CANCEL, USE ANOTHER WAY TO DO SO!

    grouped_data=df.groupby(['imsi','apn_ambr_dl'])
    newdf = grouped_data['imei'].count()
    newdf = newdf.reset_index()
    
    findf = newdf.pivot(index='imsi', columns='apn_ambr_dl', values='imei')
    total_num = len(findf)
    total_imsis = findf.reset_index()
    total_imsis.to_csv('total_imsis.csv')
    
    restrict_df = newdf[newdf['apn_ambr_dl']< 1050].pivot(index='imsi', columns='apn_ambr_dl', values='imei')
    restrict_num = len(restrict_df)
    restrict_imsis = restrict_df.reset_index()
    restrict_imsis.to_csv(restrict_imsis_file)
    
    df_1048 = df[df['apn_ambr_ul'] == 1048]
    df_1 = df_1048[df_1048['procedure_type'] == 1]
    df_8 = df_1048[df_1048['procedure_type'] == 8]
    imsi_1 = df_1['imsi'].drop_duplicates()
    imsi_8 = df_8['imsi'].drop_duplicates()
    imsi_1048 = df_1048['imsi'].drop_duplicates()
    
    grouped_1048=df_1048.groupby(['procedure_type'])
    newdf_1048 = grouped_1048['imei'].count()
    #'imsi',

def timeseries(startdate,noofdays,starttime=0,noofhours=24):
    #example startdate = '20190101'
    #example starthour = 0 
    mydatetime =[] 
    for myday in range(noofdays):
        for myhour in range(noofhours):
            dateandtime = str(myday + startdate) + str(starttime + myhour).zfill(2)
            mydatetime.append(dateandtime)
    print(mydatetime)
    return mydatetime
