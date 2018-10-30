''' this python file is going to excute the hive command'''
import os

def ip_dec2dot(ip_dec):
    ip_dec = long(ip_dec)
    ip_num1 = ip_dec // 256**3
    ip_num2 = (ip_dec % 256**3) // 256**2
    ip_num3 = (ip_dec % 256**2) // 256
    ip_num4 = ip_dec % 256
    ip_dot = "%s.%s.%s.%s" % (ip_num1,ip_num2,ip_num3,ip_num4)
    print(ip_dot)
    return(ip_dot)

    
def ip_dot2dec(ip_dot):
    ip_num = ip_dot.split(".")
    ip_dec = 0
    for i in range(4):
        ip_dec =ip_dec * 256 + int(ip_num[i])
    print(ip_dec)
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

'''def hql_createtable(desttable, sourcetable, content='*', cond='', grouby ='' , limit=0):
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

    cmd_hive = 'beeline -u "jdbc:hive2://master03.cluster-b.gdyd.com:10000/default;principal=hive/_HOST@GDSAI.COM?tez.queue.name=xingneng_exam2017_001"  --outputformat=dsv --silent=true -f %s >%s' % (hql_file, tmp_result)
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

def analyze_speed_apptype(cities,times,apptype=0,appsubtype=0,hql_filename='tmp_hql',result_filename='tmp_result',percentile='n'):
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

   
    groupby_col = 'cityname, slicetime, app_type, APP_SUB_TYPE, TRANSACTION_TYPE '
    groupby = construct_groupby(groupby_col)

    sel_content = conturct_sel_content(groupby_col, sum_col)

    if apptype == 0:
        other_cond = ' dl_data >500000' 
    elif appsubtype == 0:
        other_cond = ' app_type=%s and dl_data >500000' % apptype
    else:
        other_cond = ' app_type=%s and  APP_SUB_TYPE=%s and dl_data >500000' % (apptype,appsubtype)

    orderby = 'order by attemp desc'
    
    for cityname in cities:
        for slicetime in times:
            cond = constuct_con(slicetime, cityname, other_cond)
            hql = hql_selecttable(sourcetable, sel_content, cond, groupby, orderby)
            write_hql(hql, hql_filename)
            exec_hive(hql_filename, result_filename, op='a')


    
def analyze_speed_ip_host(cities,times,ip_dec,apptype=0,appsubtype=0,hql_filename='tmp_hql',result_filename='tmp_result',percentile='n'):
    #specify the ip, collect speed of all the apptype and host 
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
            
def inquire_ip2domain(cities,times,ip,hql_filename='tmp_dns_hql',result_filename='domainname.csv'):
    '''collect domain name form dns table'''
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
    '''collect domain name form dns table'''
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
            
