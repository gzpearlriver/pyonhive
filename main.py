#/usr/bin/env python
# -*- coding: UTF-8 -*-

from pyonhive import * 

allcities =['GZ','SZ','DG','FS','QY','SG','ST','CZ','JY','HZ','SW','HY','YF','MZ','JM','ZS','ZH','ZQ','YJ','MM','ZJ']
cities  = ['QY','MM']
times = ['2018102022']
apptype =5
appsubtype =17
hql_filename = 'select_table.hql'
result_filename = 'select_result.csv'
#analyze_apptype_host(cities,times,apptype,appsubtype,hql_filename,result_filename)

cities  = ['QY','MM']
times = ['2018102022']
apptype =5
appsubtype =17
hql_filename = 'select_table.hql'
result_filename = 'select_result.csv'
#analyze_apptype_uri(cities,times,apptype,appsubtype,hql_filename,result_filename)

cities  = ['MM']
times = ['2018102022']
apptype =5
appsubtype =17
hql_filename = 'select_table.hql'
result_filename = 'cell_speed.csv'
#analyze_speed_apptype_cell(cities,times,apptype,appsubtype,hql_filename,result_filename)


cities  = ['MM','SG']
times = ['2018102022']
apptype =0   #all
appsubtype =0  #all
hql_filename = 'select_table.hql'
result_filename = 'apptype_speeed.csv'
#analyze_speed_apptype(cities,times,apptype,appsubtype,hql_filename,result_filename,percentile='y')

cities  = ['MM']
times = ['2018102322']
hql_filename = 'select_table.hql'
result_filename = 'dnsxdr.csv'
#collect_domainname(cities,times,hql_filename,result_filename)

cities  = ['MM']
times = ['2018102322']
hql_filename = 'select_table.hql'
result_filename = 'dnsxdr.csv'
#collect_domainname(cities,times,hql_filename,result_filename)
ip = '106.75.26.39'
#inquire_ip2domain(cities,times,ip)

cities  = ['MM']
times = ['2018102322']
hql_filename = 'select_table.hql'
result_filename = 'dnsxdr.csv'
domain = 'www.baidu.com'
#inquire_domain2ip(cities,times,domain)

cities  = ['MM']
times = ['2018102822']
hql_filename = 'select_table.hql'
result_filename = 'ip_speed_app_host.csv'
ip_dec = ip_dot2dec('223.111.15.202')
#analyze_speed_ip_host(cities,times,ip_dec)


#手机HTTP表的XDR
cities  = allcities
times = ['2018102822']
hql_filename = 'httpxdr.hql'
result_filename = 'httpxrd_result.csv'
#ip_dec = ip_dot2dec('223.111.15.202')
#other_cond = " app_server_ip_ipv4 = '%s'" % ip_dec
other_cond = " app_type=%s and  APP_SUB_TYPE=%s and dl_data >500000 " % (6,1)
content = ' report_time, imei, sgw_ggsn_ip_add, enb_sgsn_ip_add, cell_id, apn, app_type_code, procedure_starttime, procedure_endtime, protocol_type, app_type, app_sub_type, app_content, app_status, user_ipv4, app_server_ip_ipv4, ul_data, dl_data, ul_ip_packet, dl_ip_packet, down_tcp_disorder_num, down_tcp_retrans_num, tcp_creactlink_response_delay, tcp_creactlink_confirm_delay, tcp_1strequest_delay, tcp_1streuest_response_delay,  tcp_window_size, session_finish_indicator,  dlrtttotaldelay /dlrtttimes as dl_rtt , dlzerowindowtimes, dlzerowindowtotaltime, ulzerowindowtimes, ulzerowindowtotaltime, sessionresetindication, sessionresetdirection, down_tcp_lost_num, transaction_type, http_wap_affair_status, http_1stack_1streq_delay, http_lastpacket_1streq_delay, http_lastack_delay, host, uri, xonlinehost, user_agent, dl_rate_data, http_host, http_x_online_host, cityname, slicetime'
#collect_http_xdr(cities,times,other_cond,content,hql_filename,result_filename,xdrlimit=0)


#AMBR限速用户数量分析
cities  = ['MM']
times =timeseries(20190110,1,starttime=0,noofhours=24)
result_dir = 'MM_AMBR/'
#REQUIRE ALL THE XDR
#ambr_analysis(cities,times,result_dir,sample_num=0)


#hive 速率分析，对比剔除零窗口前后
cities  = ['MM']
times = ['2019011622']

result_dir = 'mm_dl_speed_nozero/'
analyze_dl_speed_apptype(cities,times,apptype=0,appsubtype=0,result_dir=result_dir,exclude_zerowin='y',percentile='n')
result_dir = 'mm_dl_speed_withzero/'
analyze_dl_speed_apptype(cities,times,apptype=0,appsubtype=0,result_dir=result_dir,exclude_zerowin='n',percentile='n')

#http 速率多维分析，吃鸡
cities  = ['MM']
times = ['2019011722']
result_dir = 'GZ_APPSTORE/'
app_type = 5
app_sub_type = 2
http_analysis(app_type,app_sub_type,cities,times,result_dir,xdrlimit=0,sample_num=1000,resp_time_factor_above=5,speed_factor_below=-10)
