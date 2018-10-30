from hql import * 

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
analyze_speed_ip_host(cities,times,ip_dec)

cities  = ['MM']
times = ['2018102822']
hql_filename = 'httpxdr.hql'
result_filename = 'httpxrd_result.csv'
ip_dec = ip_dot2dec('223.111.15.202')
other_cond = " app_server_ip_ipv4 = '%s'" % ip_dec
#collect_http_xdr(cities,times,other_cond,hql_filename,result_filename,xdrlimit=10000)
