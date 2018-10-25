import pyoverhive as poh

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
poh.inquire_ip2domain(cities,times,ip)

cities  = ['MM']
times = ['2018102322']
hql_filename = 'select_table.hql'
result_filename = 'dnsxdr.csv'
domain = 'www.baidu.com'
poh.inquire_domain2ip(cities,times,domain)

