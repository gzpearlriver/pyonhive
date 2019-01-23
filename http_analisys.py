from pyonhive import * 

allcities =['GZ','SZ','DG','FS','QY','SG','ST','CZ','JY','HZ','SW','HY','YF','MZ','JM','ZS','ZH','ZQ','YJ','MM','ZJ']

    



########
#apptype
########
cities  = ['FS','GZ']
times = ['20181201222']

home_dir ='/myhome/xingneng_exam2017_001/'
result_dir ='/myhome/xingneng_exam2017_001/video/'
http_analysis(5,3,cities,times,result_dir,sample_num=100,resp_time_factor_above=5,speed_factor_below=-10)
#app_type 大类业务
#app_sub_type 小类业务
#result_dir 输出目录
#speed_factor_below 下载速率质差门槛
#sample_num 样本文件的最大行数

