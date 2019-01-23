

# 背景
目前LTE的统计来源包括SOC和HIVE。其中SOC是面对全网的固定报表，受计算力限制，不提供Host、IP等小维度的统计。而HIVE可以实现多维度统计，但由于HIVE没有存储过程，无法实现程序化，人力投入成本大；HIVE无法输出图形和抽样数据，灵活度仍有欠缺。

PYONHIVE的工作方式为通过HIVE指令（BEELINE-CLI）输出指定范围的XDR到接口机，在接口机运行PYTHON/PANDAS实现多维聚类运算，并输出数据结果和质差样本XDR抽样，全程一键自动实现。

# 特点

0. 一键输出
当然，你还是需要历尽千辛万苦登陆共享层的接口机，做报告的话还要千辛万苦SFTP文件到本地电脑。

1. 快速扩展
copy+paste就可以增加新的维度组合输出；

2. 区分HTTP方法
拨测验证显示get和post的响应成功率和响应时长代表是完全不一样的内容，瀚信正在制作两者对比的详细报告。本程序缺省区分HTTP方法。

3. Factor值
Factor值反映加权偏离度，TopN即为质差组合。

4. 质差样本抽样
对特定维度组合的分析结果开放质差样品抽样，提供第一手的信息。

# 分析过程
1. 提取XDR
按照指定的大类业务、小类业务、地市和时间，从hive提取XDR，以CSV格式存储到共享层接口机上。

2. 数据分析
在接口机上运行python程序，进行多维度聚合，计算以下指标的数值，并提取质差

-  htttp响应应成功率
针对所有XDR，分析维度包括：http方法、错误码、地市、host、ip

-  http响应时延
只针对响应时延大于0的XDR，分析维度包括：http方法、地市、host、ip

-  http下载速率
只针对下载流量大于500Kbyte而且响应时延大于0的XDR，分析维度包括：http方法、地市、host、ip


# 使用方法

修改http_analysis.py文件的内容

```python
from pyonhive import * 
allcities =['GZ','SZ','DG','FS','QY','SG','ST','CZ','JY','HZ','SW','HY','YF','MZ','JM','ZS','ZH','ZQ','YJ','MM','ZJ']

cities  = ['FS']
#指定城市
#cities  = allcities 这样就包括全省

times = ['2018120422']
#指定时间
#times = ['2018120422'，'2018120522'] #这样包括多个时间


home_dir ='/myhome/xingneng_exam2017_001/'
result_dir ='/myhome/xingneng_exam2017_001/result/'
#指定工作目录

http_analysis(5,2,result_dir,-10,100)
#def http_analysis(app_type,app_sub_type,result_dir,speed_factor_below,sample_num):
#app_type 大类业务
#app_sub_type 小类业务
#result_dir 输出目录
#speed_factor_below 下载速率质差门槛
#sample_num 样本文件的最大行数

```

执行http_analysis.py
```bash
python http_analysis.py
```

# 输出结果

### http响应成功率
成功率的Factor表示本切片距离目标值贡献的差异程度，最大负值为最差切片。

- rate_by_method.csv
按Http方法维度聚类统计响应成功率，

- rate_by_method_errcode.csv
按Http方法和错误码维度聚类统计响应成功率

- rate_by_city_method.csv
按Http方法和城市维度聚类统计响应成功率

- rate_by_host_method.csv
按Http方法和host维度聚类统计响应成功率

- rate_by_ip_method.csv
按Http方法和ip维度聚类统计响应成功率

### http响应时延
响应时延的Factor表示本切片距离总体平均值贡献的差异程度，最大正值为最差切片。

- resp_time_by_method.csv
按Http方法维度聚类统计响应成功率

- resp_time_by_city_method.csv
按Http方法和城市维度聚类统计响应成功率

- resp_time_by_host_method.csv
按Http方法和host维度聚类统计响应成功率

- resp_time_by_ip_method.csv
按Http方法和ip维度聚类统计响应成功率


### http下载速率
响应时延的Factor表示本切片距离总体平均值贡献的差异程度，最大正值为最差切片。

- speed_by_method.csv
按Http方法维度聚类统计下载速率

- speed_by_city_method.csv
按Http方法和城市维度聚类统计下载速率

- speed_by_host_method.csv
按Http方法和host维度聚类统计下载速率

- speed_by_ip_method.csv
按Http方法和ip维度聚类统计下载速率

- speed_by_host_ip_method.csv
按Http方法、host和ip维度聚类统计下载速率，**并打开采样，采样值为100行，Factor阈值为-10，**采样文件名为speed_by_host_ip_method_sample1.csv

# 后续
- 考虑增加图形输出
- 考虑增加信令面指标
- 欢迎试用、反馈意见和提供建议
