#/usr/bin/env python
# -*- coding: UTF-8 -*-

import random
import numpy as np
import pandas as pd
import matplotlib as mpl
import matplotlib.pyplot as plt
#import seaborn as sb
from mpl_toolkits.mplot3d import Axes3D  # noqa: F401 unused import
from matplotlib import cm

def ip_dec2dot(ip_dec):
    try:
        ip_dec = long(ip_dec)
        ip_num1 = ip_dec // 256**3
        ip_num2 = (ip_dec % 256**3) // 256**2
        ip_num3 = (ip_dec % 256**2) // 256
        ip_num4 = ip_dec % 256
        ip_dot = "%s.%s.%s.%s" % (ip_num1,ip_num2,ip_num3,ip_num4)
        #print(ip_dot)
        return(ip_dot)
    except:
        return ''
    
def ip_dot2dec(ip_dot):
    ip_num = ip_dot.split(".")
    ip_dec = 0
    for i in range(4):
        ip_dec =ip_dec * 256 + int(ip_num[i])
    print(ip_dec)
    return(str(ip_dec))
    
    
def clean_result_file(origin_file,dest_file):
    f1=open(origin_file)
    f2=open(dest_file,"w")
    length = 0
    
    while (length < 10):
        #skip the blank lines
        columnnames = f1.readline()
        length = len(columnnames)
    head10 = columnnames[0:10]
    
    f2.write(columnnames)
    
    while True:
        try:
            line = f1.readline()
            if not line:
                break
                #文件尾巴，跳出循环
            if len(line)>10 and line.find(head10) == -1 :
                f2.write(line)
        except:
            print("error")
            #读到异常的行
       
    f1.close()
    f2.close()
            
def process_dataframe():
    global df
    df = df[df['http_lastpacket_1streq_delay']>0]
    df['speed'] = df['dl_data'] * 8 / df['http_lastpacket_1streq_delay']
    df['speed_log'] = np.exp2(np.clip(np.floor(np.log2(df['speed']/10)),0,None))*10
    df['dl_data_log'] = np.exp2(np.clip(np.floor(np.log2(df['dl_data']/500000)),0,None))*500000
    df['ip']=df['app_server_ip_ipv4'].map(ip_dec2dot)
    return df

    
def barchart_speed_onedim(df,row,title,savefile='csvfile.csv',picfile='savefile.jpg'):
    grouped_data=df.groupby(row)
    mymedian = grouped_data.median()
    mysum = grouped_data.sum()
    sum_speed = mysum['dl_data'] * 8 / mysum['http_lastpacket_1streq_delay']
    median_speed = mymedian['speed']
    
    newdf=pd.DataFrame()
    newdf['speed_median'] = mymedian['speed']
    newdf['dl_data'] = mysum['dl_data']
    newdf['dl_time'] = mysum['http_lastpacket_1streq_delay'] /1000
    newdf['speed_normal'] = sum_speed
    newdf.to_csv(savefile)
    #print(sum_speed)
    #print(median_speed)
    
    if len(newdf) > 30:
        return
    fig1 = plt.figure()
    fig1.set_size_inches(12, 8 * 1)
    ax1 = fig1.add_subplot(111)

    x=np.arange(len(sum_speed))
    #ax1.bar( x,sum_speed,alpha=0.9, width = 0.35, facecolor = 'lightskyblue', edgecolor = 'white', label='speed_normal', linewidth = 0)
    #ax1.bar( x+0.35,median_speed, alpha=0.9, width = 0.35, facecolor = 'yellowgreen', edgecolor = 'white', label='speed_median', linewidth = 0)
    ax1.plot( x,median_speed, color = 'yellowgreen' ,label='speed_median', linewidth =2)
    ax1.plot( x,sum_speed, color = 'lightskyblue' ,label='speed_normal', linewidth =2)
    plt.xticks(x, median_speed.index, rotation=90)
    ax2=ax1.twinx()
    ax2.bar( x,mysum['dl_data'], alpha=0.5, facecolor = 'blue', edgecolor = 'white', label='dl_data', linewidth = 0)
    #ax1.set_xticks(result.index)
    ax1.set_xlabel('downlink data')
    ax1.set_ylabel('downlink speed')
    ax1.set_title(title)
    ax1.legend(loc='upper left')
    #plt.show()
    fig1.savefig(picfile, dpi=100)
    plt.close('all')
    
def bar3d_speed_pentile(df,row,title,savefile='csvfile.csv',picfile='savefile.jpg'):
    grouped_data=df.groupby(row)
    myquantile = grouped_data.quantile([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9])
    #mysum = grouped_data.sum()
    #sum_speed = mysum['dl_data'] * 8 / mysum['http_lastpacket_1streq_delay']
    newdf = myquantile.reset_index()
    newdf = newdf.pivot(index='level_1', columns=row)
    newdf = newdf['speed']
    #print(newdf)
    newdf.to_csv(savefile)

    fig1 = plt.figure()
    fig1.set_size_inches(12, 8 * 1)
    ax1 = fig1.add_subplot(1, 1, 1, projection='3d')
    #ax1.plot_wireframe(newdf['dl_data_log'],newdf['level_1'],newdf['speed'], rstride=10, cstride=10)
    
    xs = range(len(newdf))
    print(xs)
    zs = list(newdf.columns)
    zn = 1
    for z in zs:
        ys = newdf[z]
        color = plt.cm.Set2(random.choice(range(plt.cm.Set2.N)))
        ax1.bar(xs, list(ys), zn, zdir='y', color=color, alpha=0.8)
        zn = zn + 1
    
    zs.append('more')
    ax1.set_xticks(xs)
    ax1.set_xticklabels([0.1,0.2,0.3,0.4,0.5,0.6,0.7,0.8,0.9])
    ax1.set_yticklabels(zs,rotation=-90)
    #ax1.xaxis.set_major_locator(mpl.ticker.FixedLocator(xs))
    #ax1.yaxis.set_major_locator(mpl.ticker.FixedLocator(ys))
    ax1.set_xlabel('pentile')
    ax1.set_ylabel(row)
    ax1.set_zlabel('speed')
    #plt.show()
    fig1.savefig(picfile, dpi=100)
    plt.close('all')

    
home_dir ='/code/pyonhive/'
origin_file = home_dir + 'httpxrd_result.csv'
dest_file = home_dir + 'httpxrd_clean.csv'
#clean_result_file(origin_file,dest_file)
#use only at the first time
cmap=plt.get_cmap('tab20')
print(cmap)

df = pd.read_csv(dest_file,sep='|',header=0)
process_dataframe()


jpgfile = home_dir + 'speed_vs_data.jpg'
csvfile = home_dir + 'speed_vs_data.csv'
barchart_speed_onedim(df,'dl_data_log','speed vs data',savefile=csvfile,picfile=jpgfile)

jpgfile = home_dir + 'speed_vs_cityname.jpg'
csvfile = home_dir + 'speed_vs_cityname.csv'
barchart_speed_onedim(df,'cityname','speed vs cityname',savefile=csvfile,picfile=jpgfile)

jpgfile = home_dir + 'speed_vs_host.jpg'
csvfile = home_dir + 'speed_vs_host.csv'
barchart_speed_onedim(df,'host','speed vs host',savefile=csvfile,picfile=jpgfile)

#jpgfile = home_dir + 'speed_vs_uri.jpg'
#csvfile = home_dir + 'speed_vs_uri.csv'
#barchart_speed_onedim(df,'uri','speed vs uri',savefile=csvfile,picfile=jpgfile)

jpgfile = home_dir + 'speed_vs_ip.jpg'
csvfile = home_dir + 'speed_vs_ip.csv'
barchart_speed_onedim(df,'app_server_ip_ipv4','speed vs ip',savefile=csvfile,picfile=jpgfile)


jpgfile = home_dir + 'speed_3d_data.jpg'
csvfile = home_dir + 'speed_3d_data.csv'
bar3d_speed_pentile(df,'dl_data_log','speed_3d_data',savefile=csvfile,picfile=jpgfile)

jpgfile = home_dir + 'speed_3d_city.jpg'
csvfile = home_dir + 'speed_3d_city.csv'
bar3d_speed_pentile(df,'cityname','speed_3d_city',savefile=csvfile,picfile=jpgfile)

