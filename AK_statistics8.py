#Day mean, Day var, Day std, Day max, Day Min

# -*- coding: utf-8 -*-
# Auther guitar79@naver.com 

import numpy as np
import os
import pandas as pd
import matplotlib.pyplot as plt
np.set_printoptions(threshold=100)
def date(n,m):#From n to m-1
    return ((f['측정일시']-n)//(m-n) == 0)
    
#def place(n)

def array2string(x):
    return '\n'.join(','.join('%0.2f' %x for x in y) for y in x)


# data head
# ['지역', '측정소코드', '측정소명', '측정일시', 'SO2', 'CO', 'O3', 'NO2', 'PM10', 'PM25', '주소']

#th_list = [0,5,10,15,20,25,30,35,40,45,50,55,60,65]

#base directory
drbase = '/media/guitar79/8T/RS_data/Remote_Sensing/2017RNE/airkorea/'
#read directory(input data)
drin = 'temp/'
#write directory(output data)
drout = 'output/'

#make empty dataframe(1,11) with head
#head = ('지역', '측정소코드', '측정소명', '측정일시', 'SO2', 'CO', 'O3', 'NO2', 'PM10', 'PM25', '주소')
head = ('', '111111', '', '', '', '', '', '', '', '', '')
head = np.array(head)
head = head.reshape(1,11)
f = pd.DataFrame(head)
#empty dataframe(1,11) columns header
f.columns = (['지역', '측정소코드', '측정소명', '측정일시', 'SO2', 'CO', 'O3', 'NO2', 'PM10', 'PM25', '주소'])

#read data files 
for i in sorted(os.listdir(drbase+drin)):
    #read csv files
    if i[-4:] == '.csv':
        #make data frame from reading csv files (like table)
        g = pd.read_csv(drbase+drin+i, encoding='euc_kr')
        g.columns = (['지역', '측정소코드', '측정소명', '측정일시', 'SO2', 'CO', 'O3', 'NO2', 'PM10', 'PM25', '주소'])

        f = f.append(g,ignore_index=True)
        #count number of the total data 
        
f = f.drop(f.index[[0,1]])
total_datanum = len(f)

#statistics of total data
total_mean_value = array2string(f.mean().values.reshape(1,11))
total_var_value = array2string(f.var().values.reshape(1,11))
total_std_value = array2string(f.std().values.reshape(1,11))
total_max_value = array2string(f.max().values.reshape(1,11))
total_min_value = array2string(f.min().values.reshape(1,11))

#write files from statistics
with open(drbase+drout+'statistics_total.csv', 'w') as o:
    print(total_datanum,',','mean,',total_mean_value,'\n','var,',total_var_value,'\n','std,',total_std_value,'\n','max,',total_max_value,'\n','min,',total_min_value,'\n', file=o)

#make observatory code index
#ocode = list(set(np.array(f.ix[:,1])))
ocode = list(set(np.array(f.ix[:,'측정일시'])//100))
#count number of the observatory
onumber = len(ocode)

for obs in sorted(ocode): 
    fo=f.loc[date(obs*100+1,obs*100+25)]
    #fo=f[f['측정소코드']].isin(obs)
    o_datanum = len(fo)
    if o_datanum != 0:
        o_mean_value = array2string(fo.mean().values.reshape(1,11))
        o_var_value = array2string(fo.var().values.reshape(1,11))
        o_std_value = array2string(fo.std().values.reshape(1,11))
        o_max_value = array2string(fo.max().values.reshape(1,11))
        o_min_value = array2string(fo.min().values.reshape(1,11))
        obss = str(obs)
        with open(drbase+drout+'statistics.csv', 'a') as o:
            print(obs, o_datanum, o_mean_value, o_var_value, o_std_value, o_max_value, o_min_value, file=o)
            print('-----------------------------------------------------------', file=o)
            #print(obs)
    else:
        with open(drbase+drout+'statistics.csv', 'a') as o:
            print(obs, 'MaM', 'MaM', 'MaM', 'MaM', 'MaM', 'NaN', file=o)
            #print('asdf')

'''
total_mean_value = f.mean().values.reshape(11,1)
total_var_value = f.var().values.reshape(11,1)
total_std_value = f.std().values.reshape(11,1)
total_max_value = f.max().values.reshape(11,1)
total_min_value = f.min().values.reshape(11,1)

#self.output += tds.text
#csv delimeter
#self.output += ','
#csv delimeter
#self.output += '\n'



      for obs in ocode:
                fo=f.loc[f[:,1] == obs]
                #fo=f[f['측정소코드']].isin(obs)
                o_datanum = len(fo)
                if o_datanum != 0:
                    o_mean_value = fo.mean().reshape(1,11)
                    o_var_value = fo.var().reshape(1,11)
                    o_std_value = fo.std().reshape(1,11)
                    o_max_value = fo.max().reshape(1,11)
                    o_min_value = fo.min().reshape(1,11)
                    with open(drbase+drout+'statistics_'+obs+'o.txt', 'a') as o:
                        print(i, obs, o_datanum, o_mean_value, o_var_value, o_std_value, o_max_value, o_min_value, file=o)
                else:
                    with open(drbase+drout+'statistics_'+obs+'o.txt', 'a') as o:
                        print(i, obs, 'NaN', 'NaN', 'NaN', 'NaN', 'NaN', 'NaN', file=o)
'''