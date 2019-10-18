'''-*- coding: utf-8 -*-
 Auther guitar79@naver.com
'''
#import numpy as np
import os
from glob import glob
import pandas as pd
from datetime import datetime
import pymysql
import sqlalchemy
import numpy as np

#mariaDB info
db_host = 'ess.gs.hs.kr'
db_user = 'guitar79'
db_pass = 'rlgusl01'
db_name = 'AIRKOREA'
tb_name = 'hourly_all'
#tb_name = 'hourly_test'

#base directory
read_dir_name = '../AirKorea_final_data/'
save_dir_name = '../AirKorea_processing_data/'

log_file = 'AirKorea_python.log'
err_log_file = 'AirKorea_python_err.log'
def write_log(log_file, log_str):
    with open(log_file, 'a') as log_f:
        log_f.write(log_str+'\n')
    return print (log_str)

#for checking time
cht_start_time = datetime.now()
def print_working_time():
    working_time = (datetime.now() - cht_start_time) #total days for downloading
    return print('working time ::: %s' % (working_time))

if not os.path.exists(save_dir_name):
    os.makedirs(save_dir_name)
    print ('*'*80)
    print ('{0} is created'.format(save_dir_name))
else :
    print ('*'*80)
    print ('{0} is already exist'.format(save_dir_name))

#db connect
conn= pymysql.connect(host=db_host, user=db_user, password=db_pass, db=db_name,\
                      charset='utf8mb4', local_infile=1, cursorclass=pymysql.cursors.DictCursor)

cur = conn.cursor()

#cur.execute("DROP TABLE IF EXISTS `%s`;" %(tb_name))

#cur.execute("CREATE TABLE IF NOT EXISTS `{0}` (`id` int(11) NOT NULL, `OBS_code` int(6) NOT NULL, `OBS_datetime` DATETIME NOT NULL, `SO2` float DEFAULT NULL, `CO` float DEFAULT NULL, `O3` float DEFAULT NULL, `NO2` float DEFAULT NULL, `PM10` float DEFAULT NULL, `PM25` float DEFAULT NULL, `REMARKS` VARCHAR(255) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;".format(tb_name))

#cur.execute("ALTER TABLE `{0}` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;".format(tb_name))
#conn.commit()

engine = sqlalchemy.create_engine("mysql+pymysql://{0}:{1}@{2}/{3}".format(db_user, db_pass, db_host, db_name))

#'2014-01.csv' #680399
#'2014-02.csv' #680399

f_name = '2004-02.xlsx' #680399 / 672741
fullname = read_dir_name+f_name
def autoconvert_datetime(value):
    result_format = '%Y-%m-%d %H:%M:%S'  # output format
    try:        
        return datetime.strptime('{0}-{1}-{2} {3}:00:00'.format(str(value)[0:4], str(value)[4:6], str(value)[6:8], \
                                 str(int(str(value)[8:10])-1)), result_format)
    except Exception as e:  # throws exception when format doesn't match
        print(e)
        pass
    return value  # let it be if it doesn't match


def autoconvert_datetime1(value):
    result_format = '%Y-%m-%d %H:%M:%S'  # output format
    try:        
        return datetime.strptime('{0}-{1}-{2} {3}:00:00'.format(str(value)[0:4], str(value)[4:6], str(value)[6:8], \
                                 str(int(str(value)[8:10]))), result_format)
    except Exception as e:  # throws exception when format doesn't match
        print(e)
        pass
    return value  # let it be if it doesn't match

total_files = 0
total_records = 0

for fullname in sorted(glob(os.path.join(read_dir_name, '*.xlsx')))[27:]:
    fullname_el = fullname.split('/')
    f_name = fullname_el[-1]
    print('starting {0}'.format(f_name))
    if int(fullname_el[-1][0:4]) == 2017 :
        df = pd.read_excel(fullname, sheetname=0, skiprows=0,
                       names = ['Region', 'OBS_code', 'OBS_name', 'OBS_datetime', 'SO2', 'CO', 'O3', 'NO2', 'PM10', 'PM25', 'Address'])
        df = df.drop(['Region', 'OBS_name', 'Address'], axis=1)
        print('debug 3-1')
        print(df)
        
    elif int(fullname_el[-1][0:4]) == 2018 :
        df = pd.read_excel(fullname, sheetname=0, skiprows=0,
                       names = ['Region', 'OBS_code', 'OBS_name', 'OBS_datetime', 'SO2', 'CO', 'O3', 'NO2', 'PM10', 'PM25', 'Address'])
        df = df.drop(['Region', 'OBS_name', 'Address'], axis=1)
        print('debug 3-2')
        print(df)
        
    elif int(fullname_el[-1][0:4]) == 2019 :
        print('debug 3-3')
        continue
                       
    else : 
        df = pd.read_excel(fullname, sheetname=0, skiprows=0,
                       names = ['Region', 'OBS_name', 'OBS_code', 'OBS_datetime', 'SO2', 'CO', 'O3', 'NO2', 'PM10', 'Address'])
        df['PM25'] = np.nan
        df = df.drop(['Region', 'OBS_name', 'Address'], axis=1)
        print('debug 3-4')
        print(df)
    '''
    df = pd.read_csv(fullname, skiprows=1, sep=',', header=None, 
         names = ['Region', 'OBS_code', 'OBS_name', 'OBS_datetime', 'SO2', 'CO', 'O3', 'NO2', 'PM10', 'PM25', 'Address'],
         skipfooter=1, engine='python')
    '''
    
    if fullname_el[-1][0:7] == '2004-02' :
        df['OBS_datetime'] = df['OBS_datetime'].apply(autoconvert_datetime1)
        print('debug 4')
        print(df)
    else :
        df['OBS_datetime'] = df['OBS_datetime'].apply(autoconvert_datetime)
        print('debug 4')
        print(df)
        
    df = df.dropna(subset=('OBS_code', 'OBS_datetime'), how='all').reset_index(drop=True)
    print('debug 5')
    print(df)
    
    prodessing_log = '#This file is created using python \n' \
                '#https://github.com/guitar79/AirKorea_Python \n' \
                + 'filename : ' + f_name +'\n'\
                + 'total records : ' + str(len(df)) +'\n'
    
    total_files += 1
    total_records += len(df)
    
    print('total_files: {0}'.format(total_files))
    print('total_records: {0}'.format(total_records))
            
    df.to_sql(name=tb_name, con=engine, if_exists = 'append', index=False)
    conn.commit()
    with open('{0}{1}_insert.log'.format(save_dir_name, f_name), 'w') as f:
        f.write(prodessing_log)

print('total_files: {0}'.format(total_files))
print('total_records: {0}'.format(total_records))

conn.commit()
#total_files: 12
#total_records: 8355175
