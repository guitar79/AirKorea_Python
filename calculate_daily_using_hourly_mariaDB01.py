'''-*- coding: utf-8 -*-
 Auther guitar79@naver.com
'''
#np.set_printoptions(threshold=100)

import numpy as np
import os
#import multiprocessing as proc
import pymysql
import pandas as pd
from datetime import datetime
#import numpy as np
import sqlalchemy
#import pymysql.cursorsv
#from datetime import datetime, timedelta

from dateutil.relativedelta import relativedelta

start_time=str(datetime.now())

#mariaDB info#mariaDB info
db_host = 'ess.gs.hs.kr'
db_user = 'guitar79'
db_pass = 'rlgusl01'
db_name = 'AIRKOREA'
tb_hourly = 'hourly_all'
tb_daily = 'daily_all'
tb_monthly = 'monthly_all'

#base directory
read_dir_name = '../AirKorea_final_data/'
save_dir_name = '../AirKorea_processing_data/'
save_dir_name = '../AirKorea_daily_data/'

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

cur.execute("DROP TABLE IF EXISTS `%s`;" %(tb_monthly))

cur.execute("DROP TABLE IF EXISTS `%s`;" %(tb_daily))

#cur.execute("CREATE TABLE IF NOT EXISTS `{0}` (`id` int(11) NOT NULL, `OBS_code` int(6) NOT NULL, `OBS_datetime` DATETIME NOT NULL, `SO2` float DEFAULT NULL, `CO` float DEFAULT NULL, `O3` float DEFAULT NULL, `NO2` float DEFAULT NULL, `PM10` float DEFAULT NULL, `PM25` float DEFAULT NULL, `REMARKS` VARCHAR(255) DEFAULT NULL, PRIMARY KEY (`id`)) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;".format(tb_name))

#cur.execute("ALTER TABLE `{0}` MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;".format(tb_name))
#conn.commit()

engine = sqlalchemy.create_engine("mysql+pymysql://{0}:{1}@{2}/{3}".format(db_user, db_pass, db_host, db_name))


#db connect
conn= pymysql.connect(host=db_host, user=db_user, password=db_pass, db=db_name,\
                      charset='utf8mb4', local_infile=1, cursorclass=pymysql.cursors.DictCursor)

cur = conn.cursor()

cur.execute("DROP TABLE IF EXISTS `%s`;" %(tb_monthly))

cur.execute("DROP TABLE IF EXISTS `%s`;" %(tb_daily))

cur.execute("CREATE TABLE IF NOT EXISTS `%s` (\
            `id` int(11) NOT NULL,\
            `OBS_code` int(6) NOT NULL,\
            `OBS_date` DATE NOT NULL,\
            `mean_SO2` float DEFAULT NULL,\
            `mean_CO` float DEFAULT NULL,\
            `mean_O3` float DEFAULT NULL,\
            `mean_NO2` float DEFAULT NULL,\
            `mean_PM10` float DEFAULT NULL,\
            `mean_PM25` float DEFAULT NULL,\
            `std_SO2` float DEFAULT NULL,\
            `std_CO` float DEFAULT NULL,\
            `std_O3` float DEFAULT NULL,\
            `std_NO2` float DEFAULT NULL,\
            `std_PM10` float DEFAULT NULL,\
            `std_PM25` float DEFAULT NULL,\
            `max_SO2` float DEFAULT NULL,\
            `max_CO` float DEFAULT NULL,\
            `max_O3` float DEFAULT NULL,\
            `max_NO2` float DEFAULT NULL,\
            `max_PM10` float DEFAULT NULL,\
            `max_PM25` float DEFAULT NULL,\
            `min_SO2` float DEFAULT NULL,\
            `min_CO` float DEFAULT NULL,\
            `min_O3` float DEFAULT NULL,\
            `min_NO2` float DEFAULT NULL,\
            `min_PM10` float DEFAULT NULL,\
            `min_PM25` float DEFAULT NULL,\
            `count` int(4) DEFAULT NULL,\
            `REMARKS` VARCHAR(255) DEFAULT NULL,\
            PRIMARY KEY (`id`))\
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"\
            %(tb_monthly))

cur.execute("CREATE TABLE IF NOT EXISTS `%s` (\
            `id` int(11) NOT NULL,\
            `OBS_code` int(6) NOT NULL,\
            `OBS_date` DATE NOT NULL,\
            `mean_SO2` float DEFAULT NULL,\
            `mean_CO` float DEFAULT NULL,\
            `mean_O3` float DEFAULT NULL,\
            `mean_NO2` float DEFAULT NULL,\
            `mean_PM10` float DEFAULT NULL,\
            `mean_PM25` float DEFAULT NULL,\
            `std_SO2` float DEFAULT NULL,\
            `std_CO` float DEFAULT NULL,\
            `std_O3` float DEFAULT NULL,\
            `std_NO2` float DEFAULT NULL,\
            `std_PM10` float DEFAULT NULL,\
            `std_PM25` float DEFAULT NULL,\
            `max_SO2` float DEFAULT NULL,\
            `max_CO` float DEFAULT NULL,\
            `max_O3` float DEFAULT NULL,\
            `max_NO2` float DEFAULT NULL,\
            `max_PM10` float DEFAULT NULL,\
            `max_PM25` float DEFAULT NULL,\
            `min_SO2` float DEFAULT NULL,\
            `min_CO` float DEFAULT NULL,\
            `min_O3` float DEFAULT NULL,\
            `min_NO2` float DEFAULT NULL,\
            `min_PM10` float DEFAULT NULL,\
            `min_PM25` float DEFAULT NULL,\
            `count` int(4) DEFAULT NULL,\
            `REMARKS` VARCHAR(255) DEFAULT NULL,\
            PRIMARY KEY (`id`))\
            ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;"\
            %(tb_daily))

cur.execute("ALTER TABLE `%s`\
            MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;"  %(tb_monthly))

cur.execute("ALTER TABLE `%s`\
            MODIFY `id` int(11) NOT NULL AUTO_INCREMENT;"  %(tb_daily))
conn.commit()

#all OBS_code read!!
cur = conn.cursor()
print("TRUNCATE TABLE %s;" %(tb_monthly))
cur.execute("TRUNCATE TABLE %s;" %(tb_monthly))
print("TRUNCATE TABLE %s;" %(tb_daily))
cur.execute("TRUNCATE TABLE %s;" %(tb_daily))

conn.commit()

def check_none(x):
    if np.isnan(x): return -9999
    return x

#OBS_codes = OBS_codes
print("SELECT DISTINCT OBS_code from %s ORDER BY OBS_code ASC " %(tb_hourly))
cur.execute("SELECT DISTINCT OBS_code from %s ORDER BY OBS_code ASC " %(tb_hourly))
OBS_code_rows = cur.fetchall()
#print(len(OBS_codes))
OBS_codes = []
for i in range(len(OBS_code_rows)):
    OBS_codes.append(OBS_code_rows[i]['OBS_code'])

#OBS_codes=[131372]
print(OBS_codes)


'''
print("SELECT * from %s ORDER BY `OBS_code` ASC, `OBS_datetime` ASC" %(tb_hourly))
cur.execute("SELECT * from %s ORDER BY `OBS_code` ASC, `OBS_datetime` ASC" %(tb_hourly))
all_rows = cur.fetchall()
'''   

for OBS_code in OBS_codes[1:]:
    print("SELECT * from %s WHERE `OBS_code` = %s ORDER BY `OBS_datetime` ASC" %(tb_hourly,OBS_code))
    cur.execute("SELECT * from %s WHERE `OBS_code` = %s ORDER BY `OBS_datetime` ASC" %(tb_hourly,OBS_code))
    all_data_by_OBS_code = cur.fetchall()
    print(OBS_code,len(all_data_by_OBS_code))
    
    df = pd.DataFrame(all_data_by_OBS_code)
    df = df.replace(to_replace=[-999.0], value=np.nan)
    
    result_rows = []
    
    start_datetime = datetime(2001, 1, 1, 0, 0, 0)
    end_datetime = datetime(2019, 1, 1, 0, 0, 0)
    processing_sdatetime = start_datetime
    OBS_code_daily_df = pd.DataFrame(columns=('OBS_code', 'OBS_date', 
       'mean_SO2', 'mean_CO', 'mean_O3', 'mean_NO2', 'mean_PM10', 'mean_PM25', 
       'std_SO2', 'std_CO', 'std_O3', 'std_NO2', 'std_PM10', 'std_PM25', 
       'max_SO2', 'max_CO', 'max_O3', 'max_NO2', 'max_PM10', 'max_PM25', 
       'min_SO2', 'min_CO', 'min_O3', 'min_NO2', 'min_PM10', 'min_PM25', 
       'count', 'REMARKS'))
    while processing_sdatetime < end_datetime :
        processing_edatetime = processing_sdatetime + relativedelta(days=+1)
        df['OBS_datetime'] = pd.to_datetime(df['OBS_datetime'])
        selected_df = df.loc[df['OBS_datetime'] >= processing_sdatetime]
        selected_df = selected_df.loc[selected_df['OBS_datetime'] < processing_edatetime]
        
        if len(selected_df) == 0 :
            print('data is empty %s from %s to %s' %(OBS_code, processing_sdatetime, processing_edatetime))
        else:
            #print(selected_df)
            
            statistics_df = selected_df.describe(include='all')
    
            OBS_code_daily_df = OBS_code_daily_df.append({'OBS_code':'{0}'.format(int(statistics_df['OBS_code']['max'])),
                  'OBS_date':'{0}'.format(statistics_df['OBS_datetime']['first'].strftime("%Y-%m-%d")), 
                  'mean_SO2':'{0:.4f}'.format(statistics_df['SO2']['mean']),
                  'mean_CO':'{0:.4f}'.format(statistics_df['CO']['mean']), 
                  'mean_O3':'{0:.4f}'.format(statistics_df['O3']['mean']), 
                  'mean_NO2':'{0:.4f}'.format(statistics_df['NO2']['mean']), 
                  'mean_PM10':'{0:.4f}'.format(statistics_df['PM10']['mean']), 
                  'mean_PM25':'{0:.4f}'.format(statistics_df['PM25']['mean']), 
                  'std_SO2':'{0:.4f}'.format(statistics_df['SO2']['std']),
                  'std_CO':'{0:.4f}'.format(statistics_df['CO']['std']), 
                  'std_O3':'{0:.4f}'.format(statistics_df['O3']['std']), 
                  'std_NO2':'{0:.4f}'.format(statistics_df['NO2']['std']), 
                  'std_PM10':'{0:.4f}'.format(statistics_df['PM10']['std']), 
                  'std_PM25':'{0:.4f}'.format(statistics_df['PM25']['std']), 
                  'max_SO2':'{0:.4f}'.format(statistics_df['SO2']['max']), 
                  'max_CO':'{0:.4f}'.format(statistics_df['CO']['max']), 
                  'max_O3':'{0:.4f}'.format(statistics_df['O3']['max']), 
                  'max_NO2':'{0:.4f}'.format(statistics_df['NO2']['max']), 
                  'max_PM10':'{0:.4f}'.format(statistics_df['PM10']['max']), 
                  'max_PM25':'{0:.4f}'.format(statistics_df['PM25']['max']), 
                  'min_SO2':'{0:.4f}'.format(statistics_df['SO2']['min']), 
                  'min_CO':'{0:.4f}'.format(statistics_df['CO']['min']), 
                  'min_O3':'{0:.4f}'.format(statistics_df['O3']['min']), 
                  'min_NO2':'{0:.4f}'.format(statistics_df['NO2']['min']), 
                  'min_PM10':'{0:.4f}'.format(statistics_df['PM10']['min']), 
                  'min_PM25':'{0:.4f}'.format(statistics_df['PM25']['min']), 
                  'count':'{0}'.format(int(statistics_df['OBS_code']['count'])), 
                  'REMARKS':'{0} - {1}'.format(processing_sdatetime.strftime("%Y-%m-%d %H"), processing_edatetime.strftime("%Y-%m-%d %H"))}, ignore_index=True)
            
        print('processing : {0} : {1}'.format(OBS_code, processing_sdatetime))

        processing_sdatetime = processing_edatetime
        
    print('total_records: {0}'.format(len(OBS_code_daily_df)))
    
    OBS_code_daily_df = OBS_code_daily_df.replace(to_replace=['nan'], value='')
    
    OBS_code_daily_df.to_csv('{0}{1}_daily_{2}_{3}.csv'.format(save_dir_name, OBS_code, start_datetime.strftime("%Y-%m-%d"), end_datetime.strftime("%Y-%m-%d")), index=False)
    print('{0}{1}_daily_{2}_{3}.csv is created...'.format(save_dir_name, OBS_code, start_datetime.strftime("%Y-%m-%d"), end_datetime.strftime("%Y-%m-%d")))
            
    #OBS_code_daily_df.to_sql(name=tb_daily, con=engine, if_exists = 'append', index = False)
    #conn.commit()
    
#    with open('{0}{1}_insert.log'.format(save_dir_name, f_name), 'w') as f:
#       f.write(prodessing_log)

#cur.close()