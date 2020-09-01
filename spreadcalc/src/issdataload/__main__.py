'''
Created on 28 янв. 2020 г.

@author: Александр
'''
import sys
import datetime
from issdataload.issclient import MoexISSClient

upd_flag = 0

if len(sys.argv) == 2 and sys.argv[1] == '--updbonds':
    upd_flag = 1
elif len(sys.argv) == 3 and sys.argv[1] == '--updquotes':
    upd_flag = 2
    try:
        trdate = datetime.date.fromisoformat(sys.argv[2])
    except Exception as err:        
        print('Exception:', err)
        sys.exit()      

iss = MoexISSClient()

if not iss.connect():
    print('Can\'t connect to bondinfo database')
    sys.exit()

if upd_flag == 1:
    try:
        iss.update_bonds_info()
        print('Bondinfo database has been updated')
    except Exception as err:
        print('Exception:', err)    

elif upd_flag == 2:    
    try:
        res = iss.load_end_of_day(trdate)
        if res:
            print(trdate, 'OK')
    except Exception as err:
        print('Exception:', err)