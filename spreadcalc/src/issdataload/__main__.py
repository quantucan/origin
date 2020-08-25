'''
Created on 28 янв. 2020 г.

@author: Александр
'''
import sys
import os.path
import datetime
from issdataload.issclient import MoexISSClient

iss = MoexISSClient('yusupov_a@mail.ru', 'w3A3ew88!')

#upd_flag = ''
#upd_flag = '--updquotes'
upd_flag = '--updbonds'

    
#iss.check_iss_moex(url='http://iss.moex.com/iss/engines/stock/markets/bonds/securities.json')
#iss.load_emitents_info(cnx, url='http://iss.moex.com/iss/securities.json?securities.columns=shortname,regnumber,isin,emitent_id,emitent_inn,emitent_title&engine=stock&market=bonds&is_trading=1')
        

if upd_flag == '--updbonds':
    try:
        iss.update_bonds_info()
        #iss._duplicate_boards(datetime.date(2020, 6, 22))
    except Exception as err:
        print('Exception:', err)
elif upd_flag == '--updquotes':
    trdate = datetime.date(2020, 8, 25)
    try:
        res = iss.load_end_of_day(trdate)
    except Exception as err:
        print('Exception:', err)
    
    if res:
        print(trdate, 'OK')
    
#print(sys.argv[1])