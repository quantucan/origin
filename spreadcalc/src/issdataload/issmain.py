'''
Created on 28 янв. 2020 г.

@author: Александр
'''
import datetime
from issdataload.issclient import MoexISSClient

def main():    
    iss = MoexISSClient('yusupov_a@mail.ru', 'w3A3ew88!')
    
    #iss.check_iss_moex(url='http://iss.moex.com/iss/engines/stock/markets/bonds/securities.json')
    #iss.load_emitents_info(cnx, url='http://iss.moex.com/iss/securities.json?securities.columns=shortname,regnumber,isin,emitent_id,emitent_inn,emitent_title&engine=stock&market=bonds&is_trading=1')
        
    iss.update_bonds_info()
    #iss._duplicate_boards(datetime.date(2020, 6, 22))
    
    '''trdate = datetime.date(2020, 7, 27)  
    for i in range(1):        
        res = iss.load_end_of_day(trdate)
        if res == False:
            break
        else:
            print(trdate, 'OK')
            
        trdate += datetime.timedelta(days = 1)'''
    
if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print('Exception:', err)