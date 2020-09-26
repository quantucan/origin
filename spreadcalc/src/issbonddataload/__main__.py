'''
Created on 28 янв. 2020 г.

@author: Александр
'''
import sys
import datetime
from issbonddataload.issclient import MoexISSClient

def main(cmd_args):
    iss = MoexISSClient()
    
    if not iss.connect():
        print('Can\'t connect to bondinfo database')                
        return
    
    if len(cmd_args) == 2 and cmd_args[1] == '--updbonds':
        iss.update_bonds_info()        
    elif len(cmd_args) == 3 and cmd_args[1] == '--updquotes':
        trdate = datetime.date.fromisoformat(cmd_args[2])
        res = iss.load_end_of_day(trdate)
        if res:
            print(trdate, 'OK')        
    else:
        iss.update_bonds_info()
        iss.load_quotes()
    
    return        

if __name__ == '__main__':
    try:
        main(sys.argv)
    except Exception as err:
        print('Exception:', err)