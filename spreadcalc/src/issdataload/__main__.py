'''
Created on 28 янв. 2020 г.

@author: Александр
'''
import sys
import datetime
from issdataload.issclient import MoexISSClient

def main(cmd_args):
    iss = MoexISSClient()
    
    if not iss.connect():
        print('Can\'t connect to bondinfo database')        
        return
    
    if len(cmd_args) == 2 and cmd_args[1] == '--updbonds':
        try:
            iss.update_bonds_info()
            print('Bondinfo database has been updated')
        except Exception as err:
            print('Exception:', err.msg)
    elif len(cmd_args) == 3 and cmd_args[1] == '--updquotes':        
        try:
            trdate = datetime.date.fromisoformat(cmd_args[2])
            res = iss.load_end_of_day(trdate)
            if res:
                print(trdate, 'OK')
        except Exception as err:
                print('Exception:', err)

if __name__ == '__main__':
    main(sys.argv)