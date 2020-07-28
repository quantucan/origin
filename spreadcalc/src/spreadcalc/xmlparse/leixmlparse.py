'''
Created on 12 июн. 2020 г.

@author: Александр
'''
from lxml import etree
import mysql.connector

def main():
    cnx = mysql.connector.connect(user = 'root', password = 'admiN123', database = 'bondinfo', host = '127.0.0.1', port = 3306)    
    
    leifile = etree.parse(r'C:\Users\Александр\Desktop\LEI_2020_06_10_FULL_CDF2_0-0.xml')
    lei_ogrn_path = r'/lei:LEIData/lei:LEIRecords/lei:LEIRecord/lei:Entity/lei:RegistrationAuthority[lei:RegistrationAuthorityEntityID="{0}"]'
    
    cur = cnx.cursor()
    cur.execute('SELECT id, ogrn FROM bondinfo.emitents WHERE ogrn IS NOT NULL')
    ogrns = list(cur)
    
    print('ОГРН: ', len(ogrns))
    cnt = 0
    for ogrn in ogrns:
        #lei_recs = leifile.xpath(r'/lei:LEIData/lei:LEIRecords/lei:LEIRecord/lei:Entity/lei:RegistrationAuthority/lei:RegistrationAuthorityEntityID', namespaces={'lei' : 'http://www.gleif.org/data/schema/leidata/2016'})
        lei_recs = leifile.xpath(lei_ogrn_path.format(ogrn[1]), namespaces={'lei' : 'http://www.gleif.org/data/schema/leidata/2016'})
        if len(lei_recs) > 0:
            lei_code = lei_recs[0].xpath(r'../../lei:LEI', namespaces={'lei' : 'http://www.gleif.org/data/schema/leidata/2016'})
            
            cur.execute('UPDATE emitents SET lei = %s WHERE id = %s', (lei_code[0].text, ogrn[0]))
            
            cnt += 1
    
    cnx.commit()
    
    print('LEI: ', cnt)
    
if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print(err)
