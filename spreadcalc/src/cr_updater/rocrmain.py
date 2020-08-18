'''
Created on 5 авг. 2020 г.

@author: Александр
'''
import os.path
import mysql.connector
from datetime import date
from lxml import etree

def main():
    sp_rating_path = r'.\SP'
    ns = {'r' : 'http://xbrl.sec.gov/ratings/2015-03-31'}
    
    cnx = mysql.connector.connect(user = 'root', password = 'admiN123', database = 'bondinfo', host = '127.0.0.1', port = 3306)
    cur = cnx.cursor()
    
    fname = 'SP-NAME-YY287944738-OJSC-First-Collection-Bureau-2020-08-01-OBLIGOR.xml'
    emitent_id = 204
    
    rfile = os.path.join(sp_rating_path, fname)    
    
    rocra = etree.parse(rfile)        
            
    #lei_rec = rocra.xpath(r'r:ROCRA/r:OD/r:LEI', namespaces=ns)        
    #lei_code = lei_rec[0].text
        
    lt_ord_recs = rocra.xpath(r'r:ROCRA/r:OD/r:ORD[r:RT="Issuer Credit Rating" and r:RST="Foreign Currency LT"]', namespaces=ns)
    for ord_rec in lt_ord_recs:                    
        rd = {'emitent_id' : emitent_id, 'rad' : None, 'rating' : None, 'rol' : None, 'wst' : None}
                    
        for i in ord_rec:
            tag = etree.QName(i).localname
                        
            if tag == 'R':
                rd['rating'] = i.text
            elif tag == 'RAD':
                rd['rad'] = date.fromisoformat(i.text)
            elif tag == 'WST':
                if i.text is None:
                    continue
                else:
                    rd['wst'] = i.text.lower()
            elif tag == 'ROL':
                if i.text is None:
                    continue
                else:
                    rd['rol'] = i.text.lower()                    
                                        
        cur.execute('INSERT INTO sp (emitent_id, rad, rating, rol, wst) VALUES (%(emitent_id)s, %(rad)s, %(rating)s, %(rol)s, %(wst)s)', rd)
                        
    cnx.commit()
    cnx.close()
    
if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print('Exception:', err)