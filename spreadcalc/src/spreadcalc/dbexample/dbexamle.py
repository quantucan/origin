'''
Created on 23 февр. 2020 г.

@author: Александр
'''
import urllib.request
import mysql.connector
import json

def main():    
    url = 'https://suggestions.dadata.ru/suggestions/api/4_1/rs/findById/party'
    headers = {'Content-Type' : 'application/json', 'Accept' : 'application/json', 'Authorization' : 'Token 60078f31f98fd1be097b8c382042070bfa6f36e6'}
    data = {'query' : '', 'branch_type' : 'MAIN', 'type' : 'LEGAL'}
    
    cnx = mysql.connector.connect(user='root', password='admiN123', database='bondinfo', host='127.0.0.1', port=3306)    
    
    f = open('emitents.log', 'wt', encoding='utf-8')    
    
    '''data['query'] = '7710152113'
    req = urllib.request.Request(url, json.dumps(data).encode('ascii'), headers)
    conn = urllib.request.urlopen(req)    
    jresult = json.loads(conn.read().decode())    
    print(len(jresult['suggestions']))'''
    
    cur = cnx.cursor()
    cur.execute('SELECT id, inn FROM emitents WHERE inn IS NOT NULL ORDER BY id')     
    inns = list(cur)
    
    cnt = 0
    for inn in inns:
        data['query'] = inn[1]        
        req = urllib.request.Request(url, json.dumps(data).encode('ascii'), headers)
        conn = urllib.request.urlopen(req)
        
        jresult = json.loads(conn.read().decode())
        if len(jresult['suggestions']) == 0:
            f.write('{0}/t{1}\n'.format(inn[0], inn[1]))
            continue
        
        shortname = jresult['suggestions'][0]['data']['name']['short_with_opf']
        if shortname is None or len(shortname) > 50:
            f.write('{0}/t{1}/t{2}\n'.format(inn[0], inn[1], shortname))
            shortname = None
        
        ogrn = jresult['suggestions'][0]['data']['ogrn']
        
        cur.execute('UPDATE emitents SET ogrn = %s, shortname = %s WHERE id = %s', (ogrn, shortname, inn[0]))
        
        cnt += 1
        
        if cnt % 100 == 0:
            print(cnt)
        
    cnx.commit()
    
if __name__ == '__main__':
    try:
        main()
    except Exception as err:
        print(err)
        