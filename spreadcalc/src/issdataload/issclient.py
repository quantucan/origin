'''
Created on 28 янв. 2020 г.

@author: Александр
'''
from issdataload.bondrecord import BondRecord, BoardRecord, HolidayWeekDays
from urllib import request  
from http import cookiejar
import ssl
import json
import re
import mysql.connector
import datetime
import os

class MoexISSClient:
    _moex_auth_url = 'https://passport.moex.com/authenticate:443'
    _hwd = HolidayWeekDays()
    #_non_tqcb_bonds = ('RU000A0ZZ8A2', )
    
    def __init__(self, user = '', password = ''):
        self.pswd_mgr = request.HTTPPasswordMgrWithDefaultRealm()
        self.pswd_mgr.add_password(None, self._moex_auth_url, user, password)
        self.auth_handler = request.HTTPBasicAuthHandler(self.pswd_mgr)
        
        self.ctx = ssl.create_default_context(purpose = ssl.Purpose.SERVER_AUTH)
        self.https_handler = request.HTTPSHandler(self.ctx) 
        
        self._coo_jar = cookiejar.CookieJar()
        self.cookie_processor = request.HTTPCookieProcessor(self._coo_jar)
        
        self.iss_opener = request.build_opener()
        #self.iss_opener.add_handler(self.auth_handler)
        #self.iss_opener.add_handler(self.https_handler) 
        self.iss_opener.add_handler(self.cookie_processor)
        
        #Connect to bondinfo database
        self._connect()
                
        #Fetch boards and settlement codes from database
        self._boards_dict = self._fetch_boards_records('MAIN')        
        
        log_dir = os.path.expanduser('~Александр\\issdataload\\') 
        if not os.path.isdir(log_dir):
            os.mkdir(log_dir)
        self._f_log = open(os.path.join(log_dir, 'update.log'), 'at', encoding='utf-8')        
            
        return
    
    def _connect(self, user='root', password='admiN123', database='bondinfo', host='127.0.0.1', port=3306):
        self.cnx = mysql.connector.connect(user=user, password=password, database=database, host=host, port=port)        
        
        self.db_cursor = self.cnx.cursor()
        
        return 
    
    def load_end_of_day(self, tradedate, url='http://iss.moex.com/iss/history/engines/stock/markets/bonds/sessions/1/securities.json'):
        #Fetch all securities from database
        bonds_dict = self._fetch_bonds_records(tradedate, False)
                
        jdata = list()
        history_len = 1
        firstpass = True
            
        y1date = self._get_settlement_date(tradedate, 'Y1')
        
        while len(jdata) < history_len:
            req = request.Request(url + '?date=' + datetime.date.isoformat(tradedate) + '&start=' + str(len(jdata)), None, headers={'User-Agent' : 'Chrome/51.0'})
            
            try:
                issconn = self.iss_opener.open(req)
                
                jresult = json.loads(issconn.read())
                
                jhistory = jresult['history']
                
                if firstpass:
                    history_len = jresult['history.cursor']['data'][0][1]
                
                    jcols = jhistory['columns']
                    tmp_dict = dict(enumerate(jcols))
                    #swap keys and values
                    c = dict((v,k) for k,v in tmp_dict.items())
                
                    firstpass = False
            
                jdata_inc = jhistory['data']
                jdata.extend(jdata_inc)
            
            except Exception as err:
                print(err)
                break            
            
        self._f_log.write('{:%d-%m-%Y %H:%M:%S} Tradedate {}: {} quotes loaded out of {}\n'.format(datetime.datetime.now(), tradedate, len(jdata), history_len))
        
        if len(jdata) < history_len:
            return False
        elif history_len == 0:
            return True        
    
        inserted_quotes = 0        
            
        for jrec in jdata:
            board_rec = self._boards_dict.get(jrec[c['BOARDID']], None)            
            
            if board_rec is None:
                continue
            
            #Check for duplicated quotes for the same bond on different boards
            #if jrec[c['BOARDID']] == 'TQCB' and jrec[c['SECID']] in self._non_tqcb_bonds:
            #    continue
            
            try:
                vol = int(jrec[c['VOLUME']])
                if vol > 4294967295:
                    vol = 0
                    self._f_log.write('{:%d-%m-%Y} volume for bond {}, {} = {}\n'.format(tradedate, jrec[c['SHORTNAME']], jrec[c['SECID']], jrec[c['VOLUME']])) 
            except:
                vol = 0
            
            bond_rec = bonds_dict.pop(jrec[c['SECID']], None)                
                
            updated_bond_rec = self._update_bond_record(bond_rec, jrec, c)
            
            if updated_bond_rec is not None:
                #set settlement date
                if board_rec.settlecode == 'Y1':
                    settledate = y1date
                elif board_rec.settlecode == 'T0':
                    settledate = tradedate

                ins_quote_stmt = ("INSERT INTO quotes (tradedate, settledate, secid, putoption_id, boardid, setlcurrency, numtrades, volume, value, open, low, high, close, lowoffer, highbid, waprice, yieldwap, yieldclose, accint, duration, facevalue)"
                                  "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
                
                quote_data = (tradedate, settledate, updated_bond_rec.sid, updated_bond_rec.pid, board_rec.boardid, jrec[c['CURRENCYID']], jrec[c['NUMTRADES']], 
                              vol, jrec[c['VALUE']], jrec[c['OPEN']], jrec[c['LOW']], jrec[c['HIGH']], jrec[c['CLOSE']], None, None, 
                              jrec[c['WAPRICE']], jrec[c['YIELDATWAP']], jrec[c['YIELDCLOSE']], jrec[c['ACCINT']], jrec[c['DURATION']], jrec[c['FACEVALUE']])
            
                try:
                    self.db_cursor.execute(ins_quote_stmt, quote_data)
                except Exception as err:
                    msg = '{:%d-%m-%Y %H:%M:%S} MySQL exception: {}\n'.format(datetime.datetime.now(), err) 
                    self._f_log.write(msg) 
                    print(msg)
                                        
                    self.cnx.rollback()
                    
                    return False
                
                inserted_quotes += self.db_cursor.rowcount
            
        self._f_log.write('{:%d-%m-%Y %H:%M:%S} {} quotes inserted out of {}\n'.format(datetime.datetime.now(), inserted_quotes, history_len))
        
        self.cnx.commit()        
            
        return True            
        
    def update_bonds_info(self, url = 'http://iss.moex.com/iss/engines/stock/markets/bonds/securities.json'):                                
        d = datetime.date.today()
        self._f_log.write('Updating bondinfo database...{:%d-%m-%Y}\n'.format(d))        
        #Fetch all securities from database
        bonds_dict = self._fetch_bonds_records(d, True)

        created_bonds = list()
        coupons_updated = list()
        putoptions_updated = list()
        
        last_moex_secid=''
        unchanged_bonds_count = 0
        absent_bonds_count = 0        
        mark_matured_count = 0        
                                
        req = request.Request(url, None, headers={'User-Agent' : 'Chrome/51.0'})            
        issconn = self.iss_opener.open(req)
            
        jresult = json.loads(issconn.read())
            
        jsec = jresult['securities']
            
        jcols = jsec['columns']
        tmp_dict = dict(enumerate(jcols))
        #swap keys and values
        c = dict((v,k) for k,v in tmp_dict.items())   
            
        jdata = jsec['data']
                                    
        for jrec in jdata:                
            if jrec[c['BOARDID']] not in self._boards_dict:
                continue
            
            last_moex_secid = jrec[c['SECID']]                 
                
            bond_rec = bonds_dict.pop(last_moex_secid, None)                
                
            updated_bond_rec = self._update_bond_record(bond_rec, jrec, c)
                
            if bond_rec is None:
                if updated_bond_rec is None:
                    #absent_bonds[last_moex_secid] = (last_moex_secid, jrec[c['SHORTNAME']], jrec[c['FACEUNIT']])
                    absent_bonds_count += 1
                else:
                    created_bonds.append(updated_bond_rec.sid)
                    if updated_bond_rec.cid is not None:
                        coupons_updated.append(updated_bond_rec.cid)
                    if updated_bond_rec.pid is not None:
                        putoptions_updated.append(updated_bond_rec.pid)
            else:
                if updated_bond_rec == bond_rec:
                    unchanged_bonds_count += 1
                else:
                    if updated_bond_rec.cid != bond_rec.cid:
                        coupons_updated.append(updated_bond_rec.cid)
                    if updated_bond_rec.pid != bond_rec.pid:
                        putoptions_updated.append(updated_bond_rec.pid)                       
                    
        if len(bonds_dict) > 0:
            mark_matured_count = self._mark_matured_bonds(bonds_dict)
                
        self.cnx.commit()
        
        d = datetime.datetime.now()
        self._f_log.write('{:%d-%m-%Y %H:%M:%S} Records processed: {}\n'.format(d, len(jdata)))        
        if len(created_bonds) > 0:
            self._f_log.write('{0:%d-%m-%Y %H:%M:%S} Bonds created: {1} (id {2}..{3})\n'.format(d, len(created_bonds), created_bonds[0], created_bonds[-1]))
        if len(coupons_updated) > 0:
            self._f_log.write('{0:%d-%m-%Y %H:%M:%S} Coupons updated: {1} (id {2}..{3})\n'.format(d, len(coupons_updated), coupons_updated[0], coupons_updated[-1]))
        if len(putoptions_updated) > 0:
            self._f_log.write('{0:%d-%m-%Y %H:%M:%S} Putoptions updated: {1} (id {2}..{3})\n'.format(d, len(putoptions_updated), putoptions_updated[0], putoptions_updated[-1]))
        self._f_log.write('{:%d-%m-%Y %H:%M:%S} Bonds unchanged: {}\n'.format(d, unchanged_bonds_count))
        self._f_log.write('{:%d-%m-%Y %H:%M:%S} Bonds marked as matured: {}\n'.format(d, mark_matured_count))
        self._f_log.write('{:%d-%m-%Y %H:%M:%S} Eurobonds: {}\n'.format(d, absent_bonds_count))        
        
        return        
    
    def _update_bond_record(self, bond_rec, jrec, c):                                                                                
        buybackdate_not_found = False
        coupondate_not_found = False
        
        if bond_rec is None:            
            updated_bond_rec = self._create_bond_record(jrec[c['SECID']])
            
            if updated_bond_rec is None:
                return None
        else:                
            updated_bond_rec = bond_rec.copy()
        
        #Update buybackdate 
        if 'BUYBACKDATE' in c:
            valid_buybackdate = self._get_valid_date(jrec[c['BUYBACKDATE']])
            
            if valid_buybackdate is not None:                
                if updated_bond_rec.buybackdate is not None and valid_buybackdate != updated_bond_rec.buybackdate:
                    #assume no valid buybackdate in database records 
                    buybackdate_not_found = True
                    self.db_cursor.execute("SELECT id, buybackdate FROM putoptions1 WHERE secid = %s", (updated_bond_rec.sid, ))                
                
                    for row in self.db_cursor:
                        #try to find valid buybackdate in database records
                        if valid_buybackdate == row[1]:
                            buybackdate_not_found = False
                
                if updated_bond_rec.buybackdate is None or buybackdate_not_found:            
                    valid_offerdate = self._get_valid_date(jrec[c['OFFERDATE']])
                    #putoptions1                    
                    self.db_cursor.execute("INSERT INTO putoptions1 (secid, buybackdate, offerdate) VALUES (%s, %s, %s)", (updated_bond_rec.sid, valid_buybackdate, valid_offerdate))            
            
                    if self.db_cursor.lastrowid is not None:
                        updated_bond_rec.pid = self.db_cursor.lastrowid 
                        updated_bond_rec.buybackdate = valid_buybackdate

        #Update couponinfo      
        if 'NEXTCOUPON' in c:
            valid_coupondate = self._get_valid_date(jrec[c['NEXTCOUPON']])
            
            if valid_coupondate is not None:
                if updated_bond_rec.coupondate is not None and valid_coupondate != updated_bond_rec.coupondate:
                    #assume no valid coupondate in database records
                    coupondate_not_found = True
                    self.db_cursor.execute("SELECT id, coupondate FROM coupons1 WHERE secid = %s", (updated_bond_rec.sid, ))
                    
                    for row in self.db_cursor:
                        #try to find valid coupondate in database records
                        if valid_coupondate == row[1]:
                            coupondate_not_found = False
                
                if updated_bond_rec.coupondate is None or coupondate_not_found:            
                    #coupons1
                    self.db_cursor.execute("INSERT INTO coupons1 (secid, coupondate, couponperiod, couponpercent, couponvalue) VALUES (%s, %s, %s, %s, %s)", (updated_bond_rec.sid, valid_coupondate, jrec[c['COUPONPERIOD']], jrec[c['COUPONPERCENT']], jrec[c['COUPONVALUE']]))
            
                    if self.db_cursor.lastrowid is not None:
                        updated_bond_rec.cid = self.db_cursor.lastrowid 
                        updated_bond_rec.coupondate = valid_coupondate
                
        return updated_bond_rec
                
    def _create_bond_record(self, moex_secid):
        new_bond_rec = BondRecord()
        
        insert_flag = False
        
        regnumber = None
        emitent_seccode = None        
        early_r = 0
        issuesize = None
        matdate = None
        is_matured = 0
        bond_type = 1
        issuer_type = 5
        
        url = 'http://iss.moex.com/iss/securities/{}.json'.format(moex_secid)
        
        req = request.Request(url, None)
            
        issconn = self.iss_opener.open(req)
            
        jresult = json.loads(issconn.read())
        
        jdes = jresult['description']
        
        jdata = jdes['data']
        
        #Create and fill name-value dictionary
        bonddes_dict = dict()        
        for jrec in jdata:
            bonddes_dict[jrec[0]] = jrec[2]
            
        if bonddes_dict['ISIN'][0:2] != 'RU' or bonddes_dict['FACEUNIT'] != 'SUR':
            return None
        
        if 'EMITTER_ID' in bonddes_dict:
            moex_emitent_id = bonddes_dict['EMITTER_ID']            
        else:
            return None            
                    
        if 'REGNUMBER' in bonddes_dict:
            regnumber = bonddes_dict['REGNUMBER']
            emitent_seccode = self._get_emitent_seccode(bonddes_dict['REGNUMBER'])            

        if 'EARLYREPAYMENT' in bonddes_dict:
            early_r = bonddes_dict['EARLYREPAYMENT']
            
        if 'ISSUESIZE' in bonddes_dict:
            issuesize = bonddes_dict['ISSUESIZE']            
                                
        if 'MATDATE' in bonddes_dict:
            matdate = self._get_valid_date(bonddes_dict['MATDATE'])
            if matdate < datetime.date.today():
                is_matured = 1        
        
        #emitents
        self.db_cursor.execute("SELECT id FROM emitents WHERE moex_emitent_id = %s", (moex_emitent_id, ))
        row = self.db_cursor.fetchone()        
        if self.db_cursor.rowcount > 0:
            emitent_id = row[0]
        else:                         
            if emitent_seccode is None:
                insert_flag = True
            else:
                self.db_cursor.execute("SELECT id FROM emitents WHERE emitent_code = %s", (emitent_seccode, ))
                row = self.db_cursor.fetchone()
                if self.db_cursor.rowcount > 0:
                    emitent_id = row[0]
                    self.db_cursor.execute("UPDATE emitents SET moex_emitent_id = %s WHERE id = %s", (moex_emitent_id, emitent_id))
                else:
                    insert_flag = True
            
        if insert_flag:            
            #emitents
            self.db_cursor.execute("INSERT INTO emitents (emitent_code, moex_emitent_id, issuer_type) VALUES (%s, %s, %s)", (emitent_seccode, moex_emitent_id, issuer_type))
            
            if self.db_cursor.lastrowid is not None:                
                emitent_id = self.db_cursor.lastrowid                                
            else:
                return None
        
        #bonds1
        ins_stmt = ("INSERT INTO bonds1 "
                    "(shortname, moex_secid, regnumber, isin, emitent_id, initialfacevalue, faceunit, matdate, earlyrepayment, issuesize, is_matured, bond_type) "
                    "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)")
        
        sec_data = (bonddes_dict['SHORTNAME'], bonddes_dict['SECID'], regnumber, bonddes_dict['ISIN'], emitent_id, bonddes_dict['INITIALFACEVALUE'], bonddes_dict['FACEUNIT'], 
                    matdate, early_r, issuesize, is_matured, bond_type)
                        
        self.db_cursor.execute(ins_stmt, sec_data)
        
        if self.db_cursor.lastrowid is not None:
            new_bond_rec.sid = self.db_cursor.lastrowid            
                                                
            return new_bond_rec
        else:
            return None
            
    def _fetch_bonds_records(self, trdate, filter_matured):            
        bonds_dict = dict()
                
        sel_stmt = ("SELECT s.moex_secid, s.id, c.id, c.coupondate, p.id, p.buybackdate FROM bonds1 AS s "
                    "LEFT JOIN (SELECT c1.id, c1.secid, c1.coupondate FROM coupons1 AS c1 INNER JOIN (SELECT cmin.secid AS secid, MIN(cmin.coupondate) AS min_ccd FROM coupons1 AS cmin WHERE cmin.coupondate >= %s GROUP BY cmin.secid) AS c2 ON c1.secid = c2.secid AND c1.coupondate = c2.min_ccd) AS c ON s.id = c.secid "
                    "LEFT JOIN (SELECT p1.id, p1.secid, p1.buybackdate FROM putoptions1 AS p1 INNER JOIN (SELECT pmin.secid AS secid, MIN(pmin.buybackdate) AS min_bbd FROM putoptions1 AS pmin WHERE pmin.buybackdate >= %s GROUP BY pmin.secid) AS p2 ON p1.secid = p2.secid AND p1.buybackdate = p2.min_bbd) AS p ON s.id = p.secid")                     
        
        if filter_matured:
            sel_stmt += " WHERE s.is_matured = 0"
        
        self.db_cursor.execute(sel_stmt, (trdate, trdate))        
        
        for row in self.db_cursor:            
            bond_rec = BondRecord(row[1:])
            bonds_dict[row[0]] = bond_rec        
                
        return bonds_dict
    
    def _fetch_boards_records(self, boardtype):
        boards_dict = dict()
        
        self.db_cursor.execute("SELECT b.boardid, b.id, b.settlecode FROM boards AS b WHERE b.boardtype = %s", (boardtype, ))
        
        for row in self.db_cursor:
            board_rec = BoardRecord(row[1:])
            boards_dict[row[0]] = board_rec
            
        return boards_dict
    
    def _get_valid_date(self, jdate):
        d = jdate
        
        if d is None or d == '0000-00-00':
            return None
        else:            
            return datetime.date.fromisoformat(d)
    
    def _get_settlement_date(self, tradedate, settlecode):        
        s_date = tradedate
        flag = True 
        
        if settlecode == 'Y1':
            while flag:
                if s_date.isoweekday() == 5:
                    s_date += datetime.timedelta(days = 3)
                else:
                    s_date += datetime.timedelta(days = 1)
                
                if s_date not in self._hwd.dates:
                    flag = False        
            
        return s_date

    def _get_emitent_seccode(self, reg_number):        
        if reg_number is None or len(reg_number) == 0:
            return None
        
        i_code = re.findall(r'\d{5}-[A-Z]|[A-Z]{3}[01]$|RMFS', reg_number)

        if len(i_code) == 0:
            i_code = re.findall(r'\d{5}B', reg_number)
        
        if len(i_code) == 0:
            return None
        
        return i_code[0]
    
    def _mark_matured_bonds(self, matured_bonds):
        matured_to_date = list((datetime.date.today(), v.sid) for v in matured_bonds.values())
        
        self.db_cursor.executemany("UPDATE bonds1 SET is_matured = TRUE WHERE matdate < %s AND id = %s", matured_to_date)
        
        return self.db_cursor.rowcount                
    
    def check_iss_moex(self, url='http://iss.moex.com/iss/securities.json?'):                        
        req = request.Request(url, None)
            
        conn = self.iss_opener.open(req)
            
        for c in self._coo_jar:
            print('Cookie name: ', c.name)
                
        f = open('some_page.txt', 'wt', encoding="utf-8")
            
        try:
            jres = json.loads(conn.read())
            print(len(jres))
        except json.JSONDecodeError as err:
            print(err)        
            
        jsec = jres['securities']
            
        jcols = jsec['columns']
            
        jdata = jsec['data']
        print('Records: {}'.format(len(jdata)))
            
        for c in jcols:                
            f.write(str(c) + '\n')
            
        return
    
    def _duplicate_boards(self, tradedate, url='http://iss.moex.com/iss/history/engines/stock/markets/bonds/sessions/1/securities.json'):
        jdata = list()
        history_len = 1
        firstpass = True        
        
        while len(jdata) < history_len:
            req = request.Request(url + '?date=' + datetime.date.isoformat(tradedate) + '&start=' + str(len(jdata)), None, headers={'User-Agent' : 'Chrome/51.0'})
            
            try:
                issconn = self.iss_opener.open(req)
                
                jresult = json.loads(issconn.read())
                
                jhistory = jresult['history']
                
                if firstpass:
                    history_len = jresult['history.cursor']['data'][0][1]
                
                    firstpass = False
            
                jdata_inc = jhistory['data']
                jdata.extend(jdata_inc)
            
            except Exception as err:
                print(err)
                break
            
        secids = dict()        
        
        for jrec in jdata:                        
            sid = jrec[3]
            boardid = jrec[0]
            
            if boardid not in self._boards_dict:
                continue
            
            if sid in secids:
                secids[sid].append(boardid)                
            else:
                secids[sid] = [boardid]
                             
        for k in secids:
            if len(secids[k]) > 1:
                print("'{}',".format(k))
        
        return            
    
    def load_emitents_info(self, cnx, url = 'http://iss.moex.com/iss/securities.json?securities.columns=shortname,regnumber,isin,emitent_id,emitent_inn,emitent_title&engine=stock&market=bonds&is_trading=1'):
                        
        cursor = cnx.cursor()
        
        f = open('eminfo.txt', 'wt', encoding="utf-8")
        
        start_pos = 0
        cnt = 1        
        last_isin = ''
        issuer_type = 5
        
        emitents_dict = dict()
        
        while cnt > 0:            
            req = request.Request(url+'&start='+str(start_pos), None)
            
            conn = self.iss_opener.open(req)
            
            jres = json.loads(conn.read())
            
            jsec = jres['securities']
            
            jcols = jsec['columns']
            tmp_dict = dict(enumerate(jcols))
            cols_dict = dict((v,k) for k,v in tmp_dict.items())   
            
            jdata = jsec['data']
                                    
            for r in jdata:                
                if r[cols_dict['isin']] == last_isin:
                    continue
                else:
                    last_isin = r[cols_dict['isin']]
                
                emitent_seccode = self._get_emitent_seccode(r[cols_dict['regnumber']])
                if len(emitent_seccode) is None:
                    continue
                
                emitent_inn = r[cols_dict['emitent_inn']]
                if emitent_inn is None or len(emitent_inn) == 0:
                    continue
                
                emitent_id = r[cols_dict['emitent_id']]
                if emitent_id not in emitents_dict:
                    emitent_stmt = ("INSERT IGNORE INTO emitents (emitent_code, moex_emitent_id, emitent_inn, emitent_title, issuer_type) VALUES (%s, %s, %s, %s, %s)")
                    emitent_data = (emitent_seccode, emitent_id, emitent_inn, r[cols_dict['emitent_title']], issuer_type)                     

                    cursor.execute(emitent_stmt, emitent_data)
                    
                    if cursor.lastrowid != 0:
                        emitents_dict[emitent_id] = cursor.lastrowid
                
                sec_stmt = ("INSERT IGNORE INTO securities (shortname, regnumber, isin, emitent_id) VALUES (%s, %s, %s, %s)")
                sec_data = (r[cols_dict['shortname']], r[cols_dict['regnumber']], r[cols_dict['isin']], emitents_dict[emitent_id])  
                
                cursor.execute(sec_stmt, sec_data)
                
                #self.cnx.commit()
            
            #100 strings per query
            cnt = len(jdata)            
            start_pos += cnt
            print('Records processed: {}'.format(start_pos)) 
        
        cursor.close()
        
        f.write('Distinct emitents found: {}\n'.format(len(emitents_dict)))
        
        for k,v in emitents_dict.items():                        
            f.write('{} : {}\n'.format(k, v))