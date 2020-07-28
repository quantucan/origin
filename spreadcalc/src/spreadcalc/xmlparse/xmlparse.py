'''
Created on 8 янв. 2020 г.

@author: Александр
'''
from lxml import etree
from spreadcalc import secfuncs

f_quotes = open('secquotes.txt', 'wt', encoding='utf-8')

sem21 = etree.parse(r'C:\Users\Александр\Desktop\SEM21\MM00001_SEM21_00T_271219_004864526.xml')

#main_board = sem21.xpath(r'descendant-or-self::node()/child::SEM21/child::BOARD[attribute::BoardType="MAIN"]')
sec_records = sem21.xpath(r'/MICEX_DOC/SEM21/BOARD[@BoardType="MAIN"and(@BoardId="EQOB"or@BoardId="TQCB"or@BoardId="TQOB")]/RECORDS[@EngType="bn"and@FaceUnit="RUR"and@RegNumber!=""and(@MatDate)]')

for r in sec_records:        
    waPrice =  r.get('WAPrice')
    if waPrice is None:
        waPrice = ''
    
    yieldWAP = r.get('YieldAtWAP')
    if yieldWAP is None:
        yieldWAP = ''         
        
    f_quotes.write("{0:>8}  {1:>12.2f}  {2:>12}  {3:>8}  {4:>7}  {5}\n".format(secfuncs.issuer_code(r.attrib['RegNumber']), float(r.attrib['Value']), r.attrib['MatDate'], waPrice, yieldWAP, r.attrib['SecShortName']))    