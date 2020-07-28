'''
Created on 6 янв. 2020 г.

@author: Александр
'''
import csv
import re
    

f_issuers = open('issuerscodes.txt','wt', encoding='utf-8')

issuers = dict()

lmax = 0

with open(r'C:\Users\Александр\Desktop\SEM21\ListingSecurityList.csv', 'rt', newline='') as seclist:     
    csvreader = csv.DictReader(seclist, delimiter=';')
    
    for row in csvreader:                        
        reg_num = re.findall(r'\d{5}-[A-Z]|[A-Z]{3}[01]$|RMFS', row['REGISTRY_NUMBER'])
        
        if len(reg_num) == 0:
            reg_num = re.findall(r'\d{5}B', row['REGISTRY_NUMBER'])
                
        if len(reg_num) == 0:
            continue
                                
        issuers[reg_num[0]] = (row['INN'], row['EMITENT_FULL_NAME'])        
        
        lmax = max(lmax, len(reg_num[0]))

for k, v in issuers.items():    
    f_issuers.write("{0}\t{1} {2}\n".format(k, v[0], v[1]))

print('Max length: ', lmax)