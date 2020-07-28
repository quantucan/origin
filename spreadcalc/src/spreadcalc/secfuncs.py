'''
Created on 9 янв. 2020 г.

@author: Александр
'''
import re

def issuer_code(registry_number):
    i_code = re.findall(r'\d{5}-[A-Z]|[A-Z]{3}[01]$|RMFS', registry_number)
        
    if len(i_code) == 0:
        i_code = re.findall(r'\d{5}B', registry_number)
                
    if len(i_code) == 0:
        return ""
    
    return i_code[0]
