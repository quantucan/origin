'''
Created on 5 апр. 2020 г.

@author: Александр
'''
from datetime import date

class BondRecord:
    def __init__(self, *args):
        if len(args) == 1 and len(args[0]) == 5:
            self._sid = args[0][0] #bond id
            self._cid = args[0][1] #coupon id
            self._coupondate = args[0][2] #coupon date
            self._pid = args[0][3] #putoption id
            self._buybackdate = args[0][4] #putoption date
        else:
            self._sid = None
            self._cid = None
            self._coupondate = None
            self._pid = None
            self._buybackdate = None
        
        return
     
    def __eq__(self, other):
        if (self.sid == other.sid and 
            self.cid == other.cid and
            self.coupondate == other.coupondate and
            self.pid == other.pid and
            self.buybackdate == other.buybackdate):
            
            return True
        else:
            return False 
    
    @property
    def sid(self):
        return self._sid
    
    @property
    def cid(self):
        return self._cid

    @property
    def coupondate(self):
        return self._coupondate
    
    @property
    def pid(self):
        return self._pid
    
    @property
    def buybackdate(self):
        return self._buybackdate

    @sid.setter
    def sid(self, value):
        self._sid = value
    
    @cid.setter
    def cid(self, value):
        self._cid = value

    @coupondate.setter
    def coupondate(self, value):
        self._coupondate = value

    @pid.setter
    def pid(self, value):
        self._pid = value

    @buybackdate.setter
    def buybackdate(self, value):
        self._buybackdate = value
    
    def copy(self):
        return BondRecord((self.sid, self.cid, self.coupondate, self.pid, self.buybackdate))
    
    def bond_print(self):
        return str('{}, {}, {}, {}, {}').format(self.sid, self.cid, self.coupondate, self.pid, self.buybackdate)
    
class BoardRecord:
    def __init__(self, *args):
        if len(args) == 1 and len(args[0]) == 2:
            self._boardid = args[0][0] #id from database
            self._settlecode = args[0][1] #settlement code
        else:
            self._boardid = None
            self._settlecode = None
        
        return

    @property
    def boardid(self):
        return self._boardid
    
    @property
    def settlecode(self):
        return self._settlecode
    
    @boardid.setter
    def boardid(self, value):
        self._boardid = value
    
    @settlecode.setter
    def settlecode(self, value):
        self._settlecode = value

class HolidayWeekDays:
    def __init__(self):
        self._dates = (date(2019, 1, 1),
                    date(2019, 1, 2),
                    date(2019, 1, 7),
                    date(2019, 3, 8),
                    date(2019, 5, 1),
                    date(2019, 5, 9),
                    date(2019, 6, 12),
                    date(2019, 11, 4),
                    date(2019, 12, 31),
                    date(2020, 1, 1),
                    date(2020, 1, 2),
                    date(2020, 1, 7),
                    date(2020, 2, 24),
                    date(2020, 3, 9),
                    date(2020, 5, 1),
                    date(2020, 5, 11),
                    date(2020, 6, 12),
                    date(2020, 6, 24),
                    date(2020, 7, 1),
                    date(2020, 11, 4))
    
    @property
    def dates(self):
        return self._dates