# ===========================================================

def add_months(base_ym, _interval):
    '''
    Return str(date)(* format: 'YYYYMM') plus _interval months 
    
    Parameters 
    ----------
    base_ym : str or integer value with 6-Digits (* eg.: '202012' or 202012)\
    
    _interval : integer. 
    
    Return 
    ---------
    added_ym : the date plus _interval 
    
    Examples
    ---------
    >>> add_months('202012', 7)
    '202107'
    
    >>> add_months(base_ym='202012', _interval=-18)
    '201906'
    '''
    
    _YYYY = int(str(base_ym)[:4])           # ex: 2020
    _MM = int(str(base_ym)[-2:])            # ex: 12

    diff_y, diff_m = divmod(_interval, 12)  # ex: -2, 6

    added_y = _YYYY + diff_y                # ex: 2020 - 2 = 2018
    added_m = _MM + diff_m                  # ex: 12 + 6   = 18 

    if added_m > 12:                        # ex: if 18 > 12 :
        added_y += 1                        # ex: 2018 + 1 = 2019
        added_m -= 12                       # ex: 18 - 12  = 6 
    
    added_ym = str(added_y)+str(added_m).rjust(2,'0') 
    return(added_ym)                        # ex: '201906'

# ===========================================================