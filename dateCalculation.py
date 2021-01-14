import pandas as pd
from datetime import datetime
from datetime import timedelta



def ytd_table (datelist):
    year_to_date = pd.DataFrame ({'Date': [], 'YTD': [], 'YTD_PY': []  })

    for date_s in datelist:
        if (date_s.month == 1 and date_s.day == 1):
            year_start = date_s
        else:
            year_start = date_s - pd.tseries.offsets.YearBegin()
        ytd_aux = pd.DataFrame({'YTD': pd.date_range(start = year_start, end = date_s)})
        ytd_py_aux = pd.DataFrame({'YTD_PY': pd.date_range(start = year_start.replace(year=year_start.year-1), end = date_s.replace(year=date_s.year-1))})
        date_string = str(date_s)
        date_column = pd.DataFrame({'Date': [date_string]*len(ytd_aux)})
        date_column = date_column.astype('datetime64')
        year_to_date_aux = pd.concat ([date_column, ytd_aux, ytd_py_aux], axis = 1)
        year_to_date = year_to_date.append(year_to_date_aux)
    return year_to_date

def mtd_table (datelist):

    month_to_date = pd.DataFrame ({'Date': [], 'MTD': [], 'MTD_PY': []  })

    for date_s in datelist:
        if (date_s.day == 1):
            month_start = date_s
        else:
            month_start = date_s - pd.tseries.offsets.MonthBegin()
        mtd_aux = pd.DataFrame({'MTD': pd.date_range(start = month_start, end = date_s)})
        mtd_py_aux = pd.DataFrame({'MTD_PY': pd.date_range(start = month_start.replace(year=month_start.year-1), end = date_s.replace(year=date_s.year-1))})
        date_string = str(date_s)
        date_column = pd.DataFrame({'Date': [date_string]*len(mtd_aux)})
        date_column = date_column.astype('datetime64')
        month_to_date_aux = pd.concat ([date_column, mtd_aux, mtd_py_aux], axis = 1)
        month_to_date = month_to_date.append(month_to_date_aux)
    return month_to_date
        
def pd_table (datelist):
    previous_day = pd.DataFrame({'Date': datelist})
    previous_day['PD'] = previous_day['Date'] - timedelta(days=1)
    return previous_day

def py_table (datelist):
    # Faster but not that much... prefer the new method
    #datepy = datelist.to_pydatetime() #Convert to nparray as datetime index does not support mutable operations
    #for a in range(datepy.size): #replace values for previous year
    #    datepy[a] = datepy[a].replace(year = datepy[a].year-1)
    #   
    #previous_year = pd.DataFrame({'Date': datelist, 'PY': datepy})
    previous_year = pd.DataFrame({'Date': datelist})
    previous_year['PY'] = previous_year['Date'] - pd.tseries.offsets.DateOffset(years=1)
    return previous_year

def pm_table (datelist):
    previous_month = pd.DataFrame({'Date': datelist})
    previous_month['PM'] = previous_month['Date'] - pd.tseries.offsets.DateOffset(months=1)
    return previous_month

def last_december_table (datelist):
    last_december = pd.DataFrame({'Date': datelist})
    last_december['LD'] = last_december['Date'] + pd.tseries.offsets.DateOffset(month=12) + pd.tseries.offsets.DateOffset(day=31) - pd.tseries.offsets.DateOffset(years=1)
    return last_december

def previous_month_LD_table (datelist):
    previous_month_LD = pd.DataFrame({'Date': datelist})
    previous_month_LD['PM_LD'] = previous_month_LD['Date'] + pd.tseries.offsets.DateOffset(day=1) - pd.tseries.offsets.DateOffset(days=1)
    return previous_month_LD



if __name__=='__main__':
    #datelist = pd.date_range(end = pd.datetime.date(pd.datetime.today()), periods=100)
    datelist = pd.date_range(end = pd.datetime.date(datetime(2019,12,31)), periods=730)
    ytd = ytd_table (datelist)
    mtd = mtd_table (datelist)
    pday = pd_table (datelist)
    pyear = py_table (datelist)
    pmonth = pm_table (datelist)
    december = last_december_table (datelist)
    pmonth_last_day = previous_month_LD_table (datelist)
    print (pmonth_last_day)

    #export_csv = mtd.to_csv (r'C:\Users\dgoncalves\DateCalculations\Code\export_dataframe.csv', index = None, header=True) 

    