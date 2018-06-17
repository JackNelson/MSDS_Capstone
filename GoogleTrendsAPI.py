from pytrends.request import TrendReq
import pandas as pd

def normalizedMerge(df, tmp, kw_list):
    #casting keyword as string instead of list of strings
    key = kw_list[0]
    
    #normalize values based on df with the larger value in overlapping date
    if df[key].iloc[-1] < tmp[key].iloc[0]:
        tmp[key] *= (float(df[key].iloc[-1]) / float(tmp[key].iloc[0]))
    else:
        df[key] *= (float(tmp[key].iloc[0]) / float(df[key].iloc[-1]))
    
    #union dataframes together drop overlapping dates
    df = df.append(tmp).reset_index().drop_duplicates('date').set_index('date')
    #cast values as ints
    df[key] = df[key].astype(int)
    #remove zeros due to rounding, minimum value is 1
    df.loc[df[key]==0, key] = 1
    
    return df

def pytrendsOverTimeQuery(pytrends, kw_list, cat=0, timeframe='today 5-y', geo='', gprop=''):
    #building query parameters for pytrends
    pytrends.build_payload(kw_list=kw_list, cat=cat, timeframe=timeframe, geo=geo, gprop=gprop)

    return pytrends.interest_over_time()

def extendedDailyPytrendsOverTimeQuery(kw_list, start, end):
    #initialize pytrends objects for querying
    pytrends = TrendReq(hl='en-US')
    
    #assign first query time window
    tmp_start = start
    tmp_end = str(pd.to_datetime(start) + pd.Timedelta(250, unit='D'))[0:10]
    
    while pd.to_datetime(tmp_end) < pd.to_datetime(end):
        #build timeframe window string and query trend values over time
        timeframe = tmp_start + ' ' + tmp_end
        tmp_df = pytrendsOverTimeQuery(pytrends=pytrends, kw_list=kw_list, timeframe=timeframe)
        
        #normalize and union query results unless first query in while loop
        if tmp_start != start:
            df = normalizedMerge(df=df, tmp=tmp_df, kw_list=kw_list)
        else:
            df = tmp_df
        
        #move window forward, overlapping previous window by one day
        tmp_start = tmp_end
        tmp_end = str(pd.to_datetime(tmp_end) + pd.Timedelta(250, unit='D'))[0:10]
    
    #build final timeframe window string and query trend values over time
    timeframe = tmp_start + ' ' + end
    tmp_df = pytrendsOverTimeQuery(pytrends=pytrends, kw_list=kw_list, timeframe=timeframe)
    #normalize and union final query results
    df = normalizedMerge(df=df, tmp=tmp_df, kw_list=kw_list)
    
    return df
