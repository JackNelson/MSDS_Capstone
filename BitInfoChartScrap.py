from requests import get
from bs4 import BeautifulSoup
import pandas as pd
import re

def extractBitInfoChart(metric, crypto):
    #build url string and pull source code off http request
    url = 'https://bitinfocharts.com/comparison/%s-%s.html' % (metric, crypto)
    url_source = get(url).content
    
    #convert source code string to parsed object
    soup = BeautifulSoup(url_source, 'html.parser')
    
    #pulling html tag block with chart data content
    table_script_block = soup.find_all('script')[5]
    #parsing to get only script line with chart data
    table_script = [x for x in str(table_script_block).split(";") if 'new Date' in x]
    #parsing string of chart data to individual row elements
    table_elements = table_script[0].split('],[')
    table_elements = table_elements[:-1] #subset to remove today
    
    metric_dict = {'tweets':'\d+', 'price':'\d+\.\d+|\d+'}
    
    #extracting meaningful data with each row element string
    regex = re.compile("new Date\(\"(.*)\"\)\,(%s|null)" % metric_dict[metric])
    table_elements_clean = [regex.search(x).group(0) for x in table_elements]
    
    #extracting date and value from each row elements into their own lists 
    reg_key = re.compile("(\d{4}\/\d{2}\/\d{2})")
    reg_value = re.compile("(%s|null)$" % metric_dict[metric])

    keys = [reg_key.search(x).group(0) for x in table_elements]
    values = [reg_value.search(x).group(0) for x in table_elements if reg_value.search(x)]
    
    #converting lists into pd.Series with proper date index
    data = pd.Series(values,index=pd.to_datetime(keys))
    
    return data