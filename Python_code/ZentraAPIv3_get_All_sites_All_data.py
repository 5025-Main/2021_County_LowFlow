# -*- coding: utf-8 -*-
"""
Created on Wed May 26 15:44:03 2021

@author: alex.messina
"""

import requests
import json
import pandas as pd
import datetime as dt
import time

startTime = dt.datetime.now()

## GET DATA FUNCTION
def get_zentra_df(url, params, headers, retries=3):
    ## Initial request
    response = requests.get(url, params=params, headers=headers)
    ## If good response [200] then parse data and return the df
    if response.ok:
        print ('Succesful response.')
        content = json.loads(response.content)
        if content['pagination']['page_num_readings']>0:
            df = pd.read_json(content['data'], convert_dates=False, orient='split')
            print (df[['datetime', 'mrid', 'measurement','value', 'units']].head())
            print (df[['datetime', 'mrid', 'measurement','value', 'units']].tail())
        else:
            df = pd.DataFrame()
    ## If no good response [429], need to wait and try again
    else:
        success = False #False til good response
        fails = 1 #set fails to 1 for first loop
        fail_count = retries # 3 retries by default
        while fails <= fail_count and  success==False:
            print ('Failed '+str(fails)+' time(s). '+str(response))
            print ('Waiting to retry...')
            time.sleep(65)
            print ('trying....')
            response = requests.get(url, params=params, headers=headers)
            ## If good response after waiting 65 sec...
            if response.ok:
                success = True
                content = json.loads(response.content)
                if content['pagination']['page_num_readings']>0:
                    df = pd.read_json(content['data'], convert_dates=False, orient='split')
                    print ('Device data: ')
                    print (df[['datetime', 'mrid', 'measurement','value', 'units']].head())
                    print (df[['datetime', 'mrid', 'measurement','value', 'units']].tail())
                else:
                    df = pd.DataFrame()
            else:
                print ('Failed '+str(response))
                pass
            fails +=1 ## count fails up 1 every loop
        ## If 3 tries are all fails raise error
        if fails > fail_count and success==False:
            raise Exception(response)
            print ('Failed after '+str(retries)+' response: '+str(response))
        if success == True:
            print ('Successful after '+str(fails)+' tries')
            pass
    return df, content

### DEVICE
site_device_dict = {'CAR-059':('06-02192',3),
                  'CAR-070':('06-02245',3),
                  'CAR-070E':('06-02351',3),
                  'CAR-072':('06-02301',3),
                  'CAR-072C':('06-02127',3),
                  'CAR-072Q':('06-02190',3),
                  'CAR-072R':('06-02298',3),
                  'SDG-072':('06-02290',3),
                  'SDG-072F':('06-02299',3),
                  'SDG-084':('06-02207',3),
                  'SDG-084J':('06-02230',3),
                  'SDG-085':('06-02293',3),
                  'SDG-085G':('06-01630',3),
                  'SDG-085M':('06-02235',3),
                  'SDR-036':('06-02199',3),
                  'SDR-041':('06-02246',3),
                  'SDR-064':('06-02212',3),
                  'SDR-064A':('06-02229',3),
                  'SDR-098':('06-02203',3),
                  'SDR-127':('06-02204',3),
                  'SDR-130':('06-02198',3),
                  'SDR-203A':('06-02255',2),
                  'SDR-204A':('06-02255',5),
                  'SDR-768':('06-02193',3),
                  'SLR-045':('06-02227',3),
                  'SLR-045A':('06-02210',2),
                  'SLR-045B':('06-02210',5),
                  'SLR-156':('06-02211',3),
                  'SLR-160':('06-02337',3),
                  'SLR-160A':('06-02219',3),
                  'SWT-030':('06-02256',3),
                  'SWT-049':('06-02285',3)}

site_device_dict = {'SLR-045A':('06-02210',2),
                  'SLR-045B':('06-02210',5)}


                    
# Reorder the dictionary    
device_site_dict = {value[0]:([key],value[1]) for key, value in site_device_dict.items()}




### START/END DATES
today = dt.datetime.now()
days_ago = today - dt.timedelta(days=2) ## get data from past 12 days
startdate = days_ago.strftime("%m-%d-%Y")
enddate = today.strftime("%m-%d-%Y")

# or hardcode start/end
startdate = '05-5-2021'
#enddate = '05-14-2021'

## Empty dataframe for data
level_dat = pd.DataFrame(index=pd.date_range(dt.datetime(2021,5,5,0,0),dt.datetime(2021,9,16,0,0),freq='5Min'))
temp_dat = pd.DataFrame(index=pd.date_range(dt.datetime(2021,5,5,0,0),dt.datetime(2021,9,16,0,0),freq='5Min'))
cond_dat = pd.DataFrame(index=pd.date_range(dt.datetime(2021,5,5,0,0),dt.datetime(2021,9,16,0,0),freq='5Min'))
## iterate over loggers, can get 60 loggers in 1 min from API or else throttled and have to wait 1min
for site, (device_sn, port_s) in site_device_dict.items():
    print()
    print ('Site: '+site)
    print ('Device: '+device_sn)
    print ('Port: '+str(port_s))
    
    ### API PARAMS
    token = "Token {TOKEN}".format(TOKEN="5570f23feeb666615003051140cea73ccdb18639")
    url = "https://zentracloud.com/api/v3/get_readings/"
    headers = {'content-type': 'application/json', 'Authorization': token}
    output_format = "df"
    perpage = 2000
    params = {'device_sn': device_sn, 'output_format': output_format,'per_page':2000,'sort_by':'ascending','start_date':startdate,'end_date':enddate}

    device_dat = pd.DataFrame()
    ## FIRST PAGE OF DATA
    df, content = get_zentra_df(url, params=params, headers=headers)
    df['datetime'] = pd.to_datetime(df['datetime'],utc=True).dt.tz_convert(tz='US/Pacific').dt.tz_localize(None) # in case some aren't on PST
    df.index = df['datetime']
    
    device_dat = device_dat.append(df)
    
    ## NEXT PAGE(S)
    length_data = content['pagination']['page_num_readings']
    next_url = content['pagination']['next_url']
    ## loop until a page returns with no data
    while length_data > 0:
        next_df, next_content = get_zentra_df(next_url, params, headers, retries=3)
        
        if next_content['pagination']['page_num_readings'] > 0: ## if it actually returns data, if not df is empty
            next_df['datetime'] = pd.to_datetime(next_df['datetime'],utc=True).dt.tz_convert(tz='US/Pacific').dt.tz_localize(None) # in case some aren't on PST
            next_df.index = next_df['datetime']
            device_dat = device_dat.append(next_df)
            ## update length data and next_url with this page's values
            length_data = next_content['pagination']['page_num_readings']
            next_url = next_content['pagination']['next_url']
        else:
            length_data = 0
    
    ## For each logger, if multiple sites then iterate over sites

    print ('Saving data for...')
    print (site, port_s)
    
    site_data = device_dat[device_dat['port_num']==port_s]
    
    site_level_data = site_data[site_data['measurement']=='Water Level']      [['datetime','mrid', 'measurement','value', 'units']]
    site_temp_data = site_data[site_data['measurement']=='Water Temperature'] [['datetime','mrid', 'measurement','value', 'units']]
    site_cond_data = site_data[site_data['measurement']=='EC']                [['datetime','mrid', 'measurement','value', 'units']]

    level_dat[site+'_level_in'] = site_level_data.drop_duplicates(subset=['datetime']).dropna(how='all')['value']
    temp_dat[site+'_temp_F'] = site_temp_data.drop_duplicates(subset=['datetime']).dropna(how='all')['value']
    cond_dat[site+'_cond_mScm'] = site_cond_data.drop_duplicates(subset=['datetime']).dropna(how='all')['value']
    
    
#Python 3: 
print ('Script downloaded data in:')
print(dt.datetime.now() - startTime)   
    
    
level_dat = level_dat.replace(2579.29, np.nan)
temp_dat = temp_dat.replace(1121.6, np.nan)  
cond_dat = cond_dat.replace(3.4197944250000004e+62, np.nan) 
    
outputdir = 'C:/Users/alex.messina/Documents/GitHub/2021_County_LowFlow/PowerBI/County2021/Flow_data_from_API_v3/'
level_dat.to_csv(outputdir+'Level_data_SLR-045AB.csv')
temp_dat.to_csv(outputdir+'Temp_data_SLR-045AB.csv')
cond_dat.to_csv(outputdir+'Cond_data_SLR-045AB.csv')
    
    
    
    
    
    
    