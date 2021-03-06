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
import numpy as np

startTime = dt.datetime.now()

## Read in existing Wood data
## inputdir is defined in the script that downloads Wood data, make sure to run that one first
all_level_dat = pd.read_csv(inputdir+'Level_data_raw.csv',index_col=0, parse_dates=True)
all_level_dat = all_level_dat.reindex(pd.date_range(dt.datetime(2021,5,1),dt.datetime(2021,9,22,0,0),freq='5Min'))

all_temp_dat = pd.read_csv(inputdir+'Temp_data_raw.csv',index_col=0, parse_dates=True)
all_temp_dat = all_temp_dat.reindex(pd.date_range(dt.datetime(2021,5,1),dt.datetime(2021,9,22,0,0),freq='5Min'))

all_cond_dat = pd.read_csv(inputdir+'Cond_data_raw.csv',index_col=0, parse_dates=True)
all_cond_dat = all_cond_dat.reindex(pd.date_range(dt.datetime(2021,5,1),dt.datetime(2021,9,22,0,0),freq='5Min'))

#%%
## Read in existing KLI data
ex_kli_level_dat = pd.read_csv(inputdir+'Level_data_KLI_raw.csv',index_col=0, parse_dates=True)
ex_kli_level_dat = ex_kli_level_dat.reindex(pd.date_range(dt.datetime(2021,5,1),dt.datetime(2021,9,22,0,0),freq='5Min'))

ex_kli_temp_dat = pd.read_csv(inputdir+'Temp_data_KLI_raw.csv',index_col=0, parse_dates=True)
ex_kli_temp_dat = ex_kli_temp_dat.reindex(pd.date_range(dt.datetime(2021,5,1),dt.datetime(2021,9,22,0,0),freq='5Min'))

ex_kli_cond_dat = pd.read_csv(inputdir+'Cond_data_KLI_raw.csv',index_col=0, parse_dates=True)
ex_kli_cond_dat = ex_kli_cond_dat.reindex(pd.date_range(dt.datetime(2021,5,1),dt.datetime(2021,9,22,0,0),freq='5Min'))

#%%

### START/END DATES
today = dt.datetime.now()
days_ago = today - dt.timedelta(days=6) ## get data from past n days
startdate = days_ago.strftime("%m-%d-%Y")
enddate = today.strftime("%m-%d-%Y")

## Get start date for new data as the last date in the existing data. Duplicate data will be overwritten with new data
startdate = ex_kli_level_dat.dropna(how='all').index[-1].strftime("%m-%d-%Y")

# or hardcode start/end
#startdate = '07-24-2021'
#enddate = '05-13-2021'

#%%

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

### DEVICES
site_device_dict = {
                  'SDG-074':('06-02238',1),
                  'SDG-074F':('06-02296',1),
                  'SDG-074K':('06-02284',1),
                  'SDG-077':('06-02206',1),
                  'SDG-077E':('06-02349',1),
                  'SDG-080':('06-02223',1),
                  'SDG-577':('06-02208',1),
                  'SDG-579':('06-02237',1),
                  'SMG-015':('06-02289',1),
                  'SMG-021':('06-02225',1),
                  'SMG-062':('06-02236',3),
                  'SMG-062D':('06-02195',1),
                  'SMG-098':('06-02202',1)
                  }

                    
# Reorder the dictionary    
device_site_dict = {value[0]:([key],value[1]) for key, value in site_device_dict.items()}




## Empty dataframes for new data
level_dat = pd.DataFrame(index=pd.date_range(startdate,enddate,freq='5Min'))
temp_dat = pd.DataFrame(index=pd.date_range(startdate,enddate,freq='5Min'))
cond_dat = pd.DataFrame(index=pd.date_range(startdate,enddate,freq='5Min'))
## iterate over loggers, can get 60 loggers in 1 min from API or else throttled and have to wait 1min
for device_sn, (site_s, port_s) in device_site_dict.items():
    print()
    print ('Device: '+device_sn)
    print ('Site: '+site_s[0])
    
    ### API PARAMS
    #token = "Token {TOKEN}".format(TOKEN="5570f23feeb666615003051140cea73ccdb18639") ## Wood token
    token = "Token {TOKEN}".format(TOKEN="0e599fae024923f65b51a73cbb19ee94ae729444") ## KLI token
    url = "https://zentracloud.com/api/v3/get_readings/"
    headers = {'content-type': 'application/json', 'Authorization': token}
    output_format = "df"
    perpage = 2000
    params = {'device_sn': device_sn, 'output_format': output_format,'per_page':2000,'sort_by':'ascending','start_date':startdate,'end_date':enddate}

    device_dat = pd.DataFrame()
    ## FIRST PAGE OF DATA
    df, content = get_zentra_df(url, params=params, headers=headers)
    
    if len(df) > 0:
        df['datetime'] = pd.to_datetime(df['datetime'],utc=True).dt.tz_convert(tz='US/Pacific').dt.tz_localize(None) # in case some aren't on PST
        df.index = df['datetime']
        
        device_dat = device_dat.append(df)
        
    #    ## NEXT PAGE(S)
    #    length_data = content['pagination']['page_num_readings']
    #    next_url = content['pagination']['next_url']
    #    ## loop until a page returns with no data
    #    while length_data > 0:
    #        next_df, next_content = get_zentra_df(next_url, params, headers, retries=3)
    #        
    #        if next_content['pagination']['page_num_readings'] > 0: ## if it actually returns data, if not df is empty
    #            next_df['datetime'] = pd.to_datetime(next_df['datetime'],utc=True).dt.tz_convert(tz='US/Pacific').dt.tz_localize(None) # in case some aren't on PST
    #            next_df.index = next_df['datetime']
    #            device_dat = device_dat.append(next_df)
    #            ## update length data and next_url with this page's values
    #            length_data = next_content['pagination']['page_num_readings']
    #            next_url = next_content['pagination']['next_url']
    #        else:
    #            length_data = 0
        
        ## For each logger, if multiple sites then iterate over sites
        for site in site_s:
            port_num = site_device_dict[site][1]
            print ('Saving data for...')
            print (site, port_num)
            
            site_data = device_dat[device_dat['port_num']==port_num]
            
            site_level_data = site_data[site_data['measurement']=='Water Level']      [['datetime','mrid', 'measurement','value', 'units']]
            site_temp_data = site_data[site_data['measurement']=='Water Temperature'] [['datetime','mrid', 'measurement','value', 'units']]
            site_cond_data = site_data[site_data['measurement']=='EC']                [['datetime','mrid', 'measurement','value', 'units']]
        
            level_dat[site+'_level_in'] = site_level_data.drop_duplicates(subset=['datetime']).dropna(how='all')['value']
            temp_dat[site+'_temp_F'] = site_temp_data.drop_duplicates(subset=['datetime']).dropna(how='all')['value']
            cond_dat[site+'_cond_mScm'] = site_cond_data.drop_duplicates(subset=['datetime']).dropna(how='all')['value']
            
            #level_dat = level_dat.replace(2579.29, np.nan)
            #temp_dat = temp_dat.replace(1121.6, np.nan)  
            #cond_dat = cond_dat.replace(3.4197944250000004e+62, np.nan) 
            
            level_dat = np.round(level_dat / 25.4,2).replace(2579.29, np.nan)
            temp_dat = np.round((temp_dat * 9/5)+32, 1).replace(1121.5, np.nan)  
            cond_dat = cond_dat.replace(3.4197944250000004e+62, np.nan) 
            
            ex_kli_level_dat.loc[level_dat.index,site+'_level_in'] = level_dat[site+'_level_in']
            ex_kli_temp_dat.loc[temp_dat.index,site+'_temp_F'] = temp_dat[site+'_temp_F']
            ex_kli_cond_dat.loc[cond_dat.index,site+'_cond_mScm'] = cond_dat[site+'_cond_mScm']
    
#Python 3: 
print ('Script downloaded data in:')
print(dt.datetime.now() - startTime)   
    
    


#%%
## drop na and save to csv
    
ex_kli_level_dat = ex_kli_level_dat.dropna(how='all')
ex_kli_level_dat.to_csv(outputdir+'Level_data_KLI_raw.csv')
ex_kli_temp_dat = ex_kli_temp_dat.dropna(how='all')
ex_kli_temp_dat.to_csv(outputdir+'Temp_data_KLI_raw.csv')
ex_kli_cond_dat = ex_kli_cond_dat.dropna(how='all')
ex_kli_cond_dat.to_csv(outputdir+'Cond_data_KLI_raw.csv')
    
#%%

## Append to Wood data
for col in ex_kli_level_dat:
    print (col)
    all_level_dat.loc[ex_kli_level_dat.index, col] = ex_kli_level_dat[col]
    
for col in ex_kli_temp_dat:
    print (col)
    all_temp_dat.loc[ex_kli_temp_dat.index, col] = ex_kli_temp_dat[col]

for col in ex_kli_cond_dat:
    print (col)
    all_cond_dat.loc[ex_kli_cond_dat.index, col] = ex_kli_cond_dat[col]    


#%%
    
## Save updated Wood data
## outputdir defined in script that downloads Wood data run it first
all_level_dat.dropna(how='all').to_csv(outputdir+'Level_data_raw.csv')
all_temp_dat.dropna(how='all').to_csv(outputdir+'Temp_data_raw.csv')
all_cond_dat.dropna(how='all').to_csv(outputdir+'Cond_data_raw.csv')
