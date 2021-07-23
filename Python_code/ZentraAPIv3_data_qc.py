# -*- coding: utf-8 -*-
"""
Created on Fri Jul 23 13:51:49 2021

@author: alex.messina
"""


import pandas as pd
import datetime as dt
import time
import numpy as np

startTime = dt.datetime.now()

maindir = 'C:/Users/alex.messina/Documents/GitHub/2021_County_LowFlow/'
inputdir = maindir + 'PowerBI/County2021/Flow_data_from_API_v3/'

## Read in existing data
raw_level_dat = pd.read_csv(inputdir+'Level_data_raw.csv',index_col=0, parse_dates=True)
raw_level_dat = raw_level_dat.reindex(pd.date_range(dt.datetime(2021,5,1),dt.datetime(2021,9,16,0,0),freq='5Min'))
qc_level_dat = raw_level_dat

ex_temp_dat = pd.read_csv(inputdir+'Temp_data_raw.csv',index_col=0, parse_dates=True)
ex_temp_dat = ex_temp_dat.reindex(pd.date_range(dt.datetime(2021,5,1),dt.datetime(2021,9,16,0,0),freq='5Min'))

ex_cond_dat = pd.read_csv(inputdir+'Cond_data_raw.csv',index_col=0, parse_dates=True)
ex_cond_dat = ex_cond_dat.reindex(pd.date_range(dt.datetime(2021,5,1),dt.datetime(2021,9,16,0,0),freq='5Min'))



## Read in data clips from SiteList
site_list = pd.read_excel(maindir+'PowerBI/Site_List.xlsx',sheetname='Site_List',index_col=0)
## Special Offsets and Bad Data Clips, Global Offsets
spec_offsets  = pd.read_excel(maindir+'PowerBI/Site_List.xlsx',sheetname='SpecialOffsets',index_col=0)
clips =  pd.read_excel(maindir+'PowerBI/Site_List.xlsx',sheet_name='ClipBadData',index_col=0)
glob_offsets = pd.read_excel(maindir+'PowerBI/Site_List.xlsx',sheet_name='GlobalOffsets',index_col=0)


for site_name in site_list[site_list['Consultant']=='Wood'].index:
    site_name = site_name.split('MS4-')[1] 
    print (site_name)
    WL = raw_level_dat.loc[:,[site_name+'_level_in']]


#%%
#### Special offsets
    offsets_list_for_site = spec_offsets[spec_offsets.index  == 'MS4-'+ site_name]
    ## Add column of zero for data offset
    WL['spec_offset'] = 0.
    ## THIS IS AS TUPLES SO THE TUPLE IS INDEXED BY NUMBER NOT STRING
    for spec_offset in offsets_list_for_site.itertuples():
        print ('Special offsets: ')
        print (spec_offset)
        ## set data in bad_data indices to nan
        if pd.notnull(spec_offset.Start)==True and pd.notnull(spec_offset.End)==True:
            print ('Special offset: '+spec_offset.Start.strftime('%m/%d/%y %H:%M')+' - '+spec_offset.End.strftime('%m/%d/%y %H:%M')+' = '+str(spec_offset.SpecialOffset_in)+ ' inches')
            ## insert each offset value to Offset column
            offset_range = (WL.index > spec_offset.Start) & (WL.index <= spec_offset.End)
            WL.loc[offset_range, ['spec_offset']] = spec_offset.SpecialOffset_in
        else:
            pass
        print ('')   

#### Clip Bad data
    try: # one entry dataframes are weird
        clips_for_site = pd.DataFrame(clips.loc['MS4-'+site_name,:]).sort_values(by='Start')
        bad_data_clips = clips_for_site[clips_for_site['Reason'].isin(['Invalid','Obstruction'])]
    except:
        try:
            clips_for_site = pd.DataFrame(clips.loc['MS4-'+site_name,:]).T # have to make DF and Transpose it 
            bad_data_clips = clips_for_site[clips_for_site['Reason'].isin(['Invalid','Obstruction'])]
        except KeyError:
            print ('No clips found')
            bad_data_clips = pd.DataFrame()
        
    ## iterate over list of bad data and clip from 'offset_flow_clipped'....
    print ('Clipping bad/invalid data....')
    WL['Level_spec_off_clipbad'] = WL['Level_spec_off']
    for clip in bad_data_clips.iterrows():
        clip_start, clip_end = clip[1]['Start'], clip[1]['End']
        if pd.isnull(clip_start)==False and pd.isnull(clip_end) == False:
            print ('Clipped Invalid data from: '+clip_start.strftime('%m/%d/%y %H:%M')+'-'+clip_end.strftime('%m/%d/%y %H:%M'))
            ## set data in WL indices to nan
            WL.loc[clip_start:clip_end, ['Level_spec_off_clipbad']] = np.nan
        else:
            print ('No data to clip...')
            pass   
            
#### Offset data with Level offset
    # Calculated offset from Power BI analysis
    calculated_offset = site_list.loc['MS4-'+site_name,'Offset_in']
    print ('Calculated offset from Power BI, Excel sheet = '+str(calculated_offset)+' in')
    ## Copy over data that has already had special offsets applied and bad data clipped
    ## Apply calculated offset
    WL['Level_spec_off_clipbad_calc_off'] = WL['Level_spec_off_clipbad'] + calculated_offset
            
#### Global offset
    glob_offset = glob_offsets.loc['MS4-'+site_name,'GlobalOffset_in']
    print ('Global offset for '+site_name+' = '+str(glob_offset) +' in.')
    ## Apply global offset
    WL['Level_spec_off_clipbad_calc_off_glob_off'] = WL['Level_spec_off_clipbad_calc_off'] + glob_offset
    ## Final, offset and cleaned water level data for flow calculation
    WL['Level_in'] = WL['Level_spec_off_clipbad_calc_off_glob_off']
    
### Calculate Flow
    a, b = site_list.loc['MS4-'+site_name]['alpha'], site_list.loc['MS4-'+site_name]['beta']
    WL['Flow_gpm'] = a *  WL['Level_in']^b
    

#### Clip stormflow data from Flow gpm
    try: # one entry dataframes are weird
        clips_for_site = pd.DataFrame(clips.loc['MS4-'+site_name,:])
        storm_clips = clips_for_site[clips_for_site['Reason']=='Storm']
    except:
        try:
            clips_for_site = pd.DataFrame(clips.loc['MS4-'+site_name,:]).T # have to make DF and Transpose it 
            storm_clips = clips_for_site[clips_for_site['Reason']=='Storm'] 
        except KeyError:
            storm_clips = pd.DataFrame()
    ## iterate over list of bad data and clip from 'Flow_gpm'....
    print ('Clipping stormflow data....')
    WL['Flow_gpm_storm_clipped'] = WL['Flow_gpm']
    for clip in storm_clips.iterrows():
        clip_start, clip_end = clip[1]['Start'], clip[1]['End']
        if pd.isnull(clip_start)==False and pd.isnull(clip_end) == False:
            print ('Clipped storm data from: '+clip_start.strftime('%m/%d/%y %H:%M')+'-'+clip_end.strftime('%m/%d/%y %H:%M'))
            ## set data in WL indices to nan
            WL.loc[clip_start:clip_end, ['Flow_gpm_storm_clipped']] = np.nan
        else:
            print ('No data to clip...')
            pass   
        
    qc_level_data.loc[:,site_name+'_level_in'] = WL.loc[:,'Level_in']
    
    
    
    
    
    
    
#%%
    
qc_level_data.to_csv(outputdir + 'Level_data_qc.csv')
    
    