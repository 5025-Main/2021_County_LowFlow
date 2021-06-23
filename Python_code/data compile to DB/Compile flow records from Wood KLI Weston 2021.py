# -*- coding: utf-8 -*-
"""
Created on Wed Jun 17 12:26:43 2020

@author: alex.messina
"""

import os
import pandas as pd
import pyodbc
import datetime as dt


#%% 2021 ###############

## 2020 Wood Flow data
flow_df_Wood2021 = pd.DataFrame(index=pd.date_range(dt.datetime(2021,5,1,0,0),dt.datetime(2021,9,15,23,55),freq='5Min'))
flow_datadir = 'C:/Users/alex.messina/Documents/GitHub/2021_County_LowFlow/Flow_Output_Excel_files/'

for f in [d for d in os.listdir(flow_datadir) if d.endswith('.xlsx')]:
    site_name = 'MS4-' +f.split('-working draft.xlsx')[0]
    print (f)
    df = pd.read_excel(datadir + f,sheet_name=site_name.replace('MS4-','')+'-stormflow clipped', index_col=0)
    df['Datetime'] = df.index
    flow_col = site_name + '_Flow_gpm'
    ## add column to df
    flow_df_Wood2021.loc[:,flow_col] = df[u'Flow compound weir stormflow clipped (gpm)']

## 2021 Wood Flow data output
flow_df_Wood2021.to_csv('C:/Users/alex.messina/Documents/GitHub/2021_County_LowFlow/Flow_Output_Excel_Files/compiled_for_database/2021_Flow_Wood_compiled.csv')

#%%
## 2020 Flow data-KLI
flow_df_KLI2021 = pd.DataFrame(index=pd.date_range(dt.datetime(2021,5,1,0,0),dt.datetime(2021,9,15,23,55),freq='5Min'))
flow_datadir = 'C:/Users/alex.messina/Documents/GitHub/2021_County_LowFlow/Flow_Output_Excel_files/KLI data/'

for f in [d for d in os.listdir(flow_datadir) if d.endswith('.xlsx')]:
    print (f)
    site_name = 'MS4-' +f.split(' ')[0]
    print (site_name)
    
    if site_name == 'MS4-SMG-062':
        print ('Combining left and right for '+site_name)
        ## Open each deliverable file
        ## Left
        left = pd.read_excel(flow_datadir + f,sheet_name=site_name.replace('MS4-','')+'L 2021 Data',index_col=0)
        left = left[pd.notnull(left.index)][['Wet Flow (gpm)','Dry Flow (gpm)']]
        left['Datetime'] = left.index
        left = left.drop_duplicates(subset=['Datetime'])
        ## Right
        right = pd.read_excel(flow_datadir + f,sheet_name=site_name.replace('MS4-','')+ 'R 2021 Data',index_col=0)
        right = right[pd.notnull(right.index)][['Wet Flow (gpm)','Dry Flow (gpm)']]
        right['Datetime'] = right.index
        right = right.drop_duplicates(subset=['Datetime'])
        flow_col = site_name + '_Flow_gpm'
        
        combined = pd.DataFrame({'left':left['Dry Flow (gpm)'],'right':right['Dry Flow (gpm)']},index=right.index)
        
        def my_func(x):
            if np.isnan(x['left']) and np.isnan(x['right']):
                return np.nan
            else:
                return np.nansum([x['left'],x['right']])
            
        combined['combined'] = combined[['left','right']].apply(lambda x: my_func(x[['left','right']]),axis=1)
        
        ## add column to df
        flow_df_KLI2021.loc[:,flow_col] = combined['combined']
        
    else:
        print ('opening file...')
        ## Open each deliverable file
        df = pd.read_excel(flow_datadir + f,sheet_name=site_name.replace('MS4-','') + ' 2021 Data',index_col=0)
        df = df[pd.notnull(df.index)][['Wet Flow (gpm)','Dry Flow (gpm)']]
        df['Datetime'] = df.index
        df = df.drop_duplicates(subset=['Datetime'])
        flow_col = site_name + '_Flow_gpm'
        ## add column to df
        flow_df_KLI2021.loc[:,flow_col] = df['Dry Flow (gpm)']
        
## 2021 KLI Flow data output
flow_df_KLI2021.to_csv('C:/Users/alex.messina/Documents/GitHub/2021_County_LowFlow/Flow_Output_Excel_Files/compiled_for_database/2021_Flow_KLI_compiled.csv')
        
#%% KLI MPLD3

import mpld3
from mpld3 import plugins

fig, ax = plt.subplots(1,1,figsize=(14,7))


for key, val in flow_df_KLI2021[[u'MS4-SDG-074_Flow_gpm', u'MS4-SDG-074F_Flow_gpm',
       u'MS4-SDG-074K_Flow_gpm', u'MS4-SDG-077_Flow_gpm',
       u'MS4-SDG-077E_Flow_gpm', u'MS4-SDG-080_Flow_gpm',
       u'MS4-SDG-577_Flow_gpm', u'MS4-SDG-579_Flow_gpm']].iteritems():
#    
#for key, val in flow_data_df2020KLI[[u'MS4-SMG-021_Flow_gpm',
#       u'MS4-SMG-021A_Flow_gpm']].iteritems():
    
    l, = ax.plot(val.index, val.values, label=key.split('_Flow_gpm')[0])
ax.set_ylabel('FLOW GPM',fontsize=16,fontweight='bold')

# define interactive legend
plt.title('Click on the legend entry to view the data')

handles, labels = ax.get_legend_handles_labels() # return lines and labels
interactive_legend = plugins.InteractiveLegendPlugin(zip(handles,
                                                         ax.lines),
                                                     labels,
                                                     alpha_unsel=0.1,
                                                     alpha_over=1.5, 
                                                     start_visible=False)
plugins.connect(fig, interactive_legend)
    
#html_file = open(flow_datadir+'KLI_2020_SDG_flowdata.html','w')
#html_file = open(flow_datadir+'KLI_2020_SMG21_flowdata.html','w')
#mpld3.save_html(fig,html_file)
#html_file.close()
mpld3.show()
                  



                           
#%% COMBINE all Wood, KLI data

flow_df = flow_df_Wood2021.join(flow_df_KLI2021)


    
flow_df.round(2).to_csv('C:/Users/alex.messina/Documents/GitHub/2021_County_LowFlow/Flow_Output_Excel_files/compiled_for_database/Flow data KLI-Wood 2021.csv')   


#%%


## Access database
dbpath = 'C:/Users/alex.messina/Documents/GitHub/2021_County_LowFlow/Flow_Output_Excel_files/2021_Flow_data.accdb'
conn = pyodbc.connect(r'Driver={Microsoft Access Driver (*.mdb, *.accdb)};DBQ='+dbpath+';')
crsr = conn.cursor()
table_name = "FLow_gpm"
## Get existing records
db = pd.read_sql('select * from Flow_gpm',conn,index_col='Datetime')
## append new records to old
df = db.append(flow_df)




## add fields for new sites
## Make field (column) for new data
for col in flow_df.columns:
    if col not in db.columns:
        print (col)
        try:
            crsr.execute( "ALTER TABLE %s ADD COLUMN [%s] FLOAT;"% (table_name,col) )
            conn.commit()
        except:
            pass


## append all new data


new_data = df[dt.datetime(2021,5,1,0,0):]
new_data['Datetime'] = [str(idx) for idx in new_data.index]



# insert the rows from the DataFrame into the Access table    
cols  =new_data.columns.values.tolist()
values_list = ['?' for i in range(len(cols))]

data = new_data.ix[:1].fillna('NULL').values.tolist()

sql = "INSERT INTO %s (%s) VALUES (%s)"%(table_name,','.join(cols).replace('Datetime','[Datetime]'),','.join(['1' for i in range(len(cols))]))



crsr.execute(sql)
crsr.commit()
    
    
crsr.executemany(sql,new_data.index.itertuples())




## update column values where Datetime Index row is same
#crsr.executemany("UPDATE %s SET [%s] = ? WHERE [Datetime] = ?"% (table_name,flow_col), df[['Flow (gpm)','Datetime']].itertuples(index=False))
for row in new_data.itertuples():
    print (row)
    crsr.execute("UPDATE %s SET [%s] = ? WHERE [Datetime] = ?"% (table_name,flow_col), (row,index) )
    conn.commit()

conn.close()
#%%

# insert the rows from the DataFrame into the Access table   
#query = "INSERT INTO %s (Datetime, %s) VALUES (?)" % (table_name, flow_col)

#crsr.executemany("INSERT INTO %s ([Datetime], [%s]) VALUES (?,?);"% (table_name,flow_col), df[['Flow (gpm)']].itertuples())
for index,row in df[['Flow (gpm)']].itertuples():
    #print row
    crsr.execute("UPDATE %s SET [%s] = ? WHERE [Datetime] = ?"% (table_name,flow_col), (row,index) )


#%%  Visualize KLI "RATING CURVES"
    
    
fig, ax = plt.subplots(1,1)
    
flow_datadir = 'C:/Users/alex.messina/Documents/GitHub/2021_County_LowFlow/Flow_Output_Excel_files/KLI data/'

f_22_list = ['SMG-062 May 2021 Deliverable.xlsx', 'SDG-077 May 2021 Deliverable.xlsx', 'SDG-074K May 2021 Deliverable.xlsx'] #22.5
f_45_list = ['SMG-062D May 2021 Deliverable.xlsx','SDG-074 May 2021 Deliverable.xlsx','SMG-098 May 2021 Deliverable.xlsx'] #45
f_60_list = ['SDG-074F May 2021 Deliverable.xlsx'] #60
f_90_list = ['SDG-077E May 2021 Deliverable.xlsx', 'SDG-080 May 2021 Deliverable.xlsx', 'SDG-577 May 2021 Deliverable.xlsx', 'SDG-579 May 2021 Deliverable.xlsx', 'SMG-015 May 2021 Deliverable.xlsx', 'SMG-021 May 2021 Deliverable.xlsx']
 
 
 

#for f in [d for d in os.listdir(flow_datadir) if d.endswith('.xlsx') and ]:
for f in f_22_list:
    print (f)
    site_name = 'MS4-' +f.split(' ')[0]
    print (site_name)
    ## Open each deliverable file
    df = pd.read_excel(flow_datadir + f,sheet_name='Rating Table',index_col=0)
    ax.plot(df.index,df['Flow (gpm)'],label=site_name,c='k')

for f in f_45_list:
    print (f)
    site_name = 'MS4-' +f.split(' ')[0]
    print (site_name)
    ## Open each deliverable file
    df = pd.read_excel(flow_datadir + f,sheet_name='Rating Table',index_col=0)
    ax.plot(df.index,df['Flow (gpm)'],label=site_name,c='b')
for f in f_60_list:
    print (f)
    site_name = 'MS4-' +f.split(' ')[0]
    print (site_name)
    ## Open each deliverable file
    df = pd.read_excel(flow_datadir + f,sheet_name='Rating Table',index_col=0)
    ax.plot(df.index,df['Flow (gpm)'],label=site_name,c='r')
for f in f_90_list:
    print (f)
    site_name = 'MS4-' +f.split(' ')[0]
    print (site_name)
    ## Open each deliverable file
    df = pd.read_excel(flow_datadir + f,sheet_name='Rating Table',index_col=0)
    ax.plot(df.index,df['Flow (gpm)'],label=site_name,c='g')





plt.legend()
    
    
curves = pd.DataFrame({'stage_mm':np.arange(0,350,1)},index=np.arange(0,350,1))
curves['stage_ft'] = curves['stage_mm'] * 0.00328084
curves['22.5'] = 223.1* (curves['stage_ft']**2.5)
curves['45'] = 464.5* (curves['stage_ft']**2.5)
curves['60'] = 647.6* (curves['stage_ft']**2.5)
curves['90'] = 1122 * (curves['stage_ft']**2.5)

plt.plot(curves['stage_mm'],curves['22.5'],label='22.5 degree',c='k',alpha=0.5,ls='--')
plt.plot(curves['stage_mm'],curves['45'],label='45 degree',c='b',alpha=0.5,ls='--')
plt.plot(curves['stage_mm'],curves['60'],label='60 degree',c='r',alpha=0.5,ls='--')
plt.plot(curves['stage_mm'],curves['90'],label='90 degree',c='g',alpha=0.5,ls='--')
    
    
plt.annotate('22.5 degree',(curves.iloc[-1]['stage_mm'],curves.iloc[-1]['22.5']),c='k')
plt.annotate('45 degree',(curves.iloc[-1]['stage_mm'],curves.iloc[-1]['45']),c='b')
plt.annotate('60 degree',(curves.iloc[-1]['stage_mm'],curves.iloc[-1]['60']),c='r') 
plt.annotate('90 degree',(curves.iloc[-1]['stage_mm'],curves.iloc[-1]['90']),c='g')   
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    
    



