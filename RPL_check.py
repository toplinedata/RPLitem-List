# -*- coding: utf-8 -*-
"""
Created on Wed Sep 19 10:13:39 2018

@author: Chieh-Hsu Yang
"""

import os
import re
import glob
import time
import pyodbc
import pandas as pd

### Filter the items that has been replace ###
# Set work directory
#workdir = 'C:\\Users\\User\\Desktop\\wfforecast\\auto\\itemlist\\'
try:
    workdir = 'Y:\\RPLitem_List\\'
    os.chdir(workdir)
except:
    workdir ="N:\\E Commerce\\Public Share\\! DOT COM DATA\\RPL\\"
    os.chdir(workdir)
# Connect Netsuite through OCBD and get the item information list 
conn = pyodbc.connect('dsn=NetSuite;UID=RHung@Top-Line.com;PWD=Netsuite888')
sql = """
select
    i.FULL_NAME,
    i.TYPE_NAME,
    i.SALESDESCRIPTION,
    i.DATE_LAST_MODIFIED,
    s.LIST_ITEM_NAME,
    i.ISINACTIVE,
    i.REPLACED_BY_NEW_ITEM,
    i.REPLACED_FOR_OLD_ITEM,
    i.ITEM_ID
from
    ITEMS i,
    ITEM_STATUS s

where
    i.STATUS_ID = s.LIST_ID and
    i.TYPE_NAME in ('Inventory Item') and
    s.LIST_ITEM_NAME not like '%Part%' and
    i.SALESDESCRIPTION is not null
"""
Inv_item = pd.read_sql(sql,conn)
conn.close()

Inv_item.index = Inv_item.FULL_NAME

## Find items with missing value
#Inv_cols = list(Inv_item.columns.values)
#filled_err = list(map(lambda x: Inv_item[Inv_item[x].isna()], Inv_cols))
#filled_err = pd.concat(filled_err)
#
## Exclude items with missing value
#Inv_item = Inv_item.drop(index= filled_err.index)
##Inv_item.to_csv('Inv_item.csv')

# Find items name index have been replaced
check_des = list(filter(lambda x: '@RPL' in Inv_item.SALESDESCRIPTION.loc[x], Inv_item.index))
RPL_item = Inv_item[Inv_item.index.isin(check_des)]

# Add new item name
RPL_item['NEW_NAME'] = list(map(lambda x: RPL_item.loc[x, 'SALESDESCRIPTION'][RPL_item.loc[x, 'SALESDESCRIPTION'].find('@RPL')+5:], RPL_item.index))

RPL_cols = list(RPL_item.columns.values)
cols_new = RPL_cols[-1:] + RPL_cols[:-1]
RPL_item = RPL_item[cols_new]

# Check the correction of the slice way of new item name 
# Set regular expression of item No. format
label_err = []
ItemNoRegex = re.compile(r'''\S+-\S+|\w*(\d){3,}\w*(\((\w){2,}\))?\-*\w*''', )

# Find item infomation that new name are not fit the format
for item in RPL_item.index:
    check_slice = ItemNoRegex.search(RPL_item.loc[item, 'NEW_NAME'])
    if check_slice:
        if check_slice.group() != RPL_item.loc[item, 'NEW_NAME']:
            label_err.append(RPL_item.loc[item])
    else:
        label_err.append(RPL_item.loc[item])
if label_err:
    label_err = pd.concat(label_err, axis = 1)
    label_err = label_err.transpose()
    
RPL_item = RPL_item.drop(index = label_err.index)

# Find new item No. actullay contain two type of new name
for item in label_err.index:
    multi_new = list(ItemNoRegex.finditer(label_err.loc[item, 'NEW_NAME']))
    if len(multi_new) > 1:
        for i in multi_new:
            new_ItemInfo = label_err.loc[item]
            new_ItemInfo.NEW_NAME = i.group()
            RPL_item = RPL_item.append(new_ItemInfo)
        label_err = label_err.drop(index = item)
        
# Set new index prevent index duplicate
RPL_item.index = list(range(len(RPL_item)))

#Add new name rule---------------------------------------------------------------------------
# Find items name index have been replaced
An_RPL_item = Inv_item[~Inv_item.index.isin(check_des)]
An_RPL_item=An_RPL_item[An_RPL_item["REPLACED_BY_NEW_ITEM"].notnull()]

# Add new item name
An_RPL_item['NEW_NAME'] = An_RPL_item['REPLACED_BY_NEW_ITEM']
An_RPL_cols = list(An_RPL_item.columns.values)
An_cols_new = An_RPL_cols[-1:] + An_RPL_cols[:-1]
An_RPL_item = An_RPL_item[An_cols_new]
An_RPL_item.index = list(range(len(An_RPL_item)))

#Combine RPL and An_RPL
RPL_item=pd.concat([RPL_item,An_RPL_item],axis=0)
RPL_item.reset_index(inplace=True)

#Third_check
RPL_thr=RPL_item[['NEW_NAME','FULL_NAME']]
RPL_thr=pd.merge(RPL_thr,RPL_thr,how='left',left_on='FULL_NAME',right_on='NEW_NAME')
RPL_thr=RPL_thr[RPL_thr.NEW_NAME_y.notnull()][['NEW_NAME_x','FULL_NAME_y']]
RPL_item=pd.merge(RPL_item,RPL_thr,how='left',left_on='FULL_NAME',right_on='FULL_NAME_y')

RPL_item['NEW_NAME_x'].fillna(RPL_item['NEW_NAME'],inplace=True)
RPL_item['NEW_NAME']=RPL_item['NEW_NAME_x']
del  RPL_item['index'],RPL_item['NEW_NAME_x'],RPL_item['FULL_NAME_y']




# Save RPL_item to a history CSV
date = time.strftime("%Y%m%d")
listname = workdir+ 'RPLind_list' +date+ '.csv'
RPL_item.to_csv(listname, index = False)

# Save a keep update CSV file
listname = workdir+ 'RPLind_list.csv'
if glob.glob('RPLind_list.csv'):
    os.remove(listname)
RPL_item.to_csv(listname, index = False)
