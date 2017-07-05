#!/usr/bin/env python2
# -*- coding: utf-8 -*-
"""
Created on Tue Jul  4 15:29:00 2017

@author: rayhanmomin
"""

import requests
import pandas as pd

if __name__ == "__main__":

    """
    Get max number of time series that can be returned by CompactData
    """
    max_series_url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/GetMaxSeriesInResult'
    max_series_num = requests.get(max_series_url).json()
    
    """
    Obtain  information on list of datasets registered for the Data Service.
    """
    meta_url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/Dataflow/'
    meta_data = requests.get(meta_url).json()
    dbs_raw = meta_data['Structure']['Dataflows']['Dataflow']
    dbs = [(entry['KeyFamilyRef']['KeyFamilyID'],entry['Name'][u'#text']) for \
           entry in dbs_raw]
    dbs_df = pd.DataFrame(data=dbs,columns=['Code','Name'])
    dbs_df.to_csv('imf_api_dbs.csv')
    
    struc_url = 'http://dataservices.imf.org/REST/SDMX_JSON.svc/DataStructure/IFS'
    struc_data = requests.get(struc_url).json()
    
    """
    Use CompactData Method to request data from IMF API
    """
    cc_codes = pd.read_excel('cleaned_codes.xls')  

    col_df = []
    for cc in cc_codes['ISO-2 Code']:
        api_url = "http://dataservices.imf.org/REST/SDMX_JSON.svc/CompactData/"
        database = "IFS/"
        freq = "A."
        mnemonic = ".FAAG_XDC" # Central Bank Assets in National Currency
        temp_url = api_url+database+freq+cc+mnemonic        
        while True:
            try:
                data = requests.get(temp_url).json()            
                col_df.append(pd.DataFrame(data))
                break
            except:
                continue
            
    """
    Loop through queries to collect succesful calls into a dataframe
    """
    
    good_dfs = []
    for dat in col_df:
        try:          
            temp_df = pd.DataFrame(dat['CompactData']['DataSet']['Series']['Obs'])
            temp_df = temp_df[["@OBS_VALUE","@TIME_PERIOD"]]
            
            temp_multi = int(dat['CompactData']['DataSet']['Series']['@UNIT_MULT'])
            temp_mnem = dat['CompactData']['DataSet']['Series']['@INDICATOR']
            temp_cc = dat['CompactData']['DataSet']['Series']['@REF_AREA'] 
            
            temp_df['@OBS_VALUE'] = pd.to_numeric(temp_df['@OBS_VALUE'])
            temp_df['@OBS_VALUE'] = temp_df['@OBS_VALUE']*(10**temp_multi) # Harmonizing units
            temp_df.columns = [temp_mnem.lower(), 'date']
            temp_df['country'] = temp_cc.lower()
            
            good_dfs.append(temp_df)
        except:
            continue
        
    cb_bs_long = pd.concat(good_dfs)
    cb_bs_wide = cb_bs_long.pivot(index='date',columns='country')
    