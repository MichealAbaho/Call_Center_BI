# -*- coding: utf-8 -*-
"""
Created on Thu Apr 19 17:19:13 2018

@author: user
"""
import pandas as pd
from datetime import datetime
from dateutil import relativedelta
import numpy as np
from collections import Counter
from operator import itemgetter
import matplotlib.pyplot as plt
from matplotlib import style
import matplotlib.ticker as ticker
import re

# =============================================================================

class call_center_bi:
    
    def __init__(self, inputData):
        self.__pbx = inputData
 
    def read_data(self):
        frame_dict = {}
        for i in range(len(self.__pbx)):
            frame_dict[i] = pd.read_csv(self.__pbx[i], low_memory = False, encoding='ISO-8859-1')
        
        return frame_dict
    
    
    def mtnVsPbx(self):
        pbx_frame = self.read_data()[0]
        mtn_ibd = self.read_data()[1]
        mtn_obd = self.read_data()[2]
        
        pbx_frame.dropna(inplace=True, axis=0, how='any')
        pbx_frame.drop_duplicates()
        pbx_date, pbx_weekday = [], []
        for i in pbx_frame['Start']:
            if(type(i) == str):
                i = i.strip()
                date = datetime.strptime(i,'%d/%m/%Y %H:%M')
                pbx_date.append(date.date())
                pbx_weekday.append(date.strftime('%A'))
            elif(isinstance(i, datetime)):
                pbx_date.append(i.date())
                pbx_weekday.append(date.strftime('%A'))
                
        pbx_frame['Date'] = pbx_date
        pbx_frame['Day'] = pbx_weekday
        
        pbx_frame['Caller ID'] = [int(i - 1) for i in pbx_frame['Caller ID']]
        latest_billing_date = max(pbx_frame['Date'])
        earliest_billing_date = min(pbx_frame['Date'])
        
        mtn_ibd_date, mtn_ibd_weekday = [],[]
        mtn_ibd.dropna(inplace=True, how='any', axis=0)
        mtn_ibd.rename(columns={'Called Number': 'Caller ID', '1261331':'bill_min'}, inplace=True)
        
        for i in mtn_ibd['Date']:
            if(type(i) == str):
                i = i.strip()
                date = datetime.strptime(i, '%d/%m/%Y')
                mtn_ibd_date.append(date.date())
                mtn_ibd_weekday.append(date.strftime('%A'))
            if(isinstance(i, datetime)):
                mtn_ibd_date.append(i.date())
                mtn_ibd_weekday.append(date.strftime('%A'))
        mtn_ibd['Date'] = mtn_ibd_date
        mtn_ibd['Day'] = mtn_ibd_weekday
        
        mtn_obd_date, mtn_obd_weekday = [],[]
        mtn_obd.dropna(inplace=True, how='any', axis=0)
        mtn_obd.rename(columns={'Called Number': 'Caller ID', '162093':'bill_min'}, inplace=True)
        
        for i in mtn_obd['Date']:
            if(type(i) == str):
                i = i.strip()
                date = datetime.strptime(i, '%d/%m/%Y')
                mtn_obd_date.append(date.date())
                mtn_obd_weekday.append(date.strftime('%A'))
            if(isinstance(i, datetime)):
                mtn_obd_date.append(i.date())
                mtn_obd_weekday.append(date.strftime('%A'))
                
        mtn_obd['Date'] = mtn_obd_date
        mtn_obd['Day'] = mtn_obd_weekday
        
        mtn_ibd = mtn_ibd.loc[(mtn_ibd['Date'] >= earliest_billing_date) & (mtn_ibd['Date'] <= latest_billing_date)]
        pbx_frame_ibd = pbx_frame.loc[pbx_frame['Call Direction'].str.lower() == 'inbound']
        
        mtn_obd = mtn_obd.loc[(mtn_obd['Date'] >= earliest_billing_date) & (mtn_obd['Date'] <= latest_billing_date)]
        pbx_frame_obd = pbx_frame.loc[pbx_frame['Call Direction'].str.lower() == 'outbound']
        
        mtn_ibd.to_csv('mtn_inbound.csv')
        pbx_frame_ibd.to_csv('pbx_inbound.csv')
        pbx_frame_ibd_grouped = pbx_frame_ibd.groupby(['Day'], as_index=False)['bill_min'].sum()
        pbx_frame_obd_grouped = pbx_frame_obd.groupby(['Day'], as_index=False)['bill_min'].sum()
        mtn_ibd_grouped = mtn_ibd.groupby(['Day'], as_index=False)['bill_min'].sum()
        mtn_obd_grouped = mtn_obd.groupby(['Day'], as_index=False)['bill_min'].sum()
        
        
        pbx_frame_ibd_grouped.to_csv('pbx_ibd.csv')
        mtn_ibd_grouped.to_csv('mtn_ibd.csv')
        pbx_frame_obd_grouped.to_csv('pbx_obd.csv')
        mtn_obd_grouped.to_csv('mtn_obd.csv')
        
        #print(mtn.columns.values)
        bill_rec = pd.merge(pbx_frame_ibd, mtn_ibd, on=['Date', 'bill_min', 'Caller ID'], how='left')
        bill_rec.to_csv('bill_rec.csv')
        
        width = 0.35
        
        ibd = plt.subplot2grid((8,1), (0,0), colspan =1, rowspan=4)
        x_pos = [day for day in range(len(pbx_frame_ibd_grouped['Day']))]
        ibd.bar(x_pos, pbx_frame_ibd_grouped['bill_min'], width, alpha=0.9, label='pbx')
        ibd.bar([i+width for i in x_pos], mtn_ibd_grouped['bill_min'], width, alpha=0.9, label='mtn')
        
        obd = plt.subplot2grid((8,1), (4,0), colspan =1, rowspan=4)
        x_pos = [day for day in range(len(pbx_frame_obd_grouped['Day']))]
        obd.bar(x_pos, pbx_frame_obd_grouped['bill_min'], width, alpha=0.9, label='pbx')
        obd.bar([i+width for i in x_pos], mtn_obd_grouped['bill_min'], width, alpha=0.9, label='mtn')
        plt.xticks([i + (width/2) for i in x_pos], list(pbx_frame_ibd_grouped['Day']))
        plt.show()
        
        print(pbx_frame_obd_grouped)
        print(mtn_obd_grouped)
        
        
    '''constructing dataframes extracting data to plot by hour and day for the three different parameters of number of calls, duration and call type
    '''
    def dataframes(self):
        df = self.descriptive()
        #separate the inbound calls from the outbound   
        inbd = df.loc[df['Call Direction'].str.lower() == 'inbound']
        outbd = df.loc[df['Call Direction'].str.lower() == 'outbound']
        reject = df.loc[df['Call Direction'].str.lower() == 'rejection']
        
        #group both the inbound and outbound calls by Hour, i.e. counting the number of calls per hour
        number_inbd_hr = inbd.groupby(['Hour'], as_index=False)['Row ID'].count().rename(columns={'Row ID': 'Inbound_calls_hourly'})
        number_outbdbd_hr = outbd.groupby(['Hour'], as_index=False)['Row ID'].count().rename(columns={'Row ID': 'Outbound_calls_hourly'})
        reject_hr = reject.groupby(['Hour'], as_index=False)['Row ID'].count().rename(columns={'Row ID':'Rejected_calls'})
        frame_call_hr = number_inbd_hr.merge(number_outbdbd_hr)
        frame_call_hr = frame_call_hr.merge(reject_hr, how='outer')
        frame_call_hr.to_csv('number_hr.csv')
        #group both the inbound and outbound calls by Hour, i.e. counting the number of calls per day
        number_inbd_day = pd.DataFrame({'Inbound_calls_daily': inbd.groupby(['Day'])['Row ID'].count()}).reset_index()
        number_outbdbd_day = pd.DataFrame({'Outbound_calls_daily': outbd.groupby(['Day'])['Row ID'].count()}).reset_index()
        reject_day = reject.groupby(['Day'], as_index=False)['Row ID'].count().rename(columns={'Row ID':'Rejected_calls'})
        frame_call_day = number_inbd_day.merge(number_outbdbd_day)
        frame_call_day = frame_call_day.merge(reject_day, how='outer')
        frame_call_day.to_csv('number_day.csv')
        #group both the inbound and outbound calls by Hour, i.e. counting the average call duration per hour
        duration_inbd_hr = pd.DataFrame({'Average_Inbound_call_duration': inbd.groupby(['Hour'])['Duration'].mean()}).reset_index()
        duration_outbdbd_hr = pd.DataFrame({'Average_Outbound_call_duration': outbd.groupby(['Hour'])['Duration'].mean()}).reset_index()
        frame_dur_hr = duration_inbd_hr.merge(duration_outbdbd_hr, how='outer')
        frame_dur_hr.to_csv('dur_hr.csv')
        #group both the inbound and outbound calls by Hour, i.e. counting the average call duration per day
        duration_inbd_day = pd.DataFrame({'Average_inbound_call_duration':inbd.groupby(['Day'])['Duration'].mean()}).reset_index()
        duration_outbd_day = pd.DataFrame({'Average_outbound_call_duration':outbd.groupby(['Day'])['Duration'].mean()}).reset_index()
        frame_dur_day = duration_inbd_day.merge(duration_outbd_day, how='outer')
        frame_dur_day.to_csv('dur_day.csv')
# =============================================================================
#         #group both the inbound and outbound calls by Hour, i.e. counting the total call types per hour
#         type_inbd_hr = inbd.groupby(['Call Type'], as_index=False)['Caller ID'].count().rename(columns={'Caller ID':'Total_inbound_calls per_type'}, inplace=True)
#         type_outbdbd_hr = outbd.groupby(['Call Type'], as_index=False)['Caller ID'].count().rename(columns={'Caller ID':'Total_outbound_calls per_type'}, inplace=True)
#         frame_type_hr = type_inbd_hr.merge(type_outbdbd_hr)
#         frame_type_hr.to_csv('type_hr.csv')
# =============================================================================
        #group both the inbound and outbound calls by Hour, i.e. counting the total call types per day
        type_inbd_day = pd.DataFrame({'Total_inbound_calls per_type':inbd.groupby(['Call Type'])['Row ID'].count()}).reset_index()
        type_outbd_day = pd.DataFrame({'Total_outbound_calls per_type':outbd.groupby(['Call Type'])['Row ID'].count()}).reset_index()
        type_reject = pd.DataFrame({'Total_rejected_calls': reject.groupby(['Call Type'])['Row ID'].count()}).reset_index()
        frame_inbd_outbd = type_inbd_day.merge(type_outbd_day, on='Call Type', how='outer')
        frame_inbd_outbd_rjt = frame_inbd_outbd.merge(type_reject, on='Call Type', how='outer')
        frame_inbd_outbd_rjt.fillna(0, inplace=True, axis=0)
        frame_inbd_outbd_rjt.to_csv('call_type.csv')
        
        fig, ax = plt.subplots()
        width=0.25
        try:
            x = [i for i in range(len(frame_inbd_outbd_rjt['Call Type']))]
            ax.bar(x, list(frame_inbd_outbd_rjt['Total_inbound_calls per_type']),width, alpha=0.9, label='Inbound-Calls')
            ax.bar([p+width for p in x], frame_inbd_outbd_rjt['Total_outbound_calls per_type'], width,alpha=0.9, label='Outbound-Calls')
            ax.bar([p+2(width) for p in x], frame_inbd_outbd_rjt['Total_rejected_calls'], width, alpha=0.9, label='Rejected-Calls')
            plt.show()
        except Exception as e:
            print(e)
        
        
 
    '''function that depicts number, type and duration of calls per day/hour'''    
    def descriptive(self):
        #ensure that you're dealing with calls that the ugandan fenix call center is dealing with
        pbx = self.read_data()[0]
        pbx.drop(['bill_min', 'bill_sec', 'talk_bill_sec', 'hold_bill_sec', 'Country'], axis=1, inplace=True)
        #drop all entries with nan values in all fields
        pbx = self.remove_nan(pbx)
        
        #ensure none of the dates is missing and force floating points to becpome string then calculate their difference and store in a column
        try:
            date_time_diff = []
            for i,j in zip(pbx['Start'],pbx['End']):
                diff = self.strtoDate(str(j)) - self.strtoDate(str(i))
                date_time_diff.append(int(diff.seconds)/60)
            
            #convert dates        
            days, hours = [], []
            for i in pbx['Start']:
                s = self.strtoDate(str(i))
                days.append(s.strftime('%A'))
                hours.append(s.hour)
                          
            pbx['Duration'] = date_time_diff
            pbx['Day'] = days
            pbx['Hour'] = hours
            
        except Exception as e:
            print(e)
        
        return pbx

    
    #function that formats a string into a datetime object'''   
    def strtoDate(self, str_value):
        convert_date = datetime.strptime(str_value, '%d/%m/%Y %H:%M')
        return convert_date
     
    #remove any entries that contain Nan values'''
    def remove_nan(self, input_frame):
        input_frame.dropna(inplace=True, axis=0)
        return input_frame
    
    '''Method returns number of customers who called multiple times within the two week period'''
    def nonMtnCalls(self):
        ibd = self.read_data()[0]
        self.remove_nan(ibd)
        
        ibd = ibd.loc[ibd['Call Direction'].str.lower() == 'inbound']
        mtn,airtel,africel,utl,others = [],[],[],[],[]
        network_operators = {'mtn':['(77\d{7})', '(78\d{7})'], 'airtel':['(75\d{7})', '(70\d{7})'], 'africel':'(79\d{7})', 'utl':'(71\d{7})'}
        for i in ibd['Caller ID']:
            i = str(int(i))
            for x,y in network_operators.items():
                if(x == 'mtn'):
                    for j in y:
                        if(re.match(j, i) is not None):
                            if i not in mtn:
                                mtn.append(i)
                elif(x == 'airtel'):
                    for p in y:
                        if(re.match(p, i) is not None):
                            if i not in airtel:
                                airtel.append(i)
                elif(x == 'africel'):
                    if(re.match(y, i) is not None):
                        if i not in africel:
                            africel.append(i)
                elif(x == 'utl'):
                    if(re.match(y, i) is not None):
                        if i not in utl:
                            utl.append(i)
                else:
                    if i not in others:
                        others.append(i)
        network_operators_dict = {'Mtn':mtn, 'Airtel':airtel, 'Africel':africel, 'Utl':utl, 'Others':others}
        network_operators_frame = pd.DataFrame(dict([(k,(pd.Series(v))) for k,v in network_operators_dict.items()]))
        network_operators_count = {'Networks':['Mtn','Airtel','Africel','Utl','Others'], 
                                        'Subscribers':[len(mtn), len(airtel), len(africel), len(utl), len(others)]}
        #fig,ax = plt.subplots()
        network_operators_count_frame = pd.DataFrame.from_dict(network_operators_count)
        network_operators_count_frame.to_csv('network_subscribers_count.csv')
        print(network_operators_frame)
                            
                            
        
        


files = ['PBX_Call Log_Dec25_Dec31_2017 vUpdate.csv', '2018_01_30232700_Inbound_Reviewed.csv', '2018_01_30238767_Outbound_Reviewed.csv']
s = call_center_bi(files)
s.dataframes()
