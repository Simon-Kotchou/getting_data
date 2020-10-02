# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 21:31:35 2020

@author: Morgan
"""

import audio_metadata
import pandas as pd
import uuid 
import os
import random

def collect_file_meta_data(filename, parent_id):
  '''
  Function - takes in a audio file and computes necesary metadata
  Inputs: 
    file - type: string
  Outputs:
    all_file_meta_df - type: pandas dataframe
  '''
  # file_id = uuid.uuid1().int
  # file_id = int(str(file_id)[:5])
  file_id = random.randrange(0, 1001)
  file_extension = os.path.splitext(filename)
  blob_file_location = str(str(parent_id) + "/" + str(file_id) + file_extension[1])
  # TODO STORE IN BLOB STORAGE!!!!!!!!!

  # Creating Data Frame for the data source
  data_columns = ['id','blob_storage_rul', 'bitrate', 'duration', 'sample_rate', 'filesize', 'checksum']
  all_file_meta_df = pd.DataFrame(columns=data_columns)

  # Using the audio_metadata import
  metadata = audio_metadata.load(filename)

  # Store indiviual file data into dataframe
  size_bytes = metadata['filesize']
  bitrate = metadata['streaminfo'].bitrate
  file_duration =  metadata['streaminfo'].duration
  sample_rate = metadata['streaminfo'].sample_rate
  checksum = ""#metadata['streaminfo'].md5
  
  return {'id':file_id, 'data_source_id':parent_id, 'blob_storage_url':blob_file_location, 'bitrate':bitrate, 'file_duration':file_duration, 'size_bytes':size_bytes, 'checksum':checksum}

def set_boolean_input():
    '''
    File asks user if the specific data is string labeled, cough data, and covid data. This assues that the user is doing a bulk load and the data is split nicely
    Returns
    -------
    is_cough : TYPE
        DESCRIPTION.
    is_covid : TYPE
        DESCRIPTION.
    is_strong : TYPE
        DESCRIPTION.

    '''
    is_cough = 0
    is_covid = 0
    is_strong = 0
    
    user_in = input("Is this a strong labeled data set? (y/n) ")
    
    if user_in.lower() == 'y':
        is_strong = 1
    
    user_in = input("Is this a cough data set? (y/n) ")
    
    if user_in.lower() == 'y':
        is_cough = 1

    covid_cough = input("Are the coughs covid? (y/n) ")
    if covid_cough.lower() == 'y':
        is_covid = 1
            
    return is_cough, is_covid, is_strong

def audio_files_loop(file_directory, parent_id):
    #data_columns = ['id', 'data_source_id', 'is_cough', 'is_covid', 'is_strong', 'size_bytes', 'file_duration', 'event_start', 'event_end', 'date_recorded', 'checksum', 'source_url', 'blob_storage_url', 'description']
    data_columns = ['id', 'data_source_id', 'is_cough', 'is_covid', 'is_strong', 'size_bytes', 'file_duration', 'checksum',  'blob_storage_url']

    final_df = pd.DataFrame(columns=data_columns)
    
    is_cough, is_covid, is_strong = set_boolean_input()
    meta_list = []
    
    for file in os.listdir(file_directory):
        file_name = os.path.join(file_directory, file)
        temp_dict = collect_file_meta_data(file_name, parent_id)
        meta_list.append(temp_dict)
        
    final_df = pd.DataFrame(meta_list, columns=data_columns)
    final_df.set_index('id')
    
    final_df['is_strong'] = is_strong
    final_df['is_cough'] = is_cough
    final_df['is_covid'] = is_covid
    final_df['data_source_id'] = parent_id
    final_df['checksum'] = final_df['checksum'].astype('|S')
    final_df['blob_storage_url'] = final_df['blob_storage_url'].astype('|S')
    print(final_df.info())
    print(final_df.columns)
    
    return final_df
        
        
# Need to talk to Mark about these...they do not feel reasonable at this current point. The event_start and end will be updated eventuall easily. The source URL appears to be a lot of extra work to obtain
# event_start = 
# event_end = 
# date_recorded = 
# source_url = 
# description = 