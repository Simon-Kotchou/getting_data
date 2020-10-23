# -*- coding: utf-8 -*-
"""
Created on Sun Oct  4 17:33:07 2020

@author: Morgan
"""

import mysql.connector
import os
import audio_metadata
from mysql.connector import Error
import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__


import os, uuid
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient, __version__

#############################
# GLOBAL VARS

CONTAINER_NAME = "audiodata"
CONNECTION_STR = "" # FILL IN

#############################


def main():
    '''
    main processing loop
    first get/load datasource
    then loop through files, upload to az blob & insert sound_file record
    '''

    if connect():
        print("Database open")
    else:
        return False
        
        
    # get the datasourceid, load new one if necessary
    datasourceid=0
    
    while datasourceid == 0:
        n = 0
        s = input("Enter data source id, or 0 to load a new data source, or q to quit: ")
        if s == "q":
            return
        elif s.isnumeric():
            n=int(s)
            if n > 0:
                sql="select name from data_source where id=%s"
                vals=(n,)
                cursor.execute(sql, vals)
                if cursor.rowcount:
                    datasourceid=n
                    datasourcename=cursor.fetchone()[0]
                else:
                    print("Data source with id ",n," was not found")
            else:
                datasourcename = input("Enter new data source name (must be unique): ")
                sql="select id from data_source where name=%s"
                vals=(datasourcename,)
                cursor.execute(sql, vals)
                if cursor.rowcount > 0:
                    print("we already have a data source named " + datasourcename + ", its id is " + str(cursor.fetchone()[0]))
                    datasourcename=""
                else:
                    datasourceid=getid()
                    #here we could also input any other info we want to load about the datasource and add that to the insert
                    parent_information = parent_file(datasourceid, datasourcename)
                    sql="insert into data_source (`id`, `name`, `location`, `audio_characteristics`, `verification_method`,`description`) values (%s, %s, %s, %s, %s, %s);"
                    cursor.execute(sql, parent_information)
                    conn.commit()
        else:
            print(s," is not a valid number")

        # Now we will begin to gather file data, first the file location/path then metadata then insert into database and azure
        # 1. 
    file_directory = get_dataset_files()
    audio_files_loop(file_directory, datasourceid)
    
    conn.commit()
    print("Data Committed to DB successfully!")
      
        
    finish()


def audio_files_loop(file_directory, parent_id=-1):
    '''
    Parameters
    ----------
    file_directory : string
        user entered file(s) location
    parent_id : int, optional
        This is the primary key relating the data to the parent table. The default is -1.

    Returns
    -------
    None.

    '''
    
    sql="insert into sound_file (id, data_source_id, size_bytes, file_duration, checksum, blob_storage_url, sample_rate, is_cough, is_covid, is_strong) values (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)"
    cough_tuple = is_cough()
    
    for file in os.listdir(file_directory):
        file_name = os.path.join(file_directory, file)
        file_tuple = collect_file_meta_data(file_name, parent_id)
        total_tuple = file_tuple + cough_tuple
        cursor.execute(sql, total_tuple)
        store_in_blob(file_name, total_tuple[5])
        
        print("**Data Stored Into Blob**")
        
        try:
            conn.commit()
        except mysql.connector.Error as error:
            print("Failed to insert record into the table, due to: ", error)
    print("This has worked successfully")

def is_cough():
    '''
    This function takes in user input to determine if all files are cough/covid/strong/weak labeled.
    Returns
    -------
    cough : int
        0 == not cough data, 1 == cough data, NULL = unkown
    is_covid : int
        0 == not covid data, 1 == covid data, NULL = unknown
    is_strong : int
        0 == not strong label data, 1 == strong label data

    '''
    cough = 0
    is_covid = 0
    is_strong = 0
    
    # user_in = input("Are all the files in the directory the same classification? IE ALL strong labeled, ALL cough, etc.  ")
    user_in = input("Is this a strong labeled data set IF UNKNOWN -> u? (y/n/u) ")
    
    if user_in.lower() == 'y':
        is_strong = 1
    
    user_in = input("Is this a cough data set IF UNKNOWN -> u? (y/n/u) ")
    
    if user_in.lower() == 'y':
        cough = 1
    elif user_in.lower() == 'u':
        cough = None

    covid_cough = input("Are the coughs covid POSITIVE? (y/n) ")
    if covid_cough.lower() == 'y':
        is_covid = 1
    elif user_in.lower() == 'u':
        is_covid = None
        
    return (cough, is_covid, is_strong)


def collect_file_meta_data(filename, parent_id=-1):
  '''
  Function - takes in a audio file and computes necesary metadata
  Inputs: 
    file - type: string
  Outputs:
    all_file_meta_df - type: pandas dataframe
  '''
  file_id = getid()
  file_extension = os.path.splitext(filename)
  blob_storage_url = str(str(parent_id) + "/" + str(file_id) + file_extension[1])

  # Using the audio_metadata import
  metadata = audio_metadata.load(filename)

  # Store indiviual file data into dataframe
  size_bytes = metadata['filesize']
  bitrate = metadata['streaminfo'].bitrate
  file_duration =  metadata['streaminfo'].duration
  sample_rate = metadata['streaminfo'].sample_rate
  checksum = ""#metadata['streaminfo'].md5
  
  return (file_id, parent_id, size_bytes, file_duration, checksum, blob_storage_url, sample_rate)


def store_in_blob(local_file_path, blob_storage_url):
    
    try:
        # Rename local file to store in blob
        #upload_file_path = os.rename(local_file_path, blob_storage_url)

        blob_service_client = BlobServiceClient.from_connection_string(CONNECTION_STR)
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_storage_url)
        
        print("\nUploading to Azure Storage as blob:\n\t" + blob_storage_url)
        
        # Upload the file
        with open(local_file_path, "rb") as data:
            blob_client.upload_blob(data=data)
    
    except Exception as ex:
        print('Exception:')
        print(ex)
        

def getid():
    '''
    execute the mysql getid stored procedure to get the next unique id
    returns int
    '''

    cursor.callproc('GetId')
    for result in cursor.stored_results():
        newid=result.fetchone()[0]
        
    return newid


def get_dataset_files():
    '''
    Returns
    -------
    file_path : string
        prompts user for file path
    '''
    file_path = input("Enter data directory path: ")
    
    return file_path


def parent_file(new_id, data_name):
    '''
    Returns
    -------
    parent_id : int
        dataset id, UNIQUE
    data_set_name : str
        name of dataset.
    url : str
        data source.
    audio_info : str
        audio capture device.
    verification_method : str
        covid verification method.
    description : str
        any other information on the data.
    '''
    # Get the information for the parent database, "data_source" and insert into it
    parent_id = new_id
    data_set_name = data_name
    url = input("Url of dataset: ")
    audio_info = input("What information do you have about the audio: ")
    verification_method = input("If covid, what testing method was used: ").lower()
    description = input("Describe the dataset: ")
    
    return (parent_id, data_set_name, url, audio_info, verification_method, description)


def connect():
    '''
    connect to the db, open a buffered cursor
    '''

    global conn
    conn = None
    try:
        conn = mysql.connector.connect(host='',
                                       database='',
                                       user='',
                                       password='')
        if conn.is_connected():
            print('Connected to MySQL database')
            global cursor
            cursor = conn.cursor(buffered=True)
            return True
        else:
            return False

    except Error as e:
        print(e)
        return False
        
        
def finish():
    '''
    things to do afterwards
    do we want to do any logging?
    '''
    
    conn.close()



main()
