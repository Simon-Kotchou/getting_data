# -*- coding: utf-8 -*-
"""
Created on Tue Sep 29 19:00:36 2020

@author: Morgan
"""
#from  audioMetaData import collect_file_meta_data
import mysql.connector
import os
import uuid
from audioMetaData import audio_files_loop
# Connect to the database
def db_init(database='audible'):
    '''
    Parameters
    ----------
    database : string, 
        Name of database. The default is 'audible'.
    Returns
    -------
    db : Database connection
        Connection to database.
    '''
    db = mysql.connector.connect(
        host = "dev1.audiblehealth.ai",
        user = "datascience",
        passwd = "n5NfCjJY3g9GzY84eupWbZrU",
        database = "audible"
    )
    print(db)
    return db

def get_data_table():
    '''
    This function prompts the user for table name
    Returns
    -------
    TYPE
        N/A.
    '''
    table_name = input("What data table are you inserting into: ")
    
    return '\''+table_name+'\''  # Note: \' is needed for SQL querey the table name goes between the '\'table_name\''

def parent_file():
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
    parent_id = 1
    data_set_name = input("Name of the dataset: ").lower()
    url = input("Url of dataset: ")
    audio_info = input("What information do you have about the audio: ")
    verification_method = input("If covid, what testing method was used: ").lower()
    description = input("Describe the dataset: ")
    
    return (parent_id, data_set_name, url, audio_info, verification_method, description)

def get_dataset_files():
    '''
    Returns
    -------
    file_path : string
        prompts user for file path
    '''
    file_path = input("Enter data directory path: ")
    
    return file_path
    
    
def insert_parent(insert_info, cursor):
    '''
    Parameters
    ----------
    insert_info : tuple list
        list of tuples to insert into database
    cursor : database connection
        how to send data to database
    Returns
    -------
    None.
    '''
    try:
        sql = "insert into data_source (`id`, `name`, `location`, `audio_characteristics`, `verification_method`,`description`) values (%s, %s, %s, %s, %s, %s);" 
        cursor.execute(sql, insert_info)
    except mysql.connector.Error as error:
        print("Failed to insert record into the table, due to: ", error)
        
def insert_child(df, cursor):
    '''
    Parameters
    ----------
    df : dataframe
        dataframe with all the file information, for n files
    cursor : database connection
        how to send data to database
    Returns
    -------
    None.

    '''
    try:
        cols = "',".join([str(i) for i in df.columns.tolist()])
        for i, row in df.iterrows():
            print(i)
            print(row)
            sql = "INSERT INTO `sound_file` (`" +cols+ "`) VALUES (" + "%s,"*(len(row)-1) + "%s);"
            #sql = "INSERT INTO 'sound_file' (data_source_id', 'is_cough', 'is_covid', 'is_strong', 'size_bytes', 'file_duration', 'checksum', 'blob_storage_url') VALUES (" + "%s,"*(len(row)-1) + "%s);" 
            cursor.execute(sql, tuple(row))
            
    except mysql.connector.Error as error:
        print("Failed to insert record into the table, due to: ", error)


def main_func():
    '''
    This function is the main driver to populate the databse with all the information
    Returns
    -------
    None.
    '''
    try:
        connection = db_init()
        
        # Set a cursor to talk to db
        cursor = connection.cursor()
        
        # Set statement to get information from specified datatable
        #table_name = get_data_table()
        user_in = input("Is the data parent information already stored: (y/n) ").lower()
        if user_in != 'y':
            table_name = '\'data_source\''
            column_statement = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='audible' AND `TABLE_NAME`="+table_name+";"  
            # Execute the statement on the DB
            cursor.execute(column_statement)
            
            # Print the information
            columns = cursor.fetchall()
            
            for column in columns:
                print(column)
                
            # File Loading and colelcting data
            parent_information = parent_file()
            insert_parent(parent_information, cursor)
            connection.commit()
            print("Data Committed to DB successfully!")
        else:
            # Get parents id 
            parent_information = [1]
        
        # Child Data Updating
        # Update cursor/table 
        table_name = '\'morgan_test\''
        column_statement = "SELECT `COLUMN_NAME` FROM `INFORMATION_SCHEMA`.`COLUMNS` WHERE `TABLE_SCHEMA`='audible' AND `TABLE_NAME`="+table_name+";" 
        cursor.execute(column_statement)
            
        # Print the information
        columns = cursor.fetchall()
        for column in columns:
            print(column)
            
        
        files = get_dataset_files()
        #audio_files_loop(files, parent_information['id'])
        data_frame = audio_files_loop(files, parent_information[0])
        insert_child(data_frame, cursor)
        connection.commit()
        print("Data Committed to DB successfully!")
        
        connection.close()
    except mysql.connector.Error as error:
            print("Failed to insert record into the table, due to: ", error)
            
    # finally:
    #     if(connection.is_connected()):
    #         connection.close()
    #         print("MySQL connection is closed")
    
main_func()