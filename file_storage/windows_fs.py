'''
Provide common functions for interacting with the file system
'''

import pandas as pd
import os
import config.win_storage as storage_config

import logging
log = logging.getLogger(__name__)

def list_stg_files(provider, dataset):
    pass

def validate_folder(root,provider,dataset):
    '''
    if the folder doesn't exist, create it
    '''
    try:
        if not os.path.exists(root+'\\'+provider+'\\'+dataset):
            os.makedirs(storage_config.STG_DIR+'\\'+provider+'\\'+dataset)
    except OSError as e:
        log.info('FAILURE  valdiate_folder'+e.strerror)

def get_stg_path(provider, dataset, filename=''):
    '''
    Given a provider and dataset, return STG_DIR file path
    Optional argument of filename
    '''
    validate_folder(storage_config.STG_DIR,provider, dataset)
    if filename=='':
        return storage_config.STG_DIR+'\\'+provider+'\\'+dataset
    else:
        return storage_config.STG_DIR+'\\'+provider+'\\'+dataset+'\\'+filename


def get_archive_path(provider, dataset, filename=''):
    '''
    Given a provider and dataset, return ARCHIVE_DIR path
    Optional argument of filename
    '''
    validate_folder(storage_config.ARCHIVE_DIR, provider, dataset)
    if filename=='':
        return storage_config.ARCHIVE_DIR+'\\'+provider+'\\'+dataset
    else:
        return storage_config.ARCHIVE_DIR+'\\'+provider+'\\'+dataset+'\\'+filename

def write_transformed_dataframe(dataframe, provider, dataset, orig_file_name):
    '''
    Given a dataframe and a file name,
    Generate a new file name and writes to disk
    Returns name of the transformed file
    '''
    new_name='TRNFRM_'+orig_file_name.split('.')[0]+'.csv'
    path=get_stg_path(provider,dataset,new_name)
    try:
        dataframe.to_csv(path)
    except Exception as e:
        log.info('FAILURE write_transformed_dataframe: '+e.strerror)
    else:
        log.info('SUCCESS write_transformed_dataframe: '+path)
        archive_file(provider, dataset, orig_file_name)
        return(new_name)

def write_byte_file(raw_bytes, provider, dataset, file_name):
    file_path=get_stg_path(provider,dataset,dest_file_name)
    with open(file_path, 'wb') as dest_file:
        dest_file.write(raw_bytes)



def archive_file(provider, dataset, filename):
    stg_path=get_stg_path(provider, dataset, filename)
    archive_path=get_archive_path(provider, dataset, filename)
    try:
        os.rename(stg_path,archive_path)
    except Exception as e:
        log.info('FAILURE archive_file: '+e.strerror)
    else:
        log.info('SUCCESS archive_file to :' + archive_path)

if __name__ == "__main__":
    logging.basicConfig(filename='C:\\code\\Porter\\logs\\archive.ini', level=logging.INFO)
    provider='NCO'
    dataset='PSA1to9ProgramData'

    test_file ='NCO initiative 01-02-17 to 01-08-17XX.xlsx'
    file_path=storage_config.STG_DIR+'\\'+provider+'\\'+dataset+'\\'+test_file

    try:
        df =  pd.read_excel(file_path, skiprows=2)
    except FileNotFoundError as detail:
        logging.info('File Does Not Exist:'+detail.strerror)
