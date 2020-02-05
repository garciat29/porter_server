'''
Box Authentication mechanism

Utilization:
Has routes that
authenticate
download all files in a folder
'''
from boxsdk import Client, JWTAuth
import config.box as box_config

import logging
log = logging.getLogger(__name__)

def get_client():
    '''
    Returns the client object for running Box commands
    '''
    try:
        auth = JWTAuth(
            client_id = box_config.CLIENT_ID,
            client_secret = box_config.CLIENT_SECRET,
            enterprise_id = box_config.ENTERPRISE_ID,
            jwt_key_id = box_config.JWT_KEY_ID,
            rsa_private_key_file_sys_path = box_config.CERT,
            rsa_private_key_passphrase = box_config.PASSPHRASE
        )
        client= Client(auth)
        #should only be one porter user. only app user in my account
        porter_user = client.users()[0]
        porter_client = client.as_user(porter_user)
    except Exception as e:
        log.info('FAILURE: get_client ')
        log.info(e)
    else:
        log.info('SUCCESS: get_client ')
        return porter_client


def list_files(src_folder):
    '''
    Returns list of all file objects in the folder
    '''
    try:
        porter_client = get_client()
        file_list = porter_client.folder(folder_id=src_folder).get_items(10)
    except Exception as e:
        log.info('FAILURE: list_files')
        log.info(e)
        return []
    else:
        return file_list

def remove_file(file_obj):
    '''
    Post download, remove file
    '''
    pass

def read_content(file_obj):
    '''
    only the authorized client can download
    '''
    #client=get_client()
    #with open(dest_path, 'wb') as dest_file:
        #client.file(file_obj.id).download_to(dest_pathl)
    return file_obj.content()


def get_orig_file_name(file_obj):
    '''
    Returns the original file name as it was uploaded
    '''
    try:
        url = file_obj.get_shared_link_download_url()
        file_name = url.split('/')[-1]
    except Exception as e:
        log.info('FAILURE: get_file_name')
        log.info(e)
    else:
        return file_name

def get_storage_file_name(file_obj):
    '''
    Returns file id + file name to get a unqiue name to prevent write issues
    '''
    try:
        base_name = get_orig_file_name(file_obj)
        file_id = file_obj.id
        storage_file_name=file_id+'_'+base_name
    except Exception as e:
        log.info('FAILURE: get_storage_file_name')
        log.info(e)
    else:
        return storage_file_name


if __name__ == "__main__":
    logging.basicConfig(filename='C:\\code\\Porter\\logs\\box_access.ini', level=logging.INFO,format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s')
    test_folder='45462373047'
    file_list = list_files(test_folder)
    for f in file_list:
        #read_content(f)
        print(get_orig_file_name(f))
        #file_info = get_file_dict(f)
        #print(file_info)
