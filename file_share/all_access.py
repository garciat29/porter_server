'''
#########Outdated. Up for delete. Use shared_data.py
Wrapper for calling file access modules
Functions require a platform to determining module to use
'''

import file_share.box_access as box
import logging
log = logging.getLogger(__name__)


def list_files(folder_id, platform):
    '''
    Based on the platform, import the correct library
    Return a list of platform specific file objects
    '''
    try:
        if platform == 'Box':
            all_files = box.list_files(folder_id)
            log.info('Number of files '+ str(len(all_files)))
        else:
            log.info('Platform not recognized')
            all_files = []
    except Exception as e:
        log.info('FAILURE: list_files- Box ')
        log.info(e)
        all_files = []
    else:
        return all_files

def get_byte_content(file_obj, platform):
    '''
    Returns byte content for a file along with
    '''
    try:
        if platform == 'Box':
            byte_content = file_obj.content()
        else:
            log.info('Unrecognized platform')
    except Exception as e:
        log.info('FAILURE: get_byte_content')
        log.info(e)
        return None
    else:
        return byte_content

def get_file_name(file_obj, share_platform):
    try:
        if share_platform == 'Box':
            file_name = file_obj.id + box.get_file_name(file_obj)
        else:
            log.info('Unrecognized sharing platform')
    except Exception as e:
        log.info('FAILURE: get_file_name')
        log.info(e)
        return None
    else:
        return file_name

def get_storage_file_name(file_obj, share_platform):
    try:
        if share_platform == 'Box':
            storage_file_name = box.get_storage_file_name(file_obj)
        else:
            log.info('Unrecognized platform')
    except Exception as e:
        log.info('FAILURE: get_storage_file_name')
        log.info(e)
        return None
    else:
        return storage_file_name


if __name__ == "__main__":
    logging.basicConfig(filename='C:\\code\\Porter\\logs\\all_access.ini', level=logging.INFO)
    test_platform = 'Box'
    test_folder ='45462373047'
    #all_f = list_files(test_folder,test_platform)
    #print(all_f)
    for f in list_files(test_folder,test_platform):
        #file_dict = get_file_dict(f,test_platform)
        #print(f)
        #print(file_dict)
        print(get_file_name(f,test_platform))
