'''
Generic wrapper for file storage systems
'''

import logging
import file_storage.windows_fs as win_fs
import file_storage.s3_storage as s3_fs
import config.porter_app as app_config
import metadata.project_manager as pm

log = logging.getLogger(__name__)


class StorageFolder:
    def __init__(self, provider, dataset):
        self.project=pm.ProjectManager()
        self.folder=self._get_folder(provider, dataset)

    def put_file(self, filename, body):
        self.folder.put_file(filename, body)

    def archive_file(self, filename):
        self.folder.archive_file(filename)

    def archive(self):
        self.folder.archive()

    def _get_folder(self, provider, dataset):
        if app_config.FILE_STORAGE=='S3':
            return s3_fs.S3Folder(provider, dataset,self.project)

    def get_stg_path(self):
        return self.folder.get_stg_path()

    def get_arch_path(self):
        return self.folder.get_arch_path()

    def list_stg_files(self):
        return self.folder.list_stg_files()

    def get_file_bytes(self, filename):
        return self.folder.get_file_bytes(filename)



##############old####################

def write_byte_file(raw_bytes, provider, dataset, file_name):
    '''
    Given raw bytes, provider, dataset, and output filename
    Write to stg
    '''
    try:
        if app_config.FILE_STORAGE=='Windows':
            path=win_fs.get_stg_path(provider, dataset, file_name)
            output = open(path, 'wb')
        elif app_config.FILE_STORAGE=='S3':
            s3_fs.write_byte_file(raw_bytes, provider, dataset, file_name)
    except Exception as e:
        log.info('FAILURE: write_byte_file ')
        log.info(e)
    else:
        log.info('SUCCESS: write_byte_file ')

def get_file_bytes(provider, dataset, file_name):
    if app_config.FILE_STORAGE=='S3':
        return s3_fs.get_file_bytes(provider, dataset, file_name)


def list_stg_files(provider, dataset):
    if app_config.FILE_STORAGE=='Windows':
        pass
    if app_config.FILE_STORAGE=='S3':
        return s3_fs.list_stg_files(provider, dataset)

def get_stg_path(provider, dataset, filename=''):
    if app_config.FILE_STORAGE=='Windows':
        return win_fs.get_stg_path(provider, dataset, filename)
    if app_config.FILE_STORAGE=='S3':
        return s3_fs.get_stg_path(provider, dataset, filename)

def get_arch_path(provider, dataset, filename=''):
    if app_config.FILE_STORAGE=='S3':
        return s3_fs.get_arch_path(provider, dataset, filename)

def write_transformed_dataframe(dataframe,provider, dataset, filename):
    ##outdated. storage will only write csv buffers from transform
    if app_config.FILE_STORAGE=='Windows':
        win_fs.write_transformed_dataframe(dataframe,provider, dataset, filename)

def write_csv_buffer(buff,provider, dataset, filename):
    if app_config.FILE_STORAGE=='S3':
        return s3_fs.write_csv_buffer(buff,provider, dataset, filename)

def archive_file(provider, dataset, filename):
    if app_config.FILE_STORAGE=='S3':
        s3_fs.archive_file(provider, dataset, filename)

if __name__ == "__main__":
    logging.basicConfig(filename='C:\\code\\Porter\\logs\\all_storage.ini', level=logging.INFO)
    provider='NCO'
    dataset='PSA1to9ProgramData'
    test_file='C:\\raw_data\\MAP_STG\\NCO\\PSA1to9ProgramData\\TRNFRM_269922035250_7cx9u6r05yupzeld4w6nn2joih1uu0ix.csv'
    file_name='!!!!269922035250_7cx9u6r05yupzeld4w6nn2joih1uu0ix.csv'

    #archive_file(provider, dataset, file_name)

    raw_buff=open(test_file, 'rb')
    fold=StorageFolder(provider,dataset)
    fold.put_file(file_name,raw_buff)
