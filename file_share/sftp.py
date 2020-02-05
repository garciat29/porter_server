from datetime import datetime
import pysftp
import config.extract.ftp as ftp_config
from database.sql_runner import SQLExecutor
import logging
log = logging.getLogger(__name__)

class SFTPDataset:
    def __init__(self, config):
        self.directory=config['directory']
        self.cred=ftp_config.FTP_CREDENTIALS[config['site_name']]
        self.db_table=config['delta_table']
        self.delta_column=config['delta_column']
        self.prefix=config['prefix']
        self.cnopts = pysftp.CnOpts()
        self.cnopts.hostkeys=None
        self.sql_db=SQLExecutor()

    def list_files(self):
        '''
        list all new files in folder since previous execution
        '''
        try:
            files=[]
            last_batch_date=self._previous_load_date()
            with pysftp.Connection(host=self.cred['host'],username=self.cred['user'], password=self.cred['passwd'], cnopts=self.cnopts) as sftp:
                with sftp.cd(self.directory):
                    for attr in sftp.listdir_attr():
                        load_date=datetime.fromtimestamp(attr.st_mtime)
                        if load_date > last_batch_date and attr.filename.startswith(self.prefix):
                            files.append({'filename': attr.filename})
            return files
        except Exception as e:
            logging.info('FAILURE: list_file :'+repr(e))

    def get_data(self, file_dict):
        '''
        read file located in dir specified by config
        '''
        try:
            with pysftp.Connection(host=self.cred['host'],username=self.cred['user'], password=self.cred['passwd'], cnopts=self.cnopts) as sftp:
                with sftp.cd(self.directory):
                    f=sftp.open(file_dict['filename'])
                    return f.read()
        except Exception as e:
            logging.info('FAILURE: get_data :'+repr(e))


    def _previous_load_date(self):
        '''
        get the last load time for a batch for this dataset
        '''
        query='select max('+self.delta_column+') as "delta" '
        query+='from '+self.db_table
        row=self.sql_db.exec_select_one(query)
        return row.delta

if __name__ == "__main__":
    provider='NYPD'
    dataset='Eagle Report'
    site_config={'directory': '/HOME/PROD/MOCJ',
        'site_name':'COMPSTAT',
        'tgt_table':'stg."nypd- eagle report"',
        'delta_column': '"s- insert date"',
        'prefix': 'housing.compstat'}
    fd=FTPDataset(provider,dataset,site_config)
    print(fd.list_files())
    #print(fd.cred)
