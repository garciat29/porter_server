#import file_share.box_access as box
#import file_share.sharepoint_data as sharepoint
import file_share.nyc_open_data as open_data
import file_share.zendesk as zendesk
import file_share.sftp as sftp

import file_storage.all_storage as all_storage
import logging
log = logging.getLogger(__name__)

class SharedDataset:
    def __init__(self, provider, dataset, config):
        self.provider=provider
        self.dataset_name=dataset
        self.config=config
        self.platform = self.config['platform']
        self.dataset_obj=self._get_shared_dataset()

    def list_files(self):
        return self.dataset_obj.list_files()

    def _get_shared_dataset(self):
        if self.platform == 'opendata':
            return open_data.OpenDataset(self.provider, self.dataset_name, self.config)
        if self.platform == 'Zendesk':
            return zendesk.ZendeskDataset(self.config)
        if self.platform == 'sftp':
            return sftp.SFTPDataset(self.config)

    def extract_all(self):
        try:
            folder=all_storage.StorageFolder(self.provider, self.dataset_name)
            for f in self.list_files():
                folder.put_file(f['filename'],self.dataset_obj.get_data(f))
        except Exception as e:
            logging.info('FAILURE: extract_all :'+repr(e))
            logging.info('Failed with config '+str(self.config))
        else:
            logging.info('Succeeded with config '+str(self.config))

if __name__ == "__main__":
    logging.basicConfig(filename='C:\\code\\Porter\\logs\\all_storage.ini', level=logging.INFO)
    config= {'endpoint': 'https://data.cityofnewyork.us/resource/a2h9-9z38.csv',
        #headers: config.HEADERS,
        'api_filter': "?$where=date_trunc_ymd(inspection_date) >= '##delta_date##'",
        'delta_query': 'select trunc(coalesce(max("inspection date"), dateadd(year, -1, current_date))) as dt from stg."opendata- rodent inspections"',
        'platform': 'opendata'
        }
    provider='opendata'
    dataset='rodent_inspections'
    od=SharedDataset(provider,dataset,config)
    od.extract_all()
