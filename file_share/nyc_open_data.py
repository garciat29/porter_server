import requests
from io import BytesIO
from io import StringIO
import config.socrata as socrata
import file_storage.all_storage as fs
from database.sql_runner import SQLExecutor

import logging
log = logging.getLogger(__name__)


class OpenDataset:
    def __init__(self, provider, dataset, config):
        self.provider=provider
        self.dataset=dataset
        self.config=config
        self.sql_db=SQLExecutor()
        self.limit=1000 #limit for paging

    def get_data(self, file):
        #file is ignored, since it is an API reuqest
        try:
            base=self.get_url()
            full_file=''
            counter=0
            has_content=True
            num_lines=2 #arbitrarily set to kick off while loop
            #need to loop when the last set had results.
            while num_lines>1:
                #reset exit parameter if last iteration passed
                num_lines=0
                logging.info('getting :'+self._page_url(base,counter))
                r=requests.get(self._page_url(base,counter), headers=socrata.HEADERS)
                for l in r.iter_lines(decode_unicode=True):
                    num_lines+=1
                if num_lines>1:
                    full_file+=r.text
                counter+=1
        except Exception as e:
            logging.info('FAILURE: get_data :'+repr(e))
        else:
            logging.info('SUCCESS: get_data')
            return full_file


    def list_files(self):
        #need list of filenames
        return [{'filename': self._get_delta()+'.csv'}]

    def get_url(self):
        return self.config['endpoint']+self._get_filter()

    def _page_url(self, url, counter):
        return url+' LIMIT '+str(self.limit)+' OFFSET '+str(self.limit*counter)


    def _get_delta(self):
        #pass config.delta_query to db
        #return 'YYYY-MM-DD'
        try:
            result = self.sql_db.exec_select_one(self.config['delta_query'])[0]
        except Exception as e:
            logging.info('FAILURE: _get_delta :'+repr(e))
        else:
            return str(result)

    def _get_filter(self):
        delta= self._get_delta()
        delta_repl='##delta_date##'
        return self.config['api_filter'].replace(delta_repl, delta)


if __name__ == "__main__":
    logging.basicConfig(filename='C:\\code\\Porter\\logs\\nyc_open_data.ini',
        level=logging.INFO,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s %(lineno)d')

    config= {'endpoint': 'https://data.cityofnewyork.us/resource/fhrw-4uyv.csv',
        'api_filter': "?$query=SELECT unique_key,created_date,complaint_type,latitude,longitude WHERE date_trunc_ymd(created_date) >= '2018-05-23' ORDER BY created_date",
        'delta_query': 'select trunc(dateadd(year, -1, current_date)) as dt',  ##currently hardcoded to 1 year
        'platform': 'opendata'
        }

    provider='opendata'
    dataset='311_complaints'
    od=OpenDataset(provider,dataset,config)
    print(od.get_data('fake123'))
