import requests
import config.nstat_zendesk as zendesk_config
import config.porter_app as porter_config
import datetime
from database.sql_runner import SQLExecutor
import json

import logging
log = logging.getLogger(__name__)

class ZendeskDataset:
    def __init__(self, config):
        self.curr_date_string=datetime.datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        self.sql_db=SQLExecutor()
        self.delta_query=config['delta_query']
        self.api_endpoint=config['endpoint']

        self.proxies=porter_config.PROXIES
        self.username=zendesk_config.API_USER
        self.password=zendesk_config.API_KEY
        self.header=zendesk_config.HEADER
        self.base_url=zendesk_config.BASE_URL

    def list_files(self):
        filename=self.curr_date_string+'.json'
        return [{'filename':filename}]

    def get_data(self, filename=''):
        try:
            req_url=self.base_url+self._api_replace()
            results=[]
            #using the while loop for pagination
            while req_url:
                r= requests.get(req_url,
                                auth=(self.username,self.password),
                                proxies=self.proxies,
                                headers=self.header)

                if r.json().get('results', False):
                    results+=r.json().get('results')
                if r.json().get('ticket_fields', False):
                    results+=r.json().get('ticket_fields')
                req_url=r.json().get('next_page',None)
        except Exception as e:
            logging.info('FAILURE: get_data :'+repr(e))
        else:
            logging.info('SUCCESS: get_data at endpoint '+self.api_endpoint)
            return json.dumps(results)

    def _get_delta(self):
        '''
        self.sql_db.exec_select_one()
        '''
        return self.sql_db.exec_select_one(self.delta_query)[0]

    def _api_replace(self):
        try:
            delta_dt=self._get_delta()
            delta_flag='##delta_value##'
        except Exception as e:
            logging.info('FAILURE: _api_replace :'+repr(e))
        else:
            return self.api_endpoint.replace(delta_flag, delta_dt)



if __name__ == "__main__":
    logging.basicConfig(filename='C:\\code\\Porter\\logs\\zendesk.ini',
        level=logging.INFO,
        format='%(asctime)s %(name)-12s %(levelname)-8s %(message)s %(lineno)d')

    user_delta_query='select to_char(coalesce(max("updated at"),dateadd(year,-2,current_date)),\'YYYY-MM-DD\') as dt'
    user_delta_query+=' from stg."zendesk- users"'
    users_endpoint='search.json?query=updated_at>##delta_value## type:user'

    users_config={'endpoint':users_endpoint, 'delta_query':user_delta_query}

    u=ZendeskDataset(users_config)
    print(u.get_data())
    #print(u.get_data('123')[1])
