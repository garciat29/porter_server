import requests
import json
import config.sharepoint as config #contains application credentials

class SharepointAuthorizer:
    def __init__(self):
        self.client_id=config.CLIENT_ID
        self.client_secret=config.CLIENT_SECRET

    def get_auth_api_header(self):
        return {'Authorization': self.get_auth_value()}

    def get_auth_value(self):
        return 'Bearer '+self.get_token()

    def get_token(self):
        try:
            headers= {'Content-Type': 'application/x-www-form-urlencoded'}
            payload = {'grant_type': 'client_credentials',
                        'client_id': config.CLIENT_ID,
                        'client_secret': config.CLIENT_SECRET,
                        'scope': 'https://graph.microsoft.com/.default'
                        }
            auth_url='https://login.microsoftonline.com/common/oauth2/v2.0/token'
            r = requests.post(auth_url, data=payload, headers=headers)
        except Exception as e:
            logging.info('FAILURE: get_token():'+repr(e))
        return r.json()['access_token']

class SharepointDataset:
    def __init__(self, provider, dataset):
        self.provider=provider
        self.dataset=dataset
        self.authorizer=SharepointAuthorizer()

    def get_objects(self):
        obj_list=[]
        #folder_id=get_folder_id()
        #for file in list_files(folder_id)
        #  obj_list.append(SharepointDataObject())
        #return obj_list
        pass

    def get_folder_id(self):
        #datasetconfig=db.get_dataset_config(self.provider, self.dataset)
        #return datasetconfig.source_location
        pass

    def list_files(self, folder_id):
        #list_file_url='http://www.assbkjbfsdf'+
        #return requests.get(url, headers=SharepointAuthorizer.get_auth_api_header())
        pass

class SharepointDataObject:
    def __init__(self, folder_id, object_id):
        self.folder_id=folder_id
        self.object_id=object_id
        self.authorizer=SharepointAuthorizer()

    def get_object_bytes(self):
        #define data getters off of the api. use authorizer
        pass


if __name__ == '__main__':
    auth = SharepointAuthorizer()
    print(auth.get_auth_api_header())
    folder_url= 'https://graph.microsoft.com/v1.0/sites/nyco365.sharepoint.com:/sites/MOCJFileSharing/MAPDataShare'

    #Return information about the drive b!.......
    r2 = requests.get(folder_url,headers=auth.get_auth_api_header())
    print(r2.text)
