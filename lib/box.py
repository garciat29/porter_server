import json
from boxsdk import JWTAuth
from boxsdk.config import Proxy
from boxsdk import Client
from boxsdk.session.session import Session
from file_storage import s3_storage as storage
from io import BytesIO

class Box:
    def __init__(self):
        #p=Proxy()
        #p.URL='https://bcpxy.nycnet:8080'
        #p.AUTH={'user': 'tgarcia', 'password':'Mexkimo0219'}
        #self.session=Session(proxy_config=p)
        self.session=Session()
        self.sdk = JWTAuth.from_settings_file('/code/mocj_porter/config/box_config.json')
        self.client=Client(self.sdk,self.session)

    def create_folder(self,name, parent_id):
        try:
            self.client.folder(folder_id=parent_id).create_subfolder(name)
        except Exception as e:
            return {'status': 400, 'message': e}
        else:
            return {'status':200, 'message': 'Success'}

    def post_file(self,s3bucket,file_config,box_folder_id):
        file=storage.S3File(s3bucket,file_config)
        file_bytes=BytesIO(file.get_file_bytes())
        print('####hEEEYYYY#####')
        #self.client.folder(box_folder_id).upload_stream(file_bytes, file_config['filename'])
        self.client.folder(box_folder_id).upload('/code/mocj_porter/test.csv','test')
        return {'status':200, 'message': 'Success'}
        #s3_folder=s3_storage.S3Folder('mocjbucket01','MAP', 'Zendesk', 'NStat')
        #print(s3_folder.stg_key)


if __name__ == "__main__":
    #Box().post_file('https://s3.amazonaws.com/mocjbucket01/MAP_DATA_STG/Zendesk/NStat/2018-06-21+11%3A46%3A19.json','57679221714')
    bucket='mocjbucket01'
    config={'environment':'NYCDev',
            'project': 'Testing',
            'provider': 'MOCJ',
            'dataset': 'SL_Tests',
            'filename': 'test.csv'
            }
    #print(Box().create_folder('foo','57679221714'))
    print(Box().post_file(bucket,config, '57679221714'))
