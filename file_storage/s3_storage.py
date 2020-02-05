import boto3
from io import StringIO, BytesIO
from config import app
from botocore.config import Config

class S3File:
    def __init__(self,file_config):
        self.bucket=file_config['bucket']
        self.environment=file_config['environment']
        self.project=file_config['project']
        self.provider=file_config['provider']
        self.dataset=file_config['dataset']
        self.filename=file_config['filename']
        #self.s3=boto3.session.Session().resource('s3')
        self.s3=boto3.session.Session().resource('s3', config=Config(proxies=app.PROXIES))
        #self.s3folder=S3Folder(bucket,file_config)

    def _get_stg_key(self):
        key=self.environment+'/'+self.project+'_STG/'
        key=key+self.provider+'/'+self.dataset+'/'+self.filename
        return key

    def _get_arch_key(self):
        key=self.environment+'/'+self.project+'_ARCH/'
        key=key+self.provider+'/'+self.dataset+'/'+self.filename
        return key

    def get_file(self):
        '''
        return an IO buffer of the file
        '''
        obj= self.s3.Object(self.bucket,self._get_stg_key())
        return StringIO(obj.get()['Body'].read().decode('utf-8'))

    def get_file_bytes(self):
        '''
        return bytes IO buffer of the file
        '''
        obj= self.s3.Object(self.bucket,self._get_stg_key())
        return BytesIO(obj.get()['Body'].read())

    def write_file(self, input):
        stg_key= self._get_stg_key()
        obj = self.s3.Object(self.bucket,stg_key)
        obj.put(Body=input)

    def archive(self):
        stg_key=self._get_stg_key()
        arch_key=self._get_arch_key()
        source = {'Bucket':self.bucket,
                'Key':stg_key}
        bucket = self.s3.Bucket(self.bucket)
        arch_obj = bucket.Object(arch_key)
        arch_obj.copy_from(CopySource=source)
        bucket.delete_objects(Delete={'Objects':[{'Key':stg_key}]})

class S3Folder:
    def __init__(self, folder_config):
        self.folder_config=folder_config
        self.bucket=folder_config['bucket']
        self.environment=folder_config['environment']
        self.project=folder_config['project']
        self.provider=folder_config['provider']
        self.dataset=folder_config['dataset']
        self.s3=self._get_resource()

    def _get_resource(self):
        session=boto3.session.Session()
        return session.resource('s3')

    def _get_stg_key(self, filename=''):
        key=self.environment+'/'+self.project+'_STG/'
        key=key+self.provider+'/'+self.dataset
        return key

    def get_all_files(self, exclusion_prefix=''):
        '''
        returns a list of all S3File objects, expcet those starting
        with the exclusion_prefix string
        '''
        all_files=[]
        for f in self.list_stg_files():
            if not f.startswith(exclusion_prefix):
                all_files.append(self.get_file_obj(f))
        return all_files

    def put_file(self, filename, raw_body):
        stg_key= self._get_stg_key(filename)
        obj = self.s3.Object(self.bucket,stg_key)
        obj.put(Body=raw_body)

    def get_file_obj(self, filename):
        new_config=self.folder_config.copy()
        new_config['filename']=filename
        return S3File(new_config)

    def archive(self, exclusion_prefix=''):
        #for files in folder
        for f in self.get_all_files(exclusion_prefix):
            f.archive()

    def list_stg_files(self):
        obj_summary = self.s3.Bucket(self.bucket).objects.filter(Prefix=self._get_stg_key())
        file_list=[]
        for obj in obj_summary:
            f_name=obj.key.split('/')[-1]
            if f_name.find('.') != -1:
                file_list.append(f_name)
        return file_list

    def archive_file(self, filename):
        stg_key=self._get_stg_key(filename)
        arch_key=self._get_arch_key(filename)
        source = {'Bucket':self.bucket,
                'Key':stg_key}
        bucket = self.s3.Bucket(self.bucket)
        arch_obj = bucket.Object(arch_key)
        arch_obj.copy_from(CopySource=source)
        bucket.delete_objects(Delete={'Objects':[{'Key':stg_key}]})

    def get_stg_path(self,filename=''):
        return 's3://'+self.bucket+'/'+self._get_stg_key(filename)

    def get_arch_path(self,filename=''):
        return 's3://'+self.bucket+'/'+self._get_arch_key(filename)

if __name__ == "__main__":
    config={'bucket': 'mocjbucket01',
            'environment':'NYCDev',
            'project': 'Testing',
            'provider': 'MOCJ',
            'dataset': 'SL_Tests',
            'filename': 'test.csv'
            }

    f=S3File(config )
    print(f.get_file_bytes())
