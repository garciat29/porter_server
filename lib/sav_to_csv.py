from savReaderWriter import SavReader
from file_storage import s3_storage as s3
import os
import csv

class FileConverter:
    def __init__(self, input_config):
        self.input_folder=s3.S3Folder(input_config)
        self.input_config=input_config
        self.data_root='data'
        self.transform_prefix='TRNFRM_'
        self.local_path=self.get_local_path()

    def get_local_path(self):
        project=self.input_config['project']
        provider=self.input_config['provider']
        dataset=self.input_config['dataset']
        return os.path.join('/',self.data_root,project,provider,dataset)

    def write_to_disk(self):
        for file in self.input_folder.list_stg_files():
            self.input_config['filename']=file
            f=s3.S3File(self.input_config)
            tgt_path=os.path.join(self.local_path, file)

            if not os.path.exists(self.local_path):
                os.makedirs(self.local_path)
            with open(tgt_path, 'wb+') as outfile:
                outfile.write(f.get_file_bytes().read())

    def convert_files(self):
        '''
        Read file from S3, write to disk, and upload back to S3
        '''
        try:
            self.write_to_disk()
            self.to_csv()
            self.post_csvs()
        except Exception as e:
            print(e)
        else:
            self.remove_from_disk()


    def to_csv(self):
        '''
        list all SAV files and convert to csv
        '''
        for f in os.listdir(self.local_path):
            if f.endswith('.SAV') and not f.startswith(self.transform_prefix):
                tgt_name=self.transform_prefix+f[:-4]+'.csv'
                with SavReader(os.path.join(self.local_path,f),ioUtf8=True) as reader:
                    header = reader.header
                    with open(os.path.join(self.local_path,tgt_name), 'w+',encoding='utf-8') as csvfile:
                        writer = csv.writer(csvfile)
                        writer.writerow(header)
                        for line in reader:
                            writer.writerow(line)

    def post_csvs(self):
        '''
        Move converted files to S3
        '''
        for f in os.listdir(self.local_path):
            if f.endswith('.csv') and f.startswith(self.transform_prefix):
                self.input_config['filename']=f
                tgt_s3=s3.S3File(self.input_config)
                with open(os.path.join(self.local_path,f), 'r') as csvfile:
                    tgt_s3.write_file(csvfile.read())


    def remove_from_disk(self):
        '''
        Delete everything. This isn't a file store
        '''
        for f in os.listdir(self.local_path):
            os.remove(os.path.join(self.local_path, f))

if __name__ == '__main__':
    input_config= {'bucket': 'mocjbucket01',
            'environment':'NYCDev',
            'project': 'MOCJ',
            'provider': 'DOC',
            'dataset': 'Adm_SAV'
            }

    FileConverter(input_config).convert_files()
