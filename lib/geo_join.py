import geopandas as gpd
import pandas as pd
from shapely.geometry import Point
from file_storage import s3_storage as s3
from io import StringIO
import os

class GeoJoiner:
    def __init__(self, input_config, geofile_config):
        self.input_folder=s3.S3Folder(input_config)
        self.crs=input_config['crs']
        self.lat_col=input_config['lat_col']
        self.long_col=input_config['long_col']
        self.geofile_config=geofile_config
        self.transform_prefix='TRNFRM_'

    def get_geofile_path(self):
        return os.path.join('/','code','mocj_porter','geo',self.geofile_config['folder'],self.geofile_config['filename'])

    def join_files(self):
        for s3file in self.input_folder.get_all_files(exclusion_prefix=self.transform_prefix):
            outfile_name=self.transform_prefix+s3file._get_stg_key().split('/')[-1]
            outfile=self.input_folder.get_file_obj(outfile_name)

            geo_file=gpd.read_file(self.get_geofile_path())
            geo_file= geo_file.to_crs({'init': self.crs})
            chunksize=10000
            reader=pd.read_csv(s3file.get_file_bytes(), chunksize=chunksize, quotechar='"', header=0)
            outfile.write_file(self.chunk_joiner(reader,geo_file))

    def chunk_joiner(self, reader, geo_file):
        i=0
        for raw_complaints in reader:
            raw_complaints.rename(columns=lambda x: x.lower().replace(' ','_'), inplace=True)
            geom = raw_complaints.apply(lambda x: Point(float(x[self.long_col]), float(x[self.lat_col])), axis=1)
            geo_complaints=gpd.GeoDataFrame(raw_complaints, geometry=geom)
            geo_complaints.crs={'init': self.crs}
            geo_complaints = gpd.sjoin(geo_complaints,geo_file, how='left', op='within')
            if i==0:
                out_frame=geo_complaints
            else:
                out_frame=pd.concat([out_frame,geo_complaints])
            i+=1
        return out_frame.to_csv(mode='wb')

    def archive_originals(self):
        self.input_folder.archive(exclusion_prefix=self.transform_prefix)


if __name__ == '__main__':
    input_config= {'bucket': 'mocjbucket01',
            'environment':'NYCDev',
            'project': 'MAP',
            'provider': 'Open Data',
            'dataset': '311',
            'lat_col': 'latitude',
            'long_col': 'longitude',
            'crs': 'epsg:4326'
            }

    geofile_config= {'folder': 'nycha_manual_buffer',
            'filename': 'MAP_buffer_4326.shp',
            'crs': 'epsg:4326'
            }

    gj=GeoJoiner(input_config,geofile_config)
    #gj.join_files()
    gj.archive_originals()
