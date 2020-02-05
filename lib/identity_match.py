from datetime import datetime
import dedupe
from file_storage import s3_storage as s3
import csv
from unidecode import unidecode
import re
import random
from io import StringIO, BytesIO
from itertools import permutations

#API will exec actual match code
class DataSource(s3.S3File):
    def __init__(self, file_config, id_col):
        s3.S3File.__init__(self, file_config)
        self.id_col=id_col

    def get_col_vals(self, col):
        all_vals=[]
        with self.get_file() as f:
            reader = csv.DictReader(f)
            for row in reader:
                all_vals.append(row[col])
        return all_vals

    def get_data_dict(self):
        '''
        returns a dictionary representation of the cleaned data.
        key for each record is the id_col
        '''
        data_d={}
        with self.get_file() as f:
            reader = csv.DictReader(f)
            for i, row in enumerate(reader):
                clean_row={}
                for k,v in row.items():
                    clean_row[k]=self._preprocess_col(v)
                row_id = int(row[self.id_col])
                if len(clean_row)>1:
                    data_d[row_id] = clean_row
        return data_d

    def split(self, datasource1, datasource2, file1_pct=.2):
        '''
        split the data source into to 2 files.
        split percent will take a random set of rows based on the id_col
        '''
        all_ids=self.get_col_vals(self.id_col)
        file1_size=int(len(all_ids)*file1_pct)
        file1_ids=random.sample(all_ids, file1_size)

        with StringIO() as f1, StringIO() as  f2:
            reader=csv.DictReader(self.get_file())
            f1_writer=csv.DictWriter(f1, fieldnames=reader.fieldnames)
            f2_writer=csv.DictWriter(f2, fieldnames=reader.fieldnames)
            f1_writer.writeheader()
            f2_writer.writeheader()

            for row in reader:
                clean_row=dict((k, self._preprocess_col(v)) for (k,v) in row.items())
                if row[self.id_col] in file1_ids:
                    f1_writer.writerow(clean_row)
                else:
                    f2_writer.writerow(clean_row)
            datasource1.write_file(f1.getvalue())
            datasource2.write_file(f2.getvalue())

    def _preprocess_col(self, column):
        '''
        returns the column with special characters removed
        '''
        column = unidecode(column)
        column = re.sub('\n', ' ', column)

        column = re.sub('-', '', column)
        #column = re.sub('/', ' ', column)
        column = re.sub("'", '', column)
        column = re.sub(",", '', column)
        column = re.sub(":", ' ', column)
        column = re.sub('  +', ' ', column)
        column = column.strip().strip('"').strip("'").lower().strip()
        if not column:
            column = None
        return column

class MatchedData(DataSource):
    def __init__(self, file_config, id_col, cluster_col , match_fields):
        DataSource.__init__(self, file_config, id_col)
        self.file_config=file_config
        self.cluster_col=cluster_col
        self.match_fields=match_fields

    def enumerate_matches(self):
        '''
        List every row id with every row id in its cluster.
        Optionally list with confidence score column
        Returns a list of dicts
        '''
        #raw_data=self.get_data_dict()
        header=[self.id_col, self.id_col+'_match']
        #create dict of {cluster_id:[ids]}
        cluster_dict={}
        with self.get_file() as f:
            reader = csv.DictReader(f)
            for row in reader:
                curr_val=cluster_dict.get(row[self.cluster_col],[])
                curr_val.append(row[self.id_col])
                cluster_dict[row[self.cluster_col]]=curr_val
        #use itertools.permutations for each key
        with StringIO() as outp:
            writer=csv.DictWriter(outp, fieldnames=header)
            writer.writeheader()
            for k,v in cluster_dict.items():
                combos=permutations(v,2)
                for c in combos:
                    writer.writerow({header[0]: c[0], header[1]:c[1]})
            new_config=self.file_config
            new_config['filename']='enum_'+self.file_config['filename']
            s3.S3File(new_config).write_file(outp.getvalue())

    def sample(self, pct):
        '''
        Get a percent of clusters from the original matched data.
        '''
        cluster_list=set(self.get_col_vals(self.cluster_col))
        sample_size=int(len(cluster_list)*pct)
        sample_clusters=random.sample(cluster_list, sample_size)
        #Iterate over all rows in the data.
        #reader=csv.DictReader(self.get_file())
        reader=self.get_data_dict()
        sample_data={}
        for id,row in reader.items():
            if 'NYSID' not in row.keys():
                print(id,row)
            if row[self.cluster_col] in cluster_list:
                sample_data[id]=row
        return sample_data, sample_clusters


    def get_sample_matches(self, pct):
        '''
        Return a subset of matched data
        '''
        sample_data, sample_clusters= self.sample(pct)
        match_list=[]
        for c in sample_clusters:
            curr_cluster={}
            for k,v in sample_data.items():
                if v[self.cluster_col]==c:
                    curr_cluster[k]=v
            if len(curr_cluster)>1:
                match_list+=self._get_cluster_match_tuples(curr_cluster)
        return match_list, sample_data

    def _get_cluster_match_tuples(self,cluster):
        '''
        for a given cluster of data, generate all relevant match tuples
        '''
        match_list=[]
        for k1,v1 in cluster.items():
            for k2, v2 in cluster.items():
                #make sure we aren't matching a row to itself
                if k1!=k2:
                    match_list.append(self._make_tuple(v1,v2))
        return match_list

    def _make_tuple(self, record1, record2):
        '''
        given 2 rows of data, create a tuple with just their match fields
        data can be added to a distinct or match list in training data.
        '''
        dict1={}
        dict2={}
        for f in self.match_fields:
            fieldname=f['field']
            dict1[fieldname]=record1.get(fieldname,None)
            dict2[fieldname]=record2.get(fieldname,None)
        return (dict1,dict2)

class Trainer:
    '''
    Takes a sample of given records from a file to produce training and settings files.
    All output to be used by dedupe.
    The sample_pct of the cluster col values are randomly extracted.
    '''
    def __init__(self, training_data, settings_file, match_fields, id_col, cluster_col):
        self.settings_file=s3.S3File(settings_file)
        self.training_data=MatchedData(training_data,id_col,cluster_col, match_fields)

    def write_config(self,distinct_config=None):
        '''
        Generate the training data file and then write the settings
        '''
        print('creating deduper')
        print(datetime.now())
        deduper=dedupe.Dedupe(self.training_data.match_fields)
        print('getting sample matches')
        print(datetime.now())
        match_list, sample_data=self.training_data.get_sample_matches(.3)
        print('done- sample matches')
        print(datetime.now())
        if distinct_config:
            distincts=self.get_distinct_from_file(distinct_config)
        else:
            distincts=[]
        training_dict={"distinct":distincts, "match":match_list}
        deduper.sample(sample_data)
        print('done- sample')
        print(datetime.now())
        deduper.markPairs(training_dict)
        print('done- markPairs')
        print(datetime.now())
        deduper.train(recall=.9)
        print('done- train')
        print(datetime.now())
        with BytesIO() as sf:
            deduper.writeSettings(sf)
            self.settings_file.write_file(sf.getvalue())

    def get_distinct_from_file(self, file_config):
        '''
        Read in a user generated set of near misses in format"
        FieldA1, FieldB1 ......FieldA2, FieldB2
        Name1, DOB1.....Name2, DOB2
        '''
        distinct_list=[]
        distinct_file=s3.S3File(file_config)
        with distinct_file.get_file() as f:
            reader=csv.DictReader(f)
            header=reader.fieldnames
            for row in reader:
                record1={}
                record2={}
                for f in self.training_data.match_fields:
                    fieldname=f['field']
                    record1[fieldname]=self.training_data._preprocess_col(row[fieldname+'1'])
                    record2[fieldname]=self.training_data._preprocess_col(row[fieldname+'2'])
                distinct_list.append(self._make_tuple(record1,record2))
        return distinct_list

    def _make_tuple(self, record1, record2):
        '''
        given 2 rows of data, create a tuple with just their match fields
        data can be added to a distinct or match list in training data.
        '''
        dict1={}
        dict2={}
        for f in self.training_data.match_fields:
            fieldname=f['field']
            dict1[fieldname]=record1[fieldname]
            dict2[fieldname]=record2[fieldname]
        return (dict1,dict2)

class Matcher:
    '''
    Given input data and config data, dedupe the data and write the results
    '''
    def __init__(self, data_source, settings_file, id_col, outp_file, recall_weight=.5):
        self.settings_file=s3.S3File(settings_file)
        self.outp_file=DataSource(outp_file, id_col)
        self.recall_weight=recall_weight
        self.data_source=DataSource(data_source,id_col)


    def gen_match_blocks(self, deduper, src_data):
        '''
        {pred: (ids)}
        '''
        blocker = deduper.blocker(list(src_data.items()))
        pred_dict={}
        for b in blocker:
            #print(b[0])
            pred_dict[b[0]]=pred_dict.get(b[0],set()).union(set([b[1]]))
        #hold the output data
        blocks=[]
        #create ref for what other blocks the ids belong to
        block_ref_dict={}
        for i, pd in enumerate(pred_dict.items()):
            #i=block id, don't write if 0
            block_members=[]
            if len(pd[1])>1:
                for id in pd[1]:
                    prior_blocks=block_ref_dict.get(id,set())
                    block_members.append(tuple([id, src_data[id], prior_blocks]))
                    block_ref_dict[id]=prior_blocks.union(set([i+1]))
                blocks.append(tuple(block_members))
        return blocks

    def match(self):
        '''
        generate the clusters
        '''
        print('Start Match Parent Method')
        print(datetime.now())
        with self.settings_file.get_file_bytes() as s:
            deduper=dedupe.StaticDedupe(s)

        src_data=self.data_source.get_data_dict()
        print('Start Threshold')
        print(datetime.now())
        threshold = deduper.threshold(src_data, recall_weight=self.recall_weight)
        print('threshold equals '+str(threshold))
        print('Start Dedupe Match')
        print(datetime.now())
        clustered_dupes = deduper.match(src_data, threshold)
        print('Start Writing Clusters')
        print(datetime.now())
        self.write_clusters(clustered_dupes, src_data)


    def write_clusters(self, clustered_dupes, src_data):
        '''
        write data with id, cluster_id, conf_score, <canoncial columns>
        '''
        with StringIO() as f:
            is_header=True
            writer=csv.writer(f)
            for (cluster_id, cluster) in enumerate(clustered_dupes):
                id_set, scores = cluster
                score_dict={id:score for id, score in zip(id_set,scores)}
                #zip id_set, scores together for [(id,score),(id,score)]
                all_cluster_rows= [src_data[c] for c in id_set] #list of dicts
                canonical_rep = dedupe.canonicalize(all_cluster_rows)
                #make a score dict with zip.
                for rec in all_cluster_rows:
                    rec['cluster_id']=cluster_id

                    for can_col in canonical_rep:
                        rec['canoncial_'+can_col]=canonical_rep[can_col]
                    rec['confidence']=score_dict[int(rec[self.data_source.id_col])]
                    if is_header:
                        writer.writerow(rec.keys())
                        is_header=False
                    writer.writerow(rec.values())
            self.outp_file.write_file(f.getvalue())


    def gen_cluster_members(self, clustered_dupes, src_data):
        '''
        format and canonicalize the matched data
        '''
        #clustered dupes like [((<ids>), (<conf scores>)),... ]
        cluster_membership = {}
        for (cluster_id, cluster) in enumerate(clustered_dupes):
            id_set, scores = cluster
            cluster_d = [src_data[c] for c in id_set]
            canonical_rep = dedupe.canonicalize(cluster_d)
            for record_id, score in zip(id_set, scores):
                cluster_membership[record_id] = {
                    "cluster id" : cluster_id,
                    "canonical representation" : canonical_rep,
                    "confidence": score
                }
        return cluster_membership


if __name__ == "__main__":
    def same_or_not(field_1, field_2):
        if field_1 == field_2 :
            return 0
        else:
            return 1

    def string_similarity(field1,field2):
        cntr=0
        if field1 and field2:
            if len(field1)>=len(field2):
                field_len=len(field1)
            else:
                field_len=len(field2)
            for i in range(field_len):
                if field1[i]==field2[i]:
                    cntr+=1
            return cntr/float(field)
        else:
            return cntr


    print('START')
    print(datetime.now())
    config={'bucket': 'mocjbucket01',
            'environment':'NYCDev',
            'project': 'JJDB',
            'provider': 'CJA',
            #'provider': 'MOCJ',
            'dataset': 'training_data',
            #'dataset': 'Cycle Resolution',
            'filename': 'JJDB.csv'
            #'filename': 'Cycle Resolution.csv'
            }
    id_col='cycle_id'
    cluster_col='UNIQUEID'
    match_fields=[
        #{'field':'FNAM', 'type': 'String'},
        #{'field':'LNAM', 'type': 'String'},
        {'field':'FULLNAM', 'type': 'String'},
        {'field':'DOB', 'type': 'DateTime'},
        {'field':'SEX', 'type': 'Categorical', 'categories': ['male','female'],'has missing': True, 'comparator': same_or_not},
        {'field':'NYSID', 'type': 'String','has missing': True, 'comparator': string_similarity}
        ]

    full_config=config.copy()
    full_data=MatchedData(full_config, id_col, cluster_col , match_fields)
    #SETTINGS DATA


    #TRAINING DATA
    config.update({'filename':'train_JJDB.csv'})
    training_config=config.copy()
    training_data=MatchedData(training_config, id_col, cluster_col , match_fields)
    #TRAINING BACKUP DATA
    config.update({'filename':'train_bkup_JJDB.csv'})
    training_bkup_config=config.copy()
    training_bkup_data=MatchedData(training_bkup_config, id_col, cluster_col , match_fields)
    #TEST DATA
    config.update({'filename':'test_JJDB.csv'})
    test_config=config.copy()
    test_data=MatchedData(test_config, id_col, cluster_col , match_fields)

    config.update({'filename':'outp_test_JJDB.csv'})
    outp_test_config=config.copy()
    outp_test_data=MatchedData(outp_test_config, id_col, cluster_col , match_fields)
    #VALIDATION DATA
    config.update({'filename':'valid_JJDB.csv'})
    validation_config=config.copy()
    validation_data=MatchedData(validation_config, id_col, cluster_col , match_fields)
    #VALIDATION OUTPUT
    config.update({'filename':'outp_valid_JJDB.csv'})
    outp_validation_config=config.copy()
    outp_validation_data=MatchedData(outp_validation_config, id_col, 'cluster_id' , match_fields)

    config.update({'filename':'trial_1_false_positives.csv'})
    distinct_config=config.copy()

    config.update({'provider':'MOCJ'})
    config.update({'dataset':'Individual'})
    config.update({'filename':'inferential_input.csv'})
    live_input_config=config.copy()

    config.update({'filename':'inferential_output.csv'})
    live_outp_config=config.copy()

    config.update({'dataset':'Inferential'})
    config.update({'filename':'settings_dedupe_JJDB'})
    settings_config=config.copy()
    #trnr=Trainer(training_config,settings_config,match_fields,id_col,cluster_col)
    #trnr.write_config(distinct_config)
    #!!!!!!!!!!!!!!!!!!!!!!!!!EXEC!!!!!!!!!!!!
    mtchr = Matcher(live_input_config,settings_config,id_col, live_outp_config, recall_weight=.1)
    mtchr.match()
    #outp_test_data.enumerate_matches()
    #test_data.enumerate_matches()

    #Create independent test dataset. Don't do on any subsequent tests
    #Backup is used to generate training and validation sets
    #full_data.split(test_data,training_bkup_data)

    #Create training and validation
    #training_bkup_data.split(validation_data, training_data)

    ###Test the _simple file for making the settins with more fields
    #trnr=Trainer(training_config,settings_config,match_fields,id_col,cluster_col )
    #trnr.write_config()
    ###MATCH THAT VALIDATION SET!
    #mtchr = Matcher(validation_config,settings_config,id_col, outp_validation_config)
    #mtchr.match()

    #For testing: enumerate validation matches by cluster_id (mine)
    #outp_validation_data.enumerate_matches()
    #validation_data.enumerate_matches()

    '''
    config.update({'filename':'train_JJDB.csv'})
    training_config=config.copy()
    print(training_config)


    print(settings_config)
    trnr=Trainer(training_config,settings_config, match_fields, id_col, cluster_col)

    #trnr.write_config()

    config.update({'filename':'test_JJDB.csv'})
    test_data=config.copy()
    config.update({'filename':'matched_test_JJDB.csv'})
    outp_file=config.copy()

    #mtchr = Matcher(test_data,settings_config,id_col, outp_file)
    #mtchr.match()
    '''
    print('DONE')
    print(datetime.now())
