from airflow.models import BaseOperator
from airflow.hooks.S3_hook import S3Hook
from airflow.utils.decorators import apply_defaults

import csv
import tempfile
import json


class S3JsonToCSV(BaseOperator):
    
    template_fields = ('s3_json_bucket', 's3_json_key', 's3_csv_bucket', 's3_csv_key')
    @apply_defaults
    def __init__(self,
                 s3_conn_id,
                 s3_json_bucket,
                 s3_json_key,
                 s3_csv_bucket,
                 s3_csv_key,
                 json_csv_map, 
                 s3_replace = True,
                 *args,
                 **kwargs):
        super(S3JsonToCSV, self).__init__(*args, **kwargs)
        self.s3_conn_id = s3_conn_id
        self.s3_json_bucket = s3_json_bucket
        self.s3_json_key = s3_json_key
        self.s3_csv_bucket = s3_csv_bucket
        self.s3_csv_key = s3_csv_key
        self.s3_replace = s3_replace
        self.json_csv_map = json_csv_map
        
    
    def execute(self, context):
        s3_json = S3Hook(aws_conn_id=self.s3_conn_id)
        
        s3_json_key = self.s3_json_key
        s3_json_bucket = self.s3_json_bucket
        s3_csv_key = self.s3_csv_key
        s3_csv_bucket = self.s3_csv_bucket
        
        data_json = s3_json.read_key(key=s3_json_key, bucket_name=s3_json_bucket)                   
        
        with tempfile.TemporaryFile('w+t') as fp:
            lines = data_json.splitlines()
            csv_keys = self.json_csv_map(json.loads(lines[0]))[0].keys()
            
            csv_writer = csv.DictWriter(fp, csv_keys, delimiter=',', quoting=csv.QUOTE_NONNUMERIC)
            csv_writer.writeheader()
            
            for j_row in lines:
                j_row = json.loads(j_row)
                for c_row in self.json_csv_map(j_row):             
                    csv_writer.writerow(c_row)
    
            fp.seek(0)
            
            s3_csv = S3Hook(aws_conn_id=self.s3_conn_id)
            if not s3_csv.check_for_bucket(s3_csv_bucket):  #bucket does not exist, create it
                s3_csv.get_resource_type(resource_type='s3').create_bucket(Bucket=s3_csv_bucket)
            s3_csv.load_string(string_data=fp.read(), key=s3_csv_key, bucket_name=s3_csv_bucket, replace=self.s3_replace)

