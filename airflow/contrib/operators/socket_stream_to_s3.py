import logging

from airflow.models import BaseOperator
from airflow.hooks.http_hook import HttpHook
from airflow.hooks.S3_hook import S3Hook
from requests.exceptions import ChunkedEncodingError, ConnectionError
from airflow.utils.decorators import apply_defaults

class SocketStreamToS3(BaseOperator):
    
    template_fields = ('s3_bucket', 's3_key')
    @apply_defaults
    def __init__(self,
                 endpoint,
                 http_conn_id,
                 http_query,
                 s3_conn_id,
                 s3_bucket,
                 s3_key,                
                 extra_options = {'stream':True, 'timeout':5},
                 s3_replace = True,
                 *args,
                 **kwargs
                 ):
        super(SocketStreamToS3, self).__init__(*args, **kwargs)
        self.http_conn_id = http_conn_id
        self.s3_conn_id = s3_conn_id
        self.s3_replace = s3_replace
        self.s3_bucket = s3_bucket
        self.s3_key= s3_key
        self.http_query = http_query
        self.endpoint = endpoint
        self.extra_options = extra_options
    
    def execute(self, context):
        http = HttpHook('GET', http_conn_id=self.http_conn_id)        
        response = http.run(self.endpoint,
                            data=self.http_query,
                            extra_options=self.extra_options
                            )
    
        lines = response.iter_lines()        
        try:
            payload = next(lines).decode('utf-8')
            while True:
                payload = '\n'.join([payload, next(lines).decode('utf-8')])
        except ChunkedEncodingError:
            pass
        except ConnectionError:
            pass
        
        
        s3_key = self.s3_key
        s3_bucket = self.s3_bucket
        
        logging.info(s3_key)
        
        s3 = S3Hook(aws_conn_id=self.s3_conn_id)
        if not s3.check_for_bucket(s3_bucket):  #bucket does not exist, create it
            s3.get_resource_type(resource_type='s3').create_bucket(Bucket=s3_bucket)
        s3.load_string(string_data=payload, key=s3_key, bucket_name=s3_bucket, replace=self.s3_replace)


            

    
 
 