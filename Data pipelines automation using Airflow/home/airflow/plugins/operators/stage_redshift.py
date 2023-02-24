from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    """
    operator to load json formatted files from s3 to redshift
    
    arguements:
    table{object}:inteded table to load
    redshift_conn_id{object}:for initiating redhshift instance
    aws_credentials{object}:to access aws services
    region_name{object}:the region in which files resides
    s3_bucket{object}:bucket that data exists in
    s3_key{object}: the s3 bucket key
    """
    
    
    copy_command="""  COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        {}
        """
    @apply_defaults
    def __init__(self,
                 table="",
                 redshift_conn_id="",
                 aws_credentials="",
                 region_name="",
                 s3_bucket='',
                 s3_key='',
                 format_instructions='',
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.table=table
        self.redshift_conn_id=redshift_conn_id
        self.aws_credentials=aws_credentials
        self.region_name=region_name
        self.s3_bucket=s3_bucket
        self.s3_key=s3_key
        self.format_instructions=format_instructions
        
        

    def execute(self, context):
        self.log.info('stage to redshift process has started')
        aws_hook=AwsHook(self.aws_credentials)
        credentials=aws_hook.get_credentials()
        redshift_hook=postgresHook(postgres_conn_id=self.redshift_conn_id)
        key=self.s3_key.format(**context)
        path="s3://{}/{}".format(self.s3_bucket,key)
        formatted_sql=copy_command.format(self.table,
                                          path,
                                          credentials.access_key,
                                          credentials.secret_key,
                                          self.region_name)
        try:
         self.log.info('executing  copy query')
         redshift_hook.run(formatted_sql)
        except error as e:
         print (f"error occured ,the error message was {e}")
            
              





