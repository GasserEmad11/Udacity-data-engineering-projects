from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class DataQualityOperator(BaseOperator):
    """
    operator for data quality checks
    
    arguements:
    data_tests{object}: list for queries and their expected result
    redshift_conn_id{object}: string for redshift connection instance
    
    """

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
                
                 data_tests=[],
                 redshift_conn_id="",
                 *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
 
        self.redshift_conn_id=redshift_conn_id
        self.data_tests=data_tests
        
    def execute(self, context):
        self.log.info('data_checks_started')
        failed_tests=[]
        error_count=0
        redhshift_hook=postgresHook(self.redhshift_conn_id)
        for check in self.data_tests:
            sql=check.get('sql')
            exp_result=check.get('expected_results')
            actual_result=redshift_hook.get_records(sql)[0]
            if exp_result!=actual_result:
                self.log.info(f'error,expected {exp_result},got instead {actual_result}')
                failed_tests.append(sql)
                error_count+=1
        if error_count>0:
          self.log.info("test failed")
          self.log.info("data quality checks failed ,check queries again")
                
            