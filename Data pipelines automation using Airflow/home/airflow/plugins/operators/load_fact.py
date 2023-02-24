from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadFactOperator(BaseOperator):
    """
    operator for loading the fact table
    
    arguements :
    redshift_conn_id{object}: for creating the redshift connection instance
    database{object}: the inteded database 
    sql_statment {objec}: sql statment needed to run
    
    """

    insert_sql="""
    INSERT INTO {}
    {}
    """
    
    ui_color = '#F98866'

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',
                 database='',
                 sql_statment='',
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
       
        self.redshift_conn_id=redshift_conn_id
        self.database=database
        self.sql_statment=sql_statment

    def execute(self, context):
        self.log("loading fact table")
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        formatted_sql=LoadFactOperator.insert_sql.format(self.database,
                                                         self.sql_statment)
        try:
            self.log("executing query")
            redshift_hook.run(formatted_sql)
        except error as e:
            self.log(f"an error of {e} occured")
