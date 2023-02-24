from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):

    
    """
    operator for loading the dimension table
    
    arguements :
    redshift_conn_id{object}: for creating the redshift connection instance
    database{object}: the inteded database 
    sql_statment {objec}: sql statment needed to run
    truncate_table{bool} : for truncating the table if needed, set to false by default
    
    """
    ui_color = '#80BD9E'
    
    insert_sql= """
        INSERT INTO {}
        {};
    """
    truncate_sql= """
        TRUNCATE TABLE {};
    """

    @apply_defaults
    def __init__(self,
                 database='',
                 redshift_conn_id="",
                 truncate_table=False,
                 sql_statment='',
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.database=database
        self.redshift_conn_id=redshift_conn_id
        self.truncate_table=truncate_table
        self.sql_statment=sql_statment
            

    def execute(self, context):
        self.log.info(f"loading dimension fact table {self.database}")
        redshift_hook=PostgresHook(postgres_conn_id=self.redshift_conn_id)
        if truncate_table:
                self.log("truncating table")
                redshift_hook.run(LoadDimensionOperator.truncate_sql.format(self.database))    
        formatted_sql=LoadDimensionOperator.insert_sql.format(self.database, self.sql_statment)
        try:
              self.log(f"executing query {formatted_sql}")                
              redshift_hook.run(formatted_sql)
        except error as e:
              print (e)
     
