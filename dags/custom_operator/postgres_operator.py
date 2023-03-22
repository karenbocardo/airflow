from airflow.models import BaseOperator
from airflow.hooks.postgres_hook import PostgresHook
from airflow.utils.decorators import apply_defaults

class PostgreSQLCountRows(BaseOperator):

    @apply_defaults
    def __init__(self, postgres_conn_id, table_name, *args, **kwargs):
        super(PostgreSQLCountRows, self).__init__(*args, **kwargs)
        self.postgres_conn_id = postgres_conn_id
        self.table_name = table_name

    def execute(self, context):
        """
        A Python callable that uses PostgreSQLHook to get the number of rows in a table.
        """
        # Create a PostgresHook instance
        hook = PostgresHook(postgres_conn_id=self.postgres_conn_id)
        
        # Build the query to get the row count
        query = f"SELECT COUNT(*) FROM {self.table_name}"
        
        # Execute the query and get the result
        with hook.get_conn() as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                result = cur.fetchone()[0]
        
        self.log.info(f"table {self.table_name} has {result} rows")
        
        # send message to xcom  
        context['ti'].xcom_push(key='row_count', value=result)
        
        # Return the row count
        return result
