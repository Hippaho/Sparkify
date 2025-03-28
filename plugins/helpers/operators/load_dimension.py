from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    LoadDimensionOperator: A custom Airflow operator to load dimension tables in Redshift.
    It takes SQL queries and table names, and handles the insertion.
    """

    ui_color = '#80BD9E'  # A nice green color for the Airflow UI

    # Default query template for inserting into dimension tables
    dim_query = """
        INSERT INTO public.{table}
        {sql}
    """

    @apply_defaults
    def __init__(self,
                table="",  # Name of the dimension table in Redshift
                sql="",    # SQL query to fetch data for the dimension table
                redshift_conn_id="",  # Airflow connection ID for Redshift
                *args, **kwargs):
        """
        Constructor. Sets up the operator with table, SQL, and connection details.

        Args:
            table (str): Target dimension table name in Redshift.
            sql (str): SQL query to populate the dimension table.
            redshift_conn_id (str): Airflow connection ID for Redshift.
            *args: Extra arguments for BaseOperator.
            **kwargs: Extra keyword arguments for BaseOperator.
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.conn_id = redshift_conn_id

    def execute(self, context):
        """
        Executes the SQL query to load data into the dimension table.
        Basically, it runs the insert statement.
        """
        try:
            # Get a hook to our Redshift database
            redshift = PostgresHook(postgres_conn_id=self.conn_id)

            # Let's see what query we're about to run
            self.log.info(f"Running: {self.dim_query}")

            # Run the actual insert statement
            redshift.run(self.dim_query.format(table=self.table, sql=self.sql))

            # Tell the logs we're done
            self.log.info(f"Successfully loaded data into {self.table}")

        except Exception as err:
            # If anything goes wrong, log the error and bail out
            self.log.exception(err)
            raise err