from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    """
    LoadDimensionOperator: It helps with populating dimension tables in Redshift.
    Its function is to neatly organize your data where it's supposed to go.
    """

    ui_color = '#80BD9E'  # A soothing green, for things that go well

    # The magic spell for inserting data into our dimension tables
    dim_query = """
        INSERT INTO public.{table}
        {sql}
    """

    @apply_defaults
    def __init__(self,
                 table="",  # The name of the dimension table we're filling
                 sql="",    # The SQL recipe for getting the data
                 redshift_conn_id="",  # How we connect to Redshift (Airflow connection ID)
                 *args, **kwargs):
        """
        Constructor. Sets it up with the table, the recipe (SQL), and the connection details.

        Args:
            table (str): The dimension table we're loading into.
            sql (str): The SQL query that fetches the data.
            redshift_conn_id (str): The Redshift connection ID.
            *args: Extra arguments for the BaseOperator.
            **kwargs: Extra keyword arguments for the BaseOperator.
        """
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.sql = sql
        self.table = table
        self.conn_id = redshift_conn_id

    def execute(self, context):
        """
        Executes the SQL query to load data into the dimension table.
        Run the insert statement. Check for any errors. 
        """
        try:
            # Grab our Redshift connection, the librarian's direct line to the database
            redshift = PostgresHook(postgres_conn_id=self.conn_id)

            # Let's peek at the magic spell we're about to cast
            self.log.info(f"Casting this spell: {self.dim_query}")

            # Run the actual insert spell!
            redshift.run(self.dim_query.format(table=self.table, sql=self.sql))

            # All good, let's tell everyone we're done
            self.log.info(f"Successfully shelved the data into {self.table}")

        except Exception as err:
            # Oops, something went wrong. Log the error and bail out
            self.log.exception(err)
            raise err