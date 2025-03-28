from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from airflow.contrib.hooks.aws_hook import AwsHook

class StageToRedshiftOperator(BaseOperator):
    """
    StageToRedshiftOperator: This operator moves data, with moving data from S3 into Redshift staging tables.
    It delivers raw data, ensuring it shows up in Redshift
    """

    ui_color = '#358140'  # A nice, earthy green for a job well done

    # The magic spell for copying data from S3 to Redshift
    copy_sql = """
        COPY {}  -- Where are we putting the data? (Redshift table)
        FROM '{}' -- Where's the data coming from? (S3 path)
        ACCESS_KEY_ID '{}' -- How do we access S3? (AWS access key)
        SECRET_ACCESS_KEY '{}' -- How do we access S3? (AWS secret key)
        JSON '{}' -- What's the data format? (JSON, auto or path)
    """

    @apply_defaults
    def __init__(self,
                 redshift_conn_id='',  # Airflow connection ID for Redshift
                 aws_credentials_id='',  # Airflow connection ID for AWS credentials
                 table='',  # Redshift table to load into
                 s3_bucket='',  # S3 bucket where data resides
                 s3_key='',  # S3 key (path) to the data
                 s3_json="", #JSON format 'auto' or path to json path file.
                 *args, **kwargs):
        """
        Constructor. Sets up the operator with all the necessary details for the data move.

        Args:
            redshift_conn_id (str): Airflow connection ID for Redshift.
            aws_credentials_id (str): Airflow connection ID for AWS credentials.
            table (str): Redshift table to load into.
            s3_bucket (str): S3 bucket where data resides.
            s3_key (str): S3 key (path) to the data.
            s3_json (str): 'auto' or path to the json path file.
            *args: Extra arguments for the BaseOperator.
            **kwargs: Extra keyword arguments for the BaseOperator.
        """
        super().__init__(*args, **kwargs)
        self.conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.s3_json = s3_json

    def execute(self, context):
        """
        Executes the COPY command to move data from S3 to Redshift.
        It handles all the AWS and Redshift connections, and even clears the table first.
        """
        try:
            # Let's get our AWS credentials, our ticket to S3
            self.log.info('Getting our AWS credentials...')
            aws_hook = AwsHook(self.aws_credentials_id)
            credentials = aws_hook.get_credentials()

            # Time to connect to Redshift, our destination
            self.log.info('Connecting to Redshift...')
            redshift = PostgresHook(postgres_conn_id=self.conn_id)

            # Let's clean the slate before loading, just to be safe
            self.log.info(f'Clearing out {self.table} before loading...')
            redshift.run(f"DELETE FROM {self.table}")

            # Figure out the S3 path, depending on the table
            if self.table == 'staging_events':
                rendered_key = self.s3_key.format(**context)
                s3_path = f"s3://{self.s3_bucket}/{rendered_key}"
            elif self.table == 'staging_songs':
                s3_path = f"s3://{self.s3_bucket}/{self.s3_key}"

            self.log.info(f'S3 path to data: {s3_path}')

            # Now, let's craft our COPY command, the magic spell
            formatted_sql = StageToRedshiftOperator.copy_sql.format(
                self.table,
                s3_path,
                credentials.access_key,
                credentials.secret_key,
                self.s3_json
            )

            self.log.info(f'Executing this SQL: {formatted_sql}')

            # And finally, run it
            redshift.run(formatted_sql)

        except Exception as err:
            # Oops, something went wrong. Log the error and bail out.
            self.log.exception(err)
            raise err