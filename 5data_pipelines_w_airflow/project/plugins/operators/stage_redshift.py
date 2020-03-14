from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ("s3_key",) # fields that can be parameterized 

    copy_sql = """
        COPY {table}
        FROM {s3_path}
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION '{region}'
        TIMEFORMAT as 'epochmillisecs'
        TRUNCATECOLUMNS BLANKSASNULL EMPTYASNULL
        FORMAT AS {source_file_format} 'auto'
        {extra};
    """

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 # Example:
                 table="",
                 source_file_format='JSON',
                 delimiter=",", # csv-specific params
                 ignore_headers=1, # csv-specific params
                 s3_bucket="",
                 s3_key="",
                 aws_credentials_id="",
                 redshift_conn_id="",
                 region="",
                 *args, **kwargs):
        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        # Map params here
        # Example:
        # self.conn_id = conn_id

        self.table = table
        self.source_file_format = source_file_format.lower().strip()
        self.delimiter = delimiter
        self.ignore_headers = ignore_headers

        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.aws_credentials_id = aws_credentials_id
        self.redshift_conn_id = redshift_conn_id
        self.region = region

        assert isinstance(self.aws_credentials_id, str)
        assert isinstance(self.redshift_conn_id, str)
        assert self.source_file_format in {'csv','json'}
        assert isinstance(self.ignore_headers, int)


    def execute(self, context):
        """ """

        self.log.info('StageToRedshiftOperator not implemented yet')

        aws_hook = AwsHook(self.aws_credentials_id)
        credentials = aws_hook.get_credentials()
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        extra = ''
        if self.source_file_format == 'csv':
            extra = '\n'.join([extra, "IGNOREHEADER {} DELIMITER '{}'".format(
                self.delimiter, self.ignore_headers)
            ])

        self.log.info("Clearing data from destination Redshift table")
        redshift.run("DROP TABLE IF EXISTS {}".format(self.table))

        self.log.info("Copying data from S3 to Redshift")
        rendered_key = self.s3_key.format(**context)
        s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key) # can be called like: s3_key="divvy/partitioned/{execution_date.year}/{execution_date.month}/divvy_trips.csv"
        formatted_sql = StageToRedshiftOperator.copy_sql.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            region=self.region,
            source_file_format=self.source_file_format,
            extra=self.extra
        )
        redshift.run(formatted_sql)