from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    @apply_defaults
    def __init__(self,
            tables=None,
            redshift_conn_id=None,
            debug=False,
            *args, **kwargs):

        super(DataQualityOperator, self).__init__(*args, **kwargs)
        self.tables = tables
        self.redshift_conn_id = redshift_conn_id
        self.debug = debug

        assert isinstance(self.tables, list)


    def execute(self, context):
        """ Check to ensure tables meet certain quality checks """

        self.log.info('Establishing a connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        for i, table in enumerate(self.tables):
            records = redshift.get_records("SELECT COUNT(*) FROM {}".format(table))
            if len(records) < 1 or len(records[0]) < 1:
                txt = "{} Data quality check failed. {} returned no results".format(i, table)
                if self.debug:
                    self.log.info(txt)
                else:
                    raise ValueError(txt)
            num_records = records[0][0]
            if num_records < 1:
                txt = "{} Data quality check failed. {} contained 0 rows".format(i, table)
                if self.debug:
                    self.log.info(txt)
                else:
                    raise ValueError(txt)

            logging.info("{} Data quality on table {} check passed with {} records".format(i, table, num_records))
