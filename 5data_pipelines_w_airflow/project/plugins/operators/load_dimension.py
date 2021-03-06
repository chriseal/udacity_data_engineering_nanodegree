from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

class LoadDimensionOperator(BaseOperator):
    ui_color = '#80BD9E'
    truncate_sql = "TRUNCATE TABLE {};"
    dim_sql = """
        INSERT INTO {}
        {};
    """

    @apply_defaults
    def __init__(self,
            target_table=None,
            query=None,
            redshift_conn_id=None,
            truncate_target_table=False,
            *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.target_table = target_table
        self.query = query
        self.redshift_conn_id = redshift_conn_id
        self.truncate_target_table = truncate_target_table


    def execute(self, context):
        """ Insert records into a table with the option to truncate it beforehand """

        self.log.info('Establishing a connection')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_target_table:
            self.log.info('Truncating {}'.format(self.target_table))
            redshift.run(LoadDimensionOperator.truncate_sql.format(self.target_table))

        self.log.info("Loading {}".format(self.target_table))
        redshift.run(
            LoadDimensionOperator.dim_sql.format(self.target_table, self.query)
        )