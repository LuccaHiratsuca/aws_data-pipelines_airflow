from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadFactOperator(BaseOperator):
    """Append the result of a SELECT into a Redshift fact table.

    Fact tables are large and are expected to grow monotonically, so this
    operator is append-only by default. ``truncate_before_load`` exists as an
    escape hatch for a full rebuild, but it has to be asked for explicitly --
    the safe behaviour is the default one.

    :param table: Target fact table.
    :param select_sql: SELECT statement producing the rows. Templated, so it
        can reference ``{{ ds }}`` and friends for incremental loads.
    :param target_columns: Optional column list. When given, the INSERT names
        its columns instead of relying on the table's physical column order.
    :param truncate_before_load: Full rebuild instead of an append.
    """

    ui_color = '#F98866'
    template_fields = ('select_sql',)

    def __init__(self,
                 table,
                 select_sql,
                 redshift_conn_id='redshift',
                 target_columns=None,
                 truncate_before_load=False,
                 **kwargs):
        super().__init__(**kwargs)
        self.table = table
        self.select_sql = select_sql
        self.redshift_conn_id = redshift_conn_id
        self.target_columns = target_columns
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_before_load:
            self.log.info('Truncating fact table %s before load', self.table)
            redshift.run(f'TRUNCATE TABLE {self.table}')
        else:
            self.log.info('Appending to fact table %s', self.table)

        columns = f" ({', '.join(self.target_columns)})" if self.target_columns else ''
        insert_sql = f'INSERT INTO {self.table}{columns}\n{self.select_sql}'

        self.log.info('Running insert into %s', self.table)
        redshift.run(insert_sql)

        rows = redshift.get_first(f'SELECT COUNT(*) FROM {self.table}')[0]
        self.log.info('Fact table %s now holds %s rows', self.table, rows)
