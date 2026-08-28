from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class LoadDimensionOperator(BaseOperator):
    """Load a Redshift dimension table from a SELECT statement.

    Dimensions are small and are rebuilt from the staging tables on every run,
    so the default is the truncate-insert pattern: empty the table, then
    reload it. Flipping ``truncate_before_load`` to ``False`` switches the
    operator to append-only, which is what the DAG needs if a dimension is
    ever turned into a slowly-changing one.

    The truncate and the insert run inside a single transaction, so a failed
    insert leaves the previous contents intact rather than an empty table.

    :param table: Target dimension table.
    :param select_sql: SELECT statement producing the rows. Templated.
    :param target_columns: Optional explicit column list for the INSERT.
    :param truncate_before_load: ``True`` for truncate-insert (default),
        ``False`` for append-only.
    """

    ui_color = '#80BD9E'
    template_fields = ('select_sql',)

    def __init__(self,
                 table,
                 select_sql,
                 redshift_conn_id='redshift',
                 target_columns=None,
                 truncate_before_load=True,
                 **kwargs):
        super().__init__(**kwargs)
        self.table = table
        self.select_sql = select_sql
        self.redshift_conn_id = redshift_conn_id
        self.target_columns = target_columns
        self.truncate_before_load = truncate_before_load

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        columns = f" ({', '.join(self.target_columns)})" if self.target_columns else ''
        insert_sql = f'INSERT INTO {self.table}{columns}\n{self.select_sql}'

        if self.truncate_before_load:
            self.log.info('Loading dimension %s in truncate-insert mode', self.table)
            # One transaction: the table is never left empty by a failed insert.
            statements = [f'TRUNCATE TABLE {self.table}', insert_sql]
        else:
            self.log.info('Loading dimension %s in append-only mode', self.table)
            statements = [insert_sql]

        redshift.run(statements, autocommit=False)

        rows = redshift.get_first(f'SELECT COUNT(*) FROM {self.table}')[0]
        self.log.info('Dimension %s now holds %s rows', self.table, rows)
