import operator

from airflow.exceptions import AirflowException
from airflow.models import BaseOperator
from airflow.providers.postgres.hooks.postgres import PostgresHook


class DataQualityOperator(BaseOperator):
    """Run a list of SQL assertions against Redshift and fail if any breaks.

    No test is hard-coded here. Each check is a dict supplied by the DAG:

    .. code-block:: python

        {
            'description': 'songplays has rows',
            'check_sql': 'SELECT COUNT(*) FROM songplays',
            'expected_result': 0,
            'comparison': '>',
        }

    ``comparison`` defaults to ``==`` and accepts ``==``, ``!=``, ``>``,
    ``>=``, ``<``, ``<=``. Every check runs even if an earlier one fails, so a
    single task run reports all the problems at once instead of only the first;
    the exception is raised at the end, which makes the task retry and
    ultimately fail as required.

    :param dq_checks: List of check dicts as described above.
    """

    ui_color = '#89DA59'
    template_fields = ('dq_checks',)

    COMPARISONS = {
        '==': operator.eq,
        '!=': operator.ne,
        '>': operator.gt,
        '>=': operator.ge,
        '<': operator.lt,
        '<=': operator.le,
    }

    def __init__(self,
                 dq_checks,
                 redshift_conn_id='redshift',
                 **kwargs):
        super().__init__(**kwargs)
        self.dq_checks = dq_checks
        self.redshift_conn_id = redshift_conn_id

    def execute(self, context):
        if not self.dq_checks:
            raise AirflowException('No data quality checks were configured')

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        failures = []

        for index, check in enumerate(self.dq_checks, start=1):
            check_sql = check['check_sql']
            expected = check['expected_result']
            symbol = check.get('comparison', '==')
            label = check.get('description', check_sql.strip())

            compare = self.COMPARISONS.get(symbol)
            if compare is None:
                raise AirflowException(
                    f'Unsupported comparison {symbol!r} in check {index}. '
                    f'Valid options: {", ".join(self.COMPARISONS)}'
                )

            self.log.info('Check %s/%s: %s', index, len(self.dq_checks), label)
            records = redshift.get_first(check_sql)

            if records is None:
                failures.append(f'[{label}] returned no rows at all')
                continue

            actual = records[0]
            if compare(actual, expected):
                self.log.info('  passed -- got %s (expected %s %s)', actual, symbol, expected)
            else:
                self.log.error('  FAILED -- got %s (expected %s %s)', actual, symbol, expected)
                failures.append(f'[{label}] got {actual}, expected {symbol} {expected}')

        if failures:
            raise AirflowException(
                f'{len(failures)} of {len(self.dq_checks)} data quality checks failed:\n  '
                + '\n  '.join(failures)
            )

        self.log.info('All %s data quality checks passed', len(self.dq_checks))
