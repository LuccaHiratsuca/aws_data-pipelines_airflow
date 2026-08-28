from airflow.models import BaseOperator
from airflow.providers.amazon.aws.hooks.base_aws import AwsBaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook


class StageToRedshiftOperator(BaseOperator):
    """Copy JSON files from S3 into a Redshift staging table.

    The operator builds a ``COPY`` statement from its parameters rather than
    taking a hard-coded one, so the same class stages both the event logs and
    the song metadata -- only the arguments differ.

    ``s3_bucket`` and ``s3_key`` are templated, which is what makes backfills
    possible: a key such as ``log-data/{{ execution_date.strftime('%Y/%m') }}``
    resolves to a different S3 prefix on every run, so re-running a past
    interval loads that interval's files.

    :param table: Target staging table in Redshift.
    :param s3_bucket: Bucket holding the source files. Templated.
    :param s3_key: Key prefix under the bucket. Templated.
    :param json_path: ``auto``/``auto ignorecase`` or an ``s3://`` JSONPaths
        file. This is what distinguishes the two JSON layouts -- the event logs
        need an explicit JSONPaths file, the song files map field-for-field.
    :param truncate_before_load: Empty the staging table first so re-runs stay
        idempotent instead of appending duplicates.
    :param region: AWS region of the bucket.
    :param extra_copy_options: Extra ``COPY`` clauses, e.g. ``TIMEFORMAT``.
    """

    ui_color = '#358140'
    template_fields = ('s3_bucket', 's3_key', 'json_path')

    copy_sql_template = """
        COPY {table}
        FROM '{s3_path}'
        ACCESS_KEY_ID '{access_key}'
        SECRET_ACCESS_KEY '{secret_key}'
        REGION '{region}'
        FORMAT AS JSON '{json_path}'
        {extra_copy_options}
    """

    def __init__(self,
                 table,
                 s3_bucket,
                 s3_key,
                 redshift_conn_id='redshift',
                 aws_credentials_id='aws_credentials',
                 json_path='auto',
                 region='us-east-1',
                 truncate_before_load=True,
                 extra_copy_options='',
                 **kwargs):
        super().__init__(**kwargs)
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.redshift_conn_id = redshift_conn_id
        self.aws_credentials_id = aws_credentials_id
        self.json_path = json_path
        self.region = region
        self.truncate_before_load = truncate_before_load
        self.extra_copy_options = extra_copy_options

    def execute(self, context):
        self.log.info('Fetching AWS credentials from connection "%s"', self.aws_credentials_id)
        credentials = AwsBaseHook(
            aws_conn_id=self.aws_credentials_id,
            client_type='s3',
        ).get_credentials()

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.truncate_before_load:
            self.log.info('Truncating staging table %s', self.table)
            redshift.run(f'TRUNCATE TABLE {self.table}')

        # s3_bucket/s3_key are already rendered by the time execute() runs.
        s3_path = f's3://{self.s3_bucket}/{self.s3_key}'.rstrip('/')
        self.log.info('Copying %s -> Redshift table %s', s3_path, self.table)

        # Deliberately not logged: the rendered statement embeds the AWS keys.
        copy_sql = self.copy_sql_template.format(
            table=self.table,
            s3_path=s3_path,
            access_key=credentials.access_key,
            secret_key=credentials.secret_key,
            region=self.region,
            json_path=self.json_path,
            extra_copy_options=self.extra_copy_options,
        )
        redshift.run(copy_sql)

        # Log the row count so the UI shows what each run actually staged.
        rows = redshift.get_first(f'SELECT COUNT(*) FROM {self.table}')[0]
        self.log.info('Staged %s rows into %s', rows, self.table)
