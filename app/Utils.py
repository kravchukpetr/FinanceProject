from sqlalchemy import create_engine, Table, MetaData
from sqlalchemy.dialects.postgresql import insert
import os
import psycopg2


def get_conn_to_pg():
    # config_dict = read_conn_config(CONFIG_FILE)
    return psycopg2.connect(
        # database=config_dict['database'], user=config_dict['user'], password=config_dict['pwd'], host=config_dict['host'], port=config_dict['port']
        database=os.environ['PG_DB'],
        user=os.environ['PG_USR'],
        password=os.environ['PG_PWD'],
        host=os.environ['PG_HOST'],
        port=os.environ['PG_PORT']
    )


def get_conn_pg_engine(echo=True):
    db_url = f"postgresql://{os.environ['PG_USR']}:{os.environ['PG_PWD']}@{os.environ['PG_HOST']}:{os.environ['PG_PORT']}/{os.environ['PG_DB']}"
    engine = create_engine(db_url, echo=echo)
    return engine


def merge_df_to_pg(df, engine, table_name, schema_name, merge_key):
    metadata = MetaData()
    table = Table(table_name, metadata, autoload_with=engine, schema=schema_name)

    # Define the insert statement
    insert_stmt = insert(table).values(df.to_dict(orient='records'))

    # Define the update statement for the ON CONFLICT clause
    update_stmt = insert_stmt.on_conflict_do_update(
        index_elements=merge_key,  # Specify the unique constraint or index for conflict
        set_={c.key: c for c in insert_stmt.excluded if c.key not in merge_key}
    )

    # Execute the upsert
    with engine.connect() as conn:
        conn.execute(update_stmt)
