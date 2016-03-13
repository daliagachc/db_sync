# project name: db_sync
# created by diego aliaga daliaga_at_chacaltaya.edu.bo

from sqlalchemy import Table, MetaData


# create engine, reflect existing columns, and create table object for oldTable

def clone_table(
        src_engine, src_table_name, dest_engine, dest_table_name
):
    src_engine.metadata = MetaData(bind=src_engine)
    src_engine.metadata.reflect(
            bind=src_engine,
            only=[src_table_name]
    )

    src_table = Table(src_table_name, src_engine.metadata)

    dest_engine.metadata = MetaData(bind=dest_engine)
    dest_table = Table(dest_table_name, dest_engine.metadata)

    # copy schema and create newTable from oldTable
    for column in src_table.columns:
        dest_table.append_column(column.copy())
    dest_table.create()


def check_table_exists(table_name, engine):
    con = engine.connect()
    res = engine.dialect.has_table(con, table_name)
    con.close()
    return res
