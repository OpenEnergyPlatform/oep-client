import os


def get_sqlalchemy_table(oepclient, table, schema=None):
    """
    Args:

        oepclient(OEPClient)
    """
    # NOTE: import inside of function because it's not mandatory
    import oedialect
    import sqlalchemy as sa

    os.environ["OEDIALECT_PROTOCOL"] = oepclient.protocol
    connection_string = "postgresql+oedialect://:%s@%s" % (
        oepclient.token,
        oepclient.host,
    )
    engine = sa.create_engine(connection_string)
    metadata = sa.MetaData(bind=engine)
    schema = schema or oepclient.default_schema

    parts = []
    for col in oepclient.get_table_definition(table, schema=schema)["columns"]:
        parts.append(sa.Column(col["name"]))

    tab = sa.Table(table, metadata, *parts, schema=schema)
    return tab
