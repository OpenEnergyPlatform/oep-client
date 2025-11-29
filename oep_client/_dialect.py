"""Work in Progress - requires sqlalchemy and oedialect"""

import os


def get_sqlalchemy_table(oepclient, table):
    """
    Args:

        oepclient(OEPClient)
    """
    # NOTE: import inside of function because it's not mandatory
    import sqlalchemy as sa  # noqa

    os.environ["OEDIALECT_PROTOCOL"] = oepclient.protocol

    if oepclient.token:
        connection_string = "postgresql+oedialect://:%s@%s" % (
            oepclient.token,
            oepclient.host,
        )
    else:
        connection_string = "postgresql+oedialect://%s" % (oepclient.host,)

    engine = sa.create_engine(connection_string)
    metadata = sa.MetaData(bind=engine)

    parts = []
    for col in oepclient.get_table_definition(table)["columns"]:
        parts.append(sa.Column(col["name"]))

    tab = sa.Table(table, metadata, *parts)
    return tab
