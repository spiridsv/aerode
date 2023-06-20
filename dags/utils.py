"""Utils"""


def read_sql_query_from_file(sql_file: str) -> str:
    """
    Just read sql file with SINGLE query
    :param sql_file:
    :return: query text
    """
    with open(sql_file) as f:
        return f.read()
