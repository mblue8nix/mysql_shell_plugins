"""
This plugin shows empty Partitions and can pass an arg for tables.
"""
from mysqlsh_plugins_common import run_and_show
from mysqlsh.plugin_manager import plugin, plugin_function
import mysqlsh

shell = mysqlsh.globals.shell


@plugin_function("blue.getEmptyPartitions")

def get_empty_partitions(table=None, session=None):
    """
    Prints information about partitions.

    Args:
        table (string): The name of the table to use. This is optional.
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.
    """

    where_filter = ""

    if table:
        where_filter = "and table_name = '{}'".format(table)


    stmt1 = """select table_schema, table_name, partition_name, table_rows, avg_row_length,
               data_length from information_schema.partitions
               where PARTITION_METHOD = 'RANGE' {};""".format(where_filter)
    run_and_show(stmt1)

    return
