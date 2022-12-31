from mysqlsh_plugins_common import run_and_show
from mysqlsh.plugin_manager import plugin, plugin_function
import mysqlsh
import time

shell = mysqlsh.globals.shell

"""
This plugin is created to help troublshoot MTS replication
"""

@plugin_function("blue.getMtsBalance")

def get_mts_balance(session=None):
    """
    Prints info for orientation

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return

    stmt = """SELECT COUNT(*)
                FROM information_schema.tables
                WHERE table_schema = 'sys'
                AND table_name = 'mts_summary_trx';"""

    result = session.run_sql(stmt)
    stats = result.fetch_all()
    stats = str(stats)
    stat = (stats[2])

    if (stat) == '0':
        print("You don't have the mts_summary_trx view:")
        answer = shell.prompt("Do you want to create the view sys.mts_summary_trx now? (y/N) "
                               , {'defaultValue':'n'})
        if answer.lower() == 'y':
            print("Updating Instruments &")
            print("Creating View sys.mts_summary!")

            """
            Adds needed instruments
            """

            upstmt1 = """set sql_log_bin = 0; UPDATE performance_schema.setup_consumers
                        SET ENABLED = 'YES'
                        WHERE NAME LIKE 'events_transactions%'; set sql_log_bin = 1;"""

            upstmt2 = """set sql_log_bin = 0; UPDATE performance_schema.setup_instruments
                        SET ENABLED = 'YES',
                        TIMED = 'YES'
                        WHERE NAME = 'transaction'; set sql_log_bin = 1;"""

            result = session.run_sql(upstmt1)
            result = session.run_sql(upstmt2)

            view1 = """set session sql_log_bin = 0; CREATE VIEW sys.mts_summary_trx AS
                    select performance_schema.events_transactions_summary_by_thread_by_event_name.THREAD_ID AS THREAD_ID,
                    performance_schema.events_transactions_summary_by_thread_by_event_name.COUNT_STAR AS COUNT_STAR
                    from performance_schema.events_transactions_summary_by_thread_by_event_name
                    where performance_schema.events_transactions_summary_by_thread_by_event_name.THREAD_ID in (
                    select performance_schema.replication_applier_status_by_worker.THREAD_ID
                    from performance_schema.replication_applier_status_by_worker
                    ); set session sql_log_bin = 1; """
            result = session.run_sql(view1)

            print("View Created!")

            checkview = """SELECT table_name,
                        create_time
                        FROM information_schema.tables
                        WHERE table_schema = 'sys'
                        AND table_name = 'mts_summary_trx';"""

            run_and_show(checkview)

            time.sleep(1)

            print("Running MTS Balance Query")

        else:
            print ("ok bye")

    stmt1 = """select @@slave_parallel_workers as "Workers",
            @@slave_parallel_type as "MTS Type";"""

    stmt2 = """SELECT SUM(count_star)
            FROM sys.mts_summary_trx INTO @total;"""

    stmt3 = """select thread_id,
        count_star as Count,
        100 *(COUNT_STAR / @total) AS PCT_USAGE
        from sys.mts_summary_trx
        order by Count desc;"""

    run_and_show(stmt1)
    run_and_show(stmt2)
    run_and_show(stmt3)
