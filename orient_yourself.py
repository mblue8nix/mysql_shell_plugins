from mysqlsh_plugins_common import run_and_show
from mysqlsh.plugin_manager import plugin, plugin_function
from check import workld
import mysqlsh

shell = mysqlsh.globals.shell

"""
Group of statement to get oriented on the db server
"""
@plugin_function("blue.getOriented")

def get_oriented(session=None):
    """
    Prints info for orientation

    Args:
        session (object): The optional session object used to query the
            database. If omitted the MySQL Shell's current session will be used.

    """

    print('\n Schema Sizes:')

    stmt = """
    SELECT table_schema AS 'Database',
    ROUND(SUM(data_length + index_length) / 1024 / 1024, 2) AS 'Size (MB)',
    count(table_name) as '# of tables'
FROM information_schema.TABLES
WHERE table_schema <> 'mysql'
    and table_schema <> 'sys'
    and table_schema <> 'performance_schema'
    and table_schema <> 'information_schema'
GROUP BY table_schema
    """
    run_and_show(stmt)

    print("\n Multi Threaded Replication Information:")

    stmt="""select @@slave_parallel_workers as "Workers",
    @@slave_parallel_type as "MTS Type",
    @@gtid_mode as 'GTIDs',
    @@slave_compressed_protocol as "Rep Compressed";
    """
    run_and_show(stmt)

    print("\n Replication Status:")
    stmt = """SELECT
    conn_status.channel_name as channel_name,
    conn_status.service_state as IO_thread,
    applier_status.service_state as SQL_thread,
    if(GTID_SUBTRACT(LAST_QUEUED_TRANSACTION, LAST_APPLIED_TRANSACTION) = "","0" ,
      abs(time_to_sec(if(time_to_sec(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP)=0,0,
      timediff(APPLYING_TRANSACTION_ORIGINAL_COMMIT_TIMESTAMP,now()))))) `lag_in_sec`
    FROM
    performance_schema.replication_connection_status AS conn_status
    JOIN performance_schema.replication_applier_status_by_worker AS applier_status
    ON applier_status.channel_name = conn_status.channel_name
    order by 4 desc limit 1"""
    run_and_show(stmt)

    print("\n Replicating From:")
    stmt = """
    select Host, Master_log_name, Master_log_pos from mysql.slave_master_info;
    #"""
    run_and_show(stmt)

    print("\n Replicas:")

    stmt = """select case when count(host) <= '0'
    THEN 'No Replicas!'
    ELSE count(HOST)  END as 'Connected'
    from information_schema.Processlist where COMMAND = 'Binlog Dump GTID';"""
    run_and_show(stmt)

    stmt = """
    show slave hosts;
    """
    run_and_show(stmt)

    # Get hold of the global shell object
    import mysqlsh
    shell = mysqlsh.globals.shell

    if session is None:
        session = shell.get_session()
        if session is None:
            print("No session specified. Either pass a session object to this "
                  "function or connect the shell to a database")
            return
    # Get first the total server workload
    stmt = """SELECT SUM(count_read) `tot reads`,
                     CONCAT(ROUND((SUM(count_read)/SUM(count_star))*100, 2),"%") `reads`,
                     SUM(count_write) `tot writes`,
                     CONCAT(ROUND((SUM(count_write)/sum(count_star))*100, 2),"%") `writes`
              FROM performance_schema.table_io_waits_summary_by_table
              WHERE count_star > 0 ;"""
    result = session.run_sql(stmt)
    row = result.fetch_one()
    print("\n MySQL Workload of the server: {} reads and {} writes".format(row[1], row[3]))
    tot_reads = row[0]
    tot_writes = row[2]
    table_str = ""
    table_grp = ""
    extra = ""
    where_str = ""
    stmt = """SELECT object_schema, {}
                     CONCAT(ROUND((SUM(count_read)/SUM(count_star))*100, 2),"%") `reads`,
                     CONCAT(ROUND((SUM(count_write)/SUM(count_star))*100, 2),"%") `writes`,
                     {}
                     CONCAT(ROUND((SUM(count_read)/{})*100, 2),"%") `ratio to total reads`,
                     CONCAT(ROUND((SUM(count_write)/{})*100, 2),"%") `ratio to total writes`
              FROM performance_schema.table_io_waits_summary_by_table
              WHERE count_star > 0 {} GROUP BY object_schema{}""".format(table_str, extra, tot_reads, tot_writes, where_str, table_grp)
    run_and_show(stmt)

    print("\n Buffer PCT")

    stmt = """SELECT CONCAT(FORMAT(A.num * 100.0 / B.num, 2), '%') BufferPoolFull
FROM (
        SELECT variable_value num
        FROM performance_schema.global_status
        WHERE variable_name = 'Innodb_buffer_pool_pages_data'
    ) A,
    (
        SELECT variable_value num
        FROM performance_schema.global_status
        WHERE variable_name = 'Innodb_buffer_pool_pages_total'
    ) B;"""
    run_and_show(stmt)

    print("\n Connections:")

    stmt = """select count(host) as 'Current Connections'
                from information_schema.Processlist;"""
    run_and_show(stmt)

    print("\n App Users:")
    stmt = """select user as 'App User',
              host
              from mysql.user
              where user like 'casino0%'
              or user like 'casinobatch'
              or user like 'casinologs%';"""
    run_and_show(stmt)

    print("\n General Info:")

    stmt = """
    select @@hostname as 'Hostname',
    @@read_only as 'Read Only',
    @@super_read_only as 'Super Read Only',
    @@max_connections as "Max Con",
    @@binlog_format as "Binlog Format",
    @@version as "MySQL Version",
    @@long_query_time as "Long Query Time",
    @@character_set_server as "Char Set Server";
    """

    run_and_show(stmt)

    print("\n Performance Affecting Variables:")

    stmt = """
    select @@Innodb_io_capacity_max,
    @@sync_binlog as 'Sync Binlogs',
    @@log_slave_updates,
    @@innodb_flush_log_at_trx_commit as 'Trx Commit',
    @@innodb_log_file_size / 1024 / 1024 as "Log File Size in MB",
    @@innodb_log_buffer_size / 1024 / 1024 as "Innodb Log Buffer Size in MB",
    @@open_files_limit;
    """

    run_and_show(stmt)
    

    print("\n Connection Stats & Querries running:")
    
    stmt = """
    SELECT @@max_connections;
    SHOW STATUS
    WHERE Variable_name = 'Threads_connected'
    OR Variable_name = 'Threads_running';
    
    SELECT user,
    db,
    REPLACE(SUBSTRING(info, 1, 150), "\n", "") SQLT,
    ROUND(AVG(time), 0) avg_time_secs,
    COUNT(1) cnt
    FROM information_schema.processlist
    WHERE command <> 'Sleep'
    AND INFO IS NOT NULL
    GROUP BY user,
    db,
    SQLT
    ORDER BY cnt DESC
    LIMIT 10;
    """
    run_and_show(stmt)
    
    return
