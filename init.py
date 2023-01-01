from mysqlsh.plugin_manager import plugin, plugin_function

# Create a blue class for my mysql shell plugins.

@plugin
class blue:
    """
    Group of plugins I created.

EXAMPLE Usge:

    blue.getOriented()          #Runs multiple sql statements to get oriented.
    blue.getMtsBalance()        #Runs MTS Balance Metrics.  Creates view if it does not exist.
    blue.getEmptyPartitions()   #Look at empty Partitions and can pass a table name.
    blue.statement_error        #Shows statements that errored out.

#Adds to report function

    \show dbsize          #Shows Schema Sizes
        \show dbisze -l   #Limit rows

    \show tblsize         #Shows table Sizes
                     -l   #Limit rows
    """

from blue import dbsize
from blue import tblsize
from blue import orient_yourself
from blue import mts_balance
from blue import empty_partitions
from blue import statement_error
from blue import statement_analy
from blue import table_stat_sum
