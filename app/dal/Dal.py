import pypyodbc 
import logging 
import contextlib
import time 
from functools import wraps

lggr = logging.getLogger(__name__)

def time_in_hhmmss(seconds):
    '''
    function to get timed values.
    '''
    m, s = divmod(seconds, 60)
    h, m = divmod(m, 60)
    return "%d:%02d:%02d" % (h, m, s)

def timing_decorator(func):
    """
    outputs function start and stop times logging to log. 
    """
    @wraps(func)
    def func_wrapper(*args, **kwargs):
        starttime = time.time()
        x = func(*args, **kwargs)
        enddtime = time.time()
        lggr.info("%s completed in: %s" % ( func.__name__, time_in_hhmmss(enddtime-starttime)))
        return x
    return func_wrapper

class Dal:
    def __init__(self,server, use_windows_auth, uid, pword, dbname):
        self.server = server
        self.use_windows_auth = use_windows_auth
        self.uid = uid
        self.pword = pword
        self.dbname = dbname
        
        if self.use_windows_auth is True:
            self.conn_string = "DRIVER={ODBC Driver 13 for SQL Server};SERVER=%s;DATABASE=%s;Trusted_Connection=yes;" % (self.server, self.dbname)
        elif self.use_windows_auth is False:
            self.conn_string = "DRIVER={ODBC Driver 13 for SQL Server};SERVER=%s;DATABASE=%s;UID=%s;PWD=%s" % (self.server, self.dbname, self.uid, self.pword)
        
        if self.server != "":
            try:
                self.conn = pypyodbc.connect(self.conn_string)
            except pypyodbc.Error as e:
                err = "connection failed: %s" % str(e.value)
                #lggr.error(err)
                return None

    @timing_decorator
    def getclusterServers(self, conn):
        '''
            executes a query into a flat file. 
        '''
        connection = self.conn
        query = "select CorASide, CorBSide, DWAside, DWBside, BITAside, BITBside from DeployClusterMatrix where clusterName = '%s'" % clusterName
        rows = None
        with contextlib.closing(connection.cursor()) as cursor:
            try:
                cursor.execute(query)
                rows = cursor.fetchall()

            except pypyodbc.Error as e:
                    lggr.error(str(e.value))
                    print ("error in getdata_query,check logs.")

        return rows