import time
import traceback
import cx_Oracle
import settings

class Oracle(object):
    def __init__(self):
        self.connection = None
        self.logminer_active = False
        self.cursor = None
        self.tables = ()
        self.scn = 0

    def connect(self):
        if self.cursor != None:
            self.disconnect()
                
        connected = False
        while not connected:
            try:
                self.connection = cx_Oracle.Connection( settings.SOURCE['CONNECTION_STRING'] )#, encoding = "UTF-8", nencoding = "UTF-8")
                print("Connected. Database version:", self.connection.version)
                connected = True
            except cx_Oracle.DatabaseError as e:
                traceback.print_exc()
                print("Trying again in 10...")
                time.sleep(10)
        self.cursor = self.connection.cursor()

        if self.scn == 0:
            self.cursor.execute("select min(current_scn) CURRENT_SCN from gv$database")
            row = self.cursor.fetchone()
            self.scn = row[0]
        print("CURRENT SCN: {}".format(self.scn))
    

    def disconnect(self):
        try:
            self.cursor.execute( "begin \nSYS.DBMS_LOGMNR.END_LOGMNR; \nend;" )
            print( "Ended logmanager" )
        except:
            pass
            
        try:
            self.connection.disconnect()
            self.connection = None
        except:
            pass
        
        self.logminer_active = False


    def start_logminer(self):
        while not self.logminer_active:
            if self.connection == None:
                self.connect()
            try:
                self.cursor.execute("""
                    begin
                        dbms_logmnr.start_logmnr(   STARTSCN => {}, 
                                                    OPTIONS =>  DBMS_LOGMNR.SKIP_CORRUPTION+
                                                                DBMS_LOGMNR.NO_SQL_DELIMITER+
                                                                DBMS_LOGMNR.NO_ROWID_IN_STMT+
                                                                DBMS_LOGMNR.DICT_FROM_ONLINE_CATALOG+
                                                                DBMS_LOGMNR.CONTINUOUS_MINE+
                                                                DBMS_LOGMNR.COMMITTED_DATA_ONLY+
                                                                dbms_logmnr.STRING_LITERALS_IN_STMT);
                    end;""".format(self.scn))
                self.logminer_active = True
            except (cx_Oracle.DatabaseError, cx_Oracle.OperationalError)  as e:
                error, = e.args
                if error.code == 1292:
                    print("ORA-01292 - no log file has been specified containing scn {}".format(self.scn))
                    self.scn += 1
                else:
                    traceback.print_exc()
                    self.connection = None
                    self.logminer_active = False
                    time.sleep(10)
        print("started logmanager")


    def select(self):
        tables = ""
        if settings.SOURCE['TABLES'] is not None:
            tables = " and table_name in {}".format(settings.SOURCE['TABLES'])
        sql = """SELECT 
            thread#, 
            scn, 
            start_scn, 
            commit_scn,
            timestamp, 
            operation_code, 
            operation,
            status, 
            SEG_TYPE_NAME,
            info,
            seg_owner,
            table_name,
            username,
            sql_redo,
            sql_undo,
            row_id,
            csf,
            TABLE_SPACE,
            SESSION_INFO,
            RS_ID,
            RBASQN,
            RBABLK,
            SEQUENCE#,
            TX_NAME,
            SEG_NAME,
            SEG_TYPE_NAME 
        FROM  
            v$logmnr_contents  
        WHERE 
            OPERATION_CODE in (1,2,3) 
            and commit_scn>={}
            and TABLE_SPACE='{}'{}""".format(self.scn, settings.SOURCE['TABLESPACE'],tables)
        self.cursor.execute(sql)
        
    def get(self, scn):
        self.scn = scn
        if self.connection == None:
            self.connect()
        if self.logminer_active == False:
            self.start_logminer()
            self.select()

        row=self.cursor.fetchone()
        return row[3],row[11],row[6],'{}||{}'.format(row[13],row[14])
        