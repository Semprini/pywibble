#PATH = C:\oracle\instantclient_18_3;%PATH%

import time
import traceback
import cx_Oracle
import settings

if settings.MQ_FRAMEWORK['HOST'] != '':
    import pika


class MQ(object):
    
    def __init__(self):
        self.credentials = None
        self.connection = None
        self.channel = None
        self.exchange_name = settings.MQ_FRAMEWORK['EXCHANGE_PREFIX'] + 'gentrack'


    def send(self, table_name, operation, data):
        headers_dict = {'TABLE_NAME':table_name, 'OPERATION': operation}
        if data == None:
            print("No body for MQ send")
            return
        

        if settings.MQ_FRAMEWORK['HOST'] != '':
            retry = 5
            done = False
            while not done:
                if self.channel == None:
                    self.credentials = pika.PlainCredentials(settings.MQ_FRAMEWORK['USER'], settings.MQ_FRAMEWORK['PASSWORD'])
                    self.connection = pika.BlockingConnection(pika.ConnectionParameters(host=settings.MQ_FRAMEWORK['HOST'],credentials=self.credentials))
                    self.channel = self.connection.channel()
                    self.channel.exchange_declare(exchange=self.exchange_name, exchange_type='headers')

                try:
                    self.channel.basic_publish(  exchange=self.exchange_name,
                                        routing_key='cdc',
                                        body=data,
                                        properties = pika.BasicProperties(headers=headers_dict))
                    done = True
                except (pika.exceptions.ConnectionClosed, pika.exceptions.ChannelClosed) as e:
                    time.sleep(1)
                    self.channel = None
                    retry -= 1
                    if retry == 0:
                        done = True
                        print( "ERROR! Sending payload to {}:{}".format(self.exchange_name, data) )


class CDC(object):
    def __init__(self):
        self.connection = None
        self.logminer_active = False
        self.cursor = None
        self.tables = ()
        self.mq = MQ()

        self.scn = self.read_scn()
    
    
    def write_scn(self):
        with open("scn.txt","w") as f:
            f.write("{}".format(self.scn))


    def read_scn(self):
        try:
            with open("scn.txt","r") as f:
                scn = int(f.read())
        except IOError:
            scn = 0
        return scn
            
            
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
    
    
    def connect(self):
        if self.cursor != None:
            self.disconnect()
                

        connected = False
        while not connected:
            try:
                self.connection = cx_Oracle.Connection( settings.connection_string )#, encoding = "UTF-8", nencoding = "UTF-8")
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


    def start_logminer(self):

        # self.cursor.execute("""
            # select 
                # log.GROUP#, 
                # log.THREAD#,
                # log.SEQUENCE#,
                # log.BYTES,
                # log.BLOCKSIZE,
                # log.MEMBERS,
                # log.ARCHIVED,
                # log.STATUS,
                # log.FIRST_CHANGE#,
                # log.FIRST_TIME,
                # log.NEXT_CHANGE#,
                # log.NEXT_TIME,
                # log.CON_ID,
                # lf.GROUP#,
                # lf.STATUS,
                # lf.TYPE,
                # lf.MEMBER,
                # lf.IS_RECOVERY_DEST_FILE,
                # lf.CON_ID
            # from v$log log 
                # join v$logfile lf on 'GROUP#'='GROUP#' 
            # where 
                # log.STATUS='CURRENT'""")
        # rows = self.cursor.fetchall()
        # print([row for row in self.cursor.description])
        # sql = """
                # begin
                    # dbms_logmnr.add_logfile( '{}', sys.dbms_logmnr.NEW );
                # end;""".format('/oradata/gentest/redob/genprod_redo01foo.log')
        # #self.cursor.execute( sql )

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
        self.cursor.execute("""
        SELECT 
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
            and TABLE_SPACE='{}'""".format(self.scn, settings.tablespace))


    def run(self):
        table_changes = {}
        count=0
        
        done = False
        while not done:
            if self.connection == None:
                self.connect()
            if self.logminer_active == False:
                self.start_logminer()
                self.select()

            try:
                row=self.cursor.fetchone()
                count+=1
                self.scn=row[3]
                self.write_scn()
                self.mq.send(row[11], row[6], '{}||{}'.format(row[13],row[14]))

                if row[11] in table_changes.keys():
                    table_changes[row[11]] += 1
                else:
                    table_changes[row[11]] = 1
                if count % 1000 == 0:
                    print("{} | {}".format(count, table_changes))
            except UnicodeDecodeError as e:
                print("Unicode decode error - skipping change")
                self.scn += 1
                self.write_scn()
                self.connection = None
            except (cx_Oracle.OperationalError, cx_Oracle.DatabaseError) as e:
                print("Mul")
                traceback.print_exc()
                self.connection = None
            except (KeyboardInterrupt, SystemExit):
                self.disconnect()
                raise
        
cdc = CDC()
cdc.run()

