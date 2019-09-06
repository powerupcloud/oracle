import cx_Oracle

import time

conn_srcrds = cx_Oracle.makedsn('database-1.cv7jsqukf31b.us-east-1.rds.amazonaws.com', '1521', service_name='ORCL');

conn_src = cx_Oracle.connect(user='xxxx', password='xxxx', dsn=conn_srcrds);

conn_destrds = cx_Oracle.makedsn('database-2.cv7jsqukf31b.us-east-1.rds.amazonaws.com', '1521', service_name='ORCL');

conn_target = cx_Oracle.connect(user='xxxx', password='xxxx', dsn=conn_destrds);

src = conn_src.cursor();

target = conn_target.cursor();

totakebackup = "DECLARE hdnl number; BEGIN hdnl := DBMS_DATAPUMP.OPEN( operation => 'EXPORT', job_mode => 'SCHEMA', job_name=>null);DBMS_DATAPUMP.ADD_FILE( handle => hdnl, filename => 'retailpd.dmp', directory => 'DATA_PUMP_DIR', filetype => dbms_datapump.ku$_file_type_dump_file,reusefile => 1);  DBMS_DATAPUMP.ADD_FILE( handle => hdnl, filename => 'retailpd.log', directory => 'DATA_PUMP_DIR', filetype => dbms_datapump.ku$_file_type_log_file);DBMS_DATAPUMP.METADATA_FILTER(hdnl,'SCHEMA_EXPR','IN (''dumptest'')');DBMS_DATAPUMP.START_JOB(hdnl);END;";

src.execute(totakebackup);

print("DataPump Export is Completed");

time.sleep(6);

tomovedmpfile = "BEGIN DBMS_FILE_TRANSFER.PUT_FILE(source_directory_object=>'DATA_PUMP_DIR',source_file_name=>'retailpd.dmp',destination_directory_object  => 'DATA_PUMP_DIR',destination_file_name=> 'retailpd_target.dmp', destination_database=> 'to_target_transfer');END;";

src.execute(tomovedmpfile);

print("Backup File Moved to target instance");

time.sleep(6);

tormvdmpfile = "BEGIN UTL_FILE.fremove('DATA_PUMP_DIR','retailpd.dmp'); END;";

src.execute(tormvdmpfile);

time.sleep(6);

tormvlogfile = "BEGIN UTL_FILE.fremove('DATA_PUMP_DIR','retailpd.log'); END;";

src.execute(tormvlogfile);

print("DataPump Export files are removed");

time.sleep(6);

torestorebackup = "DECLARE hdnl number; BEGIN hdnl := DBMS_DATAPUMP.OPEN( operation => 'IMPORT', job_mode => 'SCHEMA', job_name=>null);DBMS_DATAPUMP.ADD_FILE( handle => hdnl, filename => 'retailpd_target.dmp', directory => 'DATA_PUMP_DIR', filetype => dbms_datapump.ku$_file_type_dump_file); DBMS_DATAPUMP.METADATA_REMAP(hdnl,'REMAP_SCHEMA','dumptest','rdsdumpsrc_copied3');DBMS_DATAPUMP.START_JOB(hdnl);END;";

target.execute(torestorebackup);

time.sleep(6);

tormvtargetfile = "BEGIN UTL_FILE.fremove('DATA_PUMP_DIR','retailpd_target.dmp'); END;";

target.execute(tormvtargetfile);

print("DataPump Import is Completed");

time.sleep(6);