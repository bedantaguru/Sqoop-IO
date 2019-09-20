
rm(list = ls())


require(plyr)
require(dplyr)


options(stringsAsFactors = F)


source("lib/system_call.R", local = T)
source("lib/ini.R", local = T)
source("lib/read_conf.R", local = T)
source("lib/sqoop_builder.R", local = T)
source("lib/sqoop_guide_reader.R", local = T)
source("lib/store.R", local = T)
source("lib/kerberos.R")

# need to be changed (read it from ini) # required to be load before RJava
renew_kerberos_ticket(username = "ruser", password = "rbi1234")

source("lib/connection.R" , local = T)


# ok
source("source_db_code/Oracle.R", local = T)
source("lib/RSqoop_backend.R", local = T)


# need work
source("lib/sqoop_backend.R", local = T)

rsb <- RSqoop_backend()


# rsb$src_db$table_catalogue()
# rsb$src_db$select_table("BSR1_QUARTERLY_FINAL")

#rsb$hdfs$clean("/project/data_store/EDWOWNER/TBL_PART_IDL")


# 
# 
# ep <- rsb$executor$get_sqoop_import_execution_plan(tbl_name = "BSR1_QUARTERLY_FINAL", fresh = T)
# 
# rsb$executor$update_hdb_info_on_plans()
# 
# ep <- rsb$executor$get_sqoop_import_execution_plan(tbl_name = "BSR1_QUARTERLY_FINAL", fresh = F)

# rsb$executor$update_plans_for_stored_tables(tbl_name = "BSR1_QUARTERLY_FINAL")

e <- rsb$executor$get_plan(table_name = "BSR1_QUARTERLY_FINAL")

# using log transform to decluster a split column
#stop()
#specific
# e$exe_plans$jobs_10$set_custom_sqoop_args_live(split_by = "LN(PART1CD+1)", 
#                                                boundary_query = "SELECT MIN(PART1CD)*0 + 0, MAX(PART1CD)*0 + 15.76142 FROM (SELECT  * FROM EDWOWNER.BSR1_QUARTERLY_FINAL WHERE YRQTR = '31-MAR-14' AND  (rownum = 1) ) t1")

#e$exe_plans$jobs_10$set_custom_sqoop_args_live(num_mappers = 1)

e$exe_plans$jobs_10$set_custom_sqoop_args_live(query="SELECT  * FROM EDWOWNER.BSR1_QUARTERLY_FINAL WHERE YRQTR = '31-MAR-17' AND PART1CD <= '0332605'",
                                               boundary_query = "SELECT MIN(PART1CD)*0 + 0, MAX(PART1CD)*0 + 0332605 FROM (SELECT  * FROM EDWOWNER.BSR1_QUARTERLY_FINAL WHERE YRQTR = '31-MAR-14' AND  (rownum = 1) ) t1")

e$exe_plans$jobs_10$set_custom_sqoop_args_live(query="SELECT  * FROM EDWOWNER.BSR1_QUARTERLY_FINAL WHERE YRQTR = '31-MAR-17' AND PART1CD > '0332605'",
                                               boundary_query = "SELECT MIN(PART1CD)*0 + 0332605, MAX(PART1CD)*0 + 7000000 FROM (SELECT  * FROM EDWOWNER.BSR1_QUARTERLY_FINAL WHERE YRQTR = '31-MAR-14' AND  (rownum = 1) ) t1")




rsb$executor$execute$run(e$exe_plans)

while(!rsb$executor$execute$all_job_done()){
  rsb$executor$execute$next_stage()
  Sys.sleep(1)
}


f<- function(){
  rsb$sqoop$access$get_process_view()
  rsb$sqoop$access$select_process()
  rsb$sqoop$access$get_log() %>% cat()
}
