
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
renew_kerberos_ticket(username = "hive", password = "rbi123")

source("lib/connection.R" , local = T)


# ok
source("source_db_code/Oracle.R", local = T)
source("lib/RSqoop_backend.R", local = T)


# need work
source("lib/sqoop_backend.R", local = T)

rsb <- RSqoop_backend()


rsb$src_db$table_catalogue()
rsb$src_db$select_table("TBL_PART_IDL")

#rsb$hdfs$clean("/project/data_store/EDWOWNER/TBL_PART_IDL")

rsb$src_db$get_selected_table_analysis()

#d <- rsb$src_db$get_hadoop_db_type_map()

# force_pointable is the trick
#rsb$src_db$get_selected_table_import_plan(force_pointable = T)
e <- rsb$src_db$get_sqoop_import_execution_plan(SEP = rsb$executor$sqoop_execution_plan,
                                                server_num_connection_limit = 12,
                                                parallel_select = F)

rsb$executor$save_plan(e)

rsb$executor$run_plan(e)

while(!rsb$executor$execute$all_job_done()){
  rsb$executor$execute$next_stage()
  Sys.sleep(1)
}


f<- function(){
  rsb$sqoop$access$get_process_view()
  rsb$sqoop$access$select_process()
  rsb$sqoop$access$get_log() %>% cat()
}
