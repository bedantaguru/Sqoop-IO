
rm(list = ls())

options(stringsAsFactors = F)

source("lib/kerberos.R")

# need to be changed (read it from ini) # required to be load before RJava
renew_kerberos_ticket(username = "hive", password = "HIVE2018")

source("lib/encapsulated.R")

rs <- RSqoop_backend_encapsulated()

rs$RSqoop_load_configs(src_db = "rdb_neft")

rsb <- rs$RSqoop_backend()

rsb$src_db$table_catalogue()
rsb$src_db$select_table("NEFT_DATA")
rsb$src_db$set_dba_table_stat_access_info(local_path = "resource/src_db_stats/10.29.3.87/NEFT/NEFT_DATA")
rsb$src_db$select_table("NEFT_DATA")


rsb$src_db$get_selected_table_analysis(biased_column_for_split = "BATCHID",
                                       biased_column_for_job_split = "VALUE_DATE",
                                       rows_split_lim = 10^6/2)

rsb$src_db$get_selected_table_import_plan(manual_range_for_split_key = c(0,32),
                                          manual_range_for_job_split_key = c(as.Date("2017-07-18"),Sys.Date()) ) 


e <- rsb$src_db$get_sqoop_import_execution_plan(SEP = rsb$executor$sqoop_execution_plan,
                                                server_num_connection_limit = 15,
                                                parallel_select = F)

rsb$executor$save_plan(e)

rsb$executor$run_plan(e)

f<- function(){
  rsb$sqoop$access$get_process_view()
  rsb$sqoop$access$select_process()
  rsb$sqoop$access$get_log() %>% cat()
}


while(!rsb$executor$execute$all_job_done()){
  
  renew_kerberos_ticket(username = "hive", password = "HIVE2018")
  rsb$executor$execute$next_stage()
  cat("\014")
  f()
  Sys.sleep(10)
  
}

