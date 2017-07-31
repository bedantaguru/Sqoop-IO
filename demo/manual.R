
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
renew_kerberos_ticket(username = "user", password = "pass")

source("lib/connection.R" , local = T)


# ok
source("source_db_code/Oracle.R", local = T)
source("lib/RSqoop_backend.R", local = T)


# need work
source("lib/sqoop_backend.R", local = T)

rsb <- RSqoop_backend()


# table translation testing

rsb$src_db$table_catalogue()

rsb$src_db$select_table("DIM_DISTRICT_DATA")

d <- rsb$src_db$get_hadoop_db_type_map()

d[c("COLUMN_NAME","Hive_Data_Type")] %>% apply(MARGIN = 1, paste0, collapse =" ") %>% paste0(collapse = ",") %>%
  paste0("CREATE EXTERNAL TABLE DIM_DISTRICT_DATA_TEST (",.,")
  STORED AS PARQUET LOCATION '/project/data_store/EDWOWNER/DIM_DISTRICT_DATA'") %>% rsb$hadoop_db$Impala$query(is_DDL = T)


# automatic build not working : 

e <- rsb$executor$get_plan("DIM_DISTRICT_DATA")



########################################################################

# build own job
rsb$src_db$table_catalogue()
rsb$src_db$select_table("FCT_BSR1_HISTORICAL")

rsb$src_db$get_selected_table_analysis()
# force_pointable is the trick
rsb$src_db$get_selected_table_import_plan(force_pointable = T)
e <- rsb$src_db$get_sqoop_import_execution_plan(SEP = rsb$executor$sqoop_execution_plan,
                                                server_num_connection_limit = 12,
                                                parallel_select = F)
rsb$executor$save_plan(e)

rsb$executor$run_plan(e)

###########

while(!rsb$executor$execute$all_job_done()){
  rsb$executor$execute$next_stage()
  Sys.sleep(1)
}


f<- function(){
  rsb$sqoop$access$get_process_view()
  rsb$sqoop$access$select_process()
  rsb$sqoop$access$get_log() %>% cat()
}
