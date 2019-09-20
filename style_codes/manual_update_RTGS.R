
rm(list = ls())

options(stringsAsFactors = F)

source("lib/kerberos.R")

# need to be changed (read it from ini) # required to be load before RJava
renew_kerberos_ticket(username = "hive", password = "HIVE2018")

source("lib/encapsulated.R")

rs <- RSqoop_backend_encapsulated()

rs$RSqoop_load_configs(src_db = "rdb_prod_dw")

suppressPackageStartupMessages({
  rsb <- rs$RSqoop_backend()
})

if(is.null(rsb$src_db$cm$get_connection())){
  rsb$src_db$cm$connect(T)
  stop("Unable to connect to source database (Oracle)")
}
rsb$src_db$table_catalogue()
rsb$src_db$select_table("TBL_TRAN_CUST_DTL")


cat("\nTaking Hadoop DB Source dates (may take ~1 mins)\n")
rsb$hadoop_db$Impala$query("INVALIDATE METADATA", is_DDL = T)
hdb_dates <- rsb$hadoop_db$Impala$query("SELECT DISTINCT VALUEDATE from rbi_dw_prod_import.TBL_TRAN_CUST_DTL")
if(inherits(hdb_dates,"try-error")){
  cat(hdb_dates)
  stop("HDB Query did not run")
}

hdb_dates<- hdb_dates[[1]]


### keep a local tracking file
dir.create("store/runtime", showWarnings = F, recursive = T)
local_track_file <- "store/runtime/RTGS_update_dates_local_track.rds"
local_incoming_track_file <- "store/runtime/RTGS_update_dates_local_incoming_track.rds"
if(!file.exists(local_track_file)){
  saveRDS(hdb_dates, local_track_file)
  local_track <- hdb_dates
}else{
  local_track <- readRDS(local_track_file)
}

if(!file.exists(local_incoming_track_file)){
  saveRDS(character(0), local_incoming_track_file)
  local_incoming_track <- character(0)
}else{
  local_incoming_track <- readRDS(local_incoming_track_file)
}

if(length(hdb_dates)==0 & length(local_track)==0){
  stop("No date in HDB and Local Track: Check it")
}

if(length(hdb_dates)==0 & length(local_track)!=0){
  warning("No date in HDB but in Local Track: Using local track")
}

if(length(hdb_dates) < length(local_track)){
  cat("less date in HDB (compared to local track)\n")
}

hdb_dates <- c(hdb_dates, local_track) %>% unique()
saveRDS(hdb_dates, local_track_file)
rm(local_track_file, local_track)

cat("\nTaking RTGS Source dates (may take ~2-3 mins)\n")
db_dates_raw <- rsb$src_db$cm$query("SELECT /*+ PARALLEL*/ DISTINCT VALUEDATE from EDWOWNER.TBL_TRAN_CUST_DTL")

db_dates <- db_dates_raw[[1]]
cat("\nDiscarding max date:", as.character(as.Date(max(db_dates))), "\n")
db_dates <- setdiff(db_dates, max(db_dates))

cat("\nDetermining diferences\n")


non_hdfs_dates <- setdiff(db_dates, hdb_dates) %>% sort() %>% as.Date()

local_incoming_track_now <- non_hdfs_dates
common_incoming <- intersect(as.character(local_incoming_track_now), as.character(local_incoming_track))
if(length(common_incoming)>0){
  cat("common_incoming:\n")
  print(common_incoming)
  stop("Check why common incoming dates are coming.")
}else{
  saveRDS(local_incoming_track_now, file = local_incoming_track_file)
}

local_incoming_track <- local_incoming_track_now
rm(local_incoming_track_now)

# this is kept for safe evaluation of the situation
if(length(non_hdfs_dates)>20){
  stop("Check why suddenly more than 20 days data is ingesting.")
}

if(length(non_hdfs_dates)>0){
  
  if(length(non_hdfs_dates)>20){
    cat(paste0("\nFound ", length(non_hdfs_dates), " many dates\n"))
  }else{
    cat(paste0("\nFound dates : ", paste0(non_hdfs_dates, collapse = ","), "\n"))
  }
  
  
  date_groups <- list()
  
  for(dti in seq_along(non_hdfs_dates)){
    dt <- non_hdfs_dates[dti]
    wg <- date_groups %>% map_lgl(~min(abs(.-dt))<6)
    if(any(wg)){
      date_groups[[which(wg)]] <- date_groups[[which(wg)]] %>% c(dt)
    }else{
      date_groups[[length(date_groups)+1]] <- dt
    }
  }
  rm(wg, dt, dti)
  
  
  rsb$src_db$get_selected_table_analysis(biased_column_for_split = "VALUEDATE",
                                         biased_column_for_job_split = "VALUEDATE", 
                                         rows_split_lim = 10^6/2)
  
  
  rsb$src_db$get_selected_table_import_plan(manual_strict_ranges_for_job_split_key = date_groups %>% map(range), 
                                            manual_strict_num_days_for_job_split_key = 50) 
  
  
  e <- rsb$src_db$get_sqoop_import_execution_plan(SEP = rsb$executor$sqoop_execution_plan,
                                                  server_num_connection_limit = 20,
                                                  parallel_select = F)
  
  rsb$executor$run_plan(e)
  
  f<- function(){
    rsb$sqoop$access$get_process_view()
    rsb$sqoop$access$select_process()
    rsb$sqoop$access$get_log() %>% cat()
  }
  
  dtn <- Sys.Date()
  while(!rsb$executor$execute$all_job_done()){
    
    if(Sys.Date()!=dtn){
      renew_kerberos_ticket(username = "hive", password = "HIVE2018")
      dtn <- Sys.Date()
    }
    
    rsb$executor$execute$next_stage()
    # cat("\014")
    # f()
    Sys.sleep(10)
    
  }
  
  # little time to cool down (but don't know whether it is required)
  Sys.sleep(50)
  
  # check after update 
  rsb$hadoop_db$Impala$query("INVALIDATE METADATA", is_DDL = T)
  hdb_new_dates <- rsb$hadoop_db$Impala$query(
    paste0("SELECT DISTINCT VALUE_DATE from rbi_dw_prod_import.NEFT_DATA_VIEW WHERE SUBSTR(VALUE_DATE,1,10)>='",min(local_incoming_track),"'")
  )
  
  if(inherits(hdb_new_dates,"try-error")){
    cat(hdb_new_dates)
    stop("HDB Query did not run after update")
  }else{
    
    hdb_new_dates <- hdb_new_dates[[1]]
    hdb_new_dates <- as.Date(hdb_new_dates)
    
    done_dates<- intersect(as.character(hdb_new_dates), as.character(local_incoming_track)) %>% as.Date
    not_done_dates<- setdiff( as.character(local_incoming_track), as.character(hdb_new_dates)) %>% as.Date
    
    if(length(not_done_dates)==1){
      if(not_done_dates==max(local_incoming_track)){
        cat("\nOnly last date is not updated. (which is ok)\n")
      }
    }
    
    if(length(not_done_dates)>1|length(done_dates)==0){
      save.image("store/runtime/RTGS_last.RData")
      stop("Something is not right!! as not_done_dates have more days or done_dates is empty.")
    }
    
    if(length(done_dates)>0){
      local_incoming_track <- done_dates
      saveRDS(local_incoming_track, file = local_incoming_track_file)
    }
    
  }
  
  
  cat("\nUpdate completed..\n")
  
}else{
  cat("\nTable is upto date..\n")
}
