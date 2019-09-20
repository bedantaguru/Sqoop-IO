



rm(list = ls())


require(plyr)
require(purrr)
require(dplyr)

# this dependency is on local package
require(RBIRCommon)

options(stringsAsFactors = F)


source("lib/system_call.R", local = T)
source("lib/ini.R", local = T)
source("lib/read_conf.R", local = T)
source("lib/sqoop_builder.R", local = T)
source("lib/sqoop_guide_reader.R", local = T)
source("lib/store.R", local = T)
source("lib/kerberos.R")

# need to be changed (read it from ini) # required to be load before RJava
renew_kerberos_ticket(username = "hive", password = "HIVE2018")

source("lib/connection.R" , local = T)


# ok
source("source_db_code/Oracle.R", local = T)
source("lib/RSqoop_backend.R", local = T)


# need work
source("lib/sqoop_backend.R", local = T)

rsb <- RSqoop_backend()

convert_to_gold_layer <- function(tn, inherit_partition = T, 
                                  add_distinct = T, adaptive_sequential_distinct = T, split_col, split_col_type = "date", 
                                  row_limits = 100*10^6){
  
  
  rsb$src_db$table_catalogue()
  rsb$src_db$select_table(tn)
  
  partition_possible <- F
  partition_col <- NULL
  
  # not implemented yet
  if(inherit_partition){
    try({
      
      old_plan <- rsb$executor$read_plans(tbl_name = tn)[[1]]
      ota <- old_plan$import_profile$analysis_info$TA
      
      if(ota$is_partitioned){
        partition_col <- ota$partition_info$partitions$COLUMN_NAME %>% intersect(ota$suggested_columns_for_job_split) %>% .[[1]]
        if(length(partition_col)){
          partition_possible <- T
        }
      }
      
    },silent = T)
  }
  
  #rsb$src_db$get_selected_table_analysis()
  
  # partitioned table required to taken care later
  
  
  h_str <- rsb$src_db$get_hadoop_db_type_map()
  
  tbl <- paste0(rsb$RSqoop$hadoop_dest_gold_db_name,".",rsb$src_db$get_selected_table()$table_name)
  
  tbl_loc <- rsb$src_db$get_selected_table() %>% unlist() %>% as.character() %>% c(rsb$RSqoop$hdfs_dest_gold_dir,.) %>% as.list() %>% do.call(file.path,.)
  
  tbl_str <- h_str[c("COLUMN_NAME","Impala_Type")] %>% apply(1, paste0, collapse = " ") %>% paste0(collapse = ", ") %>% paste0("(",.,")")
  
  qry_create <- paste0("CREATE EXTERNAL TABLE IF NOT EXISTS ",tbl," ",tbl_str,"
STORED AS PARQUET LOCATION '",tbl_loc,"'")
  
  rsb$hadoop_db$Impala$reconnect()
  rsb$hadoop_db$Impala$query(paste0("CREATE DATABASE IF NOT EXISTS ", rsb$RSqoop$hadoop_dest_gold_db_name), is_DDL = T)
  rsb$hadoop_db$Impala$query(paste0("DROP TABLE ",tbl), is_DDL = T)
  rsb$hadoop_db$Impala$query(qry_create, is_DDL = T)
  
  qry_convert_sel <- h_str[c("COLUMN_NAME","Impala_Type")] %>% apply(1, function(x) paste0("CAST(",x[1]," AS ",x[2],") AS ",x[1])) %>% 
    paste0(collapse = ", ") %>% paste0(ifelse(add_distinct, "SELECT DISTINCT ","SELECT "),.," FROM ", rsb$RSqoop$hadoop_dest_db_name, ".", rsb$src_db$get_selected_table()$table_name)
  
  qry_insert <- paste0("INSERT OVERWRITE TABLE ", tbl, " ", qry_convert_sel)
  
  qry_insert_nodel <- paste0("INSERT INTO TABLE ", tbl, " ", qry_convert_sel)
  
  adaptive_sequential_distinct_possible <- F
  
  if(adaptive_sequential_distinct){
    if(missing(split_col)){
      adaptive_sequential_distinct <- F
    }else{
      if(split_col_type!="date"){
        stop("don't know how to deal with non dates")
      }
      cat("\nGetting summary_split_col from HDB\n")
      summary_split_col_hdb <- paste0("SELECT ", split_col,", count(1) as cnt FROM ", rsb$RSqoop$hadoop_dest_db_name, ".", rsb$src_db$get_selected_table()$table_name, 
                                      " GROUP BY ", split_col) %>% rsb$hadoop_db$Impala$query()
      
      colnames(summary_split_col_hdb)<- toupper(colnames(summary_split_col_hdb))
      split_col <- toupper(split_col)
      summary_split_col_hdb[[split_col]]<- as.Date(summary_split_col_hdb[[split_col]])
      summary_split_col_hdb <- summary_split_col_hdb %>% arrange_(split_col)
      
      summary_split_col_hdb %>% mutate(cum_cnt = cumsum(CNT),
                                       in_lim = cum_cnt %% row_limits, 
                                       in_lim_diff = c(0, diff(in_lim)), 
                                       in_lim_diff_mark = (in_lim_diff<0 | CNT>row_limits), 
                                       groups = cumsum(in_lim_diff_mark)) -> summary_split_col_hdb
      
      summary_split_col_hdb_grp <- summary_split_col_hdb %>% group_by(groups) %>% 
        summarise(mndate = min(get(split_col)),mxdate = max(get(split_col)), cnts = sum(CNT))
      
      summary_split_col_hdb_grp %>% mutate(wpt =  rsb$src_db$col_compare_transform(split_col, mndate, get_hadoop_db_col = T),
                                           where_part = paste0(wpt,">= '", mndate, "' AND ", wpt,"<= '", mxdate,"'")) %>%
        select(-wpt) ->summary_split_col_hdb_grp
      
      adaptive_sequential_distinct_possible <- T
      
    }
  }
  
  if(adaptive_sequential_distinct_possible){
    cat("adaptive_sequential_distinct insert start\n")
    
    qrys_insert <- summary_split_col_hdb_grp$where_part %>% map_chr(~paste0(qry_insert_nodel," WHERE ", .x))
    
    qrys_insert[1] <- paste0(qry_insert," WHERE ", summary_split_col_hdb_grp$where_part[1])
    
    qrys_insert_results <- qrys_insert %>% map(~rsb$hadoop_db$Impala$query(.x, is_DDL = T))
    
    if(qrys_insert_results %>% map_lgl(is.null) %>% all()){
      cat("adaptive_sequential_distinct insert :: ALL JOB RAN FINE\n")
    }else{
      iq <- rsb$hadoop_db$Impala$query
      dir.create("store/runtime", recursive = T, showWarnings = F)
      save(qrys_insert_results, qrys_insert, iq, file = "store/runtime/adaptive_sequential_distinct.RData")
      cat("adaptive_sequential_distinct insert :: SOME JOB FAILED TO RUN (check manually)\n\nDetails are in store/runtime/adaptive_sequential_distinct.RData\n\n")
    }
    
    cat("adaptive_sequential_distinct insert end\n")
  }else{
    cat("insert start\n")
    t0 <- Sys.time()
    rsb$hadoop_db$Impala$query(qry_insert, is_DDL = T)
    t1 <- Sys.time()
    print(t1-t0)
    cat("insert end\n")
  }
  
  cat("stat calc start\n")
  t0 <- Sys.time()
  rsb$hadoop_db$Impala$query(paste0("COMPUTE STATS ", tbl), is_DDL = T)
  t1 <- Sys.time()
  print(t1-t0)
  cat("stat calc end\n")
  
}


transfer_to_gold_layer <- function(tn, from, from_db, stat_calc = F, fill_null_on_col_mismatch = F,allowed_null_col_fraction = 0.5){
  
  
  if(missing(from)){
    from <- tn
  }
  
  if(missing(from_db)){
    # in case table name changed in Oracle [CRAP!]
    from_db <- from
  }
  
  
  tbl <- paste0(rsb$RSqoop$hadoop_dest_gold_db_name,".",tn)
  
  rsb$hadoop_db$Impala$reconnect()
  is_tbl_present <- isTRUE(try(rsb$hadoop_db$Impala$query(paste0("SELECT 1 FROM ", tbl," LIMIT 1"))[[1]], silent = T)==1)
  
  if(!is_tbl_present){
    convert_to_gold_layer(tn)
    return(0)
  }
  
  transfer_possible <- F
  
  if(is_tbl_present){
    # appending to the end
    ep <- rsb$executor$read_plans(tbl_name = tn)[[1]]
    
    where_part <- NULL
    to_fill_by_null_cols <- NULL
    
    if(ep$import_profile$update_check$style == "INCREMENTAL"){
      if(ep$import_profile$analysis_info$TA$suggested_columns_for_split.type %in% c("DATE","TIMESTAMP")){
        
        raw_info <- rsb$hadoop_db$Impala$query(paste0("SELECT MAX(",ep$import_profile$update_check$hadoop_db_key,") FROM ", rsb$RSqoop$hadoop_dest_db_name, ".", from))
        gold_info <- rsb$hadoop_db$Impala$query(paste0("SELECT MAX(",ep$import_profile$analysis_info$TA$suggested_columns_for_split,") FROM ", tbl))
        raw_info <- raw_info[[1]] %>% as.Date()
        gold_info <- gold_info[[1]] %>% as.Date()
        
        
        if(raw_info>gold_info){
          where_part <- paste0(ep$import_profile$update_check$hadoop_db_key," > '", gold_info,"'")
          transfer_possible <- T
        }
      }
    }
    
    if(ep$import_profile$update_check$style == "PARTITION_UPDATE"){
      raw_info <- rsb$hadoop_db$Impala$query(paste0("SELECT DISTINCT(",ep$import_profile$update_check$hadoop_db_key,") FROM ", rsb$RSqoop$hadoop_dest_db_name, ".", from))
      gold_info <- rsb$hadoop_db$Impala$query(paste0("SELECT DISTINCT(",ep$import_profile$analysis_info$TA$suggested_columns_for_job_split,") FROM ", tbl))
      raw_info <- raw_info[[1]]
      gold_info <- gold_info[[1]]
      
      mnchr <- raw_info %>% nchar() %>% c(nchar(gold_info)) %>% min()
      
      gold_info <- substr(gold_info, 1, mnchr)
      raw_info <- substr(raw_info, 1, mnchr)
      
      new_info <- raw_info %>% setdiff(gold_info)
      
      if(length(new_info)>0){
        
        if(length(new_info)==1){
          where_part <- paste0("SUBSTR(",ep$import_profile$update_check$hadoop_db_key,",1,",mnchr,") = '", new_info,"'")
        }else{
          where_part <- paste0("SUBSTR(",ep$import_profile$update_check$hadoop_db_key,",1,",mnchr,") in ", new_info %>% paste0("'",.,"'", collapse = ",") %>% paste0("(",.,")"))
        }
        
        transfer_possible <- T
      }
    }
    
    if(!exists("where_part")){
      transfer_possible<- F
      where_part <- NULL
    }
    
    if(is.null(where_part)){
      transfer_possible<- F
    }
    
    if(transfer_possible){
      
      raw_cols <- rsb$hadoop_db$Impala$query(paste0("SELECT * FROM ", rsb$RSqoop$hadoop_dest_db_name, ".", from," LIMIT 1")) %>% colnames() %>% toupper()
      gold_cols <- rsb$hadoop_db$Impala$query(paste0("SELECT * FROM ", tbl," LIMIT 1")) %>% colnames() %>% toupper()
      
      if((gold_cols %>% setdiff(raw_cols) %>% length())>0){
        cat("\nColumns not comparable\n")
        if(fill_null_on_col_mismatch){
          common_cols <- intersect(raw_cols,gold_cols)
          non_common_cols <- gold_cols %>% union(raw_cols) %>% setdiff(common_cols)
          det_pct  <- length(non_common_cols)/ length(common_cols)
          if(det_pct<allowed_null_col_fraction){
            cat("\nPossible to fill by NULL\n")
            cat("\nColumns to be filled by NULL is(are):",paste0(non_common_cols, collapse = ", "),"\n")
            to_fill_by_null_cols <- setdiff(gold_cols, raw_cols)
          }else{
            cat("\nExiting\n")
            return(F)
          }
        }else{
          cat("\nExiting\n")
          return(F)
        }
        
        
      }
      
      
      rsb$src_db$table_catalogue()
      rsb$src_db$select_table(from_db)
      
      if(isTRUE(rsb$src_db$get_selected_table()$table == from_db)){
        
        h_str <- rsb$src_db$get_hadoop_db_type_map()
        
        h_str <- h_str %>% filter(COLUMN_NAME %in% gold_cols)
        
        convert_df <- h_str
        convert_df$map <- convert_df[c("COLUMN_NAME","Impala_Type")] %>% apply(1, function(x) paste0("CAST(",x[1]," AS ",x[2],") AS ",x[1]))
        convert_df$map[convert_df$Java_Type=="String"] <- convert_df$COLUMN_NAME[convert_df$Java_Type=="String"]
        
        convert_df <- convert_df[c("COLUMN_NAME","map")]
        
        if(!is.null(to_fill_by_null_cols)){
          cat("\nFilling by NULL for these column(s):",paste0(to_fill_by_null_cols, collapse = ", "),"\n")
          convert_df<- convert_df %>% rbind(data.frame(COLUMN_NAME = to_fill_by_null_cols, map = paste0("NULL AS ",to_fill_by_null_cols))) %>% unique()
        }
        
        # i'm not sure whether this is required or not
        
        convert_df <- convert_df[which_permutation(gold_cols, convert_df$COLUMN_NAME),]
        
        qry_convert_sel <- convert_df$map %>% 
          paste0(collapse = ", ") %>% paste0("SELECT ",.," FROM ", rsb$RSqoop$hadoop_dest_db_name, ".", from)
        
        
        qry_insert <- paste0("INSERT INTO TABLE ", tbl, " ", qry_convert_sel, " WHERE ", where_part)
        
        cat("insert start\n")
        t0 <- Sys.time()
        rsb$hadoop_db$Impala$query(qry_insert, is_DDL = T)
        t1 <- Sys.time()
        print(t1-t0)
        cat("insert end\n")
        
        if(stat_calc){
          cat("stat calc start\n")
          t0 <- Sys.time()
          rsb$hadoop_db$Impala$query(paste0("COMPUTE STATS ", tbl), is_DDL = T)
          t1 <- Sys.time()
          print(t1-t0)
          cat("stat calc end\n")
        }
        
      }
      
    }
    
    
  }
  
  
}

# example

#convert_to_gold_layer("TBL_PART_IDL")
#convert_to_gold_layer("TBL_TRAN_CUST_DTL")
#convert_to_gold_layer("BSR1_QUARTERLY_FINAL")

#transfer_to_gold_layer("BSR1_QUARTERLY_FINAL")

#transfer_to_gold_layer("TBL_TRAN_CUST_DTL")

#transfer_to_gold_layer("BSR1_QUARTERLY_FINAL", from = "BSR1_QRTRLY_PUBLISH", from_db = "BSR1_QRTRLY_PUBLISH_MAR17", fill_null_on_col_mismatch = T)

# 
# h_tables <- rsb$executor$check_hadoop_db_for_existing_tables(fast = T)
# 
# h_tables$table_name %>% str_detect("DIM") %>% h_tables$table_name[.] %>% lapply(transfer_to_gold_layer)
