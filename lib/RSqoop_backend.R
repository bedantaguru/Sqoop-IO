

RSqoop_load_configs <- function(){
  read_config()
  detect_current_system()
  load_backend_config()
}


RSqoop_backend <- function(updateProgress){
  
  
  backend <- list()
  
  if(missing(updateProgress)){
    # progress tracker
    updateProgress <- function(value = NULL, detail = NULL){
      invisible(0)
    }
  }
  
  total_stages <- 5
  
  # update progress
  updateProgress(value = 1/total_stages, detail = "Loading Configurations")
  
  param <- get_param()
  if(is.null(param)){
    RSqoop_load_configs()
    param <- get_param()
  }
  
  # specific parameters
  
  param$rdb_backend$parallel_on_select <- param$rdb_backend$parallel_on_select %>% toupper() %>% as.logical()
  
  param$rdb_backend$server_limit_on_num_connections <- param$rdb_backend$server_limit_on_num_connections %>% as.numeric()
  
  
  backend$RSqoop <- param$system_backend$RSqoop
  
  backend$RSqoop$get_temp_dir_for_sqoop_codegen <- function(){
    dname<-tempfile(pattern = "sqoop_codes_")
    dir.create(dname, showWarnings = F)
    return(dname)
  }
  
  #RSqoopParam_ForSqoopExecutionPlan <- backend
  
  ##################################### Load Connection Managers #####################################
  # update progress
  updateProgress(value = 3/total_stages, detail = "Loading Connection Managers")
  
  
  
  ##################################### Load Backends #####################################
  # update progress
  updateProgress(value = 4/total_stages, detail = "Loading Backends")
  
  # get hdfs backend
  backend$hdfs <- hdfs_connection_manager(param$hdfs_backend)
  
  # get hadoop db [Hive and Impala] backend
  backend$hadoop_db <- hadoop_db_connection_manager(param$hadoop_db_backend)
  
  # get source RDB backend
  backend$src_db<- list()
  
  backend$src_db$param <- param$rdb_backend
  backend$src_db$cm <- jdbc_connection_manager(backend$src_db$param)
  backend$src_db <- c(backend$src_db, source_db_backend(backend$src_db$cm))
  
  # get sqoop builder backend
  backend$sqoop_builder <- sqoop_builder(args_info_data = read_sqoop_arguments_from_guide())
  
  backend$sqoop <- sqoop_runner_backend()
  
  backend$executor$sqoop_execution_plan <- function(...){
    sqoop_execution_plan(...,backend = backend)
  }
  
  backend$executor$execute <- executor_backend()
  
  save_plan <- function(eplan){
    dir.create("store/eplan", showWarnings = F, recursive = T)
    file_name <- paste0(eplan$import_profile$db_name, ".",eplan$import_profile$table_name)
    file_name <- file.path("store/eplan", file_name)
    saveRDS(eplan, file_name)
  }
  
  
  backend$executor$save_plan <- save_plan
  
  read_plans <- function(tbl_name = ""){
    files <- list.files("store/eplan", full.names = T, pattern = paste0(".", tbl_name,"$"))
    all_plans <- lapply( files, readRDS)
    names(all_plans) <- basename(files)
    return(all_plans)
  }
  
  backend$executor$read_plans <- read_plans
  
  clean_cached_plans <- function(){
    files <- list.files("store/eplan", full.names = T)
    unlink(files, recursive = T, force = T)
    return(invisible(0))
  }
  
  backend$executor$clean_cached_plans <- clean_cached_plans
  
  update_hdb_info_on_plans<- function(force_update = F, delete_update_info_for_tables = NULL){
    if(!backend$hadoop_db$Impala$test_query()){
      backend$hadoop_db$Impala$connect()
    }
    backend$hadoop_db$Impala$query(paste0("USE ", backend$RSqoop$hadoop_dest_db_name), is_DDL = T)
    backend$hadoop_db$Impala$query("INVALIDATE METADATA", is_DDL = T)
    all_plans <- read_plans()
    
    update_hdb_info <- function(ep){
      
      if(is.null(ep$import_profile$update_check$hadoop_db_info.update_time)){
        ep$import_profile$update_check$hadoop_db_info.update_time <- Sys.Date() - 365
      }
      
      del_info <- F
      if(length(delete_update_info_for_tables)){
        if(ep$import_profile$table_name %in% delete_update_info_for_tables){
          del_info <- T
        }
      }
      
      if(difftime(Sys.time(), ep$import_profile$update_check$hadoop_db_info.update_time, units = "day") > 1 | force_update | del_info ){
        ep$import_profile$update_check[["hadoop_db_info"]] <- NULL
      }
      
      
      if(is.null(ep$import_profile$update_check[["hadoop_db_info"]]) & !del_info){
        
        hdbinf <- backend$hadoop_db$Impala$query(ep$import_profile$update_check$hadoop_db_sql)
        if(is.data.frame(hdbinf)){
          if(nrow(hdbinf)){
            hdbinf <- hdbinf[[1]]
            ep$import_profile$update_check[["hadoop_db_info"]] <- hdbinf
            ep$import_profile$update_check$hadoop_db_info.update_time <- Sys.time()
            save_plan(ep)
          }
        }
      }
      
      
    }
    cat("\nFetching update info\n")
    llply(all_plans, update_hdb_info, .progress = "text")
    return(invisible(0))
  }
  
  
  backend$executor$update_hdb_info_on_plans <- update_hdb_info_on_plans
  
  update_plans_for_stored_tables <- function(tbl_name, delete_update_plan_for_tables = NULL){
    update_hdb_info_on_plans()
    all_plans <- read_plans()
    update_possible <- all_plans %>% lapply("[[","is_valid") %>% unlist() %>% all_plans[.]
    update_table_names <- update_possible %>% names() %>% str_split("\\.") %>% lapply("[[",2) %>% unlist()
    if(!missing(tbl_name)){
      update_now <- update_possible[ which(update_table_names == tbl_name) ]
    }else{
      update_now <- update_possible
    }
    
    get_update_plan <- function(ep){
      
      
      if(is.null(ep$import_profile$update_check$update_ep.update_time)){
        ep$import_profile$update_check$update_ep.update_time <- Sys.Date() - 365
      }
      
      del_info <- F
      if(length(delete_update_plan_for_tables)){
        if(ep$import_profile$table_name %in% delete_update_plan_for_tables){
          del_info <- T
        }
      }
      
      if(difftime(Sys.time(), ep$import_profile$update_check$update_ep.update_time, units = "day") > 1 | del_info){
        ep$import_profile$update_check[["update_ep"]] <- NULL
      }
      
      
      if(is.null(ep$import_profile$update_check[["update_ep"]]) & !del_info){
        
        up_ep <- backend$src_db$get_sqoop_import_execution_plan_update(last_import_profile = ep, hdb_out = ep$import_profile$update_check$hadoop_db_info, 
                                                                       SEP = backend$executor$sqoop_execution_plan, 
                                                                       server_num_connection_limit = param$rdb_backend$server_limit_on_num_connections,
                                                                       parallel_select = param$rdb_backend$parallel_on_select)
        if(up_ep$is_valid){
          
          ep$import_profile$update_check[["update_ep"]] <- up_ep
          ep$import_profile$update_check$update_ep.update_time <- Sys.time()
          
        }else{
          failed <- T
          if(!is.null(up_ep$import_profile$is_upto_date)){
            if(up_ep$import_profile$is_upto_date){
              failed <- F
            }
          }
          
          if(failed){
            cat("\nUpdate plan failed to generate..\n")
          }else{
            ep$import_profile$update_check[["update_ep"]] <- up_ep
            ep$import_profile$update_check$update_ep.update_time <- Sys.time()
          }
          
        }
        
        
      }
      
      
      save_plan(ep)
      
    }
    
    cat("\nFetching Update execution plans\n")
    
    llply(update_now, get_update_plan, .progress = "text")
    
    return(invisible(0))
  }
  
  backend$executor$update_plans_for_stored_tables <- update_plans_for_stored_tables
  
  .get_sqoop_import_execution_plan <- function(tbl_name, fresh = F){
    
    cached_plan <- NULL
    
    if(!fresh){
      
      all_plans <- read_plans()
      
      if(length(all_plans)){
        
        tables_in <- data.frame(full_name = names(all_plans))
        
        tables_in$table_name <- tables_in$full_name %>% str_split("\\.") %>% lapply("[[",2) %>% unlist()
        
        if(tbl_name %in% tables_in$table_name){
          cached_plan <-all_plans[which(tables_in$table_name == tbl_name)[1]][[1]]
        }
        
      }
      
    }
    
    if(is.null(cached_plan)){
      
      ep <- list(is_valid = F, import_profile = NULL, exe_plans = NULL)
      try({
        backend$src_db$table_catalogue()
        backend$src_db$select_table(tbl_name)
        ep <- backend$src_db$get_sqoop_import_execution_plan(SEP = backend$executor$sqoop_execution_plan, 
                                                             server_num_connection_limit = param$rdb_backend$server_limit_on_num_connections,
                                                             parallel_select = param$rdb_backend$parallel_on_select)
        if(ep$is_valid){
          save_plan(ep)
        }
        
      }, silent = T)
      
    }else{
      ep <- cached_plan
    }
    
    return(invisible(ep))
  }
  
  
  backend$executor$get_sqoop_import_execution_plan <- .get_sqoop_import_execution_plan
  
  # few helper functions
  
  is_hadoop_table_size_in_balance <- function(table_name){
    
    backend$hadoop_db$Impala$reconnect()
    
    backend$hadoop_db$Impala$query(paste0("USE ", backend$RSqoop$hadoop_dest_db_name), is_DDL = T)
    
    file_blocks <- backend$hadoop_db$Impala$query(paste0("SHOW FILES IN ", table_name))
    
    if(nrow(file_blocks)>1){
      
      file_blocks$Size_unit <- str_extract(file_blocks$Size, "[a-zA-Z]+")
      file_blocks$Size <- str_extract(file_blocks$Size, "[0-9\\.]+") %>% as.numeric()
      
      file_blocks$Size_unit_num <- laply(file_blocks$Size_unit, switch, B = 1/1000, KB = 1, MB = 1000, GB = 1000*1000)
      
      file_blocks$Size_unit_num <- suppressWarnings(as.numeric(file_blocks$Size_unit_num))
      
      file_blocks$Size_in_KB <- file_blocks$Size*file_blocks$Size_unit_num
      
      sdf <- sd(file_blocks$Size_in_KB, na.rm = T)
      medf <- median(file_blocks$Size_in_KB, na.rm = T)
      # sd based check sdf < 100*1000
      
      return( sdf/medf < 1 )
    }else{
      return(T)
    }
  }
  
  is_hadoop_table_stat_present <- function(table_name){
    
    backend$hadoop_db$Impala$reconnect()
    
    backend$hadoop_db$Impala$query(paste0("USE ", backend$RSqoop$hadoop_dest_db_name), is_DDL = T)
    
    stat_info <- backend$hadoop_db$Impala$query(paste0("SHOW TABLE STATS ", table_name))
    col_stat_info <- backend$hadoop_db$Impala$query(paste0("SHOW COLUMN  STATS ", table_name))
    
    if(stat_info$`#Rows` == -1 | col_stat_info$`#Distinct Values` %>% max() == -1){
      return(F)
    }else{
      return(T)
    }
    
  }
  
  hadoop_table_info <- function(table_name){
    backend$hadoop_db$Impala$reconnect()
    
    backend$hadoop_db$Impala$query(paste0("USE ", backend$RSqoop$hadoop_dest_db_name), is_DDL = T)
    
    stat_info <- backend$hadoop_db$Impala$query(paste0("SHOW TABLE STATS ", table_name))
    return(stat_info[c("Size","Location", "#Rows", "#Files")])
  }
  
  # check hadoop DB and put all plans
  
  check_hadoop_db_for_existing_tables <- function(fast = F){
    
    backend$hdfs$connect()
    hdfs_report <- backend$hdfs$ls( backend$RSqoop$hdfs_dest_dir )
    hdfs_report <- ddply(hdfs_report, "file", function(d){
      do <- backend$hdfs$ls(d$file)["file"]
      do$db <- basename(d$file[1])
      do
    }, .id = NULL)
    
    hdfs_report$table_name <- basename(hdfs_report$file)
    
    backend$hadoop_db$Impala$connect()
    
    backend$hadoop_db$Impala$query(paste0("USE ", backend$RSqoop$hadoop_dest_db_name), is_DDL = T)
    
    hadoop_db_report <- backend$hadoop_db$Impala$query(paste0("SHOW TABLES IN ",backend$RSqoop$hadoop_dest_db_name ))
    
    if(!fast){
      
      hadoop_db_report$size_in_balance <- laply(hadoop_db_report$name, is_hadoop_table_size_in_balance)
      
      hadoop_db_report$stat_present <- laply(hadoop_db_report$name, is_hadoop_table_stat_present)
      
      cat("\nGetting Table Stats\n")
      hadoop_db_report <- cbind(hadoop_db_report, 
                                ldply(hadoop_db_report$name, hadoop_table_info, .progress = "text")) 
      
    }
    
   
    
    hadoop_db_report$name <- toupper(hadoop_db_report$name)
    
    hdfs_report$table_name <- toupper(hdfs_report$table_name)
    
    hadoop_report <- merge(hdfs_report, hadoop_db_report, by.x = "table_name", by.y = "name")
    
    return(hadoop_report)
    
  }
  
  
  calculate_table_stat <- function(tables){
    if(missing(tables)){
      tables<- check_hadoop_db_for_existing_tables(fast = T)
      tables <- tables$table_name
    }
    
    if(length(tables)){
      
      backend$hadoop_db$Impala$reconnect()
      
      backend$hadoop_db$Impala$query(paste0("USE ", backend$RSqoop$hadoop_dest_db_name), is_DDL = T)
      
      tables %>% llply(function(tn){
        paste0("COMPUTE STATS ", tn) %>% backend$hadoop_db$Impala$query(is_DDL = T)
      }, .progress = "text")
      
    }
    
    return(invisible(0))
    
  }
  
  backend$hadoop_db$is_hadoop_table_size_in_balance <- is_hadoop_table_size_in_balance
  
  backend$hadoop_db$is_hadoop_table_stat_present <- is_hadoop_table_stat_present
  
  backend$hadoop_db$hadoop_table_info <- hadoop_table_info
  
  backend$hadoop_db$calculate_table_stat <- calculate_table_stat
  
  backend$executor$check_hadoop_db_for_existing_tables <- check_hadoop_db_for_existing_tables
  
  
  backend$executor$get_sqoop_import_execution_plan_for_existing_tables_in_hadoop_db <- function(){
    report <- check_hadoop_db_for_existing_tables(fast = T)
    # fetching EPLANs
    cat("\nFetching execution plans\n")
    llply(report$table_name , .get_sqoop_import_execution_plan, .progress = "text")
    # update_hdb_info_on_plans() this is included in following command : 
    update_plans_for_stored_tables()
    return(invisible(0))
  }
  
  backend$executor$get_plan <- function(table_name){
    ep_base <- list(is_valid = F, import_profile = NULL, exe_plans = NULL)
    ep <- ep_base
    report <- check_hadoop_db_for_existing_tables(fast = T)
    if(table_name %in% report$table_name){
      cat("\nTable present checking for update\n")
      update_plans_for_stored_tables(table_name)
      ep <- read_plans(table_name)
      ep <- ep[[1]]
      ep <- ep$import_profile$update_check[["update_ep"]]
    }else{
      ep <- .get_sqoop_import_execution_plan(tbl_name = table_name)
    }
    
    if(is.null(ep$is_valid)){
      ep$is_valid <- F
    }
    
    
    return(ep)
  }
  
  
  backend$executor$run_plan <- function(ep){
    
    # delete update info
    all_plans <- read_plans()
    
    this_plan <- all_plans[[ paste0(ep$import_profile$db_name, ".", ep$import_profile$table_name)]]
    
    this_plan$import_profile$update_check[["hadoop_db_info"]] <- NULL
    this_plan$import_profile$update_check[["update_ep"]] <- NULL
    
    save_plan(this_plan)
    
    backend$executor$execute$run(ep$exe_plans)
    return(invisible(0))
  }
  
  backend$executor$simple_table_summary <- function(tbl_name){
    backend$src_db$table_catalogue()
    backend$src_db$select_table(tbl_name)
    tbl <- backend$src_db$get_selected_table()
    if(!is.null(tbl)){
      hdb_info <- check_hadoop_db_for_existing_tables(fast = T)
      tbl$Number_of_Rows <- backend$src_db$get_selected_table_row_count()
      tbl$Present_in_HadoopDB <- (tbl$table_name %in% hdb_info$table_name)
      if(tbl$Present_in_HadoopDB){
        tbl$HDFS_Location <- hdb_info$file[ (hdb_info$table_name == tbl$table_name) ]
      }
    }
    return(tbl)
  }
  
  # update progress
  updateProgress(value = 5/total_stages, detail = "Done")
  
  
  return(backend)
}



