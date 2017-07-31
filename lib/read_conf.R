
library(stringr)

options(stringsAsFactors = F)

# generic function for merginf configs
merge_config <- function(...){
  l_in <- list(...)
  name_list <- l_in %>% lapply(names)
  all_configs <- name_list %>% unlist() %>% unique()
  
  if(length(all_configs)==0){
    return(NULL)
  }
  
  all_configs_take <- all_configs %>% lapply(function(y){
    name_list %>% lapply(function(x){y  %in% x}) %>% unlist() %>% which() %>% min
  }) %>% unlist()
  
  l_out <- list()
  for(i in seq(length(all_configs))){
    l_out[[all_configs[i]]] <- l_in[[all_configs_take[i]]][[all_configs[i]]]
  }
  return(l_out)
}

init_config <- function(run_time_directory = "conf/ini-format/run_time", prototype_directory = "conf/ini-format/prototype"){
  dir.create(run_time_directory, showWarnings = F, recursive = T)
  file.copy(from = list.files(prototype_directory, pattern = ".conf$", full.names = T), to = run_time_directory, overwrite = F)
  return(invisible(0))
}

# only ini is supported as of now
read_config <- function(path = "conf/ini-format/run_time", type = "ini"){
  
  init_config(run_time_directory = path)
  
  config_list <- NULL
  
  if(type == "ini"){
    conf_files <- list.files(path, pattern = ".conf$", full.names = T)
    conf_names <- basename(conf_files) %>% str_replace(".conf","")
    confs <- conf_files %>% lapply(read.ini)
    names(confs) <- conf_names
    config_list <- confs
  }
  options(R_SQOOP_CONFIG = config_list)
  return(invisible(0))
}

get_config <- function(what){
  conf <- options("R_SQOOP_CONFIG")[[1]]
  if(missing(what)){
    return(conf)
  }else{
    what <- intersect( what, names(conf))
    if(length(what) > 0){
      return(conf[what])
    }
  }
  return(conf)
}

# this code has to run from master node of the cluster or in the node where sqoop clients are installed
detect_current_system <- function(){
  conf <- get_config()
  
  info <- list()
  
  info$system_backend <- list()
  
  
  ##############################################
  ########## Hadoop Verion #####################
  ##############################################
  
  # detect hadoop version and distribution
  hadoop_version <- sys_call(conf$sqoop$main$hadoop_version_command)
  
  available_prototype_for_hadoop <- conf$sqoop %>% names() %>% setdiff("main")
  match.available_prototype_for_hadoop <- available_prototype_for_hadoop %>% lapply(function(x){
    any(str_detect(hadoop_version, x))
  }) %>% unlist() %>% available_prototype_for_hadoop[.] 
  
  if(length(match.available_prototype_for_hadoop)==0){
    match.available_prototype_for_hadoop = "default"
  }else{
    match.available_prototype_for_hadoop <- match.available_prototype_for_hadoop[1]
  }
  
  info$system_backend$hadoop_version <-  hadoop_version[1]
  info$system_backend$hadoop_distribution <- match.available_prototype_for_hadoop
  info$system_backend$RSqoop <- conf$RSqoop$main
  
  
  ##############################################
  ############## Sqoop Lib #####################
  ##############################################
  
  sqoop_version <- sys_call(conf$sqoop$main$sqoop_version_command)
  sqoop_version <- sqoop_version[2]
  
  info$system_backend$sqoop_version <- sqoop_version
  
  # check and confirm sqoop libs
  available_prototype_for_sqoop_libs <- conf$sqoop[[info$system_backend$hadoop_distribution]]
  info$system_backend$sqoop_lib_validated <- available_prototype_for_sqoop_libs %>% lapply(file.exists) %>% unlist() %>% all()
  
  if(info$system_backend$sqoop_lib_validated){
    # once validated check for further files and linkage with prototype
    available_jdbc_driver_files <- available_prototype_for_sqoop_libs %>% lapply(list.files) %>% unlist() %>% unique()
    available_prototype_rdb <- conf$db_source %>% lapply("[[","driver_file_name") %>% 
      lapply(function(x){x %in% available_jdbc_driver_files}) %>% unlist() %>% conf$db_source[.]
    if(length(available_prototype_rdb)>0){
      full_driver_paths <- available_prototype_for_sqoop_libs %>% lapply(list.files, full.names = T) %>% unlist() %>% unique()
      available_prototype_rdb <- available_prototype_rdb %>% lapply(function(x){
        x$driver_file_full_name <- full_driver_paths %>% .[basename(.)==x$driver_file_name] %>% .[1]
        x
      })
      info$available_prototype_rdb <- available_prototype_rdb
      info$available_prototype_rdb$validated <- T
    }
  }
  
  info$system_backend$validated <- info$system_backend$sqoop_lib_validated & nchar(info$system_backend$hadoop_version[1])>0
  
  options(R_SQOOP_PARAM = info)
  
  return(invisible(0))
  
}

get_param <- function(what){
  info <- options("R_SQOOP_PARAM")[[1]]
  if(missing(what)){
    return(info)
  }else{
    what <- intersect( what, names(info))
    if(length(what) > 0){
      return(info[what])
    }
  }
  return(info)
}

load_backend_config <- function(){
  
  conf <- get_config()
  
  info <- get_param()
  
  ##############################################
  ############## HDFS backend ##################
  ##############################################
  
  # check Hadoop (HDFS) backend 
  available_prototype_for_hdfs_packages <- conf$hdfs %>% lapply("[[","r_package") %>% unlist()
  available_loadable_package <- installed.packages() %>% .[,"Package"]
  available_prototype_for_loadable_hdfs_package <-  available_loadable_package %>% intersect(available_prototype_for_hdfs_packages)
  available_prototype_name_for_hdfs_loadable_package <- conf$hdfs %>% lapply(function(x){x$r_package %in% available_prototype_for_loadable_hdfs_package}) %>% names()
  
  info$hdfs_backend<- list()
  
  info$hdfs_backend$validated <- length(available_prototype_for_loadable_hdfs_package) > 0
  if(info$hdfs_backend$validated){
    # set a default hdfs backend
    
    info$hdfs_backend <- c(info$hdfs_backend, 
                           conf$hdfs[[available_prototype_name_for_hdfs_loadable_package[1]]])
    info$hdfs_backend$loaded_config_name <- available_prototype_name_for_hdfs_loadable_package[1]
    
    info$hdfs_backend$available_prototype_names <- available_prototype_name_for_hdfs_loadable_package
  }
  
  ##############################################
  ######## Hive and Impala backend #############
  ##############################################
  available_prototype_for_hadoop_db <- conf$hadoop_db_source %>% names()
  
  info$hadoop_db_backend <- list()
  
  info$hadoop_db_backend$validated <- all(c("Hive","Impala") %in% available_prototype_for_hadoop_db)
  
  if(info$hadoop_db_backend$validated){
    # check RJDBC package
    info$hadoop_db_backend$validated <- "RJDBC" %in% available_loadable_package
    
    if(info$hadoop_db_backend$validated){
      info$hadoop_db_backend <- c(info$hadoop_db_backend,
                                  conf$hadoop_db_source)
    }
  }
  
  
  ##############################################
  ########### RDBMS backend ####################
  ##############################################
  available_prototype_for_rdb <- data.frame( db_src = conf$rdb_source %>% lapply("[[","use_db_source") %>% unlist(), name = conf$rdb_source %>% names())
  available_prototype_for_rdb_in_config <- info$available_prototype_rdb %>% names()
  available_prototype_for_rdb_intersect <- available_prototype_for_rdb$db_src %>% intersect(available_prototype_for_rdb_in_config)
  
  available_prototype_for_rdb <- available_prototype_for_rdb[ available_prototype_for_rdb$db_src %in% available_prototype_for_rdb_intersect, ]
  
  info$rdb_backend <- list()
  
  info$rdb_backend$validated <- nrow(available_prototype_for_rdb) > 0
  if(info$rdb_backend$validated){
    # set a default hdfs backend
    
    selected_rdb <- available_prototype_for_rdb[1,]
    
    info$rdb_backend <- c(info$rdb_backend,
                          merge_config(conf$rdb_source[[selected_rdb$name]], info$available_prototype_rdb[[selected_rdb$db_src]]))
    info$rdb_backend$loaded_config_name <- selected_rdb$name
    info$rdb_backend$loaded_config_db_name <- selected_rdb$db_src
    
    info$rdb_backend$available_prototype_names <- available_prototype_for_rdb
  }
  
  info$validated <- info %>% lapply("[[","validated") %>% unlist() %>% all()
  
  options(R_SQOOP_PARAM = info)
  
  return(invisible(0))
  
}