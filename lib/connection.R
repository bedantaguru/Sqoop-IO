
source("lib/external_R.R", local = T)

require(RJDBC)
require(stringr)

options(stringsAsFactors = F, scipen = 15)

jdbc_connection_manager <- function(config){
  # priority wise loading
  all_configs <- names(config)
  
  #################################
  ## getting driver classpath #####
  #################################
  drv_classPath <- NULL
  
  driver_priority_configs <- c("driver_folder_full_name", "driver_folder_name", "driver_file_full_name","driver_file_name")
  driver_priority_configs <- driver_priority_configs %>% rev()
  
  for(i in seq(length(driver_priority_configs))){
    
    if(driver_priority_configs[i] %in% all_configs){
      
      if(str_detect(driver_priority_configs[i] %>% tolower(),"folder")){
        drv_classPath <- normalizePath(list.files(config[[driver_priority_configs[i]]], pattern = ".jar$", full.names = T, recursive = T))
      }else{
        drv_classPath <- config[[driver_priority_configs[i]]]
      }
      
    }
    
  }
  
  #################################
  ## getting driver class     #####
  #################################
  
  drv_class <- config$driver_class
  
  #################################
  ###### getting JDBC URL     #####
  #################################
  jdbc_url <- config$JDBC_connection_URL
  
  #################################
  ### getting credentials     #####
  #################################
  
  credentials <- list()
  credentials$username <- config$username
  credentials$password <- config$password
  
  #################################
  ## constructing the output  #####
  #################################
  
  manager <- list()
  
  driver_object <- NULL
  connection_object <- NULL
  
  # create connection
  
  connect <- function(verbose = F){
    try({
      driver_object <<- JDBC(driverClass = drv_class, classPath = drv_classPath)
      
      if(is.null(credentials)){
        connection_object <<- dbConnect(driver_object, jdbc_url)
      }else{
        if(is.null(credentials$password) & !is.null(credentials$username)){
          # only username authentication
          connection_object <<- dbConnect(driver_object, jdbc_url, credentials$username)
        }else{
          # both are present
          connection_object <<- dbConnect(driver_object, jdbc_url, credentials$username, credentials$password)
        }
      }
      
    }, silent = !verbose)
  }
  
  # this is for user and above may be required by other sub-functions
  manager$connect <- connect
  
  # create query function
  
  direct_query <- function(sql, is_DDL = F){
    if(is_DDL){
      try(dbSendUpdate(connection_object, sql),silent = T)
    }else{
      try(dbGetQuery(connection_object, sql),silent = T)
    }
  }
  
  external_query <- function(sql, is_DDL = F, wait_secs = 5*60, return_hook = F){
    
    external_R("restart")
    
    wait_for_external_R()
    
    current_db_set_qry <- paste0("use ", direct_query("select current_database()")[[1]])
    put_in_external_R(List = c("sql","is_DDL", "current_db_set_qry"), env = environment())
    
    
    put_in_external_R(List = c("drv_class", "drv_classPath","jdbc_url","credentials","connect","direct_query","connection_object","driver_object"), env = environment(connect))
    
    h <- run_in_external_R("
                           require(RJDBC)
                           require(stringr)
                           connection_object <- NULL
                           connect()
                           direct_query(current_db_set_qry, is_DDL = T)
                           direct_query(sql, is_DDL)")
    
    if(return_hook){
      return(h)
    }
    
    h$wait_for_output(wait_secs = wait_secs)
  }
  
  
  
  # create test query
  if("test_query" %in% all_configs){
    test_query <- function(){
      sql <- config$test_query
      qo <- try(dbGetQuery(connection_object, sql),silent = T)
      if("try-error" %in% class(qo)){
        return(F)
      }else{
        qo <- qo[[1]]
        if(qo == config$test_query_expect){
          return(T)
        }
      }
      return(F)
    }
    
    manager$test_query <- test_query
    
    # reconnect function
    
    reconnect <- function(){
      if(!test_query()){
        connect()
      }
    }
    
    manager$reconnect <- reconnect
    
    manager$query <- function(sql, is_DDL = F, external = F, wait_secs = 1*60, ...){
      
      if(external){
        try(external_query(sql = sql, is_DDL = is_DDL, wait_secs = wait_secs, ...),silent = T)
      }else{
        reconnect()
        direct_query(sql = sql, is_DDL = is_DDL)
      }
      
    }
    
  }else{
    # is test is not available then direct query
    manager$query <- direct_query
  }
  
  manager$is_valid <- function(){
    c("JDBCConnection") %in% class(connection_object) & c("JDBCDriver") %in% class(driver_object)
  }
  
  manager$get_connection <- function(){
    connection_object
  }
  
  return(manager)
  
}


hdfs_connection_manager <- function(config){
  # priority wise loading
  all_configs <- names(config)
  
  #################################
  ## Check for prerequisites  #####
  #################################
  
  code_to_run <-""
  
  if("prerequisites" %in% all_configs){
    code_to_run <-c(code_to_run, 
                    config$prerequisites)
  }
  
  #################################
  ## Add loading library command ##
  #################################
  
  code_to_run <-c(code_to_run, 
                  paste0("library(",config$r_package,")"))
  
  #################################
  ##### Add init code #############
  #################################
  
  if("hdfs_init" %in% all_configs){
    code_to_run <-c(code_to_run, 
                    config$hdfs_init)
  }
  
  # to run
  connect <- function(){
    try(eval(parse(text = code_to_run)), silent = T)
  }
  
  #################################
  ## constructing the output  #####
  #################################
  
  manager <- list()
  
  manager$connect <- connect
  
  manager$reconnect <- connect
  
  build_function <- function(conf_str){
    f <- try(eval(parse(text = conf_str)), silent = T)
    if(is.function(f)){
      return(f)
    }
    return(NULL)
  }
  
  
  
  # validate ls function
  # unless connected build_function will not work
  connect()
  ls <- build_function(config$hdfs_ls)
  test_out <- try(ls("/"),silent = T)
  if(!("try-error" %in% class(test_out))){
    # ls function working properly 
    manager$ls <- function(path, ...){
      try(ls(path, ...), silent = T)
    } 
    
    readlines <- build_function(config$hdfs_read_lines)
    
    manager$readline_from_single_file <- function(path, ...){
      try(readlines(path, ...), silent = T)
    }
    
    manager$list.files <- function(path, ...){
      try({
        # works as reconnect
        connect()
        ls(path, ...)
      }, silent = T)
    }
    
    manager$readlines <- function(path){
      try({
        # works as reconnect
        connect()
        flist <- ls(path, recurse = T)[["file"]]
        txt <- flist %>% lapply(readlines) %>% unlist()
        txt
      }, silent = T)
    }
    
    manager$clean <- build_function(config$hdfs_rm)
    
    
  }
  
  return(manager)
  
}


hadoop_db_connection_manager <- function(config){
  manager <- list()
  if(config$validated){
    try({
      for(cn in c("Hive","Impala")){
        manager[[cn]] <- jdbc_connection_manager(config[[cn]])
      }
    },silent = T)
    
  }
  return(manager)
}