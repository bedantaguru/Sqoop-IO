

RSqoop_backend_encapsulated <- function(){
  
  handle <- list()
  
  require(plyr)
  require(dplyr)
  
  
  options(stringsAsFactors = F)
  # fine
  source("lib/system_call.R", local = T)
  source("lib/ini.R", local = T)
  source("lib/read_conf.R", local = T)
  source("lib/sqoop_builder.R", local = T)
  source("lib/sqoop_guide_reader.R", local = T)
  source("lib/store.R", local = T)
  source("lib/kerberos.R", local = T)
  
  # need to be changed (read it from ini) # required to be load before RJava
  renew_kerberos_ticket(username = "user", password = "pass")
  
  source("lib/connection.R" , local = T)
  
  
  # ok
  source("source_db_code/Oracle.R", local = T)
  source("lib/RSqoop_backend.R", local = T)
  
  
  # need work
  source("lib/sqoop_backend.R", local = T)
  
  handle$RSqoop_load_configs <- RSqoop_load_configs
  handle$RSqoop_backend <- RSqoop_backend
  
  # from read_conf.R
  handle$get_run_time_config <- get_param
  
  return(handle)
  
}
