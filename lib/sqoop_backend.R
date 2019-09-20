

require(subprocess)

require(plyr)
require(dplyr)

sqoop_runner_backend <- function(){
  
  handle <- list()
  
  handle$init <- function(){
    options(RSQOOP_JOBS = list())
  }
  
  # this function is for running a single job
  run <- function(sqoop_args){
    
    try({
      
      # this is required for Sqoop : Ref : https://community.hortonworks.com/questions/57497/sqoop-from-oracle-connection-reset-error.html
      # subprocess can inherit environment variable defined in R
      Sys.setenv(HADOOP_OPTS = "-Djava.security.egd=file:/dev/../dev/urandom")
      
      sqooop_process <- spawn_process(command = Sys.which("sqoop"), arguments = sqoop_args, workdir = getwd())
      if(!(is_process_handle(sqooop_process))){
        sqooop_process <- NULL
      }
      
      job_name <- paste0(Sys.time() %>% format("%Y_%m_%d_time_%H_%M_%S"),"_proc_", sqooop_process$c_handle)
      sqooop_run_detail <- list()
      
      sqooop_run_detail$process <- sqooop_process
      sqooop_run_detail$start_time <- Sys.time()
      sqooop_run_detail$name <- job_name
      
      L <- list(sqooop_run_detail)
      names(L) <- job_name
      
      options(RSQOOP_JOBS = c(options("RSQOOP_JOBS")[[1]], L))
      
      return(invisible(L))
    }, silent = T)
    
    
  } 
  
  read_process_details <- function(){
    
    try({
      
      process_list <- options("RSQOOP_JOBS")[[1]]
      
      # read logs
      process_list <- process_list %>% lapply(function(x){
        x$is_running <- F
        
        # mostly process$c_handle
        # v <- try({invisible(print(x$process))},silent = T)
        v <- try(process_state(x$process),silent = T)
        
        if(!inherits(v, "try-error")){
          logs <- x$logs
          logs <- c(logs, process_read(x$process) %>% unlist())
          x$logs <- logs
          
          x$is_running <- process_state(x$process) == "running"
          x$is_valid <- T
        }else{
          x$is_valid <- F
        }
        
        x
      })
      
      options(RSQOOP_JOBS = process_list)
      
    }, silent = T)
    
  }
  
  get_done_processes <- function(simple = T){
    process_list_done <- NULL
    try({
      read_process_details()
      
      process_list <- options("RSQOOP_JOBS")[[1]]
      
      done <- process_list %>% lapply("[[","is_running") %>% unlist() %>% "!"() %>% which()
      
      process_list_done <- process_list[done]
      
    }, silent = T)
    
    if(simple){
      return(names(process_list_done))
    }else{
      return(process_list_done)
    }
    
  }
  
  archive_processes <- function(){
    
    try({
      
      read_process_details()
      
      process_list <- options("RSQOOP_JOBS")[[1]]
      
      done <- process_list %>% lapply("[[","is_running") %>% unlist() %>% "!"() %>% which()
      
      process_list <- process_list[-done]
      
      options(RSQOOP_JOBS = process_list)
      
    }, silent = T)
    
  }
  
  # putting in handle
  handle$run <- run
  
  handle$read_process_details <- read_process_details
  
  handle$get_done_processes <- get_done_processes
  
  handle$archive_processes <- archive_processes
  
  
  # these functions may be used in Shiny
  
  handle$access <- list()
  
  process_view <- NULL
  selected_process_job_name <- NULL
  
  handle$access$get_process_view <- function(){
    
    try({
      
      process_list <- options("RSQOOP_JOBS")[[1]]
      
      pv <- process_list  %>% lapply("[[","process") %>% ldply(function(x){
        x$arguments <- x$arguments %>% paste0(collapse = " ")
        x[c("c_handle","command","arguments")] %>% as.data.frame()
      })
      
      pv[["job_name"]] <- pv[[".id"]]
      
      pv[[".id"]] <- NULL
      
      pv$pid <- pv$c_handle %>% as.numeric()
      
      pv$c_handle <- NULL
      
      
      if(is.data.frame(pv)){
        process_view <<- pv
      }
      
    }, silent = T)
    
    return(process_view)
    
  }
  
  handle$access$select_process <- function(id_num){
    
    try({
      
      if(!is.null(process_view)){
        if(missing(id_num)){
          id_num <- max(nrow(process_view))
        }
        id_num <- as.integer(id_num)
        if(id_num %in% seq(nrow(process_view))){
          selected_process_job_name <<- process_view$job_name[id_num]
        }
      }
      
    }, silent = T)
  }
  
  get_log <- function(){
    
    logs <- NULL
    
    try({
      
      if(!is.null(selected_process_job_name)){
        read_process_details()
        process_list <- options("RSQOOP_JOBS")[[1]]
        selected_process <- process_list[[selected_process_job_name]]
        logs <- selected_process$logs %>% paste0(collapse = "\n")
      }
      
    }, silent = T)
    
    return(logs)
  }
  
  handle$access$get_log <- get_log
  
  handle$access$log_report <- function(log_txt){
    if(missing(log_txt)){
      log_txt <- get_log()
    }
    log_txt <- log_txt %>% tolower() %>% str_split("\n") %>% .[[1]] %>% str_replace_all("[^a-z 0-9]","") %>% str_replace_all("[ \t]{2,}", " ")
    l <- list()
    l$error_count <- log_txt %>% str_detect("error") %>% sum()
    l$attempt_failed_count <- log_txt %>% str_detect("status failed") %>% sum()
    mmapreduce_job <- log_txt %>% str_detect(" reduce ") %>% log_txt[.] %>% max()
    mmapreduce_job <- str_split(mmapreduce_job, "map ")[[1]][2]
    mmapreduce_job <- str_split(mmapreduce_job," reduce ")[[1]] %>% as.numeric()
    mmapreduce_job <- list( map = mmapreduce_job[1], reduce = mmapreduce_job[2])
    l$mmapreduce_job <- mmapreduce_job
    return(l)
  }
  
  handle$access$get <- function(){
    try({
      read_process_details()
      process_list <- options("RSQOOP_JOBS")[[1]]
      selected_process <- process_list[[selected_process_job_name]]
      return(selected_process)
    }, silent = T)
  }
  
  handle$access$kill <- function(){
    try({
      
      if(!is.null(selected_process_job_name)){
        process_list <- options("RSQOOP_JOBS")[[1]]
        selected_process <- process_list[[selected_process_job_name]]
        process_kill(selected_process$process)
      }
      
    }, silent = T)
    
    
  }
  
  return(handle)
  
}


executor_backend <- function(){
  
  handle <- list()
  sqr <- sqoop_runner_backend()
  
  init <- function(){
    options(RSQOOP_EXE_PLANS = NULL)
  }
  
  determine_state <- function(exe_plan){
    
    plan_objs <- exe_plan %>% lapply("[[","plan") %>% lapply(length) %>% unlist()
    
    l<- list()
    
    l$is_done <- F
    
    l$active_node <- which(plan_objs>0)
    
    
    l$num <- length(plan_objs)
    
    l$num_rest <- length(l$active_node)
    
    l$mode <-"unknown"
    
    if(length(l$active_node)){
      l$active_node <- min(l$active_node)
    }else{
      l$active_node <- 0
      l$is_done <- T
      l$mode <- "done"
    }
    
    if(l$active_node>0){
      
      active_node_plans <- exe_plan[[l$active_node]]$plan %>% names()
      
      if(l$num_rest==1 & !("sqoop" %in% active_node_plans)){
        l$mode <- "phase"
        u <-exe_plan %>% lapply("[[", "is_rest_part_done") %>% unlist()
        if(length(u)){
          if(all(u)){
            l$mode <- "clean_up"
            v <-exe_plan %>% lapply("[[", "clean_up_done") %>% unlist()
            if(length(v)){
              if(all(v)){
                l$is_done <- T
                l$mode <- "done"
              }
            }
          }
        }
        
      }else{
        l$mode <- "run"
      }
      
    }
    
    return(l)
  }
  
  
  run <- function(exe_plan){
    try({
      
      ep_report <- determine_state(exe_plan)
      
      if(ep_report$mode=="run"){
        
        if("sqoop" %in% names(exe_plan[[ep_report$active_node]]$plan)){
          
          cat(paste0("\nSubmitting new Sqoop job ",ep_report$active_node,"th of ",ep_report$num," . Rest number of jobs:",ep_report$num_rest , "\n"))
          
          sq_job <- exe_plan[[ep_report$active_node]]$plan$sqoop()
          
          exe_plan[[ep_report$active_node]]$sqoop_job_details <- sq_job[[1]]
          
          exe_plan[[ep_report$active_node]]$plan$sqoop <- NULL
          
          L<- list(exe_plan)
          names(L) <- sq_job[[1]]$name
          
          L_old <- options("RSQOOP_EXE_PLANS")[[1]]
          
          if(ep_report$active_node>1){
            done_jobs <- exe_plan[seq(1,ep_report$active_node-1)]
            done_jobs <- done_jobs %>% lapply("[[","sqoop_job_details") %>% lapply("[[","name") %>% unlist()
            L_old <- L_old %>% names() %>% setdiff(done_jobs) %>% L_old[.]
          }
          
          L_new <- c(L_old,L)
          
          options(RSQOOP_EXE_PLANS = L_new)
        }
        
      }
      
    }, silent = T)
  }
  
  
  sqoop_done_jobs <- function(return_plans = F){
    all_exe_plans <- options("RSQOOP_EXE_PLANS")[[1]]
    done_sqoops <- all_exe_plans %>% lapply(function(n) {n %>% lapply("[[","sqoop_job_details") %>% lapply(function(nd){
      
      job_done <- F
      ps <- try(process_state(nd$process), silent = T)
      if("try-error" %in% class(ps)){
        ps <- "terminated"
      }
      
      if(ps %in% c("exited","terminated")){
        job_done <- T
      }
      
      if(job_done){
        return(nd$name)
      }else{
        return(NULL)
      }
      
    }) %>% unlist() } ) %>% unlist() 
    job_name <- all_exe_plans %>% lapply(function(n) {n %>% lapply("[[","sqoop_job_details") %>% lapply("[[","name") %>% unlist() } ) %>% unlist() %>% intersect(done_sqoops) %>% intersect(names(all_exe_plans))
    if(return_plans){
      return(all_exe_plans[job_name])
    }else{
      job_name
    }
  }
  
  
  store_eplan <- function(exe_plan){
    
    procs <- sqr$access$get_process_view()
    
    exe_plan <- lapply(exe_plan, function(nn){
      sqr$access$select_process(which(procs$job_name == nn$sqoop_job_details$name))
      
      nn$sqoop_job_details$log <- sqr$access$get_log()
      
      return(nn)
    })
    
    exe_plan[[length(exe_plan)]]$sqoop_job_details$additional_info <- procs
    
    try(saveRDS(exe_plan, file.path("store/imported_eplan", exe_plan[[1]]$sqoop_job_details$name)), silent = T)
    
    return(invisible(0))
    
  }
  
  next_stage_standalone <- function(exe_plan){
    
    ep_info <- determine_state(exe_plan)
    
    if(!ep_info$is_done){
      
      if(ep_info$mode=="run"){
        cat("\nUsing RUN\n")
        run(exe_plan)
      }
      
      if(ep_info$mode=="phase"){
        cat("\nUsing NEXT PHASE\n")
        next_phase_standalone(exe_plan)
      }
      
    }
    
    if(ep_info$mode=="clean_up"){
      cat("\nUsing CLEAN UP\n")
      # clean up store job
      exe_plan[[length(exe_plan)]]$is_complete <- is_complete_standalone(exe_plan)
      
      exe_plan[[length(exe_plan)]]$clean_up_done <- T
      
      all_exe_plans <- options("RSQOOP_EXE_PLANS")[[1]]
      all_exe_plans[[exe_plan %>% lapply("[[","sqoop_job_details") %>% lapply("[[","name") %>% unlist() %>% intersect(names(all_exe_plans))]] <- exe_plan
      options(RSQOOP_EXE_PLANS = all_exe_plans) 
      
      store_eplan(exe_plan)
      
      archive()
      
    }
    
    return(invisible(0))
    
  }
  
  next_phase_standalone <- function(exe_plan){
    
    try({
      
      ep_info <- determine_state(exe_plan)
      
      modular_plan <- exe_plan[[ep_info$active_node]]
      
      rest_parts <- setdiff(names(modular_plan$plan),c("sqoop"))
      if(length(rest_parts)){
        cat("\nDoing rest part of the plan.\n")
        lapply(rest_parts, function(aname){
          modular_plan$plan[[aname]]()
        })
        
        all_exe_plans <- options("RSQOOP_EXE_PLANS")[[1]]
        all_exe_plans[[exe_plan %>% lapply("[[","sqoop_job_details") %>% lapply("[[","name") %>% unlist() %>% intersect(names(all_exe_plans))]][[ep_info$active_node]]$is_rest_part_done <- T
        options(RSQOOP_EXE_PLANS = all_exe_plans) 
      }
    }, silent = T)
    
  }
  
  next_stage <- function(){
    try({
      sdj <- sqoop_done_jobs(return_plans = T)
      if(length(sdj)){
        lapply(sdj, next_stage_standalone)
      }
    }, silent = T)
    return(invisible(0))
  }
  
  
  
  full_done_jobs <- function(){
    all_exe_plans <- options("RSQOOP_EXE_PLANS")[[1]]
    infos <- lapply(all_exe_plans, determine_state)
    infos <- infos %>% lapply("[[","mode") %>% unlist()
    which(infos== "done") %>% names()
  }
  
  
  is_complete_standalone <- function(exe_plan){
    ep_info <- determine_state(exe_plan)
    L<- F
    is_complete_fn <- exe_plan %>% lapply("[[", "is_complete") %>% unlist()
    if(length(is_complete_fn)){
      is_complete_fn <- is_complete_fn[[1]]
      if(is.function(is_complete_fn)){
        if(ep_info$is_done){
          L <- try(is_complete_fn(), silent = T)
          if(!is.logical(L)) L<- F
        }
      }
    }
    
    return(L)
  }
  
  
  archive <- function(){
    all_exe_plans <- options("RSQOOP_EXE_PLANS")[[1]]
    fdj <- full_done_jobs()
    all_exe_plans <- all_exe_plans[ all_exe_plans %>% names() %>% setdiff(fdj) ]
    options(RSQOOP_EXE_PLANS = all_exe_plans)
  }
  
  all_job_done <- function(){
    
    fdj <- full_done_jobs()
    sdj <- sqoop_done_jobs()
    
    all_exe_plans <- options("RSQOOP_EXE_PLANS")[[1]]
    
    rest_jobs <- all_exe_plans %>% names() %>% setdiff(intersect(fdj, sdj)) %>% length()
    
    if(rest_jobs==0){
      # archive sqoop process
      sqr$archive_processes()
    }
    
    return(rest_jobs==0)
    
  }
  
  check_plan_present <- function(exe_plan){
    cmd_this <- laply( exe_plan, 
                       function(nn){ 
                         xn <- nn$command() %>% str_split(" ") %>% .[[1]] 
                         xn %>% str_detect("outdir") %>% which() %>% seq() %>% xn[.] %>% paste0(collapse = " ")
                       }
    )
    all_exe_plans <- options("RSQOOP_EXE_PLANS")[[1]]
    
    all_cmd <- laply( all_exe_plans, 
                      function(nn){ 
                        nn$command() %>% str_split(" ") %>% .[[1]] %>% str_detect("outdir") %>% which() %>% seq() %>% xn[.] %>% paste0(collapse = " ")
                      }
    )
    
    return(cmd_this %in% all_cmd)
  }
  
  handle$run <- run
  
  handle$next_stage <- next_stage
  
  handle$all_job_done <- all_job_done
  
  handle$init <- init
  
  handle$archive <- archive
  
  handle$all_plans <- function(){
    all_exe_plans <- options("RSQOOP_EXE_PLANS")[[1]]
    return(all_exe_plans)
  }
  
  handle$check_plan_present <- check_plan_present
  
  
  return(handle)
}
# example table
# table_name <- 'DIM_ACCOUNT_TYPE_DATA'
# db_name <- 'EDWOWNER'
# query_select_tag is kept for things like 'SELECT /*+ PARALLEL */'

sqoop_execution_plan <- function(db_name, table_name, where, ...,
                                 backend, query_select_tag){
  
  # load style codes
  source("lib/sqoop-style-codes/codes.R", local = T)
  
  personal_sq_list_args <- NULL
  
  sq_list_args <- list(...)
  
  available_names <- names(backend$sqoop_builder)
  
  sq_list_args <- sq_list_args[ intersect(names(sq_list_args), available_names)]
  
  implement_custom_sqoop_args <- function(){
    if(length(sq_list_args)){
      for(nm in names(sq_list_args)){
        # try without argument
        try( backend$sqoop_builder[[nm]](), silent = T)
        # and with argument
        try( backend$sqoop_builder[[nm]](sq_list_args[[nm]]), silent = T)
      }
    }
  }
  
  set_custom_sqoop_args_live <- function(...){
    personal_sq_list_args <<- list(...)
    return(invisible(0))
  }
  
  implement_custom_sqoop_args_live <- function(){
    
    
    personal_sq_list_args <- personal_sq_list_args[ intersect(names(personal_sq_list_args), available_names)]
    
    if(length(personal_sq_list_args)){
      for(nm in names(personal_sq_list_args)){
        # try without argument
        try( backend$sqoop_builder[[nm]](), silent = T)
        # and with argument
        try( backend$sqoop_builder[[nm]](personal_sq_list_args[[nm]]), silent = T)
      }
    }
  }
  
  
  if(missing(where)){
    where <- ""
  }
  
  sql_limiter_tag <- backend$src_db$param$sql_limiter_tag %>% tolower()
  
  if(nchar(where) >0){
    
    if( sql_limiter_tag %>% str_detect("where")){
      
      sql_limiter_tag <- sql_limiter_tag %>% str_replace("where", "") %>% toupper() %>% paste0("WHERE ", where, " AND ", .)
      
    }else{
      sql_limiter_tag <- paste0("WHERE ", where, " ", sql_limiter_tag)
    }
    
  }
  
  if(missing(query_select_tag)){
    query_select_tag <- "SELECT "
  }else{
    if(query_select_tag %>% tolower() %>% str_detect("select") ){
      query_select_tag <- paste0(query_select_tag," ")
    }else{
      query_select_tag <- "SELECT "
    }
  }
  
  query <- paste0(query_select_tag, "* FROM ", db_name, ".", table_name, " ", 
                  ifelse(nchar(where) >0, paste0("WHERE ", where), ""))
  query_test <- paste0(query_select_tag, "* FROM ", db_name, ".", table_name, " ", sql_limiter_tag)
  
  
  hdfs_dest_folder <- file.path(backend$RSqoop$hdfs_dest_dir, db_name, table_name)
  
  # execution plan will be emitted
  execution_plan<- list()
  
  is_test_sqoop_conn <- F
  
  if(missing(db_name) & missing(table_name)){
    is_test_sqoop_conn <- T
  }
  
  if(is_test_sqoop_conn){
    
    sqoop_arg_lists <- function(){
      sqoop.test()
      
      backend$sqoop_builder$build()
    }
    
    execution_plan <- list(
      plan=list(sqoop = function(){
        backend$sqoop$run(sqoop_arg_lists())
      }),
      is_complete = function(){
        backend$hdfs$readlines(backend$RSqoop$test_dir) == backend$src_db$param$test_query_expect
      })
    
  }else{
    if(!missing(table_name)){
      if(!missing(db_name)){
        # case of external db or schema 
        
        
        sqoop_arg_lists <- function(){
          sqoop.main_db_no_split()
          implement_custom_sqoop_args()
          
          implement_custom_sqoop_args_live()
          
          backend$sqoop_builder$build()
        }
        
        
        
        
        execution_plan <- list(
          command = function(print = T){
            
            sqoop_arg_lists()
            
            if(print){
              backend$sqoop_builder$build_cmd("\n") %>% cat()
            }else{
              backend$sqoop_builder$build_cmd()
            }
          },
          plan = 
            list(sqoop = function(){
              backend$sqoop$run(sqoop_arg_lists())
            },
            hadoop_db = function(){
              
              file_name <- backend$hdfs$list.files(hdfs_dest_folder)$file
              file_name <- file_name %>% tolower() %>% str_detect("parquet") %>% file_name[.] %>% .[1]
              backend$hadoop_db$Impala$query(paste0("USE ", backend$RSqoop$hadoop_dest_db_name), is_DDL = T)
              backend$hadoop_db$Impala$query(paste0("DROP TABLE IF EXISTS ",table_name), is_DDL = T)
              backend$hadoop_db$Impala$query(paste0("CREATE EXTERNAL TABLE ",table_name,"\nLIKE PARQUET '",
                                                    file_name,"'\nSTORED AS PARQUET\nLOCATION '",
                                                    hdfs_dest_folder,"'"), is_DDL = T)
              
            }),
          is_complete = function(){
            d <- backend$hadoop_db$Impala$query(paste0("SELECT * FROM ",
                                                       backend$RSqoop$hadoop_dest_db_name,".", table_name, " LIMIT 20"))
            is.data.frame(d)
          }, 
          set_custom_sqoop_args_live = set_custom_sqoop_args_live)
        
      }else{
        # case of no database mention has to be developed here
      }
    }
    
  }
  
  return(execution_plan)
  
}
