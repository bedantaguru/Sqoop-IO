
require(subprocess)

external_R <- function(task = c("start","stop","restart","check","read","run","is_ready"), cmd){
  
  task <- match.arg(task)
  
  if(is.null(options("EXTERNAL_R_FILES")[[1]]) | task == "restart"){
    # temp file is best solution but not working as expected : 
    # options(EXTERNAL_R_FILES = list(infile = tempfile(fileext = ".RData"), outfile = tempfile(fileext = ".RData")))
    dir.create(".external_R", showWarnings = F)
    options(EXTERNAL_R_FILES = list(infile = ".external_R/infile.RData", outfile = ".external_R/outfile.RData"))
  }
  
  
  if(task == "start"){
    rp <- options("EXTERNAL_R")[[1]]
    if(!is_process_handle(rp)){
      rp <- spawn_process(Sys.which("R"), arguments = c("--vanilla"))
      options(EXTERNAL_R = rp)
    }else{
      if(process_state(rp)!="running"){
        rp <- spawn_process(Sys.which("R"), arguments = c("--vanilla"))
        options(EXTERNAL_R = rp)
      }
    }
  }
  
  if(task == "stop"){
    rp <- options("EXTERNAL_R")[[1]]
    if(is_process_handle(rp)){
      process_kill(rp)
      options(EXTERNAL_R = NULL)
    }
    unlink(".external_R", recursive = T, force = T)
  }
  
  if(task == "restart"){
    rp <- options("EXTERNAL_R")[[1]]
    if(is_process_handle(rp)){
      try(process_kill(rp), silent = T)
    }
    dir.create(".external_R", showWarnings = F)
    rp <- spawn_process(Sys.which("R"), arguments = c("--vanilla"))
    options(EXTERNAL_R = rp)
    
  }
  
  
  if(task == "check"){
    rp <- options("EXTERNAL_R")[[1]]
    if(is_process_handle(rp)){
      return(process_state(rp))
    }else{
      return("invalid")
    }
  }
  
  if(task == "read"){
    rp <- options("EXTERNAL_R")[[1]]
    if(is_process_handle(rp)){
      return(process_read(rp))
    }else{
      return("invalid")
    }
  }
  
  if(task == "run"){
    rp <- options("EXTERNAL_R")[[1]]
    if(is_process_handle(rp)){
      return(process_write(rp, paste0(cmd,"\n")))
    }else{
      return("invalid")
    }
  }
  
  
  if(task == "is_ready"){
    ir <- F
    rp <- options("EXTERNAL_R")[[1]]
    if(is_process_handle(rp)){
      if(process_state(rp)=="running"){
        process_read(rp)
        process_write(rp, paste0("paste0('EXTERNAL_R','_READY_NOW')","\n"))
        Sys.sleep(0.2)
        ir <- process_read(rp) %>% unlist() %>% str_detect("EXTERNAL_R_READY_NOW") %>% any()
      }
    }
    return(ir)
  }
  
  
  return(invisible(0))
}

wait_for_external_R <- function(secs = 5*60){
  external_R("start")
  time_now <- Sys.time()
  while(!external_R("is_ready") & as.numeric(difftime(Sys.time(), time_now, units = "sec")) < secs){
    Sys.sleep(0.2)
  }
  
  if(!external_R("is_ready")){
    stop("External R is not ready")
  }
}

put_in_external_R <- function(List, env = parent.frame()){
  infile <- options('EXTERNAL_R_FILES')[[1]]$infile
  unlink(infile, recursive = T, force = T)
  if(!missing(List)){
    try({
      save(list = List, file = infile, envir = env)
      if(file.exists(infile)){
        rp <- options("EXTERNAL_R")[[1]]
        process_write(rp, paste0("load('",infile,"')\n"))
        wait_for_external_R()
      }
    }, silent = T)
  }
  return(invisible(0))
}

run_in_external_R <- function(code, put, env = parent.frame()){
  
  handle <- list()
  outfile <- options('EXTERNAL_R_FILES')[[1]]$outfile
  unlink(outfile, recursive = T, force = T)
  start_time <- Sys.time()
  if(file.exists(outfile)){
    stop("Unable to delete old output. Check manually.")
  }

  rp <- options("EXTERNAL_R")[[1]]

  if(!is_process_handle(rp)){
    external_R(task = "restart")
    wait_for_external_R()
    rp <- options("EXTERNAL_R")[[1]]
  }

  if(process_state(rp)!="running"){
    external_R(task = "restart")
    wait_for_external_R()
    rp <- options("EXTERNAL_R")[[1]]
  }

  if(process_state(rp)=="running"){

    if(!missing(put)){
      put_in_external_R(List = put, env)
    }
    process_write(rp, "try(rm(._f_in_EXTERNAL_R,._out_in_EXTERNAL_R))\n")
    ._f_in_EXTERNAL_R <- eval(parse(text = paste0("function(){\n",code, "\n}")))
    put_in_external_R(List = "._f_in_EXTERNAL_R", env = environment())
    cmd <- paste0("._out_in_EXTERNAL_R <- ._f_in_EXTERNAL_R()",
                  "\n",
                  "save(._out_in_EXTERNAL_R,file='",outfile,"')\n")
    
    process_write(rp,cmd)
    start_time <- Sys.time()
  }

  get_output <- function(){
    out <- NULL
    if(file.exists(outfile)){
      try({
        load(outfile)
        out <- ._out_in_EXTERNAL_R
      }, silent = T)
    }
    return(out)
  }

  handle$get_output <- get_output

  state <- function(){
    s_now <- "stopped"
    if(file.exists(outfile)){
      s_now <- "done"
    }else{
      s_now <- process_state(rp)
    }
    return(s_now)
  }

  handle$state <- state

  handle$wait_for_output <- function(wait_secs= 5*60, time_out_event = "stop"){
    while(difftime(Sys.time(),start_time, units = "sec")  < wait_secs & state() == "running"){
      Sys.sleep(1)
    }
    msg <-NULL
    
    if(difftime(Sys.time(),start_time, units = "sec")  > wait_secs){
      msg <- c(msg, "time_out")
    }
    
    if(state() != "running"){
      msg <- c(msg, "Stopped")
    }
    
    if(state()=="done"){
      out <- get_output()
    }else{
      external_R(time_out_event)
      #may be not the correct way : out <- try(stop("Did not ran"), silent = T)
      out <- stop(paste0("Did not ran:", paste0(msg, collapse = "+")))
    }

    return(out)

  }

  return(handle)
  
}


