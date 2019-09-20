
# add it like 
# cmd <- cron_rscript("schedule.R", 
#                     rscript_log = file.path(normalizePath("schedule"),"logs"), 
#                     rscript_args = "style_codes/<code>")
# 
# cron_add(cmd, frequency = 'minutely', id = 'taskid', description = 'Sqoop-IO : Task Name', tags = c('Task Tag'))
# check list of tasks in schedule/tasks.R


suppressPackageStartupMessages(require(stringr))
cat("\n\n")
cat("\n############################################################################################\n")
cat("\n################################## New Task Start ##########################################\n")
cat("\n############################################################################################\n")
cat("\n\n")
Sys.time() %>% as.character() %>% cat("\n Start time :", .,"\n")

args <- commandArgs(trailingOnly=T)

if(length(args)==0){
  stop("The code to run has to be specified")
}

args_full <- commandArgs(trailingOnly=F)

#dir.create("schedule", showWarnings = F)

args_full %>% str_detect("--file=") %>% args_full[.] %>% str_replace_all("--file=","") %>% dirname() -> wd

setwd(wd)

cat("\n Working dir set to :", wd ,"\n")

if(file.exists(args[1])){
  fn <- args[1]
  
  cat("\n Running now :", fn ,"\n")
  source(fn)
  
}else{
  stop(paste0("File not found ",args[1]))
}


Sys.time() %>% as.character() %>% cat("\n End time :", .,"\n")
