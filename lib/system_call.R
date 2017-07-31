
# another way is to use subprocess package
# ref : https://cran.r-project.org/web/packages/subprocess/vignettes/intro.html


# still needs evaluation
# linux type of OS only 
# 2>&1 for error redirection
# sudo may not work in red hat ref : https://www.shell-tips.com/2014/09/08/sudo-sorry-you-must-have-a-tty-to-run-sudo/
# one can do Defaults:<user from where this code will run> !requiretty 

sys_call <-  function(cmd, run_as, connector = " && ", log_file, error_redirection = T,
                      external_run = F, sh_file, run_sh_file = F){
  
  s_out <- NULL
  
  try({
    
    if(!missing(run_as)){
      # this may not work in Red Hat : ref : https://unix.stackexchange.com/questions/122616/why-do-i-need-a-tty-to-run-sudo-if-i-can-sudo-without-a-password
      # keyless sudo has to be configured
      cmd <- paste0("sudo -H -u ", run_as, " ", cmd)
    }
    
    if(!missing(sh_file)){
      cmd <- paste0(cmd, collapse = "\n")
    }else{
      cmd <- paste0(cmd, collapse = connector)
    }
    
    
    if(!missing(log_file)){
      cmd <- paste0(cmd, " > ", log_file)
    }
    
    if(error_redirection){
      cmd <- paste0(cmd, " 2>&1")
    }
    
    if(!missing(sh_file)){
      writeLines(cmd, sh_file)
      cmd <- paste0("chmod 755 ", sh_file)
      if(run_sh_file){
        cmd <- c( cmd, paste0("sh ", sh_file))
      }
      cmd <- paste0(cmd, collapse = connector)
    }
    
    if(external_run){
      s_out <- suppressMessages(suppressWarnings(system(cmd, intern = F, wait = F)))
    }else{
      s_out <- suppressMessages(suppressWarnings(system(cmd, intern = T))) 
    }
    
  }, silent = T)
  
  
  return(s_out)
}




