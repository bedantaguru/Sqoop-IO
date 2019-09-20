
require(magrittr)
require(stringr)

# for hive
# "resource/hive.keytab" %>% normalizePath() %>% paste0("kinit -k -t ",.," hive@MYREALM.COM") %>% system()
renew_kerberos_ticket <- function(username, password, long_time = T, ignore_key = F){
  if(username == "hive"){
    "resource/hive.keytab" %>% normalizePath() %>% paste0("kinit -k -t ",.," hive@MYREALM.COM") %>% system()
  }
  is_key_present <- suppressWarnings(system("klist",ignore.stderr = T, intern = T) %>% str_detect("Ticket cache") %>% any())
  if(!is_key_present | ignore_key){
    
    if(long_time){
      try(system(paste0('echo "',password,'" | kinit ',username,' -l 215h -r 215h')), silent = T)
    }else{
      try(system(paste0('echo "',password,'" | kinit ',username,'')), silent = T)
    }
    
  }
  
}
