

renew_kerberos_ticket <- function(username, password){
  try(system(paste0('echo "',password,'" | kinit ',username,'')), silent = T)
}
