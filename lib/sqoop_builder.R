
require(stringr)

sqoop_builder <- function(sqoop_module = "import", args_info_data){
  
  sqoop_str <- "sqoop "
  
  builder <- list()
  
  sqoop_code <- list(base_cmd = paste0(sqoop_str, sqoop_module))
  
  sqoop_arg_list <- list(base_module = sqoop_module)
  
  builder$reset <-function(){
    sqoop_code <<- list(base_cmd = paste0(sqoop_str, sqoop_module))
    sqoop_arg_list <<- list(base_module = sqoop_module)
  }
  
  builder$add_generic_cmd_arg <-function(cmd){
    sqoop_code[['add_generic_cmd_arg']] <<- cmd %>% paste0(collapse = " ")
    sqoop_arg_list[['add_generic_cmd_arg']] <<- cmd
  }
  
  builder$revoke <-function(what){
    what <- str_replace_all(what,"[^a-zA-Z0-9]","_")
    sqoop_code[[what]] <<- NULL
    sqoop_arg_list[[what]] <<- NULL
  }
  
  builder$build_cmd <-function(collapse_char = " "){
    paste0(sqoop_code %>% unlist() %>% unique(), collapse = collapse_char)
  }
  
  builder$build <-function(){
    sqoop_arg_list %>% unlist() %>% str_trim()
  }
  
  
  if(!missing(args_info_data)){
    
    try({
      
      # create tags for easier manipulation
      
      args_info_data$r_fn_args <- args_info_data$args %>% str_replace_all("-","_") %>% str_replace_all("[^a-zA-Z0-9\\_]","") %>% str_trim()
      
      args_info_data$r_fn_name <- args_info_data$implement_tags %>% str_replace_all("-","_") %>% str_replace_all("[^a-zA-Z0-9\\_]","") %>% str_trim()
      
      args_info_data$implement_tags <- args_info_data$implement_tags %>% str_trim()
      
      args_info_data$r_fn_code <- paste0("builder$",args_info_data$r_fn_name, 
                                         " <- ", "function(",ifelse(is.na(args_info_data$r_fn_args),"",args_info_data$r_fn_args),"){",
                                         ifelse(is.na(args_info_data$r_fn_args),"",paste0("if(is.null(",args_info_data$r_fn_args,")) return(invisible(-1))")), 
                                         "\n",
                                         "sqoop_code[['",args_info_data$r_fn_name,"']] <<- paste0('--",args_info_data$implement_tags,
                                         ifelse(is.na(args_info_data$r_fn_args),"'", 
                                                paste0(" ',",ifelse(args_info_data$r_fn_args %in% c("statement","char"),
                                                                    paste0("shQuote(",args_info_data$r_fn_args,")"),args_info_data$r_fn_args)) ),
                                         ")","\n",
                                         "sqoop_arg_list[['",args_info_data$r_fn_name,"']] <<- c('--",args_info_data$implement_tags,"'",
                                         ifelse(is.na(args_info_data$r_fn_args),"", 
                                                paste0(",",ifelse(args_info_data$r_fn_args %in% c("statement","char"),
                                                                  paste0("shQuote(",args_info_data$r_fn_args,")"),args_info_data$r_fn_args)) ),
                                         ")}")
      
      # special care for query argument 
      
      if("query" %in% args_info_data$r_fn_name){
        idx <- args_info_data$r_fn_name == "query"
        args_info_data$r_fn_code[idx]<-"builder$query <- function(statement){
          if(statement %>% tolower() %>% str_detect('where')){
            statement <- paste0(statement,' AND $CONDITIONS')
          }else{
            statement <- paste0(statement,' WHERE $CONDITIONS')
          }
          statement <- paste0('\"',statement,'\"')
          sqoop_code[['query']] <<- paste0('--query ',statement)
          sqoop_arg_list[['query']] <<- c('--query ',statement)
        }"
      }
      
      if("boundary_query" %in% args_info_data$r_fn_name){
        idx <- args_info_data$r_fn_name == "boundary_query"
        args_info_data$r_fn_code[idx]<-"builder$boundary_query <- function(statement){
        statement <- paste0('\"',statement,'\"')
        sqoop_code[['boundary_query']] <<- paste0('--boundary-query ',statement)
        sqoop_arg_list[['boundary_query']] <<- c('--boundary-query ',statement)
      }"
      }
      
      for(i in seq(nrow(args_info_data))){
        try(eval(parse(text = args_info_data$r_fn_code[i])), silent = T)
      }
      
    },silent = T)
    
  }
  
  return(builder)
  
}





