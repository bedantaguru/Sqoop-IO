

# read a SQL code and check for the presence of @: in these scripts.
# these are parameters of SQL
require(stringr)


sql_parameterized_query_reader <- function(file_name){
  sql <- suppressWarnings(readLines(file_name))
  args_str <- sql %>% str_extract_all("@:[a-zA-Z_0-9]+") %>% unlist() %>% unique()
  args_list <- args_str %>% str_replace("@:","")
  
  # store current SQL
  
  SQL_NOW <- ""
  
  get_sql_now <- function(){
    SQL_NOW
  }
  
  # function to build SQL with parameters
  build_sql <- function(...){
    args_passed <- list(...)
    args_passed <- args_passed[args_list]
    
    sql_new <- sql
    
    for(arg in args_list){
      arg_to_place <- args_passed[[arg]]
      if(length(arg_to_place)>1){
        if(is.character(arg_to_place[1])){
          arg_to_place <- paste0(arg_to_place,"','")
        }else{
          arg_to_place <- paste0(arg_to_place,",")
        }
      }
      sql_new <- sql_new %>% str_replace_all(paste0("@:",arg),arg_to_place) %>% paste(collapse = "\n")
    }
    
    SQL_NOW <<- sql_new
    
    return(sql_new)
  }
  
  handle <- list(sql = get_sql_now,
                 parameter_list = args_list,
                 build_sql =  build_sql ,
                 parameterized_sql = sql, 
                 output_summary_sql = function(){get_custom_sql_for_output(get_sql_now())})
  return(handle)
}


# get the count
# ref : https://sqlparse.readthedocs.io/en/latest/
# ref : https://github.com/andialbrecht/sqlparse
# ref : http://zql.sourceforge.net/
get_custom_sql_for_output <- function(sql){
  
  sql_flat <- sql %>% paste0(collapse = "\n")%>% str_trim()
  
  # parse the SQL for different keywords
  # basic code analysis
  keywords <- c("with","select", "from", "where", "join","\\)", "\\(")
  sql_parse_seq <- sql_flat %>% tolower() %>% str_extract_all( keywords %>% paste(collapse = "|")) %>% .[[1]] %>% str_trim()
  
  if(!(any(sql_parse_seq == "select"))){
    return(function(type = "",columns = "", additional_sql = ""){""})
  }
  
  # sub query detection
  is_sub_query <- function(sloc){
    if(sloc==1){
      return(F)
    }
    sql_parse_seq_part <- sql_parse_seq[seq(sloc -1)] %>% rev()
    if(length(sql_parse_seq_part) == 1){
      if(sql_parse_seq_part == "("){
        return(F)
      }else{
        warning("Unknown SQL : Generated code may not be correct")
      }
    }else{
      if(length(sql_parse_seq_part)>1){
        sql_parse_seq_part <- sql_parse_seq_part[seq(2)]
        if(identical(sql_parse_seq_part, c("(","where")) | identical(sql_parse_seq_part, c("(","join"))){
          return(T)
        }
      }
    }
    
    return(F)
  }
  
  select_classification <- data.frame( select_locs =  which(sql_parse_seq == "select"), is_sub_query =  which(sql_parse_seq == "select") %>% lapply(is_sub_query) %>% unlist())
  
  if("with" %in% sql_parse_seq){
    # SQL which has WITH clause
    first_select_after_with_clause <- select_classification$select_locs %>% setdiff(1)
    first_select_after_with_clause <- first_select_after_with_clause[(sql_parse_seq[first_select_after_with_clause-1]==")") %>% which()]
    select_classification$is_first_select<- F
    select_classification$is_first_select[select_classification$select_locs == first_select_after_with_clause] <- T
  }else{
    # by default only first one is the main select clause
    select_classification$is_first_select <- F
    select_classification$is_first_select[1] <- T
  }
  
  # get actual position of each select 
  select_classification <- select_classification[ order(select_classification$select_locs),]
  
  select_classification$location <- sql_flat %>% tolower() %>% str_locate_all("select") %>% .[[1]] %>% as.data.frame() %>% .[["start"]]
  
  final_select_location <- select_classification$location[ !select_classification$is_sub_query & select_classification$is_first_select]
  
  if(final_select_location<=1){
    upper_chunk <-""
    select_chunk <- sql_flat
  }else{
    upper_chunk <- substr(sql_flat, 1, final_select_location-1)
    select_chunk <- substr(sql_flat, final_select_location, nchar(sql_flat))
  }
  
  if("with" %in% sql_parse_seq){
    sql_part <- c(upper_chunk,",INTERIM_FRM_R_GET_RSLT_SUMMARY_TMP_TBL AS (",select_chunk,")")
    
  }else{
    sql_part <- c("WITH INTERIM_FRM_R_GET_RSLT_SUMMARY_TMP_TBL AS (",select_chunk,")")
    if(nchar(upper_chunk)>0){
      warning("Unknown SQL : Generated code may not be correct")
    }
    
  }
  
  
  
  rebuild_sql<- function(type = c("custom","count","limit","oracle_limit"),columns = "COUNT(1)", additional_sql = ""){
    type <- match.arg(type)
    final_output_part <- switch(type,
                                custom = paste0("SELECT ",columns," FROM INTERIM_FRM_R_GET_RSLT_SUMMARY_TMP_TBL ", additional_sql),
                                count = "SELECT COUNT(1) FROM INTERIM_FRM_R_GET_RSLT_CNT_TMP_TBL",
                                limit = "SELECT * FROM INTERIM_FRM_R_GET_RSLT_SUMMARY_TMP_TBL LIMIT 10",
                                oracle_limit = "SELECT * FROM INTERIM_FRM_R_GET_RSLT_SUMMARY_TMP_TBL WHERE ROWNUM <= 10 ")
    sql_part <- c( sql_part, final_output_part)
    
    sql_part <- sql_part %>% paste0(collapse = "\n")
    
    return(sql_part)
    
  }
  
  return(rebuild_sql)
}

