
require(dplyr)

# source data visualisation requires following points
# table_catalogue : source table view : where users will have options to select specific table from specific schema or database [simply select a row, no restriction on columns]
# table_description : table description view [unrestricted table view]
# colname_suggestion : column name suggestion for splitting / controlled import [based on table stat if available] (for a selected table)
# sample_data_view : sample data query /condition / view
# query_result : result of a query by user

# jdbc_cm : jdbc connection manager
# example jdbc_cm <- jdbc_connection_manager(get_param("rdb_backend")[[1]])





source_db_backend <- function(jdbc_cm){
  
  source("lib/query_generic.R", local = T)
  
  src_db_handle <-list()
  
  jdbc_cm$connect()
  # validate the connection
  if(!jdbc_cm$is_valid() | !jdbc_cm$test_query()){
    return(src_db_handle)
  }
  
  # this functon env variable
  all_tables <- NULL
  all_tables_details <- NULL
  
  selected_table <- NULL
  selected_table_row_num <- NULL
  selected_table_description <- NULL
  selected_table_analysis <- NULL
  selected_table_import_plan <- NULL
  
  dba_table_stat_access_info <- list()
  
  ###############################################################################################
  ###############################################################################################
  
  # useful function for numeric conversion
  asNum <- function(x, blow = 0, positive = F){
    xn <- suppressWarnings(as.numeric(x))
    if(all(is.na(x) == is.na(xn))){
      x<- xn
      
      if(blow!=0){
        dig <- (log(x+0.0000001)/log(10)) %>% floor() %>% max()
        xl <- x/ 10^dig
        xl <- xl+blow
        x <- floor(xl)*10^dig
        if(positive){
          x <- pmax(x,0)
        }
      }
      
    }
    
    return(x)
  }
  
  # function for Oracle date format
  as.Oracle.Date <- function(date){
    jdbc_cm$query(paste0("select to_char(to_date('",date,"','YYYY-mm-dd')) from dual"))[[1]]
  }
  
  
  ###############################################################################################
  ###############################################################################################
  ###############################################################################################
  
  safe_dba_table_for_views <- function(table_name, odf = data.frame()){
    out <- odf
    if(!is.data.frame(out)){
      out <- data.frame()
    }
    if(!nrow(out)){
      
      if(selected_table$type=="View"){
        table_name <- toupper(table_name)
        loc_tables <- dba_table_stat_access_info$local
        names(loc_tables) <- toupper(names(loc_tables))
        if(table_name %in% names(loc_tables)){
          out <- loc_tables[[table_name]]
          
          # generic name change function
          repl <- function(rep_map, get_cname = F){
            if(get_cname){
              
              out %>% lapply(function(x){
                x<- toupper(x)
                cd <- mean(x %in% toupper(rep_map$srch_trm))
                if(cd>0.5){
                  T
                }else{
                  F
                }
              }) %>% unlist() %>% which() %>% names()
              
            }else{
              
              out %>% lapply(function(x){
                x<- toupper(x)
                cd <- mean(x %in% toupper(rep_map$srch_trm))
                if(cd>0.5){
                  tibble(x) %>% left_join(rep_map, by = c("x"="srch_trm")) %>% select(repl_trm) %>% .[[1]]
                }else{
                  x
                }
              }) %>% as_tibble()
              
            }
            
          }
          
          # change DB name
          out <- repl(tibble(srch_trm = dba_table_stat_access_info$main_db,
                             repl_trm = dba_table_stat_access_info$st$db))
          
          # change table name
          out <- repl(tibble(srch_trm = dba_table_stat_access_info$main_table,
                             repl_trm = dba_table_stat_access_info$st$table_name))
          
          # get columns name holding column
          cn <- repl(dba_table_stat_access_info$col_map %>% rename(srch_trm = orig_col, repl_trm = view_col), get_cname = T)
          
          # change column name
          out <- repl(dba_table_stat_access_info$col_map %>% rename(srch_trm = orig_col, repl_trm = view_col))
          
          if(length(cn)){
            out <- out[!is.na(out[[cn]]),]
          }
          
          out <- out %>% lapply(type.convert, as.is = T) %>% as.data.frame()
          
        }
      }
      
    }
    out
  }
  
  set_dba_table_stat_access_info <- function(..., reset = F, list_pass){
    # mention following things (otherwise will auto detect)
    # main_table
    # main_db
    # access_possible
    # local_path
    if(reset){
      dba_table_stat_access_info <<- list()
    }else{
      
      if(!missing(list_pass)){
        il <- list_pass
      }else{
        il <- list(...)
      }
      
      for(nn in names(il)){
        dba_table_stat_access_info[[nn]] <<- il[[nn]]
      }
    }
    
    dba_table_stat_access_info$st <<- selected_table
  }
  
  get_dba_table_stat_access_info <- function(){
    if(selected_table$type=="View"){
      inf1 <- jdbc_cm$query(paste0("SELECT TEXT from ALL_VIEWS where VIEW_NAME = '",selected_table$table_name,"'"))[[1]]
      
      orig_col_order <- inf1 %>% toupper() %>% str_split(" FROM") %>% unlist() %>% .[[1]] %>% str_replace("SELECT ","") %>% str_split(",") %>% unlist() %>% str_trim()
      dsmp <- jdbc_cm$query(paste0("SELECT * FROM ", selected_table$db, ".",selected_table$table_name, " WHERE ROWNUM <= 5"))
      
      
      col_map <- try(tibble(orig_col = orig_col_order, view_col = dsmp %>% colnames()), silent = T)
      if(!is.data.frame(col_map)){
        warning("Failed to map column")
        col_map <- data.frame()
      }
      
      
      orig_table <- inf1 %>% toupper() %>% str_split(" FROM ") %>% unlist() %>% .[[2]] %>% str_split(" ") %>% unlist %>% .[[1]]
      orig_table <- orig_table %>% str_split("\\.") %>% unlist()
      
      dba_table_stat_access_info_local <- list()
      if(length(orig_table)>2|length(orig_table)<=0){
        warning("Unknown case")
      }else{
        if(length(orig_table)==1){
          dba_table_stat_access_info_local <- list(main_table = orig_table[1],
                                                   main_db = selected_table$db)
        }
        if(length(orig_table)==2){
          dba_table_stat_access_info_local <- list(main_table = orig_table[2],
                                                   main_db = orig_table[1])
        }
      }
      
      inf2 <- jdbc_cm$query(paste0("SELECT * FROM ALL_TAB_COLUMNS WHERE OWNER = '",dba_table_stat_access_info_local$main_db,
                                   "' AND TABLE_NAME = '",dba_table_stat_access_info_local$main_table,"'"))
      chk <- F
      if(is.data.frame(inf2)){
        if(nrow(inf2)){
          chk <- T
        }
      }
      
      dba_table_stat_access_info_local$access_possible <- chk
      
      dba_table_stat_access_info_local$col_map <- col_map
      
      # check local path for files
      
      if(!is.null(dba_table_stat_access_info$local_path)){
        if(file.exists(dba_table_stat_access_info$local_path)){
          dba_table_stat_access_info_local$local <- list()
          fns <- list.files(dba_table_stat_access_info$local_path, full.names = T)
          dba_table_stat_access_info_local$local <- lapply(fns, read.csv, stringsAsFactors = F)
          names(dba_table_stat_access_info_local$local) <- basename(fns) %>% str_replace_all(".csv","")
        }
      }
      
      set_dba_table_stat_access_info(list_pass = dba_table_stat_access_info_local)
      
    }
  }
  
  select_table_id <- function(id_or_name){
    match_id <- NULL
    id_or_name <- id_or_name[1]
    if(is.character(id_or_name)){
      id_or_name <- id_or_name %>% tolower()
      
      # try exact match in table
      exact_match <- (all_tables$table_name %>% tolower()) == id_or_name
      if(sum(exact_match)==1){
        match_id <- which(exact_match)
        return(match_id)
      }
      
      # try exact match in db/owner/schema
      exact_match <- (all_tables$db %>% tolower()) == id_or_name
      if(sum(exact_match)>=1){
        match_id <- which(exact_match)
        return(match_id)
      }
      
      # try approximate match in tables
      partial_match <- (all_tables$table_name %>% tolower()) %>% str_detect(id_or_name)
      if(sum(partial_match)==1){
        match_id <- which(partial_match)
        return(match_id)
      }
      
      # try approximate match in db
      partial_match <- (all_tables$db %>% tolower()) %>% str_detect(id_or_name)
      if(sum(partial_match)>=1){
        match_id <- which(partial_match)
        return(match_id)
      }
      
    }
    if(is.numeric(id_or_name)){
      id_or_name <- id_or_name %>% as.integer()
      if(id_or_name >=1 & id_or_name <= nrow(all_tables)){
        match_id <- id_or_name
        return(match_id)
      }
    }
    
    return(match_id)
  }
  
  select_table <- function(id_or_name){
    
    id <- select_table_id(id_or_name)
    
    if(length(id)>1){
      all_tables <<- all_tables[id,]
    }
    
    if(length(id)==1){
      
      selected_table <<- all_tables[id,] %>% as.list()
      
      selected_table_row_num <- NULL
      selected_table_description <- NULL
      selected_table_analysis <- NULL
      selected_table_import_plan <- NULL
      
      if(!identical(dba_table_stat_access_info$st, selected_table)){
        dba_table_stat_access_info <- set_dba_table_stat_access_info(reset = T)
      }
      
      if(selected_table$type=="View"){
        get_dba_table_stat_access_info()
      }
      
    }
    
  }
  
  get_selected_table_description <- function(){
    
    if(is.null(selected_table_description)){
      
      d <- NULL
      
      try({
        if(!is.null(selected_table$table_name)){
          
          d <- jdbc_cm$query(paste0("SELECT COLUMN_NAME,DATA_TYPE,DATA_LENGTH, DATA_PRECISION, DATA_SCALE, NULLABLE,NUM_DISTINCT,NUM_NULLS
                                    FROM ALL_TAB_COLUMNS 
                                    WHERE OWNER = '", selected_table$db ,"'AND TABLE_NAME='",selected_table$table_name,"'"))
          
          d$IS_NULL_EVER <- (d$NUM_NULLS > 0)
          
          if(d %>% filter(!IS_NULL_EVER) %>% nrow() %>% "!"()){
            d <- safe_dba_table_for_views(table_name = "ALL_TAB_COLUMNS",odf = d %>% filter(!IS_NULL_EVER))
          }
          
          d$IS_NULL_EVER <- (d$NUM_NULLS > 0)
          
          d$DATA_TYPE_FULL <- d$DATA_TYPE
          
          d$DATA_TYPE <- d$DATA_TYPE %>% str_split("[^a-zA-Z0-9]") %>% lapply("[[", 1) %>% unlist()
          
        }
      }, silent = T)
      
      selected_table_description <<- d
    }
    
    
    return(selected_table_description)
  }
  
  get_selected_table_row_count <- function(compute_by_count = F){
    
    if(compute_by_count){
      dd <- jdbc_cm$query(paste0("SELECT count(1) FROM ", selected_table$db ,".",selected_table$table_name))
      tag <-""
      selected_table_row_num <<- dd[[1]]
    }else{
      # pre-calculated values
      if(is.null(selected_table_row_num)){
        dd <- jdbc_cm$query(paste0("SELECT NUM_ROWS FROM ALL_TABLES WHERE OWNER = '", selected_table$db ,"'AND TABLE_NAME='",selected_table$table_name,"'"))
        dd <- safe_dba_table_for_views(table_name = "ALL_TABLES", odf = dd)
        tag <-"approximately"
        selected_table_row_num <<- dd[[1]]
      }
      
    }
    
    return(selected_table_row_num)
  }
  
  get_hadoop_db_type_map <- function(){
    
    # ref : https://docs.oracle.com/bigdata/bda45/BDSUG/copy2bda.htm#BIGUG76755
    # ref : https://docs.oracle.com/cd/E57471_01/bigData.100/extensions_bdd/src/rext_data_type_conversion.html
    
    td <- get_selected_table_description()
    
    Oracle_to_Hive_Map <- readRDS("source_db_code/Oracle_to_Hive_info")
    
    Java_to_Hive_Map <- readRDS("source_db_code/HiveJavaMap")
    
    Hive_Impala_Col_Type_Map <- readRDS("source_db_code/Hive_Impala_Col_Type_Map")
    
    # numeric has to be handled in different way
    
    Oracle_to_Hive_Map <- Oracle_to_Hive_Map[ Oracle_to_Hive_Map$Oracle_Data_Type != "NUMBER",]
    
    type_map  <- td[c("COLUMN_NAME","DATA_TYPE","DATA_PRECISION","DATA_SCALE")]
    
    type_map <- merge(type_map, Oracle_to_Hive_Map, by.x = "DATA_TYPE", by.y = "Oracle_Data_Type", all.x = T, all.y = F)
    
    # special case of number 
    nums_t <- type_map$DATA_TYPE == "NUMBER"
    
    type_map$DATA_SCALE[nums_t][is.na(type_map$DATA_SCALE[nums_t])] <- 0
    
    type_map$DATA_PRECISION[nums_t][is.na(type_map$DATA_PRECISION[nums_t])] <- 38
    
    scale_zero_t <- type_map$DATA_SCALE == 0
    
    scale_nonzero_t <- type_map$DATA_SCALE > 0
    
    precision_less_10_t <- type_map$DATA_PRECISION < 10
    
    type_map$Hive_Data_Type[ nums_t & scale_zero_t & precision_less_10_t] <- "INT"
    
    type_map$Hive_Data_Type[ nums_t & scale_zero_t & !precision_less_10_t] <- "BIGINT"
    
    type_map$Hive_Data_Type[ nums_t & scale_nonzero_t ] <- "DECIMAL"
    
    if(type_map$Hive_Data_Type %>% is.na() %>% any()){
      warning("NA data type found. Converting them to string")
      type_map$Hive_Data_Type[ is.na(type_map$Hive_Data_Type)] <- "STRING"
    }
    
    if(type_map$Hive_Data_Type %>% tolower() %>% str_detect("unsupported") %>% any(na.rm = T)){
      warning("Unsupported data type found. Converting them to string")
      type_map$Hive_Data_Type[ type_map$Hive_Data_Type %>% tolower() %>% str_detect("unsupported") ] <- "STRING"
    }
    
    type_map$Hive_Data_Type_Orig <- type_map$Hive_Data_Type
    
    time_stamp_t <- type_map$Hive_Data_Type == "TIMESTAMP"
    
    if(any(time_stamp_t)){
      type_map$Hive_Data_Type[time_stamp_t] <- "STRING"
    }
    
    type_map <- type_map %>% merge(Java_to_Hive_Map, by.x = "Hive_Data_Type", by.y = "Hive_Type", all.x = T, all.y = F) 
    
    type_map <- type_map %>% merge(Hive_Impala_Col_Type_Map, by.x = "Hive_Data_Type_Orig", by.y = "Hive_Type", all.x = T, all.y = F) 
    
    type_map$Impala_Type[is.na(type_map$Impala_Type)] <- "STRING"
    
    type_map <- type_map[c("COLUMN_NAME","DATA_TYPE","Hive_Data_Type","Java_Type","Impala_Type")]
    
    return(type_map)
    
  }
  
  get_sqoop_java_col_map <- function(full_specification = F){
    # ref http://www.oracle-dba-online.com/sql/oracle_datatypes_and_create_table.htm
    map_str <- NULL
    
    try({
      td <- get_selected_table_description()
      if(full_specification){
        dm <- get_hadoop_db_type_map()
        dm <- dm[!is.na(dm$Java_Type),]
        if(nrow(dm)){
          map_str <- dm[c("COLUMN_NAME","Java_Type")] %>% apply(1, paste0, collapse = "=") %>% paste0(collapse = ",")
        }
      }else{
        map_str <- td$COLUMN_NAME[ td$DATA_TYPE %in% c("DATE","TIMESTAMP") ] %>% paste0("=String", collapse = ",")
      }
    },silent = T)
    
    return(map_str)
  }
  
  col_compare_transform <- function(col_name, val, comp_op = "=", get_hadoop_db_col = F){
    # detect class
    
    v_class <-"unknown"
    
    if(is.numeric(val)){
      v_class <- "num"
    }
    
    if(is.character(val)){
      v_class <- "str"
    }
    
    if(class(val)=="Date"){
      v_class <- "date"
    }
    
    if(v_class == "str"){
      dtry <- try(as.POSIXct(val), silent = T)
      if("POSIXct" %in% class(dtry)){
        # either date or timestamp
        if(nchar(val) >10){
          v_class <- "timestamp"
        }else{
          v_class <- "date"
        }
      }
    }
    
    sql_part <- switch(v_class,
                       num = paste0(col_name,comp_op, val),
                       str = paste0(col_name,comp_op," '", val,"'"),
                       timestamp = paste0("SUBSTR(TO_CHAR(",col_name,",'YYYY-MM-DD HH24:MI:SS'),1,19) ",comp_op," '", substr(val,1,19),"'"),
                       date = paste0("SUBSTR(TO_CHAR(",col_name,",'YYYY-MM-DD'),1,10) ",comp_op," '", substr(val,1,10),"'"),
                       "")
    
    if(get_hadoop_db_col){
      sql_part <-  switch(v_class,
                          num = col_name,
                          str = col_name,
                          timestamp = paste0("SUBSTR(",col_name,",1,19) "),
                          date = paste0("SUBSTR(",col_name,",1,10) "),
                          "")
    }
    
    return(sql_part)
    
  }
  
  
  col_load_type <- function(column_name, sample_seq = 3, where = "1=1"){
    
    m <- NA
    M <- NA
    
    total_num_rows <- get_selected_table_row_count()
    
    for( i in seq(sample_seq)){
      lR <-jdbc_cm$query(paste0("SELECT ", column_name, " FROM ", selected_table$db,".",selected_table$table_name," SAMPLE(",min(max((i/2/total_num_rows)*100,0.000001),0.001),") WHERE ROWNUM<100 AND ", where))[[1]]
      LR <-jdbc_cm$query(paste0("SELECT ", column_name, " FROM ", selected_table$db,".",selected_table$table_name," SAMPLE(",max(min( (1-200*i/total_num_rows)*90,100),50),") WHERE ROWNUM<100 AND ", where))[[1]]
      m<- c(m,lR)
      M<- c(M,LR)
    }
    
    m<- m[!is.na(m)]
    M<- M[!is.na(M)]
    
    w <- c(m, M)
    
    x <- table(w)
    
    mu <- mean(x, trim = 0.2, na.rm = T)
    s <- mad(x, na.rm = T)
    
    r <- list(min = min(w, na.rm = T), max = max(w, na.rm = T), distinct_in_set = unique(w[!is.na(w)]), is_chunked = any(x > mu+3*s | x< mu-3*s), high_freq = w %>% table() %>% which.max() %>% names())
    
    r$is_chrono <- r$high_freq == r$min
    
    return(r)
    
  }
  
  split_test <- function(s_range, col_name){
    
    if(is.list(s_range)){
      s_range <- c(s_range[[1]], s_range[[2]])
    }
    
    sample_split <- seq(from = min(s_range), to = max(s_range), length.out = 3)
    
    d1 <- jdbc_cm$query(paste0("SELECT /*+ PARALLEL */ * FROM ", selected_table$db ,".",selected_table$table_name,
                               " WHERE ROWNUM  = 1 AND ",
                               col_compare_transform(col_name,sample_split[1], comp_op = ">=")," AND ",
                               col_compare_transform(col_name,sample_split[2], comp_op = "<=")),external = T)
    
    d2 <- jdbc_cm$query(paste0("SELECT /*+ PARALLEL */ * FROM ", selected_table$db ,".",selected_table$table_name,
                               " WHERE ROWNUM  = 1 AND ",
                               col_compare_transform(col_name,sample_split[2], comp_op = ">=")," AND ",
                               col_compare_transform(col_name,sample_split[3], comp_op = "<=")),external = T)
    if(!is.data.frame(d1)) d1 <- data.frame()
    if(!is.data.frame(d2)) d2 <- data.frame()
    
    return(nrow(d1) & nrow(d2))
  }
  
  get_selected_table_analysis <- function(rows_split_lim = 10^6, rows_job_split_lim = 100*rows_split_lim, 
                                          server_num_connection_limit = 20,
                                          biased_column_for_split = NULL, biased_column_for_job_split = NULL){
    
    if(is.null(selected_table_analysis)){
      
      TA  <- NULL
      
      try({
        TA  <- list()
        
        TA$table <- selected_table
        
        dd <- get_selected_table_description()
        
        total_num_rows <- get_selected_table_row_count(compute_by_count = F) 
        
        if(missing(rows_job_split_lim)){
          rows_job_split_lim <- 100*rows_split_lim
        }
        
        
        TA$rows_split_lim <- rows_split_lim
        TA$rows_job_split_lim <- rows_job_split_lim
        
        TA$server_num_connection_limit <- server_num_connection_limit
        # split rules
        TA$is_split_required <- floor(total_num_rows/rows_split_lim) > 1
        TA$is_job_split_required <- (floor(total_num_rows/rows_job_split_lim) > 1) & TA$is_split_required
        TA$total_num_rows <- total_num_rows
        
        TA$sqoop_java_col_map <- get_sqoop_java_col_map()
        
        # get possible index columns 
        index_cols <- jdbc_cm$query(paste0("SELECT *
                                           FROM ALL_IND_COLUMNS 
                                           WHERE TABLE_OWNER = '", selected_table$db ,"'AND TABLE_NAME='",
                                           selected_table$table_name,"'"))
        # fix for inaccessible DBA tables and views
        index_cols <- safe_dba_table_for_views(table_name = "ALL_IND_COLUMNS", odf = index_cols)
        
        if(is.data.frame(index_cols)){
          TA$index_cols <- index_cols
          index_cols_desc <- dd[ dd$COLUMN_NAME %in% index_cols$COLUMN_NAME, ]
          TA$ideal_col_for_range_computation <- index_cols_desc$COLUMN_NAME[  which.max(index_cols_desc$NUM_DISTINCT)[1] ]
          rm(index_cols_desc)
        }
        
        dd$IS_INDEX <- dd$COLUMN_NAME %in% TA$index_cols$COLUMN_NAME
        
        dd$NON_NULL_PCT <- 1 - dd$NUM_NULLS/total_num_rows
        
        dd <- dd %>% filter(!IS_NULL_EVER)
        
        col_is_date <- function(d){
          is_date <- F
          if(d$DATA_TYPE %in% c("DATE","TIMESTAMP")){
            # txn system
            sample_date <- jdbc_cm$query(paste0("SELECT ", d$COLUMN_NAME, " FROM ", selected_table$db,".",selected_table$table_name," SAMPLE(",max(min(10000/total_num_rows,0.1),0.000001),") WHERE ROWNUM<100"))
            if(length(sample_date)<3){
              sample_date <- jdbc_cm$query(paste0("SELECT ", d$COLUMN_NAME, " FROM ", selected_table$db,".",selected_table$table_name," WHERE ROWNUM<100"))
            }
            
            sample_date <- sample_date[[1]]
            if(all(nchar(sample_date)==10)){
              dt <- try({as.Date(sample_date)}, silent = T) 
              if(class(dt) =="Date"){
                is_date <- T
              }
            }else{
              if(all(substr(sample_date,12,19)=="00:00:00")){
                is_date <- T
              }
            }
          }
          
          d$IS_DATE <- is_date
          return(d)
        }
        
        
        dd<- ddply(dd, "COLUMN_NAME",  col_is_date)
        
        
        # Splitability checks
        
        if(!TA$is_job_split_required){
          dd$NUM_ROWS_in_SPLIT <- total_num_rows/dd$NUM_DISTINCT 
        }else{
          dd$NUM_ROWS_in_SPLIT <- TA$rows_job_split_lim/dd$NUM_DISTINCT 
        }
        
        
        dd <- dd %>% filter(!IS_NULL_EVER)
        
        
        # distinch match score with TA$server_num_connection_limit
        dd$DM_SCORE <- 1/(abs(dd$NUM_DISTINCT -TA$server_num_connection_limit)+1)
        
        dd$is_NUM_ROWS_in_SPLIT_within_LIMIT <- (dd$NUM_ROWS_in_SPLIT < rows_split_lim) | dd$DM_SCORE > max(0.1,quantile(dd$DM_SCORE,3.5/4))
        dd$is_NUM_ROWS_in_SPLIT_within_JOB_SPLIT_LIMIT <- dd$NUM_ROWS_in_SPLIT < rows_job_split_lim
        
        
        get_date_info<- function(d){
          
          d$MIN <- NA
          d$MAX <- NA
          d$IS_CHRONO <- F
          d$IS_CHUNKED <- F
          
          if(d$IS_DATE){
            
            ci <- col_load_type(d$COLUMN_NAME)
            d$MIN <- ci$min
            d$MAX <- ci$max
            d$IS_CHRONO <- ci$is_chrono 
            d$IS_CHUNKED <- ci$is_chunked
            
          }
          
          return(d)
          
        }
        
        
        dd<- ddply(dd, "COLUMN_NAME",  get_date_info)
        
        
        # partition checks
        
        part_info <- jdbc_cm$query(paste0("SELECT * FROM ALL_PART_KEY_COLUMNS WHERE OWNER = '", selected_table$db ,"'AND NAME='",selected_table$table_name,"'"))
        
        part_info <- safe_dba_table_for_views(table_name = "ALL_PART_KEY_COLUMNS", odf = part_info)
        
        subpart_info <- jdbc_cm$query(paste0("SELECT * FROM ALL_SUBPART_KEY_COLUMNS WHERE OWNER = '", selected_table$db ,"'AND NAME='",selected_table$table_name,"'"))
        
        subpart_info <- safe_dba_table_for_views(table_name = "ALL_SUBPART_KEY_COLUMNS", odf = subpart_info)
        
        dd$SCORE_PARTITION <- 0
        dd$SCORE_PARTITION[dd$COLUMN_NAME %in% part_info$COLUMN_NAME] <- 1
        dd$SCORE_PARTITION[dd$COLUMN_NAME %in% subpart_info$COLUMN_NAME] <- 0.3
        
        
        
        if(sum(dd$SCORE_PARTITION)){
          TA$is_partitioned <- T
        }else{
          TA$is_partitioned <- F
        }
        
        if(TA$is_partitioned){
          partition_info1 <- jdbc_cm$query(paste0("SELECT * FROM ALL_TAB_PARTITIONS WHERE TABLE_OWNER = '", selected_table$db ,"'AND TABLE_NAME='",selected_table$table_name,"'"))
          
          partition_info1 <- safe_dba_table_for_views(table_name = "ALL_TAB_PARTITIONS", odf = partition_info1)
          
          
          if(selected_table$type!="View"){
            eval_in_db <- function(x){
              jdbc_cm$query(paste0("SELECT ", x, " FROM DUAL"))[[1]]
            }
            
            
            partition_info1$HIGH_VALUE_EVALUATED <- laply(partition_info1$HIGH_VALUE, eval_in_db)
            
            part_col_info_sql <- sql_parameterized_query_reader("source_db_code/partition_column_summary.sql")
            
            jdbc_cm$query(readLines("source_db_code/raw_convert_function.sql"), is_DDL = T)
            
            partition_info2 <- jdbc_cm$query(part_col_info_sql$build_sql(db= selected_table$db, table = selected_table$table_name))
            
            tag <- partition_info2$LOW_VALUE=="-- ::" & partition_info2$DATA_TYPE=="DATE"
            partition_info2$LOW_VALUE[tag] <- NA
            partition_info2$HIGH_VALUE[tag] <- NA
            
            dpa <- ddply(partition_info2, c("COLUMN_NAME"), function(d){
              
              if(d$DATA_TYPE[1]=="NUMBER"){
                d$LOW_VALUE <-  d$LOW_VALUE %>% as.numeric()
                d$HIGH_VALUE <-  d$HIGH_VALUE %>% as.numeric()
                d$diff <- d$HIGH_VALUE- d$LOW_VALUE
              }else{
                if(d$DATA_TYPE[1] %in% c('VARCHAR2', 'VARCHAR', 'CHAR')){
                  
                  d$LOW_VALUE_NUM <- suppressWarnings( as.numeric(d$LOW_VALUE))
                  d$HIGH_VALUE_NUM <- suppressWarnings( as.numeric(d$HIGH_VALUE))
                  if(all(((d$LOW_VALUE %>% is.na() ) & (d$LOW_VALUE_NUM %>% is.na() )) == (d$LOW_VALUE %>% is.na()) )){
                    
                    d$LOW_VALUE <-  suppressWarnings(d$LOW_VALUE %>% as.numeric())
                    d$HIGH_VALUE <-  suppressWarnings(d$HIGH_VALUE %>% as.numeric())
                    d$diff <- d$HIGH_VALUE- d$LOW_VALUE
                    
                  }else{
                    d$diff <- paste0(d$LOW_VALUE, " - ",d$HIGH_VALUE)
                    d$diff[is.na(d$LOW_VALUE)]<-NA
                    d$diff <- d$diff %>% as.factor() %>% as.numeric()
                    d$LOW_VALUE <- d$LOW_VALUE %>% as.factor() %>% as.numeric()
                    d$HIGH_VALUE <- d$HIGH_VALUE %>% as.factor() %>% as.numeric()
                  }
                  
                }else{
                  
                  if(d$DATA_TYPE[1] == "DATE"){
                    d$LOW_VALUE <-  d$LOW_VALUE %>% as.Date()
                    d$HIGH_VALUE <-  d$HIGH_VALUE %>% as.Date()
                    d$diff <- d$HIGH_VALUE- d$LOW_VALUE
                    
                    d$LOW_VALUE <- d$LOW_VALUE %>% as.numeric()
                    d$HIGH_VALUE <-  d$HIGH_VALUE %>% as.numeric()
                    d$diff <- d$diff %>% as.numeric()
                    
                  }
                }
                
              }
              
              
              s1 <- mad(d$diff, na.rm = T)/ (median(d$diff, na.rm = T) + exp(-256))
              s2 <- mad(d$LOW_VALUE, na.rm = T)/ (abs(median(d$LOW_VALUE, na.rm = T)) + exp(-256))+mad(d$HIGH_VALUE, na.rm = T)/ (abs(median(d$HIGH_VALUE, na.rm = T)) + exp(-256))
              
              do <- data.frame(COLUMN_NAME = d$COLUMN_NAME[1], WIDE_VAL = s1, SHIFT_VAL = s2)
              return(do)
              
            })
            
            dpa$WIDE_SCORE <- exp(-dpa$WIDE_VAL*10)
            dpa$SHIFT_SCORE <- exp(-dpa$SHIFT_VAL*10)
            
            dpa$IN_PART_SCORE <- (dpa$WIDE_SCORE+dpa$SHIFT_SCORE)/2
            
            dd <- merge(dd, dpa[c("COLUMN_NAME","IN_PART_SCORE")])
            
          }else{
            partition_info2 <- data.frame()
            dpa <- data.frame()
            dd$IN_PART_SCORE <- 0
          }
          
          
          TA$partition_info <- list(partitions = part_info, subpartitions = subpart_info, partition_info = partition_info1, partition_wise_column_info = partition_info2, partition_wise_column_score = dpa)
          
          
        }else{
          dd$IN_PART_SCORE <- 0
        }
        
        
        dd$SCORE_SPLIT <- dd$NUM_ROWS_in_SPLIT/rows_split_lim*dd$is_NUM_ROWS_in_SPLIT_within_LIMIT
        
        dd$SCORE_JOB_SPLIT <- exp(-abs(1-dd$NUM_ROWS_in_SPLIT/rows_job_split_lim))
        
        dd$DATE_SCORE <- dd$IS_DATE*0.3+dd$IS_DATE*(1*dd$IS_CHUNKED+2*dd$IS_CHRONO)/3*0.7
        
        dd$SCORE_SPLIT <- dd$is_NUM_ROWS_in_SPLIT_within_LIMIT*(1*dd$SCORE_SPLIT+3*dd$DATE_SCORE+6*dd$IN_PART_SCORE+2*dd$DM_SCORE)/12
        
        dd$SCORE_JOB_SPLIT <- dd$SCORE_JOB_SPLIT*(1+2*dd$DATE_SCORE+3*dd$SCORE_PARTITION+1*dd$IS_INDEX)/7
        
        if(length(biased_column_for_job_split)){
          dd$SCORE_JOB_SPLIT[toupper(dd$COLUMN_NAME) %in% toupper(biased_column_for_job_split)] <- max(max(dd$SCORE_JOB_SPLIT, na.rm = T)+0.0001,0.0001)
        }
        
        if(length(biased_column_for_split)){
          dd$SCORE_SPLIT[toupper(dd$COLUMN_NAME) %in% toupper(biased_column_for_split)] <- max(max(dd$SCORE_SPLIT, na.rm = T)+0.0001,0.001)
        }
        
        
        if(selected_table$type!="View"){
          # getting DBA summary
          jdbc_cm$query(readLines("source_db_code/raw_convert_function.sql"), is_DDL = T)
          col_info_sql <- sql_parameterized_query_reader("source_db_code/column_summary.sql")
          
          col_info <- jdbc_cm$query(col_info_sql$build_sql(db= selected_table$db, table = selected_table$table_name))
          
          col_info <- col_info[c("COLUMN_NAME", setdiff( colnames(col_info), colnames(dd)))]
          
          dd <- merge(dd, col_info)
        }
        
        
        if(all(c("LOW_VALUE","HIGH_VALUE") %in% colnames(dd))){
          TA$analysis_data<- dd 
        }else{
          TA$analysis_data<- dd %>% mutate(LOW_VALUE = MIN, HIGH_VALUE =  MAX)
        }
        
        
        TA$suggested_columns_for_split <- dd$COLUMN_NAME[which.max(dd$SCORE_SPLIT)]
        
        TA$suggested_columns_for_split.type <- dd$DATA_TYPE[ dd$COLUMN_NAME == TA$suggested_columns_for_split]
        
        TA$suggested_columns_for_job_split <- dd$COLUMN_NAME[which.max(dd$SCORE_JOB_SPLIT)]
        
        
      }, silent = T)
      
      selected_table_analysis <<- TA
      
    }
    
    return(invisible(selected_table_analysis))
    
  }
  
  get_selected_table_import_plan <- function(force_pointable = F, 
                                             manual_range_for_split_key, 
                                             manual_range_for_job_split_key, 
                                             manual_strict_ranges_for_job_split_key,
                                             manual_strict_num_days_for_job_split_key){
    
    if(is.null(  selected_table_import_plan)){
      
      IJP  <- NULL
      
      try({
        
        TA <- get_selected_table_analysis()
        
        split_key <- TA$suggested_columns_for_split
        job_split_key <- TA$suggested_columns_for_job_split
        
        # stylized import rules
        
        IJP$job_style <- "PLAIN"
        
        # state will determine whether to automatically proceed for sqoop or not
        IJP$job_style_info$state <- "UNKNOWN"
        
        IJP$job_style_info$key_js$name <- job_split_key
        IJP$job_style_info$key_s$name <- split_key
        
        IJP$job_style_info$key_js$state <- "UNKNOWN"
        IJP$job_style_info$key_s$state <- "UNKNOWN"
        
        if(TA$is_split_required){
          # key_s
          
          if(missing(manual_range_for_split_key)){
            
            if(!TA$is_job_split_required){
              key_s_actual_range <- jdbc_cm$query(paste0("SELECT MIN(",split_key,") as Min, MAX(",split_key,") as Max FROM ",selected_table$db,".",selected_table$table_name), 
                                                  external = T, 
                                                  wait_secs = 2*60)
              
            }else{
              key_s_actual_range <- jdbc_cm$query(paste0("SELECT MIN(",split_key,") as Min, MAX(",split_key,") as Max FROM ",
                                                         selected_table$db,".",selected_table$table_name, " SAMPLE(0.01)"), 
                                                  external = T, 
                                                  wait_secs = 2*60)
              
            }
            
            if(!is.data.frame(key_s_actual_range)){
              
              # try sample
              
              key_s_actual_range <- jdbc_cm$query(paste0("SELECT MIN(",split_key,") as Min, MAX(",split_key,") as Max FROM ",
                                                         selected_table$db,".",selected_table$table_name," SAMPLE(0.001)"), 
                                                  external = T, 
                                                  wait_secs = 2*60)
              
              
            }
            
          }else{
            key_s_actual_range <- tibble(MIN = min(manual_range_for_split_key), MAX = max(manual_range_for_split_key))
          }
          
          
          
          c_inf_key_s <- col_load_type(column_name = split_key)
          
          if(is.data.frame(key_s_actual_range)){
            if(nrow(key_s_actual_range)){
              key_s_actual_range <- key_s_actual_range[1,] %>% as.character()
              c_inf_key_s$min <- min(c_inf_key_s$min, min(key_s_actual_range), na.rm = T)
              c_inf_key_s$max <- max(c_inf_key_s$max, max(key_s_actual_range), na.rm = T)
            }
          }
          
          
          
          range_key_s <- TA$analysis_data[ TA$analysis_data$COLUMN_NAME == split_key,c("LOW_VALUE","HIGH_VALUE")]
          
          if(TA$analysis_data$DATA_TYPE[TA$analysis_data$COLUMN_NAME == split_key] %in% c("NUMBER","BINARY_FLOAT","BINARY_DOUBLE")){
            range_key_s <- range_key_s[1,] %>% unlist() %>% as.numeric()
            c_range <- c(c_inf_key_s$min,c_inf_key_s$max) %>% as.numeric()
          }else{
            if(TA$analysis_data$IS_DATE[TA$analysis_data$COLUMN_NAME == split_key]){
              range_key_s <- range_key_s[1,] %>% unlist() %>% as.Date() 
              c_range <- c(c_inf_key_s$min,c_inf_key_s$max) %>% as.Date() 
            }else{
              range_key_s <- range_key_s[1,] %>% unlist() %>% as.character()
              c_range <- c(c_inf_key_s$min,c_inf_key_s$max) %>% as.character() 
            }
          }
          
          range_key_s <- c(min(range_key_s, c_range,na.rm = T), max(range_key_s,c_range,na.rm = T))
          
          
          if(range_key_s %>% is.na() %>% "!"(.) %>% all()){
            
            IJP$job_style_info$key_s$range <- list(min = min(range_key_s), max = max(range_key_s))
            
            IJP$job_style_info$key_s$col_str <- col_compare_transform(split_key,range_key_s[1], comp_op = ";") %>% str_split(";") %>% .[[1]] %>% .[1] %>% str_trim()
            
            range_key_s_num <- asNum(range_key_s)
            
            if(is.numeric(range_key_s_num)){
              if(all(!is.na(range_key_s_num))){
                # elevated range
                IJP$job_style_info$key_s$expanded_range <- list(min = range_key_s_num %>% min() %>% asNum(blow = -1, positive = T), 
                                                                max = range_key_s_num %>% max() %>% asNum(blow = 1, positive = T))
                
                IJP$job_style_info$key_s$expanded_range_split_tested <- split_test(IJP$job_style_info$key_s$expanded_range, col_name = split_key )
                
              }
            }
            
            if(is.numeric(range_key_s) | class(range_key_s)=="Date"){
              
              IJP$job_style_info$key_s$range_split_tested <- split_test(IJP$job_style_info$key_s$range, col_name = split_key)
            }else{
              
              # can not do split test for character
              IJP$job_style_info$key_s$range_split_tested <- F
              
            }
            
            if(IJP$job_style_info$key_s$range_split_tested){
              IJP$job_style_info$key_s$range_main <- IJP$job_style_info$key_s$range
              IJP$job_style_info$key_s$state <- "COMPLETE"
            }else{
              if(IJP$job_style_info$key_s$expanded_range_split_tested){
                IJP$job_style_info$key_s$range_main <- IJP$job_style_info$key_s$expanded_range
                IJP$job_style_info$key_s$state <- "COMPLETE"
              }else{
                IJP$job_style_info$key_s$range_main <- IJP$job_style_info$key_s$range
                IJP$job_style_info$key_s$state <- "PARTIAL"
              }
            }
            
          }
          
        }
        
        if(TA$is_job_split_required){
          
          IJP$job_style <- "JOB_SPLIT_UNKNOWN"
          
          # job split cases
          
          # three cases covered
          # 1) key_js_POINT_key_s_FIXED
          # 2) key_js_RANGE_key_s_FIXED
          # 3) key_js_RANGE_key_s_SAME_RANGE
          
          #key_js_POINT_key_s_FIXED
          
          num_job_required <- TA$total_num_rows / TA$rows_job_split_lim
          
          num_job_required <- max(round(num_job_required),2)
          
          IJP$job_style_info$key_js$num_job_required <- num_job_required
          
          num_distinct_key_js <- TA$analysis_data$NUM_DISTINCT[TA$analysis_data$COLUMN_NAME == job_split_key]
          
          num_distinct_key_s <- TA$analysis_data$NUM_DISTINCT[TA$analysis_data$COLUMN_NAME == split_key]
          
          #may be idea for pointable case : num_map_required <- TA$analysis_data$NUM_ROWS_in_SPLIT[TA$analysis_data$COLUMN_NAME == job_split_key]/TA$rows_split_lim
          
          num_map_required <- TA$rows_job_split_lim/TA$rows_split_lim
          num_map_required <- round(num_map_required)
          
          IJP$job_style_info$key_s$num_splits <- num_map_required
          
          IJP$job_style_info$is_same_key_for_js_and_s <- job_split_key == split_key
          
          if(TA$is_partitioned & !IJP$job_style_info$is_same_key_for_js_and_s & (TA$analysis_data$DATA_TYPE[TA$analysis_data$COLUMN_NAME == TA$suggested_columns_for_job_split] != "DATE")){
            force_pointable <- T
          }
          
          if(force_pointable){
            IJP$job_style_info$is_pointable <- T
          }else{
            IJP$job_style_info$is_pointable <- abs(num_job_required-num_distinct_key_js)/(num_distinct_key_js+num_job_required) < 0.35
          }
          
          
          IJP$job_style_info$is_same_key_for_js_and_s <- job_split_key == split_key
          
          if(selected_table$type == "View"){
            cat("\nSetting is_key_s_fixed to TRUE as a View is opted for port (limited access) \n")
            IJP$job_style_info$is_key_s_fixed <- T
          }else{
            IJP$job_style_info$is_key_s_fixed <- TA$analysis_data$IN_PART_SCORE[TA$analysis_data$COLUMN_NAME == split_key] > 0.7
          }
          
          
          if(IJP$job_style_info$is_pointable & IJP$job_style_info$is_key_s_fixed & !IJP$job_style_info$is_same_key_for_js_and_s){
            IJP$job_style<- "key_js_POINT_key_s_FIXED"
          }
          
          if(!IJP$job_style_info$is_pointable & IJP$job_style_info$is_key_s_fixed & !IJP$job_style_info$is_same_key_for_js_and_s){
            IJP$job_style<- "key_js_RANGE_key_s_FIXED"
          }
          
          if(!IJP$job_style_info$is_pointable & !IJP$job_style_info$is_key_s_fixed & IJP$job_style_info$is_same_key_for_js_and_s){
            IJP$job_style<- "key_js_RANGE_key_s_SAME_RANGE"
          }
          
          # completeness info 
          
          # key_js
          
          c_inf_key_js <- col_load_type(column_name = job_split_key)
          
          if(IJP$job_style_info$is_pointable){
            
            if(TA$is_partitioned){
              
              if(TA$partition_info$partitions$COLUMN_NAME == job_split_key){
                
                info_1 <- TA$partition_info$partition_info$HIGH_VALUE_EVALUATED
                info_2 <- TA$partition_info$partition_wise_column_info$LOW_VALUE[TA$partition_info$partition_wise_column_info$COLUMN_NAME == job_split_key]
                info_3 <- c_inf_key_js$distinct_in_set
                
                if(TA$analysis_data$IS_DATE[TA$analysis_data$COLUMN_NAME == job_split_key]){
                  info_1 <- info_1 %>% as.Date()
                  info_2 <- info_2 %>% as.Date()
                  info_3 <- info_3 %>% as.Date()
                }
                
                t1 <- (intersect(info_1, info_3) %>% length()) >0 | (intersect(info_1, info_2) %>% length()) >0
                t2 <- (intersect(info_2, info_3) %>% length()) >0
                
                IJP$job_style_info$key_js$points_info <- list(dba_info = info_1, dba_points = info_2, sample_points = info_3)
                
                if(t1){
                  IJP$job_style_info$key_js$points <- info_1
                  IJP$job_style_info$key_js$state <- "COMPLETE"
                }
                
                if(t2){
                  if(length(info_2[!is.na(info_2)]) >= length(info_3[!is.na(info_3)])){
                    IJP$job_style_info$key_js$points <- info_2[!is.na(info_2)]
                  }else{
                    IJP$job_style_info$key_js$points <- info_3[!is.na(info_3)]
                  }
                  
                  IJP$job_style_info$key_js$num_points_required <- (length(info_1) - IJP$job_style_info$key_js$points %>% length())
                  IJP$job_style_info$key_js$state <- "PARTIAL"
                }
                
                IJP$job_style_info$key_js$col_str <- col_compare_transform(job_split_key,IJP$job_style_info$key_js$points[1], comp_op = ";") %>% str_split(";") %>% .[[1]] %>% .[1] %>% str_trim()
                
                # this is not optimal if the table is large 
                # where_parts <- IJP$job_style_info$key_js$points %>% paste0(IJP$job_style_info$key_js$col_str , " = '", .,"'")
                # following is applicabale in date case only and for equality only # this may be extended for boarder cases
                where_parts <- IJP$job_style_info$key_js$points %>% laply(as.Oracle.Date) %>% paste0(IJP$job_style_info$key_js$name , " = '", .,"'")
                
                IJP$job_style_info$key_js$where_parts <- where_parts
                
              }
              
            }
            
            
          }else{
            
            info_1 <- c(TA$analysis_data$LOW_VALUE[TA$analysis_data$COLUMN_NAME == job_split_key], TA$analysis_data$HIGH_VALUE[TA$analysis_data$COLUMN_NAME == job_split_key])
            info_2 <- c(c_inf_key_js$min, c_inf_key_js$max)
            
            if(TA$analysis_data$IS_DATE[TA$analysis_data$COLUMN_NAME == job_split_key]){
              info_1 <- info_1 %>% as.Date()
              info_2 <- info_2 %>% as.Date()
              if(!missing(manual_range_for_job_split_key)){
                info_3m <- manual_range_for_job_split_key %>% as.Date()
              }else{
                info_3m <- NULL
              }
            }
            
            info_wide <- range(c(info_1, info_2, info_3m), na.rm = T)
            
            IJP$job_style_info$key_js$range_info <- list(dba_range = info_1, sample_range = info_2)
            
            if(all(!is.na(info_wide))){
              IJP$job_style_info$key_js$range <- info_wide
              IJP$job_style_info$key_js$state <- "COMPLETE"
              
              if(TA$analysis_data$IS_DATE[TA$analysis_data$COLUMN_NAME == job_split_key]){
                
                # date case
                
                rng_prt<- IJP$job_style_info$key_js$range[1] + seq(from = 0 , to = diff( IJP$job_style_info$key_js$range), length.out = IJP$job_style_info$key_js$num_job_required+1)
                
                rng_prt <- data.frame(from = rng_prt[-length(rng_prt)], to = rng_prt[-1]-1)
                
                rng_prt$to[nrow(rng_prt)] <- IJP$job_style_info$key_js$range[2]
                
                # special case of manual_strict_ranges_for_job_split_key
                if(!missing(manual_strict_ranges_for_job_split_key)){
                  dt_gaps <- rng_prt %>% summarise(mean(as.numeric(to-from))) %>% .[[1]] %>% round(0)
                  dt_gaps <- dt_gaps + 1
                  
                  if(!missing(manual_strict_num_days_for_job_split_key)){
                    dt_gaps <- manual_strict_num_days_for_job_split_key
                  }
                  
                  rng_prt_custom <- manual_strict_ranges_for_job_split_key %>% map_df(~{
                    dtgn <- diff( range(.x))
                    
                    if(dtgn==0){
                      rp <- min(.x)
                    }else{
                      rp <- min(.x) + seq(from = 0 , to = dtgn, length.out = max(as.numeric(round(diff( range(.x))/min(dt_gaps,dtgn)+0.9)), 1))
                    }
                    
                    rp <- unique(rp) %>% sort()
                    if(length(rp)>1){
                      data.frame(from = rp[-length(rp)], to = rp[-1])
                    }else{
                      if(length(rp)==1){
                        data.frame(from = rp, to = rp)
                      }else{
                        NULL
                      }
                    }
                  })
                  
                  
                  IJP$job_style_info$key_js$range_manually_manipulated <- T
                  
                  IJP$job_style_info$key_js$num_job_required <- nrow(rng_prt_custom)
                  
                  IJP$job_style_info$key_js$derived_range_parts <- rng_prt
                  
                  rng_prt <- rng_prt_custom
                  
                  
                }
                
                rng_prt_ora <- rng_prt 
                
                rng_prt_ora$from <- rng_prt_ora$from %>% lapply(as.Oracle.Date) %>% unlist()
                rng_prt_ora$to <- rng_prt_ora$to %>% lapply(as.Oracle.Date) %>% unlist()
                
                
                IJP$job_style_info$key_js$range_parts_date <- rng_prt
                
                IJP$job_style_info$key_js$range_parts <- rng_prt_ora
                
                # Not requires as ORA forms are used
                # IJP$job_style_info$key_js$col_str <- col_compare_transform(job_split_key,IJP$job_style_info$key_js$range[1], comp_op = ";") %>% str_split(";") %>% .[[1]] %>% .[1] %>% str_trim()
                
                IJP$job_style_info$key_js$col_str <- job_split_key
                
                where_parts <- rng_prt_ora %>% apply(MARGIN = 1, function(x){ paste0(IJP$job_style_info$key_js$col_str, " >= '", x[1],"' AND ", IJP$job_style_info$key_js$col_str, " <= '", x[2],"'")})
                
                IJP$job_style_info$key_js$where_parts <- where_parts
              }
              
              
            }else{
              
              if(any(!is.na(info_wide))){
                IJP$job_style_info$key_js$range <- info_wide
                IJP$job_style_info$key_js$state <- "PARTIAL"
              }
              
            }
            
          }
          
          if(IJP$job_style_info$is_same_key_for_js_and_s){
            IJP$job_style_info$key_s$state <- IJP$job_style_info$key_js$state
          }
          
          
        }else{
          if(TA$is_split_required){
            # only split case
            num_map_required <- TA$total_num_rows/TA$rows_split_lim
            num_map_required <- round(num_map_required)
            IJP$job_style_info$key_s$num_splits <- num_map_required
            IJP$job_style <- "SPLIT"
            IJP$job_style_info$key_js$state <- "COMPLETE"
            
          }else{
            IJP$job_style_info$key_js$state <- "COMPLETE"
            IJP$job_style_info$key_s$state <- "COMPLETE"
          }
        }
        
        # assign job state
        if(IJP$job_style_info$key_s$state == "COMPLETE" & IJP$job_style_info$key_js$state == "COMPLETE"){
          IJP$job_style_info$state <- "COMPLETE"
        }
        
        if((IJP$job_style_info$key_s$state == "COMPLETE" & IJP$job_style_info$key_js$state == "PARTIAL") | 
           (IJP$job_style_info$key_s$state == "PARTIAL" & IJP$job_style_info$key_js$state == "COMPLETE")){
          IJP$job_style_info$state <- "PARTIAL"
        }
        
        
        
      }, silent = T)
      
      selected_table_import_plan <<- IJP
      
    }
    
    return(invisible(  selected_table_import_plan))
    
  }
  
  
  get_sqoop_import_execution_plan <- function(SEP, 
                                              server_num_connection_limit = Inf, 
                                              parallel_select = F, ...){
    L <- NULL
    exe_plans <-NULL
    import_profile <- NULL
    is_valid <- F
    try({
      
      ############################# Custom Function ################################
      
      # import_profile is required to be defined
      get_plan_part <- function( range_custom , where_part){
        
        import_profile_part <- list()
        
        import_profile_part$split_col_name <- selected_table_import_plan$job_style_info$key_s$name
        
        import_profile_part$split_by <- selected_table_import_plan$job_style_info$key_s$col_str
        
        if(missing(range_custom)){
          import_profile_part$range <- selected_table_import_plan$job_style_info$key_s$range_main
        }else{
          if(!is.list(range_custom)){
            range_custom <- list(min = min(range_custom), max = max(range_custom))
          }
          import_profile_part$range <- range_custom
        }
        
        
        import_profile_part$num_mappers_auto <- selected_table_import_plan$job_style_info$key_s$num_splits
        
        import_profile_part$num_mappers <- min(selected_table_import_plan$job_style_info$key_s$num_splits,server_num_connection_limit)
        
        min_str <- col_compare_transform(selected_table_import_plan$job_style_info$key_s$name,import_profile_part$range$min, comp_op = ";") %>% str_split(";") %>% .[[1]] %>% .[2] %>% str_trim()
        max_str <- col_compare_transform(selected_table_import_plan$job_style_info$key_s$name,import_profile_part$range$max, comp_op = ";") %>% str_split(";") %>% .[[1]] %>% .[2] %>% str_trim()
        
        # common or direct query may not be working on numbers
        import_profile_part$boundary_query <- paste0("SELECT ",min_str," as MIN," ,max_str," as MAX FROM DUAL" )
        
        if((asNum(max_str)==max_str & asNum(min_str)==min_str) & 
           selected_table_analysis$analysis_data$IS_DATE[selected_table_analysis$analysis_data$COLUMN_NAME == 
                                                         selected_table_import_plan$job_style_info$key_s$name]){
          
          a_date_col <- selected_table_analysis$analysis_data
          
          a_date_col <- a_date_col[a_date_col$DATA_TYPE %in% c("DATE","TIMESTAMP"),]
          
          if(nrow(a_date_col)){
            
            a_date_col <- a_date_col[ order(a_date_col$SCORE_SPLIT+a_date_col$SCORE_JOB_SPLIT, decreasing = T),]
            
            a_date_col <- a_date_col$COLUMN_NAME[1]
            
            a_date_col_near_val <- jdbc_cm$query(paste0("SELECT TO_CHAR(",a_date_col,") FROM ",import_profile$db_name,".",import_profile$table_name,"  WHERE  rownum = 1"))[[1]]
            
            import_profile_part$boundary_query <- paste0("SELECT MIN(",import_profile_part$split_by,")*0 + ",min_str,
                                                         ", MAX(",import_profile_part$split_by,")*0 + ",max_str,
                                                         " FROM (SELECT  * FROM ",import_profile$db_name,".",import_profile$table_name,
                                                         " WHERE ",a_date_col," = '",a_date_col_near_val,"' AND  (rownum = 1) ) t1")
            
          }else{
            import_profile_part$boundary_query <- paste0("SELECT 0*MIN(",import_profile_part$split_by,") + ",min_str,", ",
                                                         max_str," FROM (SELECT  * FROM ",import_profile$db_name,".",import_profile$table_name,"  WHERE  rownum = 1 ) t1")
          }
          
          
        }
        
        
        
        is_date <- (import_profile_part$split_by %>% toupper() %>% str_detect('YYYY-MM-DD')) & (import_profile_part$split_by %>% toupper() %>% str_detect('TO_CHAR'))
        
        if(is_date){
          
          import_profile_part$update_check$style <- "INCREMENTAL"
          
          # import_profile_part$split_by <- paste0("TO_NUMBER(REPLACE(",import_profile_part$split_by,",'-',''))") 
          # min_str <- import_profile_part$range$min %>% str_replace_all("-","") %>% as.numeric()
          # max_str <- import_profile_part$range$max %>% str_replace_all("-","") %>% as.numeric()
          
          import_profile_part$split_by <- paste0(' ( CAST(',selected_table_import_plan$job_style_info$key_s$name," AS DATE) - TO_DATE('",import_profile_part$range$min,"', 'yyyy-mm-dd')",') ')
          min_str <- 0
          max_str <- as.Date(import_profile_part$range$max) - as.Date(import_profile_part$range$min)
          max_str <-  as.numeric(max_str)
          
          import_profile_part$boundary_query <- paste0("SELECT MIN(( CAST(",selected_table_import_plan$job_style_info$key_s$name," AS DATE) - TO_DATE('1988-05-26', 'yyyy-mm-dd'))*0), ",
                                                       max_str," FROM (SELECT  * FROM ",import_profile$db_name,".",import_profile$table_name,"  WHERE  rownum = 1 ) t1")
          
        }
        
        if(missing(where_part)){
          exe_plan <- SEP(db_name = import_profile$db_name,
                          table_name = import_profile$table_name,
                          split_by = import_profile_part$split_by,
                          boundary_query = import_profile_part$boundary_query,
                          num_mappers = import_profile_part$num_mappers,
                          map_column_java = import_profile$sqoop_java_col_map,
                          query_select_tag = ifelse(import_profile$is_parallel_select & parallel_select, "SELECT /*+ PARALLEL */ ", "SELECT "),
                          ...)
          
        }else{
          exe_plan <- SEP(db_name = import_profile$db_name,
                          table_name = import_profile$table_name,
                          split_by = import_profile_part$split_by,
                          boundary_query = import_profile_part$boundary_query,
                          num_mappers = import_profile_part$num_mappers,
                          map_column_java = import_profile$sqoop_java_col_map,
                          query_select_tag = ifelse(import_profile$is_parallel_select & parallel_select, "SELECT /*+ PARALLEL */ ", "SELECT "), 
                          where = where_part,
                          ...)
          
        }
        
        L <- list(import_profile_part = import_profile_part, exe_plan_part = exe_plan)
      }
      
      ##############################################################################
      get_selected_table_analysis()
      get_selected_table_import_plan()
      
      import_profile$db_name <- selected_table_analysis$table$db
      import_profile$table_name <- selected_table_analysis$table$table_name
      
      import_profile$analysis_info <- list(TA = selected_table_analysis, JIP = selected_table_import_plan)
      
      import_profile$row_count <- selected_table_analysis$total_num_rows
      
      import_profile$job_style <- selected_table_import_plan$job_style
      
      import_profile$state <- selected_table_import_plan$job_style_info$state
      
      import_profile$sqoop_java_col_map  <- selected_table_analysis$sqoop_java_col_map
      
      import_profile$is_parallel_select <- selected_table_analysis$total_num_rows > max(selected_table_analysis$rows_split_lim/3, 10000)
      
      # embedding update plan [basic]
      
      import_profile$update_check <- list()
      import_profile$update_check$style <- "UNKNOWN"
      
      # Sequential update method
      if(selected_table_analysis$suggested_columns_for_split.type %in% c("DATE", "TIMESTAMP")){
        
        import_profile$update_check$style <- "INCREMENTAL"
        
        # embedding update plan [specific to date]
        hadoop_db_col_name_map <- col_compare_transform(selected_table_import_plan$job_style_info$key_s$name,
                                                        selected_table_import_plan$job_style_info$key_s$range_main$min, get_hadoop_db_col = T)
        
        import_profile$update_check$hadoop_db_key <- hadoop_db_col_name_map
        import_profile$update_check$hadoop_db_sql <- paste0("SELECT MAX(", hadoop_db_col_name_map,") FROM ",selected_table$table_name )
        import_profile$update_check$hadoop_db_sql_part <- paste0("SELECT MAX(", hadoop_db_col_name_map,") FROM ")
        
        import_profile$update_check$source_db_key <- selected_table_import_plan$job_style_info$key_s$col_str
        
        
        smpl_frac <- round(max(min(500/selected_table_analysis$total_num_rows*100,99.99),0.000001), 8)
        
        import_profile$update_check$source_db_sql <- paste0("SELECT MAX(", import_profile$update_check$source_db_key,") FROM ",selected_table$db,".",selected_table$table_name, " SAMPLE(", smpl_frac, ")" )
        import_profile$update_check$source_db_sql_part <- paste0("SELECT MAX(", import_profile$update_check$source_db_key,") FROM ",selected_table$db,".",selected_table$table_name , 
                                                                 " SAMPLE(", smpl_frac, ") ",
                                                                 " WHERE ", import_profile$update_check$source_db_key)
        
        import_profile$update_check$source_db_sql.exact <- paste0("SELECT MAX(", import_profile$update_check$source_db_key,") FROM ",selected_table$db,".",selected_table$table_name )
        import_profile$update_check$source_db_sql_part.exact <- paste0("SELECT MAX(", import_profile$update_check$source_db_key,") FROM ",selected_table$db,".",selected_table$table_name , 
                                                                       " WHERE ", import_profile$update_check$source_db_key)
        
        
      }
      
      # partition update method
      if(import_profile$job_style == "key_js_POINT_key_s_FIXED"){
        
        import_profile$update_check$style <- "PARTITION_UPDATE"
        
        hadoop_db_col_name_map <- selected_table_import_plan$job_style_info$key_js$name
        
        import_profile$update_check$hadoop_db_key <- hadoop_db_col_name_map
        import_profile$update_check$hadoop_db_sql <- paste0("SELECT DISTINCT(", hadoop_db_col_name_map,") FROM ",selected_table$table_name )
        import_profile$update_check$hadoop_db_sql_part <- paste0("SELECT DISTINCT(", hadoop_db_col_name_map,") FROM ")
        
        import_profile$update_check$source_db_key <- selected_table_import_plan$job_style_info$key_js$name
        
      }
      
      # job implementation
      
      if(import_profile$job_style == "PLAIN"){
        
        import_profile$job_mode <-"SINGLE"
        
        exe_plan <- SEP(db_name = import_profile$db_name,
                        table_name = import_profile$table_name,
                        map_column_java = import_profile$sqoop_java_col_map,
                        query_select_tag = ifelse(import_profile$is_parallel_select & parallel_select, "SELECT /*+ PARALLEL */ ", "SELECT "), ...)
        
        exe_plans <- list(single = exe_plan)
        
        is_valid <- T
        
      }else{
        
        if(import_profile$job_style == "SPLIT"){
          
          import_profile$job_mode <-"SINGLE"
          
          prof_part <- get_plan_part()
          
          import_profile$job_info <- list(single = prof_part$import_profile_part)
          
          exe_plans <- list(single = prof_part$exe_plan_part)
          
          is_valid <- T
          
        }else{
          
          if(import_profile$job_style == "key_js_RANGE_key_s_SAME_RANGE"){
            
            import_profile$job_mode <-"MULTIPLE"
            
            
            prof_part <- lapply(seq_along(selected_table_import_plan$job_style_info$key_js$where_parts), function(i){
              if(str_detect(selected_table_import_plan$job_style_info$key_s$col_str,"YYYY-MM-DD")){
                get_plan_part(range_custom = selected_table_import_plan$job_style_info$key_js$range_parts_date[i,] %>% t() , 
                              where_part = selected_table_import_plan$job_style_info$key_js$where_parts[i])
              }else{
                get_plan_part(range_custom = selected_table_import_plan$job_style_info$key_js$range_parts[i,] %>% t() , 
                              where_part = selected_table_import_plan$job_style_info$key_js$where_parts[i])
              }
            })
            
            names(prof_part) <- paste0("jobs_", seq_along(selected_table_import_plan$job_style_info$key_js$where_parts))
            
            import_profile$job_info <- prof_part %>% lapply("[[", "import_profile_part")
            
            exe_plans <- prof_part %>% lapply("[[", "exe_plan_part")
            
            for( i in seq(1, length(exe_plans)-1)){
              exe_plans[[i]]$plan <- exe_plans[[i]]$plan["sqoop"]
              exe_plans[[i]]$is_complete <- NULL
            }
            
            is_valid <- T
          }
          
          if(import_profile$job_style == "key_js_POINT_key_s_FIXED"){
            
            import_profile$job_mode <-"MULTIPLE"
            
            
            prof_part <- lapply(seq_along(selected_table_import_plan$job_style_info$key_js$points), function(i){
              get_plan_part(range_custom = c(selected_table_import_plan$job_style_info$key_s$range_main$min, 
                                             selected_table_import_plan$job_style_info$key_s$range_main$max) , 
                            where_part = selected_table_import_plan$job_style_info$key_js$where_parts[i])
            })
            
            names(prof_part) <- paste0("jobs_", seq_along(selected_table_import_plan$job_style_info$key_js$where_parts))
            
            import_profile$job_info <- prof_part %>% lapply("[[", "import_profile_part")
            
            exe_plans <- prof_part %>% lapply("[[", "exe_plan_part")
            
            for( i in seq(1, length(exe_plans)-1)){
              exe_plans[[i]]$plan <- exe_plans[[i]]$plan["sqoop"]
              exe_plans[[i]]$is_complete <- NULL
            }
            
            is_valid <- T
          }
          
          if(import_profile$job_style == "key_js_RANGE_key_s_FIXED"){
            
            import_profile$job_mode <-"MULTIPLE"
            
            
            prof_part <- lapply(seq_along(selected_table_import_plan$job_style_info$key_js$where_parts), function(i){
              get_plan_part(range_custom = selected_table_import_plan$job_style_info$key_s$range , 
                            where_part = selected_table_import_plan$job_style_info$key_js$where_parts[i])
            })
            
            names(prof_part) <- paste0("jobs_", seq_along(selected_table_import_plan$job_style_info$key_js$where_parts))
            
            import_profile$job_info <- prof_part %>% lapply("[[", "import_profile_part")
            
            exe_plans <- prof_part %>% lapply("[[", "exe_plan_part")
            
            for( i in seq(1, length(exe_plans)-1)){
              exe_plans[[i]]$plan <- exe_plans[[i]]$plan["sqoop"]
              exe_plans[[i]]$is_complete <- NULL
            }
            
            is_valid <- T
          }
          
        }
        
      }
      
      
    }, silent = T)
    L <- list(is_valid = is_valid, import_profile = import_profile, exe_plans = exe_plans)
    return(invisible(L))
  }
  
  get_sqoop_import_execution_plan_update <- function(last_import_profile, hdb_out,
                                                     SEP, 
                                                     server_num_connection_limit = Inf, 
                                                     parallel_select = F, ...){
    
    last_import_profile <- last_import_profile$import_profile
    
    ep <- list(is_valid = F, import_profile = NULL, exe_plans = NULL)
    
    if(last_import_profile$update_check$style == "INCREMENTAL"){
      
      try({
        
        TA <- last_import_profile$analysis_info$TA
        
        TA_AD <- TA$analysis_data
        
        is_updated <- F
        is_upto_date <- F
        
        row_m1 <- NULL
        last_rows <- jdbc_cm$query (last_import_profile$update_check$source_db_sql, external = T, wait_secs = 3*60)
        
        if(is.data.frame(last_rows)){
          if(nrow(last_rows)){
            last_rows <- last_rows[[1]]
            row_m1 <- last_rows
            if(last_rows==hdb_out){
              is_updated <- F
              is_upto_date <- T
            }
          }
        }
        
        
        
        any_new_rows <- jdbc_cm$query( last_import_profile$update_check$source_db_sql_part %>% 
                                         paste0( " > ", col_compare_transform("" , hdb_out, comp_op = ";") %>% str_split(";") %>% .[[1]] %>% .[2]),
                                       external = T, wait_secs = 3*60)
        if(is.data.frame(any_new_rows)){
          if(nrow(any_new_rows)){
            any_new_rows <- any_new_rows[[1]]
            if(is.na(any_new_rows) & !is.null(row_m1)){
              any_new_rows <- row_m1
            }
            if(!is.na(any_new_rows)){
              if(any_new_rows>hdb_out){
                is_updated <- T
              }
            }
          }
        }
        
        is_split_col_date <- TA_AD$IS_DATE[ TA_AD$COLUMN_NAME == last_import_profile$analysis_info$TA$suggested_columns_for_split]
        
        ep$import_profile <- list()
        
        if(is_updated){
          if(is_split_col_date){
            
            old_m <- TA_AD$MIN[TA_AD$COLUMN_NAME == last_import_profile$analysis_info$TA$suggested_columns_for_split] %>% as.Date()
            old_M <- TA_AD$MAX[TA_AD$COLUMN_NAME == last_import_profile$analysis_info$TA$suggested_columns_for_split] %>% as.Date()
            
            max_sql_date <- max(any_new_rows,row_m1) %>% as.Date()
            
            if(difftime(Sys.Date(), max_sql_date, tz = "day") < 20){
              max_sql_date <- Sys.Date()
            }
            
            new_m <- hdb_out %>% as.Date()
            new_M <- max_sql_date
            
            old_len <- difftime(old_M, old_m, units = "day") %>% as.numeric()
            
            new_len <- difftime(new_M, new_m, units = "day") %>% as.numeric()
            
            old_rows <- TA$total_num_rows
            
            new_rows <- round(old_rows*new_len/old_len)
            
            # init all
            selected_table <<- TA$table
            # trick get_total_rows
            selected_table_row_num <<- new_rows
            
            if(new_rows < TA$rows_split_lim){
              # simple sqoop is required
              selected_table_analysis <<- TA
              
              JIP <- last_import_profile$analysis_info$JIP
              JIP$job_style <- "PLAIN"
              
              selected_table_import_plan <<- JIP
              
            }else{
              
              selected_table_analysis <<- NULL
              get_selected_table_analysis()
              
              TA_AD_new <- selected_table_analysis$analysis_data
              
              TA_AD_new$MIN[TA_AD_new$COLUMN_NAME == selected_table_analysis$suggested_columns_for_split] <- TA_AD_new$MIN[TA_AD_new$COLUMN_NAME == selected_table_analysis$suggested_columns_for_split] %>% str_replace(as.character(old_m), as.character(new_m))
              TA_AD_new$MAX[TA_AD_new$COLUMN_NAME == selected_table_analysis$suggested_columns_for_split] <- TA_AD_new$MAX[TA_AD_new$COLUMN_NAME == selected_table_analysis$suggested_columns_for_split] %>% str_replace(as.character(old_M), as.character(new_M))
              
              selected_table_analysis$analysis_data <<- TA_AD_new
              
              selected_table_import_plan <<- NULL
              
              get_selected_table_import_plan()
              
              RNG_PATCH <- selected_table_import_plan$job_style_info$key_s$range_main
              
              RNG_PATCH$min <- new_m
              RNG_PATCH$max <- new_M
              
              selected_table_import_plan$job_style_info$key_s$range_main <<- RNG_PATCH
              
            }
            
            where_part <- last_import_profile$update_check$source_db_sql_part %>% 
              paste0( " > ", col_compare_transform("" , hdb_out, comp_op = ";") %>% str_split(";") %>% .[[1]] %>% .[2]) %>% 
              toupper() %>% str_split("WHERE") %>% .[[1]] %>% .[2]
            
            ep <- get_sqoop_import_execution_plan(SEP = SEP, 
                                                  server_num_connection_limit = server_num_connection_limit, 
                                                  parallel_select = parallel_select, 
                                                  where = where_part, 
                                                  ...)
            
            ep$import_profile$update_check$hadoop_db_info <- hdb_out
            ep$import_profile$update_check$src_db_info <- any_new_rows
            
            selected_table_row_num <<- NULL
            selected_table_analysis <<- NULL
            selected_table_import_plan <<- NULL
            
          }else{
            cat("\nSplit Col is not date. Don't know how to update\n")
          }
        }else{
          if(is_upto_date){
            ep$import_profile$is_upto_date <- T
          }
        }
        
      },silent = T)
      
    }
    
    if(last_import_profile$update_check$style == "PARTITION_UPDATE"){
      
      
      try({
        
        src_db_handle$table_catalogue()
        
        select_table(last_import_profile$table_name)
        
        selected_table_analysis <<- NULL
        selected_table_import_plan <<- NULL
        
        get_selected_table_analysis()
        
        get_selected_table_import_plan()
        
        is_updated <- F
        is_upto_date <- F
        
        partitions_now <- selected_table_import_plan$job_style_info$key_js$points %>% as.character()
        partitions_hdb_out <- as.Date(hdb_out) %>% as.character()
        
        partitions_new <- setdiff(partitions_now, partitions_hdb_out)
        
        if(length(partitions_new)){
          is_updated <- T
          is_upto_date <- F
        }else{
          is_updated <- F
          is_upto_date <- T
        }
        
        
        ep$import_profile <- list()
        
        if(is_updated){
          
          ep_now <- get_sqoop_import_execution_plan(SEP, server_num_connection_limit = server_num_connection_limit, parallel_select = parallel_select)
          
          now_jobs <- ep_now$exe_plans %>% lapply(function(nn){ nn$command() %>% str_detect(as.Oracle.Date(partitions_new))} ) %>% unlist() %>% which()
          
          if(length(now_jobs)>0){
            
            if(length(now_jobs)==1){
              ep_now$import_profile$job_style <- "SPLIT"
            }
            
            ep_now$exe_plans <- ep_now$exe_plans[now_jobs] 
            
            ep_now$import_profile$job_info <- ep_now$import_profile$job_info[ names(now_jobs)]
            
            ep <- ep_now
            
            ep$import_profile$update_check$hadoop_db_info <- hdb_out
            ep$import_profile$update_check$src_db_info <- partitions_new
          }
          
        }else{
          if(is_upto_date){
            ep$import_profile$is_upto_date <- T
          }
        }
        
      },silent = T)
      
    }
    
    return(ep)
  }
  
  # for manual commands
  get_sqoop_import_execution_plan_manual <- function(SEP,
                                                     date_key,
                                                     split_key,
                                                     date_key_value,
                                                     split_key_value,
                                                     quick = T,
                                                     server_num_connection_limit = Inf, 
                                                     parallel_select = F, ...){
    L <- NULL
    exe_plans <-NULL
    import_profile <- NULL
    is_valid <- F
    get_plan_part <- NULL
    try({
      
      ############################# Custom Function ################################
      
      fast_ta <- list()
      
      fast_ta$date_key_present <- !missing(date_key)
      
      if(fast_ta$date_key_present){
        if(!quick){
          
          fast_ta$sample_date_keys <- jdbc_cm$query(paste0("SELECT  DISTINCT ",date_key," FROM NEFT.NEFT_DATA_VIEW SAMPLE(0.00001)")) %>% 
            mutate(VALUE_DATE_D = as.Date(VALUE_DATE))
          
        }
        
        if(missing(date_key_value)){
          if(!quick){
            date_key_value <- min(fast_ta$sample_date_keys$VALUE_DATE_D)
          }else{
            date_key_value <- Sys.Date()
          }
          
          fast_ta$date_key_where <- paste0(date_key, " = '", as.Oracle.Date(date_key_value),"'")
        }else{
          if(is.list(date_key_value)){
            # multiple range case
            stop("needs implementation @DevLine")
          }else{
            if(length(date_key_value)==2){
              # simple range case
              stop("needs implementation @DevLine")
            }else{
              if(length(date_key_value)>2){
                # multiple single date case
                stop("needs implementation @DevLine")
              }else{
                if(length(date_key_value)==1){
                  # single date case
                  date_key_value <- date_key_value[1]
                  fast_ta$date_key_where <- paste0(date_key, " = '", as.Oracle.Date(date_key_value),"'")
                }else{
                  stop("needs implementation @DevLine")
                }
              }
            }
          }
          
        }
        
      }
      
      fast_ta$split_key_present <- !missing(split_key)
      
      if(fast_ta$split_key_present){
        if(!quick){
          fast_ta$sample_date_key_split_key_map <- 
            jdbc_cm$query(paste0("SELECT  ",date_key,
                                 ", MIN(",split_key,
                                 ") as MIN_SPLIT_KEY, MAX(",split_key,
                                 ") as MAX_SPLIT_KEY  FROM NEFT.NEFT_DATA_VIEW SAMPLE(0.0001) GROUP BY ",date_key)) 
          
          fast_ta$sample_split_key_range <- 
            jdbc_cm$query(paste0("SELECT  MIN(",split_key,
                                 ") as MIN_SPLIT_KEY, MAX(",split_key,
                                 ") as MAX_SPLIT_KEY  FROM NEFT.NEFT_DATA_VIEW SAMPLE(0.0001) "))
        }
        
        if(missing(split_key_value)){
          if(!quick){
            split_key_value <- fast_ta$sample_split_key_range %>% rename(min = MIN_SPLIT_KEY, max = MAX_SPLIT_KEY) %>% as.list()
          }else{
            split_key_value <- c(NA,NA)
          }
          
        }
        
        fast_ta$split_key_range <- split_key_value
        
      }
      
      
      
      # import_profile is required to be defined
      get_plan_part <- function( range_custom , where_part){
        
        
        if(fast_ta$split_key_present){
          
          import_profile_part <- list()
          
          import_profile_part$split_by <- split_key
          
          if(missing(range_custom)){
            range_custom <- fast_ta$split_key_range
          }
          
          if(!is.list(range_custom)){
            range_custom <- list(min = min(range_custom), max = max(range_custom))
          }
          
          import_profile_part$range <- range_custom
          
          import_profile_part$num_mappers <- server_num_connection_limit
          
          min_str <- col_compare_transform(split_key,import_profile_part$range$min, comp_op = ";") %>% str_split(";") %>% .[[1]] %>% .[2] %>% str_trim()
          max_str <- col_compare_transform(split_key,import_profile_part$range$max, comp_op = ";") %>% str_split(";") %>% .[[1]] %>% .[2] %>% str_trim()
          
          # common or direct query may not be working on numbers
          import_profile_part$boundary_query <- paste0("SELECT ",min_str," as MIN," ,max_str," as MAX FROM DUAL" )
          
          
          if(missing(where_part)){
            exe_plan <- SEP(db_name = import_profile$db_name,
                            table_name = import_profile$table_name,
                            split_by = import_profile_part$split_by,
                            boundary_query = import_profile_part$boundary_query,
                            num_mappers = import_profile_part$num_mappers,
                            map_column_java = import_profile$sqoop_java_col_map,
                            query_select_tag = ifelse(import_profile$is_parallel_select & parallel_select, "SELECT /*+ PARALLEL */ ", "SELECT "),
                            ...)
            
          }else{
            exe_plan <- SEP(db_name = import_profile$db_name,
                            table_name = import_profile$table_name,
                            split_by = import_profile_part$split_by,
                            boundary_query = import_profile_part$boundary_query,
                            num_mappers = import_profile_part$num_mappers,
                            map_column_java = import_profile$sqoop_java_col_map,
                            query_select_tag = ifelse(import_profile$is_parallel_select & parallel_select, "SELECT /*+ PARALLEL */ ", "SELECT "), 
                            where = where_part,
                            ...)
            
          }
          
          L <- list(import_profile_part = import_profile_part, exe_plan_part = exe_plan)
          
        }else{
          
          import_profile_part <- list()
          
          
          
          if(missing(where_part)){
            exe_plan <- SEP(db_name = import_profile$db_name,
                            table_name = import_profile$table_name,
                            num_mappers = 1,
                            map_column_java = import_profile$sqoop_java_col_map,
                            query_select_tag = ifelse(import_profile$is_parallel_select & parallel_select, "SELECT /*+ PARALLEL */ ", "SELECT "),
                            ...)
            
          }else{
            exe_plan <- SEP(db_name = import_profile$db_name,
                            table_name = import_profile$table_name,
                            num_mappers = 1,
                            map_column_java = import_profile$sqoop_java_col_map,
                            query_select_tag = ifelse(import_profile$is_parallel_select & parallel_select, "SELECT /*+ PARALLEL */ ", "SELECT "), 
                            where = where_part,
                            ...)
            
          }
          
          L <- list(import_profile_part = import_profile_part, exe_plan_part = exe_plan)
          
        }
        
        L
      }
      
      import_profile$db_name <- selected_table$db
      import_profile$table_name <- selected_table$table_name
      
      import_profile$analysis_info <- list(TA = fast_ta, JIP = NULL)
      
      import_profile$job_style <- "MANUAL"
      
      import_profile$sqoop_java_col_map  <- get_sqoop_java_col_map()
      
      import_profile$is_parallel_select <- parallel_select
      
      # embedding update plan [basic]
      
      import_profile$update_check <- list()
      import_profile$update_check$style <- "UNKNOWN"
      
      # job implementation
      
    }, silent = T)
    L <- list(is_valid = is_valid, import_profile = import_profile, exe_plans = exe_plans, ep_gen = get_plan_part)
    return(invisible(L))
  }
  
  # may not be required
  get_column_info_on_selected_table <- function(column_name, fast_approx_for_date = T, business_days = 6, force_refetch = F){
    # take min max using ROWID
    # Ref : https://docs.oracle.com/cd/B19306_01/server.102/b14200/pseudocolumns008.htm
    
    if(force_refetch){
      column_info[[paste0(selected_table$db,".", selected_table$table_name,".",column_name)]] <<- NULL
    }
    
    info <- column_info[[paste0(selected_table$db,".", selected_table$table_name,".",column_name)]]
    if(!is.null(info)){
      return(info)
    }
    
    info <- list()
    L <- list()
    
    try({
      
      if(length(column_name)==1){
        
        td <- get_selected_table_description()
        tdc <- td[ td$COLUMN_NAME == column_name,]
        
        
        info$db_info <- tdc
        
        info$is_date <- F
        if(tdc$DATA_TYPE %in% c("DATE","TIMESTAMP")){
          # txn system
          min_date <- jdbc_cm$query(paste0("SELECT ", column_name, " FROM ", selected_table$db,".",selected_table$table_name," WHERE ROWNUM<100"))[[1]]
          if(all(substr(min_date,12,19)=="00:00:00")){
            info$is_date <- T
          }
        }
        
        # fast approximate for date column
        
        if(info$is_date & fast_approx_for_date){
          # special case of date (fast approx)
          # which is updated chronologically
          min_date <- jdbc_cm$query(paste0("SELECT ", column_name, " FROM ", selected_table$db,".",selected_table$table_name," WHERE ROWNUM=1"))[[1]]
          
          min_date <- as.Date(min_date)
          
          info$range <- data.frame(MIN = min_date, MAX = min_date + tdc$NUM_DISTINCT*7/business_days)
          info$range_type <- "fast_estimate_based_on_date_huristic"
          
        }else{
          
          print("Currently not supporting non fast_approx_for_date")
          browser()
          
          if(!is.null(table_analysis$ideal_col_for_range_computation)){
            info_IDXCOL <- column_info[[paste0(selected_table$db,".", selected_table$table_name,".",table_analysis$ideal_col_for_range_computation)]]
            if(is.null(info_IDXCOL)){
              info_IDXCOL <- list()
              info_IDXCOL$range <- jdbc_cm$query(paste0("SELECT /*+ PARALLEL*/ MIN(",table_analysis$ideal_col_for_range_computation,") AS MIN, MAX(",table_analysis$ideal_col_for_range_computation,") AS MAX
                                                        FROM ",selected_table$db,".",selected_table$table_name))
              column_info[[paste0(selected_table$db,".", selected_table$table_name,".",table_analysis$ideal_col_for_range_computation)]] <<- (list(table = selected_table, col_name = table_analysis$ideal_col_for_range_computation, info = info_IDXCOL))
            }
            
          }else{
            info_ROWID <- column_info[[paste0(selected_table$db,".", selected_table$table_name,".","ROWID")]]
            if(is.null(info_ROWID)){
              info_ROWID <- list()
              info_ROWID$range <- jdbc_cm$query(paste0("SELECT /*+ PARALLEL*/ MIN(","ROWID",") AS MIN, MAX(","ROWID",") AS MAX
                                                       FROM ",selected_table$db,".",selected_table$table_name))
              column_info[[paste0(selected_table$db,".", selected_table$table_name,".","ROWID")]] <<- (list(table = selected_table, col_name = "ROWID", info = info_ROWID))
            }
          }
          
          # re load
          info_ROWID <- column_info[[paste0(selected_table$db,".", selected_table$table_name,".","ROWID")]]
          info_IDXCOL <- column_info[[paste0(selected_table$db,".", selected_table$table_name,".",table_analysis$ideal_col_for_range_computation)]]
          
          # resource consuming
          if(!is.null(info_IDXCOL)){
            # min max based on index column
            rng_m <- jdbc_cm$query(paste0("SELECT /*+ PARALLEL*/ MIN(",column_name,") AS MIN FROM ",
                                          selected_table$db,".",selected_table$table_name,"  WHERE ",
                                          col_compare_transform(table_analysis$ideal_col_for_range_computation,
                                                                info_IDXCOL$info$range$MIN)))
            rng_M <- jdbc_cm$query(paste0("SELECT /*+ PARALLEL*/ MAX(",column_name,") AS MAX FROM ",
                                          selected_table$db,".",selected_table$table_name,"  WHERE ",
                                          col_compare_transform(table_analysis$ideal_col_for_range_computation,
                                                                info_IDXCOL$info$range$MAX)))
            info$range <- cbind(rng_m, rng_M)
            info$range_type <- "estimate_based_on_INDEX"
            
          }else{
            if(!is.null(info_ROWID)){
              # min max based on ROWID
              rng_m <- jdbc_cm$query(paste0("SELECT ",column_name," AS MIN FROM ",
                                            selected_table$db,".",selected_table$table_name,"  WHERE ROWID='", info_ROWID$info$range$MIN,"'"))
              rng_M <- jdbc_cm$query(paste0("SELECT ",column_name," AS MAX FROM ",
                                            selected_table$db,".",selected_table$table_name,"  WHERE ROWID='", info_ROWID$info$range$MAX,"'"))
              info$range <- cbind(rng_m, rng_M)
              info$range_type <- "estimate_based_on_ROWID"
            }else{
              
              if(table_analysis$is_job_split_required){
                # sample min max computation
                info$range <- jdbc_cm$query(paste0("SELECT /*+ PARALLEL*/ MIN(",column_name,") AS MIN, MAX(",column_name,") AS MAX
                                                   FROM ",selected_table$db,".",selected_table$table_name," SAMPLE (10)"))
                info$range_type <- "estimate_based_on_SAMPLE"
              }else{
                info$range <- jdbc_cm$query(paste0("SELECT /*+ PARALLEL*/ MIN(",column_name,") AS MIN, MAX(",column_name,") AS MAX
                                                   FROM ",selected_table$db,".",selected_table$table_name))
                info$range_type <- "exact"
              }
              
            }
          }
          
          
          
          if(info$db_info$NUM_DISTINCT < 200){
            
            info$distinct_values <- jdbc_cm$query(paste0("SELECT /*+ PARALLEL*/ DISTINCT ",column_name,"
                                 FROM ",selected_table$db,".",selected_table$table_name,""))
            
          }
          
          colnames(info$range) <- c("MIN", "MAX")
          
        }
        
        info$get_exact_range <-function(){
          jdbc_cm$query(paste0("SELECT /*+ PARALLEL*/ MIN(",column_name,") AS MIN, MAX(",column_name,") AS MAX
                                 FROM ",selected_table$db,".",selected_table$table_name))
          
        }
        
        L <- (list(table = selected_table, col_name = column_name, info = info))
        column_info[[paste0(selected_table$db,".", selected_table$table_name,".",column_name)]] <<- L
      }
      
    }, silent = T)
    
    
    return(L)
    
    
  }
  
  
  ###############################################################################################
  ###############################################################################################
  ###############################################################################################
  
  
  src_db_handle$table_catalogue <- function(reset = F, include_views = T){
    # ref https://www.oraclerecipes.com/sql/get-list-of-all-available-tables-in-oracle-database/
    if(is.null(all_tables) | reset){
      
      d <- jdbc_cm$query("SELECT owner, table_name FROM dba_tables")
      
      if(!is.data.frame(d)){
        d <- jdbc_cm$query("SELECT owner, table_name FROM ALL_TABLES")
      }
      
      if(!is.data.frame(d)){
        d <- jdbc_cm$query("SELECT table_name FROM user_tables")
      }
      
      if(include_views){
        dp <- jdbc_cm$query("SELECT owner, view_name as table_name FROM ALL_VIEWS")
        if(is.data.frame(dp) & is.data.frame(d)){
          dp$type <- "View"
          if(is.null(d$type)){
            d$type <- "Table"
          }
          d <- d %>% bind_rows(dp) %>% unique()
        }
      }
      
      
      if(is.data.frame(d)){
        
        if(!is.null(d$OWNER)){
          da<- d %>% group_by(OWNER) %>% summarise(num_tables = n()) %>% arrange(desc(num_tables))
          d$OWNER_fact <-  factor(d$OWNER,levels = da$OWNER)
          d<-d %>% arrange(OWNER_fact)
          d$OWNER_fact<-NULL
          #option for suggestion may be added here [ask user to switch to only specific owner/db etc]
        }else{
          d$OWNER <- "current_user"
        }
        
        colnames(d) <- c("db","table_name", "type")[seq(length(colnames(d)))]
        
        
        all_tables <<- d
      }
      
    }
    
    return(invisible(all_tables))
  }
  
  src_db_handle$table_catalogue_detailed <- function(reset = F, rows_split_lim = 10^6){
    
    if(is.null(all_tables_details) | reset){
      
      da <- jdbc_cm$query("SELECT * FROM ALL_TABLES")
      
      
      if(is.data.frame(da)){
        
        rows_job_split_lim <- 100*rows_split_lim
        
        # if more than 1 million then go for split
        da$is_split_required <- (da$NUM_ROWS > rows_split_lim)
        da$is_job_split_required <- (da$NUM_ROWS > rows_job_split_lim)
        
        da$is_temp_by_table_name<- da$TABLE_NAME %>% tolower() %>% str_detect("tmp|temp")
        
        
        all_tables_details <<- da
      }
      
    }
    
    return(invisible(all_tables_details))
    
  }
  
  src_db_handle$select_table <- select_table
  
  src_db_handle$get_selected_table <- function(){
    selected_table
  }
  
  src_db_handle$get_selected_table_description <- get_selected_table_description
  
  src_db_handle$get_hadoop_db_type_map <- get_hadoop_db_type_map
  
  src_db_handle$get_selected_table_row_count <- get_selected_table_row_count
  
  src_db_handle$get_selected_table_analysis <- get_selected_table_analysis
  
  src_db_handle$get_selected_table_import_plan <- get_selected_table_import_plan
  
  src_db_handle$get_sample_data <- function(){
    jdbc_cm$query(paste0("SELECT * FROM ", selected_table$db, ".",selected_table$table_name, " WHERE ROWNUM <= 20"))
  }
  
  src_db_handle$get_current_user <- function(){
    jdbc_cm$query("select user from dual")[[1]]
  }
  
  src_db_handle$col_compare_transform <- col_compare_transform
  
  src_db_handle$get_column_info_on_selected_table <- get_column_info_on_selected_table
  
  # may be this function can be kept outside the scope of this handle
  src_db_handle$get_sqoop_import_execution_plan <- get_sqoop_import_execution_plan
  
  src_db_handle$get_sqoop_import_execution_plan_manual <- get_sqoop_import_execution_plan_manual
  
  src_db_handle$get_sqoop_import_execution_plan_update <- get_sqoop_import_execution_plan_update
  
  src_db_handle$set_dba_table_stat_access_info <- set_dba_table_stat_access_info
  
  src_db_handle$show_dba_table_stat_access_info <- function(){
    dba_table_stat_access_info
  }
  
  ###############################################################################################
  
  
  return(src_db_handle)
  
}
