
# ref : https://hadoop.apache.org/docs/r1.0.4/api/org/apache/hadoop/io/compress/
# list of compresssion codecs
# org.apache.hadoop.io.compress.SnappyCodec
# org.apache.hadoop.io.compress.BZip2Codec
# org.apache.hadoop.io.compress.GzipCodec

require(stringr)


read_sqoop_guide <- function(sqoop_doc_url="https://sqoop.apache.org/docs/1.4.6/SqoopUserGuide.html", 
                             local_file_name = "resource/sqoop_r_guide_info"){
  
  
  
  
  get_possible_module_names <- function(){
    uheading_data$module_name %>% str_replace_all("[^a-zA-Z\\-]","") %>% tolower()
  }
  
  get_argument_info <- function(for_what = "sqoop-import"){
    
    take_only_data <- NULL
    
    try({
      
      for_what <- for_what %>% tolower()
      
      # derived 
      # take_only_module_num <- uheading_data$module_num[str_detect(uheading_data$module_name,take_only)]
      take_only_module_num <- uheading_data$module_num[(uheading_data$module_name %>% str_replace_all("[^a-zA-Z\\-]","") %>% tolower()) %in% for_what]
      
      take_only_data_ids <- which(utables_info$module_num == take_only_module_num)
      
      take_only_data <- utables_data_list[take_only_data_ids]
      
      take_only_data <- do.call("rbind", utables_data_list[take_only_data_ids])
      
    },silent = T)
    
    if(!is.data.frame(take_only_data)){
      take_only_data<-data.frame()
    }
    return(take_only_data)
  }
  
  save_info <- function(file_name = local_file_name){
    try(save(utables_data_list, utables_info, uheading_data, file = file_name),silent = T)
  }
  
  load_info <- function(file_name = local_file_name){
    try(load(file = file_name, envir = parent.env(environment())),silent = T)
  }
  
  
  if(!file.exists(local_file_name)){
    # try to read the info from web
    try({
      
      require(rvest)
      
      u <- sqoop_doc_url %>% read_html()
      
      uheading <-  u %>% html_nodes(".titlepage .title")
      
      uheading_data <- data.frame(module_name = uheading %>% html_text())
      uheading_data$module_num <- strsplit(uheading_data$module_name,"\\.") %>% lapply("[[", 1) %>% unlist()
      
      utables <- u %>% html_nodes(".table")
      
      find_parent_title <- function(xnode){
        title_name <- xnode %>% html_node(".title") %>% html_text()
        xnu <- xnode
        while((xnu %>% html_node(".title") %>% html_text())==title_name){
          xnu <- xnu %>% xml_parent()
        }
        parent_title_name <- xnu %>% html_node(".title") %>% html_text()
        
        module_num <- strsplit(parent_title_name,"\\.")[[1]][1]
        
        return(data.frame(title_name, parent_title_name, module_num))
      }
      
      utables_info <- ldply( utables, find_parent_title)
      
      utables_data_list <- utables %>% lapply(function(xn){
        (xn %>% html_nodes("div > table") %>% html_table())[[1]]
      })
      
      
    }, silent = T)
    
    save_info(file_name = local_file_name)
    
  }else{
    load_info(file_name = local_file_name)
  }
  
  handle <- list(load_info = load_info, 
                 save_info = save_info, 
                 get_possible_module_names = get_possible_module_names,
                 get_argument_info = get_argument_info)
  
  return(handle)
  
}


read_sqoop_arguments_from_guide <- function(for_module = "sqoop-import", detailed = F){
  
  guide_arg_handle <- read_sqoop_guide()
  
  arg_df <- guide_arg_handle$get_argument_info(for_what = for_module)
  
  arg_df$Argument <- arg_df$Argument %>% str_trim()
  
  for(i in seq(2,nrow(arg_df))){
    if(arg_df$Argument[i] %>% nchar() == 0){
      arg_df$Argument[i] <- arg_df$Argument[i-1]
    }
  }
  
  arg_df <- arg_df[ nchar(arg_df$Argument)>0, ]
  
  arg_df <- aggregate( arg_df["Description"], arg_df["Argument"], paste0, collapse = "\n")
  
  arg_df_with_arg <- arg_df[ arg_df$Argument %>% str_detect("<|\\("),]
  
  arg_df_with_arg$args <- arg_df_with_arg$Argument %>% str_split("<|\\(") %>% lapply(rev) %>% lapply("[[",1) %>% str_replace(">|\\)","")
  
  arg_df_with_arg$Argument <- arg_df_with_arg$Argument %>% str_split("<|\\(") %>% lapply("[[",1) %>% unlist()
  
  arg_df_without_arg <- arg_df[ !(arg_df$Argument %>% str_detect("<|\\(")),]
  
  arg_df_without_arg$args <- NA
  
  arg_df <- rbind(arg_df_with_arg, arg_df_without_arg)
  
  arg_df$implement_tags <- arg_df$Argument %>% str_split(",") %>% lapply(function(x) {
    y <- x %>% str_detect("--") %>% x[.] 
    if(length(y)>1) y <- y[1]
    y %>% paste0(collapse = "")
  }) %>% str_replace("--","")
  
  only_implement_args <- arg_df[ arg_df$implement_tags %>% nchar() > 0,]
  
  if(detailed){
    l <-list(args = only_implement_args, 
             all_args = arg_df)
    
    return(l)
  }else{
    return(only_implement_args)
  }
  
  
}



