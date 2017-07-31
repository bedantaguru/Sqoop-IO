
store <- function(what, value, key, method = c("session","local"), local_path = ".store"){
  method <- match.arg(method)
  
  if(method == "session"){
    lex <-options("RSESSION_STORE")[[1]]
  }
  
  if(method == "local"){
    if(file.exists(local_path)){
      lex <- readRDS(local_path)
    }else{
      lex <-NULL
    }
  }
  
  
  
  if(!missing(key)){
    value_new <- value
    # data frame types
    if(!is.data.frame(value_new)){
      warning("If key is specified only data.frame is supported")
    }else{
      if(!(key %in% colnames(value_new))){
        warning("key is not present in data.frame")
      }else{
        value_old <- lex[[what]]
        if(!is.null(value_old)){
          value_old_keys <- value_old[[key]]
          value_new_keys <- value_new[[key]]
          value_old_keys_take <- setdiff(value_old_keys, value_new_keys)
          value_old <- value_old[ value_old[[key]] %in% value_old_keys_take, ]
          value <- rbind(value_old, value_new)
        }
      }
    }
    
  }
  
  
  lex[[what]] <- value
  
  if(method == "session"){
    options(RSESSION_STORE=lex)
  }
  
  if(method == "local"){
    saveRDS(object = lex, file = local_path)
  }
  
  return(invisible())
}

retrieve <- function(what, method = c("session","local"), local_path = ".store"){
  method <- match.arg(method)
  
  if(method == "session"){
    lex <-options("RSESSION_STORE")[[1]]
  }
  
  if(method == "local"){
    if(file.exists(local_path)){
      lex <- readRDS(local_path)
    }else{
      lex <-NULL
    }
  }
  
  lex[[what]]
  
}

