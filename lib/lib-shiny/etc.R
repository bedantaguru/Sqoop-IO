

config_diplay <- function(config){
  
  if(length(config)){
    
    config <- config %>% lapply(is.list) %>% unlist() %>% which() %>% config[.]
    
    if(length(config)){
      
      config_names <- config %>% names()
      
      display_list <- function(L){
        L <- L %>% unlist()
        tl <- tagList()
        for(i in 1:length(L)){
          tl <- c(tl,tagList(h4(span(code(names(L)[i])),
                                span(code(L[i]))),
                             hr()))
        }
        
        return(tl)
      }
      
      tl <- list()
      
      for(i in 1:length(config)){
        tl[[i]] <- tabPanel(config_names[i], display_list(config[[i]]))
      }
      
      return(tl)
      
    }
  }
  
  
  return(NULL)
  
}


list_to_text <- function(l, join_sep="\n"){
  if(is.list(l)){
    nclass <- laply(l, is.list)
    if(any(nclass)){
      ll <- l[which(nclass)]
      ll <- llply(ll, list_to_text)
      l[which(nclass)] <- ll
    }
    paste0(names(l)," : ", l %>% lapply(paste,collapse = ",") %>% unlist(), collapse = join_sep)
  }
}
