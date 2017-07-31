
library(shiny)

library(DT)

library(shinyBS)

# DT options

options(
  DT.options = list(pageLength = 5, 
                    language = list(search = 'Filter:'), 
                    scrollX='100px', 
                    keys = TRUE,
                    sDom  = '<"top">lrt<"bottom">Bip',
                    deferRender = TRUE,
                    scrollY = 200,
                    scroller = TRUE,
                    colReorder = list(realtime = FALSE)
  )
)

function(input, output, session) {
  
  rv <- reactiveValues(config_loaded = F, submitted_jobs = NULL, active_jobs_to_display = F)
  
  ########################################
  ############# Dashboard ################
  ########################################
  
  output$dashboardUI <- renderUI({
    if(rv$config_loaded){
      h3("Loading Done ...")
    }else{
      list(
        actionButton("load_r_sqoop_backend_config", label = "Load Configurations"),
        
        conditionalPanel( condition = "input.load_r_sqoop_backend_config == 1", 
                          hr(),
                          actionButton("load_r_sqoop_backend", label = "Load Backends"))
      )
    }
    
  })
  
  
  observe({
    if(!rv$config_loaded){
      invalidateLater(1000)
      if(length(rsb)){
        rv$config_loaded <<- T
      }
    }
  })
  
  ########################################
  ########### SRC DB view ################
  ########################################
  
  #table_catalogue
  output$src_db_table_catalogue <- renderDataTable({
    tbl <- NULL
    if(rv$config_loaded){
      if(is.function(rsb$src_db$table_catalogue)){
        if(!is.null(input$filter_src_db_table_catalogue)){
          if(nchar(input$filter_src_db_table_catalogue)>0){
            rsb$src_db$select_table(input$filter_src_db_table_catalogue)
          }else{
            rsb$src_db$table_catalogue(reset = T)
          }
        }
        tbl <- rsb$src_db$table_catalogue()
      }else{
        showNotification("Load Backends", duration = 1, closeButton = F)
      }
    }
    tbl
  }, 
  selection = "single", rownames= FALSE, style = 'bootstrap',
  filter = 'top',extensions = c('ColReorder','Scroller'), options = list(scrollY = 200))
  
  # select table
  observe({
    if(!is.null(input$filter_src_db_table_catalogue)){
      if(length(input$filter_src_db_table_catalogue)){
        if(is.function(rsb$src_db$table_catalogue)){
          tbl <- rsb$src_db$table_catalogue()
        }
      }
    }
    
  })
  
  # description table
  output$src_db_selected_table_description <- renderDataTable({
    # reactive dendency
    if(!is.null(input$src_db_table_catalogue_rows_selected)){
      if(length(input$src_db_table_catalogue_rows_selected)){
        rsb$src_db$select_table(input$src_db_table_catalogue_rows_selected)
      }
    }
    
    tbl <- NULL
    if(is.function(rsb$src_db$get_selected_table_description)){
      tbl <- rsb$src_db$get_selected_table_description()
    }
    
    if(is.null(tbl)){
      showNotification("Select a Table", duration = 1, closeButton = F)
    }
    
    tbl
  }, 
  selection = "single", rownames= FALSE, style = 'bootstrap',
  extensions = c('ColReorder','Scroller'), options = list(scrollY = 250))
  
  # Sample data 
  output$src_db_selected_table_sample <- renderDataTable({
    
    selection_done<-F
    
    if(!is.null(input$src_db_table_catalogue_rows_selected)){
      if(length(input$src_db_table_catalogue_rows_selected)){
        selection_done <-T
      }
    }
    
    tbl <- NULL
    if(selection_done){
      if(is.function(rsb$src_db$get_sample_data)){
        tbl <- rsb$src_db$get_sample_data()
      }
    }
    
    tbl
  },
  rownames= FALSE, style = 'bootstrap', options = list(scrollY = 350))
  
  
  output$src_db_selected_table <- renderText({
    if(rv$config_loaded){
      input$src_db_table_catalogue_rows_selected
      list_to_text(rsb$src_db$get_selected_table())
    }
  })
  
  output$src_db_selected_table_details <- renderText({
    input$src_db_table_catalogue_rows_selected
    if(rv$config_loaded){
      list_to_text(rsb$executor$simple_table_summary(rsb$src_db$get_selected_table()$table))
    }
  })
  
  # formulate as sqoop code
  output$formulate_sqoop_test <- renderText({
    sqoop_cmd <- NULL
    if(rv$config_loaded){
      input$formulate_sqoop_job_from_src_to_dst
      if(input$formulate_sqoop_job_from_src_to_dst>0){
        isolate({
          e <- rsb$executor$get_plan(rsb$src_db$get_selected_table()$table)
          disp_inf <- e$import_profile
          disp_inf$analysis_info$TA$analysis_data <- NULL
          disp_inf$update_check <- NULL
          disp_inf$analysis_info$TA$index_cols <- NULL
          sqoop_cmd <- list_to_text(disp_inf)
        })
      }
    }
    sqoop_cmd
  })
  
  observe({
    if(rv$config_loaded){
      if(input$run_this_sqoop_job_from_src_to_dst>0){
        
        isolate({
          e <- rsb$executor$get_plan(rsb$src_db$get_selected_table()$table)
          if(!e$is_valid){
            showNotification("Invalid Plan", duration = 2, closeButton = F)
          }else{
            job_new <- data.frame(db = e$import_profile$db_name, table_name = e$import_profile$table_name, job_style = e$import_profile$job_style)
            put_this <- F
            
            if(is.null(rv$submitted_jobs)){
              rv$submitted_jobs <<- job_new
              put_this <- T
            }else{
              old_job <- rv$submitted_jobs
              old_job <- old_job[ old_job$db == e$import_profile$db_name & old_job$table_name == e$import_profile$table_name,]
              if(nrow(old_job)){
                if(rsb$executor$execute$check_plan_present(e$exe_plans)){
                  put_this <- F
                  showNotification("Job Present Not Submitting Plan", duration = 2, closeButton = F)
                }else{
                  put_this <- F
                  showNotification("Job Present in Cache but Not Submitting Plan", duration = 5, closeButton = F)
                }
              }else{
                rv$submitted_jobs <- rbind(rv$submitted_jobs, job_new)
                put_this <- T
              }
            }
            
            if(put_this){
              showNotification("Submitting Plan", duration = 2, closeButton = F)
              rsb$executor$run_plan(e)
            }
          }
        })
      }
    }
  })
  
  
  observe({
    if(!is.null(rv$submitted_jobs)){
      if(nrow(rv$submitted_jobs)){
        rv$active_jobs_to_display <- T
      }else{
        rv$active_jobs_to_display <- F
      }
    }
  })
  
  output$src_job_view <- renderUI({
    # depend on this
    rv$submitted_jobs
    if(!rv$active_jobs_to_display){
      return(NULL)
    }else{
      div(
        dataTableOutput("src_job_table_view"),
        div(actionButton("src_job_log", "Logs"), actionButton("src_job_kill", "Kill this job")),
        bsModal("modal_src_job_log", "Job Console Log", "src_job_log", size = "large",
                verbatimTextOutput("src_job_log_txt"))
      )
    }
  })
  
  output$src_job_table_view <- renderDataTable({
    rv$submitted_jobs
  },
  selection = "single", rownames= FALSE, style = 'bootstrap', options = list(scrollY = "auto"))
  
  output$src_job_log_txt <- renderText({
    invalidateLater(2000)
    input$src_job_table_view_rows_selected
    if(input$src_job_log > 1 ){
      tar <- (input$src_job_table_view_rows_selected)
      if(is.null(tar)){
        tar <- nrow(rv$submitted_jobs)
      }
      if(tar<1){
        tar <- NULL
      }
      if(!is.null(tar)){
        this_job <- rv$submitted_jobs[tar,]
        if(nrow(this_job)==1){
          procs <- rsb$sqoop$access$get_process_view()
          procs$arguments %>% str_detect(paste0(" ",this_job$db,".", this_job$table_name, " " )) %>% which() %>% max() %>% rsb$sqoop$access$select_process()
          rsb$sqoop$access$get_log()
        }
      }
    }
  })
  
  ########################################
  ########### Job Operations #############
  ########################################
  
  observe({
    if(!is.null(rv$submitted_jobs)){
      if(nrow(rv$submitted_jobs)){
        invalidateLater(3000)
        if(!rsb$executor$execute$all_job_done()){
          rsb$executor$execute$next_stage()
        }
      } 
    }
  })
  
  ########################################
  ########### DST DB view ################
  ########################################
  
  # HDFS Info
  output$dst_hdfs_info <-renderText({
    if(rv$config_loaded){
      input$refresh_dst_hdfs_info
      files_in_folder <- rsb$hdfs$list.files(rsb$RSqoop$hdfs_dest_dir)
      if(!is.null(rsb$src_db$get_selected_table())){
        files_in_folder <- rsb$hdfs$list.files(file.path(rsb$RSqoop$hdfs_dest_dir, rsb$src_db$get_selected_table()$db))
      }
      files_in_folder$file %>% paste0(collapse = "\n")
    }
  })
  
  #table_catalogue
  output$dst_db_table_catalogue <- renderDataTable({
    tbl <- NULL
    if(rv$config_loaded){
      rsb$hadoop_db$Impala$reconnect()
      if(rsb$hadoop_db$Impala$test_query()){
        rsb$hadoop_db$Impala$query(paste("USE", rsb$RSqoop$hadoop_dest_db_name),is_DDL = T)
        tbl <- rsb$hadoop_db$Impala$query("SHOW TABLES")
      }else{
        showNotification("Load Backends", duration = 1, closeButton = F)
      }
    }
    tbl
  }, 
  selection = "single", rownames= FALSE, style = 'bootstrap')
  
  
  # Sample data 
  output$dst_db_selected_table_sample <- renderDataTable({
    
    selection_done<-F
    
    if(!is.null(input$dst_db_table_catalogue_rows_selected)){
      if(length(input$dst_db_table_catalogue_rows_selected)){
        selection_done <-T
      }
    }
    
    tbl <- NULL
    if(selection_done){
      rsb$hadoop_db$Impala$query(paste("USE", rsb$hadoop_dest_db_name),is_DDL = T)
      all_tbls<- rsb$hadoop_db$Impala$query("SHOW TABLES")
      tbl <- rsb$hadoop_db$Impala$query(paste0("SELECT * FROM ", all_tbls$name[input$dst_db_table_catalogue_rows_selected]," LIMIT 5"))
    }
    
    tbl
  },
  rownames= FALSE, style = 'bootstrap')
  
  
  # cleaning of small tables
  observe({
    if(rv$config_loaded){
      input$delete_dst_data_and_table
      isolate({
        selected_table <- input$dst_db_table_catalogue_rows_selected
        if(!is.null(selected_table)){
          if(length(selected_table)==1){
            # check of number of rows
            rsb$hadoop_db$Impala$query(paste("USE", rsb$hadoop_dest_db_name),is_DDL = T)
            all_tbls<- rsb$hadoop_db$Impala$query("SHOW TABLES")
            num_rows <- rsb$hadoop_db$Impala$query(paste0("SELECT count(1) FROM ", all_tbls$name[selected_table]))[[1]]
            if(num_rows<10^5){
              try({
                selected_table <- all_tbls$name[selected_table]
                table_location <- rsb$hadoop_db$Hive$query(paste0("show create table ", rsb$RSqoop$hadoop_dest_db_name,".",selected_table))
                table_location <- table_location[[1]]
                table_location <- table_location[((table_location %>% tolower() == "location") %>% which()+1)] %>% str_trim()
                table_location <- table_location %>% str_replace_all("'","") %>% str_split(rsb$RSqoop$hdfs_dest_dir) %>% unlist() %>% .[2] %>% file.path(rsb$RSqoop$hdfs_dest_dir, .)
                rsb$hdfs$clean(table_location)
                rsb$hadoop_db$Impala$query(paste0("DROP TABLE ", rsb$RSqoop$hadoop_dest_db_name,".",selected_table),is_DDL = T)
              }, silent = T)
              
            }else{
              showNotification("Only small tables can be deleted from here. Rest tables need to be cleaned manually.", duration = 5, closeButton = F)
            }
          }
        }
      })
    }
  })
  
  
  ########################################
  ########### config view ################
  ########################################
  observe({
    if(!is.null(input$load_r_sqoop_backend_config)){
      if(input$load_r_sqoop_backend_config==1){
        rs$RSqoop_load_configs()
      }
    }
  })
  
  observe({
    
    # create a progressbar
    
    if(!is.null(input$load_r_sqoop_backend)){
      
      if(input$load_r_sqoop_backend==1){
        
        progress <- Progress$new()
        progress$set(message = "Loading Backends", value = 0)
        
        # Close the progress when this reactive exits (even if there's an error)
        on.exit(progress$close())
        
        rsb <<- rs$RSqoop_backend(progress$set)
        
      }
    }
    
  })
  
  output$out_config_display <- renderUI({
    if(!is.null(input$load_r_sqoop_backend_config)){
      input$load_r_sqoop_backend_config
      tl <- config_diplay(rs$get_run_time_config())
      if(!is.null(tl)){
        tagList(do.call( navlistPanel, tl),
                hr())
      }
    }
  })
  
  
}