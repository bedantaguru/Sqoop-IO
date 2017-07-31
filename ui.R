
library(shiny)

library(DT)

library(shinythemes)

library(shinyBS)



tagList(
  themeSelector(),
  navbarPage("R Sqoop",
             tabPanel("Dashboard",uiOutput("dashboardUI")),
             tabPanel("Source Database",
                      ###########################################
                      ############### Source DB View ############
                      ###########################################
                      fluidRow(
                        column(6, h4("Operations"),
                               div(actionButton("formulate_sqoop_job_from_src_to_dst",label = "Formulate Sqoop job"),
                                   actionButton("run_this_sqoop_job_from_src_to_dst",label = "Run This Sqoop job")),
                               hr(),
                               verbatimTextOutput("formulate_sqoop_test")),
                        column(6, h4("Type table name or database name for filtering or selecting"),
                               textInput("filter_src_db_table_catalogue",label = "Filter"),
                               uiOutput("src_job_view"))
                      ),
                      hr(),
                      h2('Selected Table Details'),
                      
                      fluidRow(
                        column(4, h3("Selected Table"),
                               verbatimTextOutput('src_db_selected_table')),
                        column(8, h3("Selected Table Info"),
                               verbatimTextOutput('src_db_selected_table_details'))
                      ),
                      hr(),
                      fluidRow(
                        column(6, h3("Table Catalogue"),
                               dataTableOutput('src_db_table_catalogue')),
                        column(6, h3("Selected Table Description"),
                               dataTableOutput('src_db_selected_table_description'))
                      ),
                      hr(),
                      h2("Sample Data"),
                      fluidRow(
                        fluidRow(dataTableOutput('src_db_selected_table_sample'))
                      )
                      
             ),
             tabPanel("Destination Database",
                      ###########################################
                      ############# Destination Database ########
                      ###########################################
                      h2("Hadoop HDFS Info"),
                      actionButton('refresh_dst_hdfs_info',label = "Refresh"),
                      p(),
                      verbatimTextOutput('dst_hdfs_info'),
                      hr(),
                      h2("Hadoop DB View"),
                      fluidRow(
                        column(6, h3("Table Catalogue"),
                               dataTableOutput('dst_db_table_catalogue')),
                        column(6, h3("Selected Table Sample Data"),
                               dataTableOutput('dst_db_selected_table_sample'))
                      ),
                      p(),
                      h2("Operations"),
                      actionButton('delete_dst_data_and_table',label = "Delete HDFS Data and Table")
             ),
             navbarMenu("Options",
                        ###########################################
                        ################# Options### ##############
                        ###########################################
                        
                        tabPanel("Config View", 
                                 ###########################################
                                 ############# Configuration View ##########
                                 ###########################################
                                 titlePanel("Configuration View"),
                                 hr(),
                                 fluidRow(
                                   uiOutput("out_config_display")
                                 )),
                        tabPanel("Change Config", "Not Yet Implemented"))
  ),
  # Required for Plots/Tables [They will not grey out]
  tags$style(type="text/css",
             ".recalculating { opacity: 1.0; }"),
  # following is not working may be this is not the correct way
  tags$style(type="text/css",
             ".dataTables_scrollBody{}")
)
