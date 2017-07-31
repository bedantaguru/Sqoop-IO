# writing style code is to be done as follows
# backend <- RSqoop_backend()

# create a test job to check all connections
sqoop.test <- function(){
  backend$sqoop_builder$reset()
  
  backend$sqoop_builder$connect(backend$src_db$param$JDBC_connection_URL)
  backend$sqoop_builder$username(backend$src_db$param$username)
  backend$sqoop_builder$password(backend$src_db$param$password)
  backend$sqoop_builder$direct()
  backend$sqoop_builder$query(backend$src_db$param$test_query)
  backend$sqoop_builder$num_mappers(1)
  backend$sqoop_builder$as_textfile()
  backend$sqoop_builder$target_dir(backend$RSqoop$test_dir)
  backend$sqoop_builder$delete_target_dir()
  backend$sqoop_builder$outdir(backend$RSqoop$get_temp_dir_for_sqoop_codegen())
  
}

# for testing data transmission
sqoop.test_db <- function(){
  
  backend$sqoop_builder$reset()
  
  backend$sqoop_builder$connect(backend$src_db$param$JDBC_connection_URL)
  backend$sqoop_builder$username(backend$src_db$param$username)
  backend$sqoop_builder$password(backend$src_db$param$password)
  backend$sqoop_builder$direct()
  backend$sqoop_builder$query(query_test)
  backend$sqoop_builder$num_mappers(1)
  backend$sqoop_builder$as_parquetfile()
  backend$sqoop_builder$target_dir(backend$RSqoop$test_dir)
  backend$sqoop_builder$delete_target_dir()
  backend$sqoop_builder$null_non_string("\\\\N")
  backend$sqoop_builder$null_string("\\\\N")
  backend$sqoop_builder$outdir(backend$RSqoop$get_temp_dir_for_sqoop_codegen())
}

# single unit of data transmission
sqoop.main_db_no_split <- function(){
  backend$sqoop_builder$reset()
  
  # required for Java 
  # ref : https://community.hortonworks.com/questions/57497/sqoop-from-oracle-connection-reset-error.html
  backend$sqoop_builder$add_generic_cmd_arg(c('-D','mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom"'))
  
  backend$sqoop_builder$connect(backend$src_db$param$JDBC_connection_URL)
  backend$sqoop_builder$username(backend$src_db$param$username)
  backend$sqoop_builder$password(backend$src_db$param$password)
  backend$sqoop_builder$direct()
  backend$sqoop_builder$query(query)
  backend$sqoop_builder$num_mappers(1)
  backend$sqoop_builder$compress()
  backend$sqoop_builder$compression_codec("snappy")
  backend$sqoop_builder$as_parquetfile()
  backend$sqoop_builder$target_dir(hdfs_dest_folder)
  backend$sqoop_builder$append()
  backend$sqoop_builder$null_non_string("\\\\N")
  backend$sqoop_builder$null_string("\\\\N")
  backend$sqoop_builder$outdir(backend$RSqoop$get_temp_dir_for_sqoop_codegen())
  
}



sqoop.main_db_no_split_no_compress <- function(){
  backend$sqoop_builder$reset()
  
  # required for Java 
  # ref : https://community.hortonworks.com/questions/57497/sqoop-from-oracle-connection-reset-error.html
  backend$sqoop_builder$add_generic_cmd_arg(c('-D','mapred.child.java.opts="-Djava.security.egd=file:/dev/../dev/urandom"'))
  
  backend$sqoop_builder$connect(backend$src_db$param$JDBC_connection_URL)
  backend$sqoop_builder$username(backend$src_db$param$username)
  backend$sqoop_builder$password(backend$src_db$param$password)
  backend$sqoop_builder$direct()
  backend$sqoop_builder$query(query)
  backend$sqoop_builder$num_mappers(1)
  backend$sqoop_builder$as_parquetfile()
  backend$sqoop_builder$target_dir(hdfs_dest_folder)
  backend$sqoop_builder$append()
  backend$sqoop_builder$null_non_string("\\\\N")
  backend$sqoop_builder$null_string("\\\\N")
  backend$sqoop_builder$outdir(backend$RSqoop$get_temp_dir_for_sqoop_codegen())
  
}
