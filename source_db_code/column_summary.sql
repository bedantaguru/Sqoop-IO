--- taken from https://www.pythian.com/blog/oracle-internal-datatype-storage/  
--- ref https://mwidlake.wordpress.com/2010/01/03/decoding-high_value-and-low_value/ 
--- ref https://chandlerdba.com/2015/11/13/decoding-dba_tab_columns-high_value-and-low_value/
SELECT OWNER, 
       TABLE_NAME, 
       COLUMN_NAME, 
       DATA_TYPE, 
       DATA_LENGTH, 
       NULLABLE, 
       NUM_DISTINCT, 
       NUM_NULLS, 
       LAST_ANALYZED, 
       AVG_COL_LEN, 
       CHAR_LENGTH, 
       DISPLAY_RAW(LOW_VALUE, SUBSTR(DATA_TYPE, 1, 9))  AS LOW_VALUE, 
       DISPLAY_RAW(HIGH_VALUE, SUBSTR(DATA_TYPE, 1, 9)) AS HIGH_VALUE 
FROM   ALL_TAB_COLUMNS 
WHERE  OWNER = '@:db' 
       AND TABLE_NAME = '@:table' 
ORDER  BY COLUMN_ID 