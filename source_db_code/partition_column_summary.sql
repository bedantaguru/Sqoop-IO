SELECT us.TABLE_NAME, 
       uc.DATA_TYPE, 
       us.COLUMN_NAME, 
       us.PARTITION_NAME, 
       DISPLAY_RAW(us.LOW_VALUE, SUBSTR(uc.DATA_TYPE, 1, 9))  AS LOW_VALUE, 
       DISPLAY_RAW(us.HIGH_VALUE, SUBSTR(uc.DATA_TYPE, 1, 9)) AS HIGH_VALUE 
FROM   ALL_PART_COL_STATISTICS us 
       join ALL_TAB_COLUMNS uc 
         ON uc.OWNER = us.OWNER 
            AND uc.TABLE_NAME = us.TABLE_NAME 
            AND uc.COLUMN_NAME = us.COLUMN_NAME 
            AND us.OWNER = '@:db' 
            AND us.TABLE_NAME = '@:table' 
ORDER  BY uc.COLUMN_ID 