-- ref http://structureddata.org/2007/10/16/how-to-display-high_valuelow_value-columns-from-user_tab_col_statistics/ 
--  
--  display_raw.sql 
-- 
--  DESCRIPTION 
--    helper function to print raw representation of column stats minimum or maximum 
--   
--  Created by Greg Rahn on 2011-08-19. 
--  
CREATE OR replace FUNCTION DISPLAY_RAW (rawval RAW, 
                                        TYPE   VARCHAR2) 
RETURN VARCHAR2 
IS 
  cn  NUMBER; 
  cv  VARCHAR2(32); 
  cd  DATE; 
  cnv NVARCHAR2(32); 
  cr  ROWID; 
  cc  CHAR(32); 
  cbf BINARY_FLOAT; 
  cbd BINARY_DOUBLE; 
BEGIN 
    IF ( TYPE = 'VARCHAR2' ) THEN 
      dbms_stats.CONVERT_RAW_VALUE(RAWVAL, CV); 

      RETURN TO_CHAR(CV); 
    ELSIF ( TYPE = 'DATE' ) THEN 
      dbms_stats.CONVERT_RAW_VALUE(RAWVAL, CD); 

      RETURN TO_CHAR(TO_CHAR(CD, 'YYYY-MM-DD HH24:MI:SS')); 
    ELSIF ( TYPE = 'TIMESTAMP' ) THEN 
      dbms_stats.CONVERT_RAW_VALUE(RAWVAL, CD); 

      RETURN TO_CHAR(TO_CHAR(CD, 'YYYY-MM-DD HH24:MI:SS')); 
    ELSIF ( TYPE = 'NUMBER' ) THEN 
      dbms_stats.CONVERT_RAW_VALUE(RAWVAL, CN); 

      RETURN TO_CHAR(CN); 
    ELSIF ( TYPE = 'BINARY_FL' ) THEN 
      dbms_stats.CONVERT_RAW_VALUE(RAWVAL, CBF); 

      RETURN TO_CHAR(CBF); 
    ELSIF ( TYPE = 'BINARY_DO' ) THEN 
      dbms_stats.CONVERT_RAW_VALUE(RAWVAL, CBD); 

      RETURN TO_CHAR(CBD); 
    ELSIF ( TYPE = 'NVARCHAR2' ) THEN 
      dbms_stats.CONVERT_RAW_VALUE(RAWVAL, CNV); 

      RETURN TO_CHAR(CNV); 
    ELSIF ( TYPE = 'ROWID' ) THEN 
      dbms_stats.CONVERT_RAW_VALUE(RAWVAL, CR); 

      RETURN TO_CHAR(CR); 
    ELSIF ( TYPE = 'CHAR' ) THEN 
      dbms_stats.CONVERT_RAW_VALUE(RAWVAL, CC); 

      RETURN TO_CHAR(CC); 
    ELSE 
      RETURN NULL; 
    END IF; 
END; 
