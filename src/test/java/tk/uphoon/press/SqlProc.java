package tk.uphoon.press;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;

/**
 * 
 * @author mars
 *
 */
public class SqlProc {

    public static void main(String[] args) {
        SqlProc sti =   new SqlProc();
        sti.execute();
    }   
    
    /**
     *  
     */
    public void execute(){
        Connection con  = null;
        PreparedStatement   pstmt   =   null;
        PreparedStatement   pstmt2  =   null;
        
        StringBuffer sqlBuffer1 = new StringBuffer();
        sqlBuffer1.append(" DELETE FROM  TBLSIZE                                                                             \n");
        sqlBuffer1.append(" WHERE   TDATE =     TO_CHAR(SYSDATE,'YYYYMMDD')                                \n");


        StringBuffer sqlBuffer2 = new StringBuffer();
        sqlBuffer2.append(" INSERT INTO TBLSIZE                                                                             \n");
        sqlBuffer2.append("         (TDATE                                                                                  \n");
        sqlBuffer2.append("         ,TBLID                                                                                  \n");
        sqlBuffer2.append("         ,TBLDESC                                                                                \n");
        sqlBuffer2.append("         ,TBLROWS                                                                                \n");
        sqlBuffer2.append("         ,TBLRESERVED                                                                            \n");
        sqlBuffer2.append("         ,TBLDATA                                                                                \n");
        sqlBuffer2.append("         ,TBLINDEX_SIZE                                                                          \n");
        sqlBuffer2.append("         ,TBLUNUSED)                                                                            \n");
        sqlBuffer2.append("         SELECT   TO_CHAR(SYSDATE,'YYYYMMDD') TDATE                                                 \n");
        sqlBuffer2.append("         ,TB.TABLE_NAME AS TBLID                                                                 \n");
        sqlBuffer2.append("         ,NVL(TB.COMMENTS,'NOT REG') AS TBLDESC                                                      \n");
        sqlBuffer2.append("         ,NVL(TB.NUM_ROWS,0) AS TBLROWS                                                          \n");
        sqlBuffer2.append("         ,NVL(TB.MAX_EXTENTS,0) AS  TBLRESERVED                                                  \n");
        sqlBuffer2.append("         ,NVL(TI.SIZE_MB,0) AS TBLDATA                                                           \n");
        sqlBuffer2.append("         ,NVL(TI.SIZE_MB,0) AS TBLINDEX_SIZE                                                     \n");
        sqlBuffer2.append("         ,(NVL(TB.MAX_EXTENTS,0) - NVL(TI.SIZE_MB,0) - NVL(TI.SIZE_MB,0))   AS TBLUNUSED         \n");
        sqlBuffer2.append(" FROM (                                                                                          \n");
        sqlBuffer2.append("     SELECT T.TABLE_NAME,ROUND((T.MAX_EXTENTS/1000000),2) AS MAX_EXTENTS,T.NUM_ROWS,             \n");
        sqlBuffer2.append("            TC.COMMENTS                                                                          \n");
        sqlBuffer2.append("     FROM TABS T,(SELECT TABLE_NAME,COMMENTS                                                     \n");
        sqlBuffer2.append("                  FROM ALL_TAB_COMMENTS                                                          \n");
        sqlBuffer2.append("                  WHERE OWNER ='HIIS') TC                                                        \n");
        sqlBuffer2.append("     WHERE T.TABLESPACE_NAME LIKE 'HIIS%'                                                        \n");
        sqlBuffer2.append("         AND T.TABLE_NAME = TC.TABLE_NAME(+)                                                     \n");
        sqlBuffer2.append("     ) TB,                                                                                       \n");
        sqlBuffer2.append("     (                                                                                           \n");
        sqlBuffer2.append("     SELECT A.SEGMENT_NAME,                                                                      \n");
        sqlBuffer2.append("               ROUND(SUM(A.BYTES)/1024/1024) AS SIZE_MB,                                         \n");
        sqlBuffer2.append("               A.SEGMENT_TYPE                                                                    \n");
        sqlBuffer2.append("     FROM DBA_SEGMENTS A,                                                                        \n");
        sqlBuffer2.append("                 DBA_TABLES B                                                                    \n");
        sqlBuffer2.append("     WHERE A.SEGMENT_NAME = B.TABLE_NAME                                                         \n");
        sqlBuffer2.append("           AND A.SEGMENT_TYPE IN ('TABLE','TABLE PARTITION')                                     \n");
        sqlBuffer2.append("           AND A.OWNER = 'HIIS'                                                                  \n");
        sqlBuffer2.append("     GROUP BY A.SEGMENT_NAME, A.SEGMENT_TYPE                                                     \n");
        sqlBuffer2.append("     ) TS,                                                                                       \n");
        sqlBuffer2.append("     (                                                                                           \n");
        sqlBuffer2.append("     SELECT REPLACE(REPLACE(A.SEGMENT_NAME,'_PK',''),'PK_','') AS SEGMENT_NAME,                  \n");
        sqlBuffer2.append("                   ROUND(SUM(A.BYTES)/1024/1024) AS SIZE_MB                                      \n");
        sqlBuffer2.append("     FROM DBA_SEGMENTS A,                                                                        \n");
        sqlBuffer2.append("                 DBA_INDEXES B                                                                   \n");
        sqlBuffer2.append("     WHERE A.SEGMENT_NAME = B.INDEX_NAME                                                         \n");
        sqlBuffer2.append("           AND A.SEGMENT_TYPE IN ('INDEX','INDEX PARTITION')                                     \n");
        sqlBuffer2.append("           AND A.OWNER = 'HIIS'                                                                  \n");
        sqlBuffer2.append("           AND A.SEGMENT_TYPE = 'INDEX'                                                          \n");
        sqlBuffer2.append("     GROUP BY REPLACE(REPLACE(A.SEGMENT_NAME,'_PK',''),'PK_','')                                 \n");
        sqlBuffer2.append("     ) TI                                                                                        \n");
        sqlBuffer2.append(" WHERE TB.TABLE_NAME =   TS.SEGMENT_NAME(+)                                                      \n");
        sqlBuffer2.append(" AND TB.TABLE_NAME   =   TI.SEGMENT_NAME(+)                                                      \n");
        
        String jdbcUrl          =   "jdbc:oracle:thin:@10.216.80.220:1521:HIIS";
        String dbUserId         =   "hiis";
        String dbPassword   =   "hiis";

        try{
            Class.forName("oracle.jdbc.driver.OracleDriver");
            con = DriverManager.getConnection(jdbcUrl,dbUserId,dbPassword);
            pstmt   =   con.prepareStatement(sqlBuffer1.toString());
            pstmt2  =   con.prepareStatement(sqlBuffer2.toString());

            pstmt.executeUpdate(); 
            pstmt2.executeUpdate(); 
        }catch(Exception e){
            e.printStackTrace();
        }finally{
            try{
                if(pstmt != null){ pstmt.close();}
            }catch(Exception e){
                e.printStackTrace();
            }
            try{
                if(pstmt2 != null){ pstmt2.close();}
            }catch(Exception e){
                e.printStackTrace();
            }           
            try{
                if(con != null){ con.close();}
            }catch(Exception e){
                e.printStackTrace();
            }

        }
    }
}
