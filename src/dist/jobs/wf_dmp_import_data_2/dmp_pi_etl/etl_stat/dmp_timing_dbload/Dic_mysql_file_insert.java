import com.mysql.jdbc.Driver.*;

import java.io.*;
import java.sql.*;
import java.lang.*;
import java.util.*;


public class Dic_mysql_file_insert {
        public static void main(String[] args) {

                Connection con = null;
                Statement stmt = null;
                int rs = 0;

                try 
                {

                    Class.forName("com.mysql.jdbc.Driver");

                }//end try 
                catch (ClassNotFoundException e) 
                {
                    e.printStackTrace();
                    System.exit(1);
                }//end catch

                try 
                {

                    con = DriverManager.getConnection("jdbc:mysql://172.22.136.50:3306/dmp","dmpuser", "!dmp.ypco6#");
                    stmt = con.createStatement();
                    System.out.println("Mysql conn OK");

                }//end try 
                catch (SQLException e)  
                {
                    e.printStackTrace();
                    System.exit(1);
                }//end catch

                try 
                {
                    String sqlbase1 = "insert into data_source_etl_duration_statistics(date,data_source_id,start_time,end_time,duration_min) values(";                    
                    

                    File[] allFiles = new File(args[0]).listFiles();
                    
                    for(int i=0; i < allFiles.length; i++)
                    {



                           System.out.println("qualified=" + allFiles[i].getName());


                           FileReader fr = new FileReader(args[0]+"/"+allFiles[i].getName());

                           BufferedReader br = new BufferedReader(fr);

                           String s = new String();

                           while((s = br.readLine()) != null) //for every line of opened file
                           {                            

                              String sql = sqlbase1;                           
 
                              sql = sql + s + ")";

                              System.out.println(sql);

                              stmt.addBatch(sql);


                           }//end while

                           br.close();
                           fr.close();



                    }//end for all files in folder


                    System.out.println("executeBatch");
                    stmt.executeBatch();

                    stmt.close();
                    con.close();


 
                }//end try 
                catch(Exception e ) 
                {
                    e.printStackTrace();
                    System.exit(1);
                }//end catch

        }//end main




}//end class




