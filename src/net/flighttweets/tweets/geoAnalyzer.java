package net.flighttweets.tweets;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.*;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.io.*;

public class geoAnalyzer
{

       public static void main(String args[])
       {
               ArrayList<String> airport_codes= new ArrayList<String>();
               ArrayList<String> airport_names = new ArrayList<String>();

               try{
               // Open the file that is the first command line parameter
                 FileInputStream fstream = new FileInputStream("airportCodes.txt");

                 // Get the object of DataInputStream
                 DataInputStream in = new DataInputStream(fstream);
                 BufferedReader br = new BufferedReader(new InputStreamReader(in));
                 String strLine;

                 int i=0;
                 int n;

                 while ((strLine = br.readLine()) != null)     //read the text from file and enter in array
                 {
                         int pos=strLine.indexOf(',');
                         airport_names.add(i, strLine.substring(0, pos));
                         airport_codes.add(i, strLine.substring(pos+1));
                         i=i+1;
                 }

                n=i;                   //n is the number of rows in the AIRPORT_CODES table
                i=0;

                Connection conn=StorageManager.getInstance().getConnection();

                       //conn.prepareStatement("DROP TABLE AIRPORT_CODES").execute();
                  conn.prepareStatement("CREATE TABLE PUBLIC.AIRPORT_CODES (AIRPORT_CODE VARCHAR(5), AIRPORT_NAME VARCHAR(25))").execute();
            i=0;
                       while (n>i) {
                               System.out.println(airport_codes.get(i));
                               System.out.println(airport_names.get(i));
               String insertQuery = "INSERT INTO AIRPORT_CODES(AIRPORT_CODE,AIRPORT_NAME) VALUES ('" + airport_codes.get(i) + "','" + airport_names.get(i) + "')";
               Statement insertStatement = conn.createStatement();
               insertStatement.execute(insertQuery);
               insertStatement.close();
           i++;
                       }




                       /*      PreparedStatement str=conn.prepareStatement("select * from kw_tweet");
                      ResultSet rs=str.executeQuery();
                      while(rs.next())
                      {
                              System.out.println(rs.getLong(1)+" "+rs.getLong(3));




                      }*/


      /*                PreparedStatement str1=conn.prepareStatement("select * from tweets");
                      ResultSet rs1=str1.executeQuery();

                      PreparedStatement str;
                      ResultSet rs;

                      while(rs1.next())
                      {
                               String strSource=rs1.getString(2);
                               String strTweet=rs1.getString(4);

                               str=conn.prepareStatement("select * from airport_codes");
                                     rs=str.executeQuery();
                                      while(rs.next())
                                      {
                                          String strName=rs.getString(2);
                                          String strCode=rs.getString(1);

                                          if(strTweet.toLowerCase().indexOf(strName.toLowerCase())!=-1||strTweet.toLowerCase().indexOf(strCode.toLowerCase())!=-1)
                                          {
                                                System.out.println(strName);
                                                System.out.println(strTweet);
                                                break;
                                          }


                                      }
                                     rs.close();
                      }
                      rs1.close();
                     conn.close();
*/
                      System.out.println("ended");

               }
               catch(Exception e)
               {
                       System.out.println("error");
                       e.printStackTrace();
               }

       }
}