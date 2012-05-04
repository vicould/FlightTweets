package net.flighttweets.tweets;

import java.sql.*;
import java.util.ArrayList;
import java.io.*;


//the code finds the geographic locations affected by the weather events
public class GeoAnalyzer
{

       public void geographicalAnalyzer()
       {
    	   	try{

        	ArrayList<Integer> event= new ArrayList<Integer>();
                ArrayList<String> geographic_location = new ArrayList<String>();
    	   		
                Connection conn=StorageManager.getInstance().getConnection();
    	   		
    	   		//fetch rows from the intermediate table kw_tweets
                PreparedStatement str2=conn.prepareStatement("select * from kw_tweet");
                ResultSet rs2=str2.executeQuery();
             
                      PreparedStatement str;
                      PreparedStatement str1;
                      ResultSet rs;
                      ResultSet rs1;
                      Long tweet_identifier;
                      int event_id;
                      int i=0;

                      while(rs2.next())
                      {
                    	  tweet_identifier=rs2.getLong(1);		//stores the Tweet_id of the Tweet
                    	  event_id=rs2.getInt(3);				//stores the event_id of the weather event
                    	  
                    	  //fetch the corresponding tweet from the tweets table
                      	  str1=conn.prepareStatement("SELECT * FROM TWEETS WHERE TWEET_ID =" + tweet_identifier);
                    	  rs1=str1.executeQuery();
                    	                    	  
                    	  if(!rs1.next())
                    	  {
                    		  continue;
                    	  }
                                    
                              
                    	//  String strSource=rs1.getString(2);                          	 
                          String strTweet=rs1.getString(4);
                          
                              rs1.close();
                              //fetch rows from the airport_codes table
                              str=conn.prepareStatement("select * from airport_codes");
                                    rs=str.executeQuery();
                                     while(rs.next())
                                     {
                                         String strName=rs.getString(2);
                                         String strCode=rs.getString(1);
                                         
                                         //check if the tweet contains a city name or airport code
                                         if(strTweet.indexOf(strName)!=-1||strTweet.indexOf(strCode)!=-1)
                                         {
                                              // System.out.println(strName);
                                        	 //  System.out.println(strTweet);
                                               
                                              if(checkRepeatRegion(event,geographic_location,event_id,strName,i))
                                            		  {
                                            	  		event.add(i, event_id);		
                                            	  		geographic_location.add(i, strName);
                                            	  		i++;
                                            		  }
                                            
                                               break;
                                         }
                                     }
                                    rs.close();
                                    rs1.close();
                       }
                      
                      rs2.close();
                     conn.close();
                     outputToFile(event,geographic_location,i);			//print the geographic locations to file
                     System.out.println(i);  	   	
    	   	}
    	   	catch(Exception e){
                System.out.println("error");
                e.printStackTrace();
    	   		
    	   	}

       }

       
       //this method creates and populates the airport_codes table
       public void populateAirport_Codes(){

    	   	ArrayList<String> airport_codes= new ArrayList<String>();
            ArrayList<String> airport_names = new ArrayList<String>();

            try{
            // Open the file that contains the city names/airport names and codes
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

    	   	}
            catch(Exception e)
            {
                System.out.println("error");
                e.printStackTrace();
     	
            }

       
       }
       
       
       
       //the method checks for duplicate airport/city names for a particular weather event
       public static boolean checkRepeatRegion(ArrayList<Integer> event,ArrayList<String> geographic_location,int event_id, String geoName,int i)
       {
    	   
    	   for(int n=0;n<i;n++)
    	   {
    		   if(event.get(n)==event_id && geographic_location.get(n).indexOf(geoName)!=-1)
    		   {
    			   return false;
    		   }
    		   
    	   }
    	   return true;
    	      	   
       }


//the method gets the event names from the events table and writes the output to a file
public static void outputToFile(ArrayList<Integer> event,ArrayList<String> geographic_location,int i)
{
	try
	{
	
		  // Create file 
		  FileWriter fstream = new FileWriter("outGeoAnalyzer.txt");
		 
		  BufferedWriter out = new BufferedWriter(fstream);
			int n=0;


	   		Connection conn=StorageManager.getInstance().getConnection();
            PreparedStatement str3=conn.prepareStatement("select * from events");
            ResultSet rs3=str3.executeQuery();
            //for each event, write the affected airport/city names to the outGeoAnalyzer.txt
            while(rs3.next())
            {
            int count=rs3.getInt(1);
            String events=rs3.getString(2);
            out.append("----- Areas/ Airports affected by "+events+" -----\n\n");
           
            for(n=0;n<i;n++)
    		{
    
               	if(event.get(n)==count)
    			{
    			out.append(geographic_location.get(n)+ "\n");
    			}
    		
       		}
            
            }
	
	out.close();
	
	}
	catch(Exception e)
	{
        System.out.println("error");
        e.printStackTrace();
		
	}
}


}