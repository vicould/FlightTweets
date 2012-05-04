package net.flighttweets.tweets;

import java.sql.*;
import java.util.ArrayList;
import java.util.Calendar;
import java.io.*;


//the code finds the geographic locations affected by the weather events
public class GeoAnalyzer
{
       public void geographicalAnalyzer()
       {
    	   	try{
    	   		
    	        Calendar cal1;
                cal1=Calendar.getInstance();
                //System.out.println("The start time is : "+cal1.getTime().toString());
                Long time;
                time=cal1.getTimeInMillis();
                
                
        	   	ArrayList<Integer> event= new ArrayList<Integer>();
                ArrayList<String> geographic_location = new ArrayList<String>();
                ArrayList<Integer> frequency= new ArrayList <Integer>();
    	   		
                ArrayList<Integer> event1= new ArrayList<Integer>();
                ArrayList<String> geographic_location1 = new ArrayList<String>();
                ArrayList<String> airline_name=new ArrayList<String>();
    	   		ArrayList<Integer> keyword= new ArrayList<Integer>();
    	   		
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
                      int keyword_id;
                      int i=0;
                      int a=0;
                      frequency.add(i,0);

                      while(rs2.next())
                      {
                    	  
                    	  tweet_identifier=rs2.getLong(1);		//stores the Tweet_id of the Tweet
                    	  event_id=rs2.getInt(3);				//stores the event_id of the weather event
                    	  keyword_id=rs2.getInt(2);				//stores the keyword_id of the tweet
                    	  
                    	  //if(event_id==1)
                    	  {
                    	  
                    	  //fetch the corresponding tweet from the tweets table
                      	  str1=conn.prepareStatement("SELECT * FROM TWEETS WHERE TWEET_ID =" + tweet_identifier);
                    	  rs1=str1.executeQuery();
                    	                    	  
                    	  if(!rs1.next())
                    	  {
                    		  continue;
                    	  }
                                    
                                                        	 
                          String strTweet=rs1.getString(4);			//stores the tweet content
                          String airline=rs1.getString(2);			//stores who tweeted the tweet
                     
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
                                                             
                                        	 //check for repeat locations for a particular event
                                        	 if(checkRepeat(geographic_location1, keyword, event1,airline_name,a,strCode,keyword_id,event_id, airline))
                                        	 	{	
                                        		 	geographic_location1.add(a,strCode);
                                    	  			keyword.add(a, keyword_id);
                                    	  			event1.add(a, event_id);		
                                    	  			airline_name.add(a,airline);
                                    	  			a++;
                                        	 	}
                                        	 
                                              if(checkRepeatRegion(event,geographic_location,event_id,strCode,i,frequency))
                                            		  {
                                            	  		event.add(i, event_id);		
                                            	  		geographic_location.add(i,strCode);                                   		
                                            	  		i++;
                                            	  		frequency.add(i,0);
                                            		  }
                                            
                                               break;
                                         }
                                     }
                                    rs.close();
                                    rs1.close();
                    	  }
                       }
                      
                      rs2.close();
                     conn.close();
                     // sort locations based on the number of times they were tweeted about(frequency count)
                     Integer temp;
                    for(int j=0;j<i;j++)
                     {
                     	for(int k=j+1;k<i;k++)
                     	{
                     		if(frequency.get(j)<frequency.get(k))
                     		{
                     			
                     			temp=frequency.get(j);                     			
                     			frequency.set(j,frequency.get(k));
                     			frequency.set(k,temp);
                     		
                     			
                     			temp=event.get(j);                     			
                     			event.set(j,event.get(k));
                     			event.set(k,temp);
                     		
                    			
                     		String temp1=geographic_location.get(j);                     			
                     			geographic_location.set(j,geographic_location.get(k));
                     			geographic_location.set(k,temp1);
                     	
                    	        }
                     	}
                     }
                     
                     

                     outputToFile(event,geographic_location,i, frequency, airline_name, event1, geographic_location1, keyword,a);			//print the geographic locations to file
                     System.out.println("Analyzer ended successfully");  	   
                     Runtime.getRuntime().gc();
               
                  // Long run=  Runtime.getRuntime().totalMemory()-Runtime.getRuntime().freeMemory();
                   //System.out.println("Allocated memory ="+run+"bytes"); 
                   Calendar cal;
                   cal=Calendar.getInstance();
                   //System.out.println("The end time is : "+cal.getTime().toString());
                 
                   Long time1;
                   time1=cal.getTimeInMillis();
                   
                   double time_sec=(double)(time1-time)/1000;
                   System.out.println("Time required to execute the geoAnalyzer module is : "+time_sec+" seconds");
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
       
       
       
       public static boolean checkRepeat(ArrayList<String> geographic_location1, ArrayList<Integer> keyword, ArrayList<Integer> event1,ArrayList<String> airline_name,int a,String strCode,int keyword_id,int event_id, String airline)
       {
    	   
    	   for(int n=0;n<a;n++)
    	   {
    		   if(event1.get(n)==event_id && geographic_location1.get(n).indexOf(strCode)!=-1 && keyword.get(n)==keyword_id && airline_name.get(n)==airline)
    		   {	
    			   
    			   return false;
    		   }
    	   }
    	   
    	   return true; 	   
       }
       
       
       
       
       //the method checks for duplicate airport/city names for a particular weather event
       public static boolean checkRepeatRegion(ArrayList<Integer> event,ArrayList<String> geographic_location,int event_id, String geoCode,int i, ArrayList<Integer> frequency)
       {
    	   int freq;
    	   for(int n=0;n<i;n++)
    	   {
    		   if(event.get(n)==event_id && geographic_location.get(n).indexOf(geoCode)!=-1)
    		   {	
    			   freq=frequency.get(n);		//if the location exists in the event ArrayList, increase it's frequency count
    			   freq=freq+1;
    			   frequency.set(n, freq);	   
    			   return false;
    		   }
    		   
    	   }
    	   return true;
    	      	   
       }


//the method gets the event names from the events table and writes the output to a file
public static void outputToFile(ArrayList<Integer> event,ArrayList<String> geographic_location,int i, ArrayList<Integer> frequency, ArrayList<String> airline_name, ArrayList<Integer> event1, ArrayList<String> geographic_location1, ArrayList<Integer> keyword, int a)
{
	try
	{
	
		  // Create file 
		  FileWriter fstream = new FileWriter("outGeoAnalyzer.txt");
		 
		  BufferedWriter out = new BufferedWriter(fstream);
			int n=0;
			int b=0;

			ArrayList<String> keyword_name=new ArrayList<String>();
	   		Connection conn=StorageManager.getInstance().getConnection();
            PreparedStatement str3=conn.prepareStatement("select * from events");
            ResultSet rs3=str3.executeQuery();
            String key;
            PreparedStatement str=conn.prepareStatement("select * from keywords");
            ResultSet rs=str.executeQuery();
            int cnt=0;
            while(rs.next())
            {
            	key=rs.getString(2);
            	keyword_name.add(cnt, key);
            	cnt++;
            }
            out.append("geoAnalyzer output \n\n");			
            out.append("The areas/ airport codes of areas affected by each event are found by the code.");			
            out.append("For each event, the locations are in descending order of the number of times they were tweeted about.");			
            out.append("Location outliers have been removed. Repeat Keyword+Tweeted by pairs have been removed \n\n\n");			
            
			
            //for each event, write the affected airport/city names to the outGeoAnalyzer.txt
            while(rs3.next())
            {
            int count=rs3.getInt(1);
            String events=rs3.getString(2);
            out.append("-----Areas/Airports affected by "+events+" -----\n\n");
           
           
            
         for(n=0;n<i;n++)
    		{
    
               	if(event.get(n)==count && frequency.get(n)>1)
    			{
             
               		out.append("Location: "+geographic_location.get(n)+ "   Number of Tweets: "+(frequency.get(n)+1)+"\n");
               		
        			
    			
    			for(b=0;b<a;b++)
    			{
    				if(event1.get(b)==count && geographic_location1.get(b).equals(geographic_location.get(n)))
    				{
    	    			
    					key=keyword_name.get(keyword.get(b));
    					out.append("Keyword: "+key+ "   Tweeted by: "+airline_name.get(b)+"\n");
       				}
    			}
    			
    			out.append("\n");
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