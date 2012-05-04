package net.flighttweets.tweets;

//import java.io.BufferedWriter;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterFactory;

import java.util.ArrayList;
//import java.io.FileWriter;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.Statement;

public class RelevanceChecker{
  
    public static void main(String [] args){
        try{
        	
        	/*long statusId = 23137289156689920L;
        	Twitter twitter = new TwitterFactory().getInstance();
        	Status status = twitter.showStatus(statusId);
        	System.out.println(status.getRetweetCount());
        	System.out.println(status.getCreatedAt());
        	
        	long inreplyID=status.getInReplyToStatusId();
        	System.out.println(inreplyID);
        	Status status1 = twitter.showStatus(inreplyID);
        	System.out.println(status1.getCreatedAt());*/
        	
            ArrayList<String> flightIDs = new ArrayList<String>();
            ArrayList<Integer> tweets1 = new ArrayList<Integer>();
            ArrayList<Integer> tweets2 = new ArrayList<Integer>();
            ArrayList<Float> relevancy = new ArrayList<Float>();
          
            flightIDs.add(0, "united");
            flightIDs.add(1, "SpiritAirlines");
            flightIDs.add(2, "SouthwestAir");
            flightIDs.add(3, "Delta");
            flightIDs.add(4, "AmericanAir");
            flightIDs.add(5,"JetBlue");
            
            //Connection connection= null;
          
            Connection connection = DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:9001/flightsdb","sa", "");
            //Connection connection = StorageManager.getInstance().getConnection();
           
            for(int i=0;i<6;i++){
                int j=0;
              
                String query1 = "SELECT TWEET_ID FROM TWEETS WHERE USERNAME='"+flightIDs.get(i)+"'";
              
                Statement selectstmt = connection.createStatement();
                ResultSet result1 = selectstmt.executeQuery(query1);
              
                while(result1.next())
                {
                    j++;
                }
                System.out.println(j);
                tweets1.add(i,j);
            }
          
            for(int i=0;i<6;i++)
            {int g=0;
                String query2 = "SELECT TWEET_ID FROM KW_TWEET";
                Statement selectstmt = connection.createStatement();
                ResultSet result2 = selectstmt.executeQuery(query2);
              
                while(result2.next())
                {
                    long twID=result2.getLong(1);
                    String query3 = "SELECT * FROM TWEETS WHERE TWEET_ID="+twID+"AND USERNAME='"+flightIDs.get(i)+"'";
                    Statement selectstmt2 = connection.createStatement();
                    ResultSet result3 = selectstmt2.executeQuery(query3);
                  
                    while(result3.next())
                    {
                    	Twitter twitter = new TwitterFactory().getInstance();
                    	Status status = twitter.showStatus(twID);
                    	long rtweet = status.getRetweetCount();
                    	String query4 = "INSERT INTO TWEETS";
                    			
                    	long inreplyID=status.getInReplyToStatusId();
                    	Status status1 = twitter.showStatus(inreplyID);
                    	java.util.Date replyDate = status1.getCreatedAt();
                    	String query5 = "INSERT INTO TWEETS";
                    	
                        g++;
                                         
                    }
                } System.out.println(g); tweets2.add(i,g);
            }
           
          
            for(int i=0;i<6;i++)
            {
                float x = tweets1.get(i);
                float y = tweets2.get(i);
              
                float z = (y/x)*100;
              
                relevancy.add(i,z);
                //System.out.println(i+" "+relevancy.get(i));
              
              
            }
          
          
          
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}