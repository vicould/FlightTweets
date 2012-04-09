package net.flighttweets.tweets;

import java.io.BufferedWriter;
import java.util.regex.*;
import java.io.FileWriter;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;

public class FlightNumberAnalyzer{
    
    /**
     *Method to analyze tweets in KW_TWEET table for flight number occurrence.
     *@return A text file with all the tweets which specify a particular flight.
     */
    
    public static void TweetsWithFlight()
    {
        try
        {
            // Gets TWEET_ID from KW_TWEET which is used to extract corresponding tweets from TWEETS table
             
            String getTID = "SELECT TWEET_ID FROM KW_TWEET";
            Connection connection = StorageManager.getInstance().getConnection();
            Statement stmt = connection.createStatement();
            ResultSet results = stmt.executeQuery(getTID);
            
            while(results.next())
            {
                
                long twID = results.getLong(1);
                String query = "SELECT TWEET FROM TWEETS WHERE TWEET_ID="+twID;
                Statement selectstmt = connection.createStatement();
                ResultSet result = selectstmt.executeQuery(query);
                
                while(result.next())
                {
                    //Gets the tweet into a string
                    String tweet = result.getString(1);
                    //Calls a method that checks for flight number occurrence
                    patternMatch(tweet);
                }
            }

        
        }
        catch (Exception e)
        {
            e.printStackTrace();
        }
        
        
    }
    
    public static void patternMatch(String tweet)
    {
        /* Method to check tweets for occurence of flight number
         */
        try
        {
            //Regular expression to pattern match occurrence of flight number
            String regex = "[A-Z]{1,2}[0-9]{1,4}";
            Pattern pattern = Pattern.compile(regex);
            Matcher matcher = null;
            matcher = pattern.matcher(tweet);
            
            //Initialize FileWriter and BufferedWriter for writing into a text file
            FileWriter fstream = new FileWriter("outFLTAnalyzer.txt");
            BufferedWriter out = new BufferedWriter(fstream);
            
            if(matcher.matches()){
                //Appending the tweet to output text file if flight number occurs in the tweet
                out.append(tweet);
            } out.close();
        }
        catch(Exception e)
        {
            e.printStackTrace();
        }
    }
}