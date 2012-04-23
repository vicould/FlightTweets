package net.flighttweets.tweets;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;

public class TweetFilter {

	/**
	 * Takes a list of tweets and keywords, and provides a database link for each. Executes as a batch the insertion for optimization reasons.
	 * @param statusesToSave A list of statuses.
	 */

         public static void populateEventList(List<String> eventList) {
             //event name;start-date;end-date
             int i = 0;
             try {
                 Connection connection = StorageManager.getInstance().getConnection();
                 String countQuery = "SELECT event_id FROM events";
                 Statement countStmt = connection.createStatement();
                 ResultSet rsc = countStmt.executeQuery(countQuery);
                 int minInt = -1;
                 while (rsc.next()) {
                     int testInt = rsc.getInt("event_id");
                     if (testInt > minInt) {
                         minInt = testInt;
                     }
                     
                 }
                 minInt = minInt + 1;
                 for (String word : eventList) {
                     String[] tokens = word.split(";");
                     String name = tokens[0];
                     String startDate = tokens[1];
                     String endDate = tokens[2];
                     String checkQuery = "SELECT event_id FROM EVENTS WHERE EVENT_NAME='" + name + "'";
                     Statement checkStmt = connection.createStatement();
                     ResultSet rs = checkStmt.executeQuery(checkQuery);
                     if (rs.next()) {
                         continue;
                     }
                     String insertQuery = "INSERT INTO EVENTS (event_id,event_name,start_date,end_date) VALUES (" + minInt +",'" + name + "','" + startDate + "','" + endDate + "')";
                     Statement insertStmt = connection.createStatement();
                     insertStmt.execute(insertQuery);
                     minInt++;
                 }
             } catch (Exception e) {
                 e.printStackTrace();
             }
         }
    
         public void populateKeywordList(List<String> keywordList) {
             int i = 0;
             try {
                Connection connection = StorageManager.getInstance().getConnection();                 
                for (String word : keywordList) {
                    
                    //do not insert keywords which are already there.
                    String checkQuery = "SELECT keyword_id FROM KEYWORDS WHERE word='" + word + "'";
                    Statement checkStmt = connection.createStatement();
                    ResultSet rs = checkStmt.executeQuery(checkQuery);
                    if (rs.next()) {
                        i++;
                        continue;
                    }
                    String query = "INSERT INTO KEYWORDS (keyword_id, word) VALUES (" + i + ",'" + word + "')";
                    Statement insertStmt = connection.createStatement();
                    insertStmt.execute(query);
                    i++;
                    insertStmt.close();
                }
                //stmt.close();
             } catch (Exception e) {
                 e.printStackTrace();
             }
         }
	public void filterTweets(List<String> keywordList) {
            this.populateKeywordList(keywordList);
		try {
                    Connection connection = StorageManager.getInstance().getConnection();
                   
                        for (String string : keywordList) {
                        //get keyword id
                            String keywordQuery = "SELECT keyword_id FROM KEYWORDS WHERE WORD = '" + string + "'";
                            //System.out.println(keywordQuery);
                            Statement kwStatement = connection.createStatement();
                            ResultSet rs1 = kwStatement.executeQuery(keywordQuery);
                            if (!rs1.next()) {
                                continue;
                            }
                            int kw_id = rs1.getInt("keyword_id");
                            String likeClause = "TWEETS.TWEET LIKE '%" + string + "%' AND TWEETS.CREATED > EVENTS.START_DATE AND TWEETS.CREATED < EVENTS.END_DATE";
                            String retrieveQuery = "SELECT TWEETS.TWEET_ID,EVENTS.EVENT_ID FROM TWEETS FULL JOIN EVENTS ON 1=1 WHERE " + likeClause;
                            //System.out.println(retrieveQuery);
                            Statement fetchStatement = connection.createStatement();
                            ResultSet rs2 = fetchStatement.executeQuery(retrieveQuery);
                            while (rs2.next()) {
                                long tweet_id = rs2.getLong("TWEETS.TWEET_ID");
                                long event_id = rs2.getLong("EVENTS.EVENT_ID");
                                String checkQuery = "SELECT * FROM KW_TWEET WHERE TWEET_ID=" + tweet_id + " AND KEYWORD_ID=" + kw_id + " AND EVENT_ID=" + event_id;
                                Statement checkStatement = connection.createStatement();
                                ResultSet crs = checkStatement.executeQuery(checkQuery);
                                if (crs.next()) {
                                    continue;
                                }
                                String insertQuery = "INSERT INTO KW_TWEET (TWEET_ID,KEYWORD_ID,EVENT_ID) VALUES (" + tweet_id + "," + kw_id + "," + event_id + ")";
                                Statement insertStatement = connection.createStatement();
                                insertStatement.execute(insertQuery);
                                insertStatement.close();
                            }
                            fetchStatement.close();
                        }    
                } catch (Exception e) {
                    e.printStackTrace();
                }
            }
}

