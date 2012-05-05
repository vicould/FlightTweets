package net.flighttweets.tweets;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.Statement;
import java.util.List;
import net.flighttweets.tweets.jaxb.*;

public class TweetFilter {

	/**
	 * Takes a list of tweets and keywords, and provides a database link for each. Executes as a batch the insertion for optimization reasons.
	 * @param statusesToSave A list of statuses.
	 */

         public static void populateEventKeywordList(int eventId,int keywordId) {
             String testQuery = "SELECT event_id from EVENT_KEYWORD WHERE event_id=" + eventId + " AND keyword_id=" + keywordId;
             try {
                 Connection connection = StorageManager.getInstance().getConnection();
                 Statement testStmt = connection.createStatement();
                 ResultSet rst = testStmt.executeQuery(testQuery);
                 if (rst.next()) {
                     return;
                 }
                 String insertQuery = "INSERT INTO EVENT_KEYWORD (event_id,keyword_id) VALUES (" + eventId + "," + keywordId + ")";
                 Statement insertStmt = connection.createStatement();
                 insertStmt.execute(insertQuery);
             } catch (Exception e) {
                 e.printStackTrace();
             }
         }
         
         public static int getEventIdFromName(String name) {
             int retInt = -1;
             try {
             Connection connection = StorageManager.getInstance().getConnection();
             String query = "SELECT EVENT_ID FROM EVENTS WHERE EVENT_NAME='" + name + "'";
             Statement testStmt = connection.createStatement();
             ResultSet rsc = testStmt.executeQuery(query);
                 if (rsc.next()) {
                     int testInt = rsc.getInt("EVENT_ID");
                     if (testInt > retInt) {
                         retInt = testInt;
                     }
                     
                 }
             } catch (Exception e) {
                 e.printStackTrace();
             }
             return retInt;
         }
         
         public static int getKeywordId(String word) {
             //returns id of word  If word is not in database, insert word.
             int returnVal = -1;
             String checkQuery = "SELECT KEYWORD_ID FROM KEYWORDS WHERE WORD='" + word + "'";
             try {
                Connection connection = StorageManager.getInstance().getConnection();
                Statement testStmt = connection.createStatement();
                ResultSet rsc = testStmt.executeQuery(checkQuery);
                if (rsc.next()) {
                    returnVal = rsc.getInt("KEYWORD_ID");
                    return returnVal;
                } else {
                    String maxQuery = "SELECT MAX(KEYWORD_ID) AS MAXID FROM KEYWORDS";
                    Statement maxStmt = connection.createStatement();
                    ResultSet msc = testStmt.executeQuery(maxQuery);
                    if (msc.next()) {
                        returnVal = msc.getInt("MAXID") + 1;
                    } else {
                        returnVal = 0;
                    }
                    String insertQuery = "INSERT INTO KEYWORDS (KEYWORD_ID,WORD) VALUES (" + returnVal + ",'" + word + "')";
                    Statement insertStmt = connection.createStatement();
                    testStmt.execute(insertQuery);
                    return returnVal;
                }
             } catch (Exception e) {
                 e.printStackTrace();
             }
             return returnVal;
         }
         
         public static void populateUniqueKeywordEvents(TweetConfigType tcfg) {
             for (int i = 0; i < tcfg.getEvent().size();i++) {
                 EventType e = tcfg.getEvent().get(i);
                 String eventName = e.getEventName();
                 int eventId = getEventIdFromName(eventName);
                 if (eventId < 1) {
                     continue;
                 }
                 for (int j = 0; j < e.getKeyword().size();j++) {
                     int keywordId;
                     String keyword = e.getKeyword().get(j);
                     keywordId = getKeywordId(keyword);
                     populateEventKeywordList(eventId,keywordId);
                     
                 }
             }
         }
    
    
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
                    this.populateEventKeywordList(0, i);
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
	public void filterTweets(List<String> keywordList,TweetConfigType tct) {
            this.populateKeywordList(keywordList);
            TweetFilter.populateUniqueKeywordEvents(tct);
		try {
                    Connection connection = StorageManager.getInstance().getConnection();
                   //global tweets
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
                        //local tweets
                        //get all event and keyword pairs with keyword_id and event_id in EVENT_KEYWORDS
                        String uniqueTweetQuery = "SELECT EVENTS.EVENT_ID,KEYWORDS.KEYWORD_ID,KEYWORDS.WORD FROM EVENT_KEYWORD LEFT JOIN KEYWORDS ON (EVENT_KEYWORD.KEYWORD_ID = KEYWORDS.KEYWORD_ID) LEFT JOIN EVENTS ON (EVENT_KEYWORD.EVENT_ID = EVENTS.EVENT_ID) WHERE EVENT_KEYWORD.EVENT_ID > 0";
                        Statement utStmt = connection.createStatement();
                        ResultSet utSet = utStmt.executeQuery(uniqueTweetQuery);
                        while (utSet.next()) {
                            long event_id = utSet.getLong("EVENTS.EVENT_ID");
                            long kw_id = utSet.getLong("KEYWORDS.KEYWORD_ID");
                            String word = utSet.getString("KEYWORDS.WORD");
                            String likeClause = "TWEETS.TWEET LIKE '%" + word + "%' AND TWEETS.CREATED > EVENTS.START_DATE AND TWEETS.CREATED < EVENTS.END_DATE AND EVENTS.EVENT_ID=" + event_id;
                            String retrieveQuery = "SELECT TWEETS.TWEET_ID,EVENTS.EVENT_ID FROM TWEETS FULL JOIN EVENTS ON 1=1 WHERE " + likeClause;
                            //System.out.println(retrieveQuery);
                            Statement fetchStatement = connection.createStatement();
                            ResultSet rs2 = fetchStatement.executeQuery(retrieveQuery);
                            while (rs2.next()) {
                                long tweet_id = rs2.getLong("TWEETS.TWEET_ID");
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

