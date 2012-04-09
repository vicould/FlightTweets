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

         public void populateKeywordList(List<String> keywordList) {
             int i = 0;
             try {
                Connection connection = StorageManager.getInstance().getConnection();
                String removeQuery = "DELETE FROM KEYWORDS";
                Statement stmt = connection.createStatement();
                if (!stmt.execute(removeQuery)) {
                    System.err.println("Error executing query: " + removeQuery);
                    
                }
                for (String word : keywordList) {
                    String query = "INSERT INTO KEYWORDS (keyword_id, word) VALUES (" + i + ",'" + word + "')";
                    Statement insertStmt = connection.createStatement();
                    insertStmt.execute(query);
                    i++;
                    insertStmt.close();
                }
                stmt.close();
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

