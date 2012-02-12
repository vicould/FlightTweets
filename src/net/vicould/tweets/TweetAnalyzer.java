package net.vicould.tweets;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import net.vicould.tweets.data.SimpleTweet;

public class TweetAnalyzer {

	/**
	 * Search for tweets containing a specific keyword in the database, inside 
	 * a time frame given as parameter.
	 * @param keyword The keyword to look for.
	 * @param startDate The oldest date of the time frame.
	 * @param endDate The newest date.
	 * @return A list of SimpleTweet matching the query.
	 */
	public List<SimpleTweet> searchForKeyword(String keyword, Date startDate, Date endDate) {
		List<SimpleTweet> rawResults = fetchTweetsFromDatabase(startDate, endDate);
		
		List<SimpleTweet> filteredResults = filterListAgainstQuery(keyword, rawResults);
		
		return filteredResults;
	}
	
	/**
	 * Fetch the tweets from the db that are between the time frame.
	 * @param startDate
	 * @param endDate
	 * @return Returns all the tweets between startDate and endDate
	 */
	private List<SimpleTweet> fetchTweetsFromDatabase(Date startDate, Date endDate) {
		ArrayList<SimpleTweet> results = new ArrayList<SimpleTweet>();
		
		try {
			Connection connection = StorageManager.getInstance().getConnection();
			PreparedStatement statement = connection.prepareStatement("SELECT * FROM TWEETS WHERE CREATED BETWEEN ? AND ?");
			
			statement.setDate(1, new java.sql.Date(startDate.getTime()));
			statement.setDate(2, new java.sql.Date(endDate.getTime()));
			
			ResultSet resultSet = statement.executeQuery();
			while (resultSet.next()) {
				// creates a tweet to store the result, using the columns 1 -> tweetId, 2 -> userId, 3 -> content, 4 -> createdAt
				SimpleTweet tweet = new SimpleTweet(resultSet.getString(3), resultSet.getLong(1), resultSet.getLong(2), resultSet.getDate(4)); 
				results.add(tweet);
			}
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return results;
	}
	
	/**
	 * Filters the list of tweets to return only those containing the keyword.
	 * @param keyword The keyword to get
	 * @param rawTweets A list of tweets to filter.
	 * @return The filtered list.
	 */
	private List<SimpleTweet> filterListAgainstQuery(String keyword, List<SimpleTweet> rawTweets) {
		ArrayList<SimpleTweet> filtered = new ArrayList<SimpleTweet>();
		
		for (SimpleTweet tweet : rawTweets) {
			if (tweet.getContent().toLowerCase().contains(keyword.toLowerCase())) {
				filtered.add(tweet);
				System.out.println(tweet);
			}
		}
		
		return filtered;
	}
}
