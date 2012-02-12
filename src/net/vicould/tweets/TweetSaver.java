package net.vicould.tweets;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Writer;
import java.nio.charset.Charset;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

import twitter4j.Status;

public class TweetSaver {

	/**
	 * Saves a list of tweets to the DB. Executes as a batch the insertion for optimization reasons.
	 * @param statusesToSave A list of statuses.
	 */
	public void saveTweets(List<Status> statusesToSave) {
		try {
			Connection connection = StorageManager.getInstance().getConnection();
			PreparedStatement insertStatement = connection.prepareStatement("INSERT INTO TWEETS VALUES (?, ?, ?, ?)");
			
			for (Status status : statusesToSave) {
				insertStatement.setLong(1, status.getId());
				insertStatement.setLong(2, status.getUser().getId());
				insertStatement.setString(3, status.getText());
				// conversion between a java.util.Date and a java.sql.Date
				insertStatement.setDate(4, new java.sql.Date(status.getCreatedAt().getTime()));
				insertStatement.addBatch();
			}
			insertStatement.executeBatch();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Saves all the tweets to a CSV file.
	 * @param filename The filename to save to, which will be placed in the results directory.
	 */
	public void saveTweetsToFile(String filename) {
		File file = new File("results");
		if (!file.exists()) {
			file.mkdir();
		}
		
		try {
			Writer output = new BufferedWriter(new OutputStreamWriter(new FileOutputStream("results/" + filename), Charset.forName("UTF-8")));
			
			Connection connection = StorageManager.getInstance().getConnection();
			PreparedStatement retrievalStatement = connection.prepareStatement("SELECT * FROM TWEETS ORDER BY TWEET_ID");
			ResultSet results = retrievalStatement.executeQuery();

			output.write("Tweet id, User id, Tweet, Created at\n");
			while (results.next()) {
				output.write("\"" + results.getString(1) + "\",\"" + results.getString(2) + "\",\"" + results.getString(3)  +"\",\"" + results.getString(4) + "\"\n");
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
