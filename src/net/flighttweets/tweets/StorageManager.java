package net.flighttweets.tweets;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * Manager for the storage of the tweets. It implements the singleton pattern, 
 * get an instance of the class through the {@link getInstance} method.
 * The manager provides the connection to the tables, creating them as needed. 
 *
 */
public class StorageManager {

	private static StorageManager instance;
	
	private StorageManager() {
		super();
	}
	
	/**
	 * Checks if the database contains all the necessary tables.
	 */
	public void verifyDB() {
		if (!checkIfTableExist("TWEETS")) {
			this.createTableTweets();
		}
		if (!this.checkIfTableExist("FETCH_STATUS")) {
			this.createTableFetchStatus();
		}
	}
	
	/**
	 * Getter to retrieve an instance of the class.
	 * @return The only instance of this singleton class.
	 */
	public static StorageManager getInstance() {
		if (instance == null) {
			instance = new StorageManager();
		}
		
		return instance;
	}
	
	/**
	 * Get a connection to the datatabase.
	 * @return A connection
	 * @throws SQLException
	 */
	public Connection getConnection() throws SQLException {
		try {
			Class.forName("org.hsqldb.jdbcDriver");
		} catch (ClassNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return DriverManager.getConnection("jdbc:hsqldb:hsql://localhost:9001/flightsdb", "sa", "");
	}
	
	/**
	 * Utility method in order to know if the tables for the application 
	 * exist.
	 * @param table The name of the table to check
	 * @return False if the table does not exist in the current db.
	 */
	public boolean checkIfTableExist(String table) {
		try {
			Connection localConnection = this.getConnection();
			DatabaseMetaData metaData = localConnection.getMetaData();
			String[] types = new String[1];
			types[0] = "TABLE";
			ResultSet result = metaData.getTables(null, null, table, types);
			// result.next() is true if we found at least one matching table. Caution, table is case sensitive (should be uppercase)
			return result.next();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
			System.out.println(e.getErrorCode());
		}
		
		return false;
	}
		
	private void createTableTweets() {
		try {
			Connection localConnection = this.getConnection();
			localConnection.prepareStatement("CREATE TABLE TWEETS (TWEET_ID BIGINT PRIMARY KEY, USERNAME VARCHAR(16), USER_ID BIGINT, TWEET VARCHAR(140), CREATED TIMESTAMP, RETWEET_COUNT BIGINT, IN_REPLY_TO BIGINT)").execute();
			} catch (SQLException e) {
				e.printStackTrace();
			}
	}
	
	private void createTableFetchStatus() {
		try {
		Connection localConnection = this.getConnection();
		localConnection.prepareStatement("CREATE TABLE FETCH_STATUS (USERNAME VARCHAR(16) PRIMARY KEY, LAST_TWEET_ID BIGINT, LAST_TWEET_DATE DATE, COMPLETE BOOLEAN)").execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	public void removeTweets() {
		try {
			Connection localConnection = this.getConnection();
			localConnection.prepareStatement("DELETE FROM TWEETS").execute();
			localConnection.prepareStatement("DELETE FROM FETCH_STATUS").execute();
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}
	
	/**
	 * Cleans the database.
	 */
	public void dropTables() {
		try {
			Connection localConnection = this.getConnection();
			localConnection.prepareStatement("DROP TABLE TWEETS").execute();
			localConnection.prepareStatement("DROP TABLE FETCH_STATUS").execute();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
}
