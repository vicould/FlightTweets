package net.flighttweets.tweets;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Timer;

import net.flighttweets.tweets.data.FetchItemBundle;

public class TweetFetcher implements FetcherCallback {
	private ArrayList<String> usernamesToFetch;
	private Timer timer;
	
	public static final Long USERNAME_NOT_FETCHED = - 1L;
	
	/**
	 * Creates an instance of the fetcher with the specified usernames to use for the fetching.
	 * @param usernames The usernames we are interested in.
	 */
	public TweetFetcher(ArrayList<String> usernames) {
		super();
		
		this.setUsernamesToFetch(usernames);
	}
	
	// getter / setters
	
	public ArrayList<String> getUsernamesToFetch() {
		return this.usernamesToFetch;
	}
	
	public void setUsernamesToFetch(ArrayList<String> usernames) {
		this.usernamesToFetch = usernames;
	}
	
	// fetch saving / restoring
	
	public Timer getTimer() {
		return timer;
	}

	public void setTimer(Timer timer) {
		this.timer = timer;
	}

	/**
	 * 
	 */
	public void resumeTweetFetching() {
		// explore the db, to see which users have been completely fetched, which are partial, and which are missing 
		// use the specific meta table
		PriorityQueue<FetchItemBundle> toDo = new PriorityQueue<FetchItemBundle>(this.getUsernamesToFetch().size());
		PreparedStatement usernameState;
		ResultSet status;
		
		Connection connection;
		try {
			connection = StorageManager.getInstance().getConnection();

			for (String username: this.getUsernamesToFetch()) {
				usernameState = connection.prepareStatement("SELECT LAST_TWEET_ID, COMPLETE FROM FETCH_STATUS WHERE USERNAME = ?");
				usernameState.setString(1, username);
				status = usernameState.executeQuery();
				if (!status.next()) {
					System.out.println(username + " is new, creating state row");
					// nothing has been fetched for this user
					toDo.add(new FetchItemBundle(username, USERNAME_NOT_FETCHED));
					// creates an entry in the fetch_status table for it
					PreparedStatement usernameStateCreation = connection.prepareStatement("INSERT INTO FETCH_STATUS VALUES (?, ?, ?, ?)");
					usernameStateCreation.setString(1, username);
					usernameStateCreation.setLong(2, USERNAME_NOT_FETCHED);
					usernameStateCreation.setDate(3, new java.sql.Date(0));
					usernameStateCreation.setBoolean(4, false);
					
					usernameStateCreation.execute();
				} else if (status.getBoolean("COMPLETE")) {
					// user has been totally fetched, carry on
					System.out.println(username + " is complete, not adding to the todo.");
					continue;
				} else {
					System.out.println("Restoring " + username + " from " + status.getLong("LAST_TWEET_ID"));
					toDo.add(new FetchItemBundle(username, status.getLong("LAST_TWEET_ID")));
				}
			}
			System.out.println("Launching fetch task");
			this.launchFetcherTasks(toDo);
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	private void launchFetcherTasks(PriorityQueue<FetchItemBundle> items) {
		Timer timer = new Timer();
		this.setTimer(timer);
		FetcherTask task = new FetcherTask(items, this);
		timer.schedule(task, 0, 2000);
	}

	@Override
	public void fetchComplete() {
		// TODO Auto-generated method stub
		this.getTimer().cancel();
		System.out.println("Fetch terminated");
		System.exit(0);
	}

	@Override
	public void handleFetchFailure(PriorityQueue<FetchItemBundle> currentPoint) {
		this.getTimer().cancel();
		FetcherTask task = new FetcherTask(currentPoint, this);
		this.getTimer().schedule(task, 5000, 2000);
	}

}
