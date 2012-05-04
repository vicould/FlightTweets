package net.flighttweets.tweets;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.PriorityQueue;
import java.util.Timer;

import net.flighttweets.tweets.data.FetchItemBundle;

/**
 * 
 * This class takes care of the fetching process, with some error handling,
 * fetch resuming, etc.
 *
 */
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
	 * Restores the fetching state from the db, comparing the usernames passed as initial
	 * input to this instance to what is currently stored in the db.
	 */
	public void resumeTweetFetching() {
		// explore the db, to see which users have been completely fetched, which are partial, and which are missing 
		// use the specific meta table
		PriorityQueue<FetchItemBundle> usernamesToDo = new PriorityQueue<FetchItemBundle>(this.getUsernamesToFetch().size());
		PriorityQueue<FetchItemBundle> repliesToDo = new PriorityQueue<FetchItemBundle>();
		PreparedStatement usernameState;
		ResultSet status;
		
		Connection connection;
		try {
			connection = StorageManager.getInstance().getConnection();

			// goes through the usernames passed as initial input, and check if 
			// they are already present in the db.
			for (String username: this.getUsernamesToFetch()) {
				usernameState = connection.prepareStatement("SELECT LAST_TWEET_ID, COMPLETE FROM FETCH_STATUS WHERE USERNAME = ?");
				usernameState.setString(1, username);
				status = usernameState.executeQuery();
				if (!status.next()) {
					System.out.println(username + " is new, creating state row");
					// nothing has been fetched for this user
					usernamesToDo.add(new FetchItemBundle(username, USERNAME_NOT_FETCHED));
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
					usernamesToDo.add(new FetchItemBundle(username, status.getLong("LAST_TWEET_ID")));
				}
			}
			
			// goes through the replies
			PreparedStatement repliesQuery = connection.prepareStatement("SELECT * FROM REPLIES_TO_FETCH");
			ResultSet repliesResults = repliesQuery.executeQuery();
			
			while (repliesResults.next()) {
				repliesToDo.add(new FetchItemBundle(repliesResults.getString("USERNAME"), repliesResults.getLong("ID")));
			}
			
			if (usernamesToDo.size() != 0 || repliesToDo.size() != 0) {
				System.out.println("Launching fetch task");
				this.launchFetcherTasks(usernamesToDo, repliesToDo);
			} else {
				System.out.println("Nothing to do, all the tweets have been fetched");
			}
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	/**
	 * Configure a timer to fetch tweets for the specified usernames. 
	 * @param usernames A priority queue of {@link FetchItemBundle} configuring what to fetch.
	 */
	private void launchFetcherTasks(PriorityQueue<FetchItemBundle> usernames, PriorityQueue<FetchItemBundle> replies) {
		Timer timer = new Timer();
		this.setTimer(timer);
		FetcherTask task = new FetcherTask(usernames, replies, this);
		timer.schedule(task, 0, 5000);
	}

	/**
	 * Stops the app.
	 */
	@Override
	public void fetchComplete() {
		// TODO Auto-generated method stub
		this.getTimer().cancel();
		System.out.println("Fetch terminated");
		
		Launcher.performAnalysis();
	}

	/**
	 * Reschedules the fetching task for later, after a small delay.
	 */
	@Override
	public void handleFetchFailure(PriorityQueue<FetchItemBundle> currentPointForUsers, PriorityQueue<FetchItemBundle> currentPointForReplies) {
		this.getTimer().cancel();
		this.setTimer(new Timer());
		FetcherTask task = new FetcherTask(currentPointForUsers, currentPointForReplies, this);
		this.getTimer().schedule(task, 30000, 5000);
	}

}
