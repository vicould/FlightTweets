package net.flighttweets.tweets;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.List;
import java.util.PriorityQueue;
import java.util.TimerTask;

import net.flighttweets.tweets.data.FetchItemBundle;
import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class FetcherTask extends TimerTask {
	private PriorityQueue<FetchItemBundle> usernamesToFetch;
	private String currentUser;
	private long currentId;
	private TweetSaver tweetSaver;
	private StorageManager storageManager;
	private FetcherCallback callback;
	
	public FetcherTask(PriorityQueue<FetchItemBundle> itemsToFetch, FetcherCallback callback) {
		super();
		
		this.setUsernamesToFetch(itemsToFetch);
		this.setTweetSaver(new TweetSaver());
		this.setStorageManager(StorageManager.getInstance());
		this.setCallback(callback);
		
		FetchItemBundle firstItem = this.getUsernamesToFetch().poll();
		if (firstItem != null) {
			this.setCurrentId(firstItem.getTweetId());
			this.setCurrentUser(firstItem.getUsername());
		}
	}
	
	public PriorityQueue<FetchItemBundle> getUsernamesToFetch() {
		return this.usernamesToFetch;
	}

	private void setUsernamesToFetch(PriorityQueue<FetchItemBundle> usernames) {
		this.usernamesToFetch = usernames;
	}
	
	private String getCurrentUser() {
		return currentUser;
	}

	private void setCurrentUser(String currentUser) {
		this.currentUser = currentUser;
	}

	private Long getCurrentId() {
		return currentId;
	}

	private void setCurrentId(Long currentId) {
		this.currentId = currentId;
	}

	private TweetSaver getTweetSaver() {
		return this.tweetSaver;
	}
	
	private void setTweetSaver(TweetSaver saver) {
		this.tweetSaver = saver;
	}
	
	private StorageManager getStorageManager() {
		return storageManager;
	}

	private void setStorageManager(StorageManager storageManager) {
		this.storageManager = storageManager;
	}

	private FetcherCallback getCallback() {
		return callback;
	}

	private void setCallback(FetcherCallback callback) {
		this.callback = callback;
	}

	private List<Status> fetchUserTweets() throws TwitterException {
		if (this.getCurrentUser() != null || this.getCurrentId() != null) {
			Twitter twitter = new TwitterFactory().getInstance();

			Paging paging = new Paging();
			// if no tweets have been fetched for this username there shouldn't be a maxid
			if (this.getCurrentId() != TweetFetcher.USERNAME_NOT_FETCHED) {
				paging.maxId(this.getCurrentId() - 1L);
			}
			paging.count(200);

			return twitter.getUserTimeline(this.getCurrentUser(), paging);
		}
		return new ArrayList<Status>();
	}
	
	private void saveTweets(List<Status> statuses) {
		TweetSaver saver = this.getTweetSaver();
		saver.saveTweets(statuses);
	}
	
	private void updateFetchingStatus(Status lastFetchedStatus) throws SQLException {
		// checks the date, to see if it is less than January 1st, 2011
		if (lastFetchedStatus.getCreatedAt().compareTo(new GregorianCalendar(2011, 0, 1).getTime()) <= 0) {
			// updates the db
			PreparedStatement deleteStatement = this.getStorageManager().getConnection().prepareStatement("UPDATE FETCH_STATUS SET (COMPLETE) = (TRUE) WHERE USERNAME = ?");
			deleteStatement.setString(1, lastFetchedStatus.getUser().getName());
			deleteStatement.execute();
			
			// updates the currentUser and currentId for next run, takes it from the head of the queue
			FetchItemBundle nextRound = this.getUsernamesToFetch().poll();
			if (nextRound != null) {
				this.setCurrentId(nextRound.getTweetId());
				this.setCurrentUser(nextRound.getUsername());
			} else {
				this.getCallback().fetchComplete();
			}
			
		} else {
			// updates the db
			PreparedStatement updateStatement = this.getStorageManager().getConnection().prepareStatement("UPDATE FETCH_STATUS SET (LAST_TWEET_ID, LAST_TWEET_DATE) = (?, ?) WHERE USERNAME = ?");
			updateStatement.setLong(1, lastFetchedStatus.getId());
			updateStatement.setDate(2, new java.sql.Date(lastFetchedStatus.getCreatedAt().getTime()));
			updateStatement.setString(3, lastFetchedStatus.getUser().getName());
			
			updateStatement.execute();
			
			// updates the ID of the starting point for the next fetch from the last tweet
			this.setCurrentId(lastFetchedStatus.getId());
		}
	}
	
	@Override
	public void run() {
		try {
			System.out.println("Fetching a new batch for " + this.getCurrentUser() + "before " + this.getCurrentId());
			List<Status> statuses = this.fetchUserTweets();
			if (statuses.size() != 0) {
				System.out.println("Saving " + statuses.size() + " results, last one being " + DateFormat.getDateInstance(DateFormat.MEDIUM).format(statuses.get(statuses.size() - 1).getCreatedAt()));
				this.saveTweets(statuses);
				updateFetchingStatus(statuses.get(statuses.size() - 1));
			}
		} catch (TwitterException e) {
			this.getUsernamesToFetch().add(new FetchItemBundle(this.getCurrentUser(), this.getCurrentId()));
			this.getCallback().handleFetchFailure(this.getUsernamesToFetch());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
