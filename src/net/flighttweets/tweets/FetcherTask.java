package net.flighttweets.tweets;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.text.DateFormat;
import java.util.ArrayList;
import java.util.GregorianCalendar;
import java.util.HashSet;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.TimerTask;

import net.flighttweets.tweets.data.FetchItemBundle;
import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

/**
 * 
 * The atomic task processing all the fetching task, retrieving from a queue the
 * usernames to process and the corresponding starting point for this username.
 * This task should be bound to its launcher in order to receive the messages
 * specified by the FetcherCallback interface.
 *
 */
public class FetcherTask extends TimerTask {
	private boolean fromReplies;
	private PriorityQueue<FetchItemBundle> usernamesToFetch;
	private PriorityQueue<FetchItemBundle> repliesToFetch;
	// The user that is is used at this step.
	private String currentUser;
	// The id where to start fetching
	private long currentId;
	// An instance of the class used to save the fetched tweets 
	private TweetSaver tweetSaver;
	// The manager giving access to the db
	private StorageManager storageManager;
	private FetcherCallback callback;
	// A counter to know how many times the couple currentUsername/currentID has been tried if errors happen
	private int retrialCount;
	
	/**
	 * Prepares an instance of FetcherTask, configuring the elements that need 
	 * to be fetched, and an instance of a class following the {@link FetcherCallback}
	 * interface which will be receiving the messages sent during the execution
	 * of the task.
	 * @param usersToFetch A sorted queue containing the elements to fetch, as instances of
	 * {@link FetchItemBundle}. 
	 * @param callback The instance of the class to use when message passing is needed. 
	 */
	public FetcherTask(PriorityQueue<FetchItemBundle> usersToFetch, PriorityQueue<FetchItemBundle> repliesToFetch, FetcherCallback callback) {
		super();
		
		this.setUsernamesToFetch(usersToFetch);
		this.setRepliesToFetch(repliesToFetch);
		this.setTweetSaver(new TweetSaver());
		this.setStorageManager(StorageManager.getInstance());
		this.setCallback(callback);
		this.setRetrialCount(0);
		
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

	private int getRetrialCount() {
		return retrialCount;
	}

	private void setRetrialCount(int retrialCount) {
		this.retrialCount = retrialCount;
	}
	
	private PriorityQueue<FetchItemBundle> getRepliesToFetch() {
		return repliesToFetch;
	}

	private void setRepliesToFetch(PriorityQueue<FetchItemBundle> repliesToFetch) {
		this.repliesToFetch = repliesToFetch;
	}

	private boolean isFromReplies() {
		return fromReplies;
	}

	private void setFromReplies(boolean fromReplies) {
		this.fromReplies = fromReplies;
	}

	private void retrialCountPlusOne() {
		this.retrialCount++;
	}
	
	/**
	 * 
	 * @param statuses
	 * @throws SQLException 
	 */
	private void populateRepliesQueue(List<Status> statuses) throws SQLException {
		long replyId;
		FetchItemBundle reply;
		HashSet<FetchItemBundle> replies = new HashSet<FetchItemBundle>(statuses.size());
		
		for (Status status: statuses) {
			replyId = status.getInReplyToStatusId();
			if (replyId != -1) {
				reply = new FetchItemBundle(status.getInReplyToScreenName(), status.getInReplyToStatusId());
				replies.add(reply);
				this.getRepliesToFetch().add(reply);
			}
		}
		
		this.setRepliesToFetch(replies);
	}

	/**
	 * The real fetch method, dealing with the twitter API. It picks the username
	 * and ID as defined by the currentUsername and currentId fields.
	 * @return Returns a list of the {@link Status}, sorted on the id (newest first). 
	 * The maximum size of the list is 200, as Twitter does not allow to fetch more tweets at
	 * a time/
	 * @throws TwitterException If Twitter is not happy some exceptions might be raised,
	 * telling to slow down, etc.
	 */
	private List<Status> fetchUserTweets() throws TwitterException {
		if (this.getCurrentUser() != null || this.getCurrentId() != null) {
			Twitter twitter = new TwitterFactory().getInstance();

			Paging paging = new Paging();
			// if no tweets have been fetched for this username there shouldn't be a maxid
			if (this.getCurrentId() != TweetFetcher.USERNAME_NOT_FETCHED) {
				paging.maxId(this.getCurrentId() - 1L);
			}
			// retrieves 200 tweets at a time.
			paging.count(200);

			return twitter.getUserTimeline(this.getCurrentUser(), paging);
		}
		return new ArrayList<Status>();
	}
	
	private Status getTweet() throws TwitterException {
		if (this.getCurrentUser() != null || this.getCurrentId() != null) {
			Twitter twitter= new TwitterFactory().getInstance();
			return twitter.showStatus(this.getCurrentId());
		}
		return null;
	}
	
	/**
	 * Saves the tweets to the db.
	 * @param statuses The list of statuses to save.
	 */
	private void saveTweets(List<Status> statuses) {
		TweetSaver saver = this.getTweetSaver();
		saver.saveTweets(statuses);
	}
	
	private void saveTweet(Status status) {
		TweetSaver saver = this.getTweetSaver();
		saver.saveTweet(status);
	}
	
	/**
	 * Prepares the task for the next run, retrieving a new user from the queue
	 * and installing it in the current fields.
	 */
	public void feedFetcher() {
		// updates the currentUser and currentId for next run, takes it from the head of the queue
		FetchItemBundle nextRound = this.getUsernamesToFetch().poll();
		if (nextRound != null) {
			this.setCurrentId(nextRound.getTweetId());
			this.setCurrentUser(nextRound.getUsername());
			this.setFromReplies(false);
		} else {
			nextRound = this.getRepliesToFetch().poll();
			if (nextRound != null) {
				this.setCurrentId(nextRound.getTweetId());
				this.setCurrentUser(nextRound.getUsername());
				this.setFromReplies(true);
			} else {
				this.getCallback().fetchComplete();
			}
		}
	}
	
	private void setRepliesToFetch(Set<FetchItemBundle> replies) throws SQLException {
		PreparedStatement insertStatement = this.getStorageManager().getConnection().prepareStatement("INSERT INTO REPLIES_TO_FETCH VALUES (?, ?)");
		try {
			for (FetchItemBundle singleReply: replies) { 
				insertStatement.setLong(1, singleReply.getTweetId());
				insertStatement.setString(2, singleReply.getUsername());
				insertStatement.addBatch();
			}
			insertStatement.executeBatch();
		} catch (SQLException e) {
			
		}
	}
	
	private void markReplyAsFetched(Status fetchedStatus) throws SQLException {
		PreparedStatement updateStatement = this.getStorageManager().getConnection().prepareStatement("DELETE REPLIES_TO_FETCH WHERE ID = ?");
		updateStatement.setLong(1, fetchedStatus.getId());
	}
	
	/**
	 * Saves in the db the fetching state, comprised of the id of the last tweet, 
	 * and the eventual completeness.
	 * @param lastFetchedStatus The status which should be used to update the db.
	 * Its creation date is used to know if we reached the date limit.
	 * @throws SQLException
	 */
	private void updateFetchingStatus(Status lastFetchedStatus) throws SQLException {
		// checks the date, to see if it is less than January 1st, 2011
		if (lastFetchedStatus.getCreatedAt().compareTo(new GregorianCalendar(2011, 0, 1).getTime()) <= 0) {
			// updates the db, this user is completed
			PreparedStatement updateStatement = this.getStorageManager().getConnection().prepareStatement("UPDATE FETCH_STATUS SET (LAST_TWEET_ID, LAST_TWEET_DATE, COMPLETE) = (?, ?, TRUE) WHERE USERNAME = ?");
			updateStatement.setLong(1, lastFetchedStatus.getId());
			updateStatement.setDate(2, new java.sql.Date(lastFetchedStatus.getCreatedAt().getTime()));
			updateStatement.setString(3, lastFetchedStatus.getUser().getScreenName());

			updateStatement.execute();

			this.feedFetcher();
		} else {
			// updates the db
			PreparedStatement updateStatement = this.getStorageManager().getConnection().prepareStatement("UPDATE FETCH_STATUS SET (LAST_TWEET_ID, LAST_TWEET_DATE) = (?, ?) WHERE USERNAME = ?");
			updateStatement.setLong(1, lastFetchedStatus.getId());
			updateStatement.setDate(2, new java.sql.Date(lastFetchedStatus.getCreatedAt().getTime()));
			updateStatement.setString(3, lastFetchedStatus.getUser().getScreenName());
			
			updateStatement.execute();
			
			// updates the ID of the starting point for the next fetch from the last tweet
			this.setCurrentId(lastFetchedStatus.getId());
		}
	}
	
	/**
	 * Saves in the db that the username has been processed enough (and potentially generating
	 * too many errors).
	 * @param username The username for which no tweets should be retrieved anymore.
	 * @throws SQLException
	 */
	public void enough(String username) throws SQLException {
		PreparedStatement completeStatement = this.getStorageManager().getConnection().prepareStatement("UPDATE FETCH_STATUS SET (COMPLETE) = (TRUE) WHERE USERNAME = ?");
		completeStatement.setString(1, username);
		completeStatement.execute();
		
		this.feedFetcher();
	}
	
	/**
	 * The entry point for the task. It triggers the fetching, handling the errors 
	 * and passing messages to the callback.
	 */
	@Override
	public void run() {
		try {
			if (this.isFromReplies()) {
				System.out.println("Fetching single tweet for " + this.getCurrentUser() + " with the id " + this.getCurrentId());
				Status status = this.getTweet();
				if (status != null) {
					this.markReplyAsFetched(status);
					System.out.println("Saving one tweet");
					this.saveTweet(status);
				}
			} else {
				System.out.println("Fetching a new batch for " + this.getCurrentUser() + " before " + this.getCurrentId());
				List<Status> statuses = this.fetchUserTweets();
				if (statuses.size() != 0) {
					this.populateRepliesQueue(statuses);
					System.out.println("Saving " + statuses.size() + " results, last one being " + DateFormat.getDateInstance(DateFormat.MEDIUM).format(statuses.get(statuses.size() - 1).getCreatedAt()));
					this.saveTweets(statuses);
					// twitter returns the list with the oldest tweet at the end of the array
					updateFetchingStatus(statuses.get(statuses.size() - 1));
				} else {
					// counts the tweets in the db
					PreparedStatement countStatement = this.getStorageManager().getConnection().prepareStatement("SELECT COUNT(TWEET_ID) FROM TWEETS WHERE USERNAME = ?");
					countStatement.setString(1, this.getCurrentUser());
					ResultSet countResult = countStatement.executeQuery();
					if (countResult.next()) {
						// if it is over 3200, Twitter does not allow us to get more tweets
						// so we have to consider the fetch is complete
						if (countResult.getInt(1) >= 3200) {
							System.out.println("Enough for " + this.getCurrentUser());
							// updates the db
							this.enough(this.getCurrentUser());
						} else {
							System.out.println("Twitter returned an empty response, but I don't know why.");
							// try 5 times, otherwise we get bored
							this.retrialCountPlusOne();
							if (this.getRetrialCount() == 5) {
								// enough!
								System.out.println("5 times is enough for " + this.getCurrentUser());
								this.enough(this.getCurrentUser());
							}
						}
					}
				}
			}
		} catch (TwitterException e) {
			if (!this.isFromReplies()) {
			// restores the queue with the username and the id the task was processing before the exception
			this.getUsernamesToFetch().add(new FetchItemBundle(this.getCurrentUser(), this.getCurrentId()));
			} else {
				this.getRepliesToFetch().add(new FetchItemBundle(this.getCurrentUser(), this.getCurrentId()));
			}
			// forwards the message to the callback
			this.getCallback().handleFetchFailure(this.getUsernamesToFetch(), this.getRepliesToFetch());
		} catch (SQLException e) {
			e.printStackTrace();
		}
	}

}
