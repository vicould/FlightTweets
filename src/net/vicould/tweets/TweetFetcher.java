package net.vicould.tweets;

import java.util.ArrayList;
import java.util.List;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class TweetFetcher {
	private TweetSaver dbSaver;

	private TweetSaver getDbSaver() {
		return dbSaver;
	}
	private void setDbSaver(TweetSaver dbSaver) {
		this.dbSaver = dbSaver;
	}
	
	public TweetFetcher() {
		setDbSaver(new TweetSaver());
	}
	
	/**
	 * Fetch some (20) statuses that are older than the id specified as a parameter.
	 * @param user The user's name from whom to retrieve the tweets.
	 * @param maxTweetId The upper bound for the tweets (i.e. the method will retrieve tweets with an id lesser than this one).
	 * @return A list of statuses corresponding to the constraints. Might be empty.
	 */
	public List<Status> fetchSome(String user, long maxTweetId) {
		Twitter twitter = new TwitterFactory().getInstance();
		
		try {
			Paging paging = new Paging();
			paging.maxId(maxTweetId - 1L);
			
			List<Status> statuses = twitter.getUserTimeline(user, paging);
			for (Status status : statuses) {
				System.out.println(status.getCreatedAt() + " - " + status.getId() + " - " + status.getUser().getId());
			}
			return statuses;
		} catch (TwitterException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return new ArrayList<Status>();
	}
	
	/**
	 * Fetch a set of tweets for the specified user, inside the specified ids.
	 * @param user The name of the user.
	 * @param newestTweetId The upper bound for the interval of tweets.
	 * @param oldestTweetId The lower bound.
	 */
	public void fetch(String user, long newestTweetId, long oldestTweetId) {
		long currentId = newestTweetId;
		List<Status> results;
		
		do {
			results = fetchSome(user, currentId);
			if (results.size() > 0) {
				currentId = results.get(results.size() - 1).getId();
				getDbSaver().saveTweets(results);
			}
		} while (currentId > oldestTweetId);
	}

}
