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
	
	public TweetFetcher() {
		setDbSaver(new TweetSaver());
	}
	
	public List<Status> fetchSome(String user, long previousTweetId) {
		Twitter twitter = new TwitterFactory().getInstance();
		
		try {
			Paging paging = new Paging();
			paging.maxId(previousTweetId - 1L);
			
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

	private TweetSaver getDbSaver() {
		return dbSaver;
	}

	private void setDbSaver(TweetSaver dbSaver) {
		this.dbSaver = dbSaver;
	}

}
