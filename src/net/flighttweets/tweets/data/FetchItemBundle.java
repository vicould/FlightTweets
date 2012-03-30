package net.flighttweets.tweets.data;

public class FetchItemBundle {

	private String username;
	private long tweetId;
	
	public FetchItemBundle(String username, long tweetId) {
		this.setUsername(username);
		this.setTweetId(tweetId);
	}

	public String getUsername() {
		return username;
	}

	public void setUsername(String username) {
		this.username = username;
	}

	public long getTweetId() {
		return tweetId;
	}

	public void setTweetId(long tweetId) {
		this.tweetId = tweetId;
	}
	
	public int compare(FetchItemBundle object) {
		return this.getUsername().compareTo(object.getUsername());
	}
	
}
