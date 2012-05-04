package net.flighttweets.tweets.data;

/**
 * 
 * This class stores the information necessary to know what to fetch for a particular
 * user and from where, id related.
 * It implements the {@link Comparable} interface for use in a sorted queue for
 * example.
 *
 */
public class FetchItemBundle implements Comparable<FetchItemBundle> {

	private String username;
	private long tweetId;
	
	/**
	 * Creates an instance holding the information for a username and its related
	 * id.
	 * @param username
	 * @param tweetId
	 */
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

	@Override
	public int compareTo(FetchItemBundle o) {
		return new Long(this.getTweetId()).compareTo(o.getTweetId());		
	}
	
}
