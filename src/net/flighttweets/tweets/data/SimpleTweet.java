package net.flighttweets.tweets.data;

import java.util.Date;

/**
 * A simple tweet information holder, to use with what is stored in the database.
 * It does not follow the twitter4j.Status or twitter4j.Tweet interface, as less 
 * information is useful for us.
 */
public class SimpleTweet {

	private String content;
	private long tweetId;
	private long userId;
	private Date createdAt;
	
	public SimpleTweet(String content, long tweetId, long userId, Date createdAt) {
		super();
		this.content = content;
		this.tweetId = tweetId;
		this.userId = userId;
		this.createdAt = createdAt;
	}
	
	public String getContent() {
		return content;
	}
	public void setContent(String content) {
		this.content = content;
	}
	public long getTweetId() {
		return tweetId;
	}
	public void setTweetId(long tweetId) {
		this.tweetId = tweetId;
	}
	public long getUserId() {
		return userId;
	}
	public void setUserId(long userId) {
		this.userId = userId;
	}
	public Date getCreatedAt() {
		return createdAt;
	}
	public void setCreatedAt(Date createdAt) {
		this.createdAt = createdAt;
	}
	
	public String toString() {
		return getTweetId() + ": " + getCreatedAt().toString() + " (" + getUserId() + ") \n" + getContent();
	}
}
