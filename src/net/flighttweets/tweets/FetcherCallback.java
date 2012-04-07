package net.flighttweets.tweets;

import java.util.PriorityQueue;

import net.flighttweets.tweets.data.FetchItemBundle;

/**
 * Interface specifying the methods used inside a fetcher task to signify to 
 * the initiator some messages.
 *
 */
public interface FetcherCallback {

	/**
	 * Inside this method should be implemented the action to take once a fetch
	 * operation is terminated. 
	 */
	public void fetchComplete();
	
	/**
	 * When the task encounters an error, it stops and inform where it was before
	 * having this error.
	 * Here should be implemented recovery attempts, such as waiting before
	 * trying again, etc.
	 * @param currentPoint The item representing where the tweet
	 */
	public void handleFetchFailure(PriorityQueue<FetchItemBundle> currentPoint);
}
