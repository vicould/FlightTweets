package net.flighttweets.tweets;

import java.util.PriorityQueue;

import net.flighttweets.tweets.data.FetchItemBundle;

public interface FetcherCallback {

	public void fetchComplete();
	
	public void handleFetchFailure(PriorityQueue<FetchItemBundle> currentPoint);
}
