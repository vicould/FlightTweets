package net.flighttweets.tweets;

import java.util.GregorianCalendar;

public class Launcher {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		// TODO Auto-generated method stub

//		TweetFetcher fetcher = new TweetFetcher();
//		fetcher.fetchSome("delta", 108000000000000000L);
		
//		StorageManager manager = StorageManager.getInstance();
//		manager.verifyDB();
//
//		TweetFetcher fetcher = new TweetFetcher();
//		//		100 000 000 000 000 000
//		fetcher.fetch("united", 108000000000000000L, 106000000000000000L);
		
//		TweetSaver saver = new TweetSaver();
//		saver.saveTweetsToFile("test.txt");
		
		TweetAnalyzer analyzer = new TweetAnalyzer();
		// these fuckers start their month at 0 ...
		analyzer.searchForKeyword("irene", (new GregorianCalendar(2011, 7, 22)).getTime(), (new GregorianCalendar(2011, 8, 1)).getTime());
	}

}
