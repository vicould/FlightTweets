package net.vicould.tweets;

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
		
		TweetSaver saver = new TweetSaver();
		saver.saveTweetsToFile("test.txt");
	}

}
