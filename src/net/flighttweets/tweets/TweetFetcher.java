package net.flighttweets.tweets;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import twitter4j.Paging;
import twitter4j.Status;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;

public class TweetFetcher {

	/**
	 * Fetch some tweets for the specified user, that were sent before the id.
	 * @param user The username of the user we want to explore the tweets.
	 * @param previousTweetId The id of the tweet creating the superior bond for 
	 * the query. There is no way to work with dates with what provides as 
	 * parameters for the query, so we use ids.
	 * @return A list of twitter4j.Status, being all the tweets fetched for 
	 * the current query. 
	 * @throws IOException
	 */
	public List<Status> fetchSome(String user, long previousTweetId) {
		Twitter twitter = new TwitterFactory().getInstance();
		FileWriter fstream;
		try {
			// Creates the output stream to write the results
			fstream = new FileWriter("results.txt");
			BufferedWriter output = new BufferedWriter(fstream);
			String [] weather ={"joplin","missouri tornado","tuscaloosa","alabama tornado","hurricane","irene","october snowstorm","tornado","snowstorm"};
			try {
				// paging is a structure allowing to specify the range of ids we want, allowing a fragmented query.
				Paging paging = new Paging();
				// we want all the tweets that were emitted before the current id
				paging.maxId(previousTweetId - 1L);

				// retrieves the tweets for the current user
				List<Status> statuses = twitter.getUserTimeline(user, paging);
				for (Status status : statuses) {
					// dumps to the console what we fetched
					System.out.println(status.getCreatedAt() + " - " + status.getId() + " - " + status.getUser().getId()+" - " +status.getText());
					int i=0;
					for(i=0;i<weather.length;i++) {
						if (status.getText().toLowerCase().indexOf(weather[i]) != -1) {
							output.write(status.getText()+"\n\n");
						}
					}
				}
				output.close();
				
				return statuses;
			} catch (TwitterException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		} catch (IOException e1) {
			// TODO Auto-generated catch block
			e1.printStackTrace();
		}

		return new ArrayList<Status>();
	}

	/**
	 * Fetches a range of tweets for a specific user, and calls another method that saves them.
	 * @param user The username we are interested to fetch.
	 * @param newestTweetId The upper bond for the tweets.
	 * @param oldestTweetId The lower bond.
	 */
	public void fetch(String user, long newestTweetId, long oldestTweetId) {
		long currentId = newestTweetId;
		List<Status> results;

		// loops inside the range, as twitter allows only a progressive query.
		do {
			results = fetchSome(user, currentId);
			if (results.size() > 0) {
				currentId = results.get(results.size() - 1).getId();
			}
		} while (currentId > oldestTweetId);
	}

}