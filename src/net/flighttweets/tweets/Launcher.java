package net.flighttweets.tweets;

import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;

/**
 * Entry point for the application, triggering the fetch and the analysis.
 *
 */
public class Launcher {
	public static final int ARGS_ERROR = 1;
	public static final int FILE_FORMAT_ERROR = 2;

	/**
	 * Method to parse the input, in order to retrieve the usernames and the keywords. 
	 * @param filename The name of the file containing the input.
	 * @param usernames An empty array that we will fill with the usernames.
	 * @param keywords An empty array that we will fill with the keywords.
	 * @return True if we parsed everything correctly. False otherwise.
	 */
	public static boolean readInputFile(String filename, ArrayList<String> usernames, ArrayList<String> keywords,ArrayList<String>events) {
		try {
			BufferedReader reader = new BufferedReader(new InputStreamReader(new FileInputStream(filename)));
			
			// first line is the usernames
			String usernamesLine = reader.readLine();
			if (usernamesLine != null) {
				String usernamesContent[] = usernamesLine.split(":");
				// the line should have the pattern "username: bab, ar" 
				if (usernamesContent.length != 2) {
					return false;
				}
				for (String username: usernamesContent[1].split(",")) {
					usernames.add(username.trim());
				}
			} else {
				return false;
			}
			
			// second line contains the keywords
			String keywordsLine = reader.readLine();
			if (keywordsLine != null) {
				String keywordsContent[] = keywordsLine.split(":");
				// the line should have the pattern "keyword: bab, ar"
				if (keywordsContent.length != 2) {
					return false;
				}
				for (String keyword: keywordsContent[1].split(",")) {
					keywords.add(keyword.trim());
				}
			} else {
				return false;
			}
			String eventString;
                        while ((eventString = reader.readLine()) != null) {
                            events.add(eventString);
                        }
                
			return true;
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		return false;
	}
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {
		if (args.length != 1) {
			System.out.println("Please provide as argument the filename of the input file\n" +
							   "containing the lines usernames: user, name and \n" +
							   "keywords: key, words\n");
			System.exit(ARGS_ERROR);
		}
		
		ArrayList<String> usernames = new ArrayList<String>();
		ArrayList<String> keywords = new ArrayList<String>();
                ArrayList<String> events = new ArrayList<String>();
		
		boolean success = readInputFile(args[0], usernames, keywords,events);
		
		if (!success) {
			System.out.println("There was an issue with the input file, please check the format.");
			System.out.println("Please provide as argument the filename of the input file\n" +
					   "containing the lines usernames: user, name and \n" +
					   "keywords: key, words\n");
			System.exit(FILE_FORMAT_ERROR);
		}
		
		// starts fetching
		TweetFetcher fetcher = new TweetFetcher(usernames);
		fetcher.resumeTweetFetching();

                //populate event list with any new events.
                TweetFilter.populateEventList(events);
		// filters the keywords
		TweetFilter filter = new TweetFilter();
		filter.filterTweets(keywords);
		
		// processes the tweets
		TimelinessAnalyzer analyze = new TimelinessAnalyzer();
		try {
			analyze.timeliness();
		} catch (SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		GeoAnalyzer geo = new GeoAnalyzer();
		geo.geographicalAnalyzer();
		
		FlightNumberAnalyzer.TweetsWithFlight();
	}

}
