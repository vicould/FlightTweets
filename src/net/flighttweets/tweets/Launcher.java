package net.flighttweets.tweets;

import java.io.FileInputStream;
import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.SQLException;
import java.util.ArrayList;
import net.flighttweets.tweets.jaxb.*;
import javax.xml.bind.JAXBContext;
import javax.xml.bind.JAXBElement;
import javax.xml.bind.Marshaller;
import javax.xml.bind.Unmarshaller;
import java.io.File;

/**
 * Entry point for the application, triggering the fetch and the analysis.
 *
 */
public class Launcher {
	public static final int ARGS_ERROR = 1;
	public static final int FILE_FORMAT_ERROR = 2;
	
	private static ArrayList<String> usernames;
	private static ArrayList<String> keywords;
	private static ArrayList<String> events;
	
	private static JAXBContext jc = null;
	private static Marshaller marshaller = null;
	private static ObjectFactory factory = null;
	private static Unmarshaller unmarshaller;
	
	/**
	 * Method to parse the input, in order to retrieve the usernames and the keywords. 
	 * @param filename The name of the file containing the input.
	 * @param usernames An empty array that we will fill with the usernames.
	 * @param keywords An empty array that we will fill with the keywords.
	 * @return True if we parsed everything correctly. False otherwise.
	 */


	public static TweetConfigType readInput (String filename, ArrayList<String> usernames, ArrayList<String> keywords, ArrayList<String>events) {
		try {
			jc = JAXBContext.newInstance("net.flighttweets.tweets.jaxb");
			marshaller = jc.createMarshaller();
			marshaller.setProperty(Marshaller.JAXB_FORMATTED_OUTPUT,Boolean.TRUE);
			unmarshaller = jc.createUnmarshaller();
			factory = new ObjectFactory();

		} catch (Exception e) {
			e.printStackTrace();
		}
		TweetConfigType returnType = null;
		File file = new File(filename);
		if (file != null && file.exists()) {
			try {
				JAXBElement<TweetConfigType> jaxbelem = (JAXBElement<TweetConfigType>) unmarshaller.unmarshal(file);
				returnType = jaxbelem.getValue();
			} catch (Exception e) {
				e.printStackTrace();
			}
		}    
		int i = 0;
		for (i = 0; i < returnType.getKeyword().size();i++) {
			keywords.add(returnType.getKeyword().get(i));
		}
		for (i = 0; i < returnType.getUsername().size();i++) {
			usernames.add(returnType.getUsername().get(i));
		}
		for (i = 0;i < returnType.getEvent().size();i++) {
			EventType event = returnType.getEvent().get(i);
			events.add(event.getEventName() + ";" + event.getStartDate() + ";" + event.getEndDate());
		}
		return returnType;
	}

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

		usernames = new ArrayList<String>();
		keywords = new ArrayList<String>();
		events = new ArrayList<String>();
		
		TweetConfigType tweetConf = readInput(args[0],usernames,keywords,events);
		//boolean success = readInputFile(args[0], usernames, keywords,events);

		if (tweetConf == null) {
			System.out.println("There was an issue with the input file, please check the format.");
			System.out.println("Please provide as argument the filename of the input file\n" +
					"with appropriate xml containing keywords, usernames, and events \n");
			System.exit(FILE_FORMAT_ERROR);
		}

		// starts fetching
		TweetFetcher fetcher = new TweetFetcher(usernames);
		fetcher.resumeTweetFetching();

		
	}
	
	/**
	 * Method to be called by {@link TweetFetcher} when the fetch is complete, as it is asynchronous.
	 */
	public static void performAnalysis() {
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
