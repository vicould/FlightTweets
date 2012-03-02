package net.vicould.tweets;

import java.io.BufferedReader;
import java.io.DataInputStream;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;


public class Launcher {

	/**
	 * @param args
	 * @throws IOException 
	 */
	public static void main(String[] args) throws IOException {
		// TODO Auto-generated method stub
		try {
			FileInputStream fstream = new FileInputStream("tweeterid.txt");
			// Get the object of DataInputStream
			DataInputStream in = new DataInputStream(fstream);
			BufferedReader br = new BufferedReader(new InputStreamReader(in));
			String strLine;

			while ((strLine = br.readLine()) != null)   {
				// Print the content on the console 
				TweetFetcher fetcher = new TweetFetcher();
				fetcher.fetchSome(strLine, 174110356057231361L);
				fetcher.fetch(strLine, 174110356057231361L, 39564783707623425L);
			}
			//Close the input stream
			in.close();
		} catch (Exception e) {
			System.err.println("Error: " + e.getMessage()); 
		}
	}
}
