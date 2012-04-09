
package net.flighttweets.tweets;

import java.io.IOException;
import java.sql.SQLException;

class Analyzer{
	
	public static void main(String []args) throws SQLException, IOException
	{
		
		TimelinessAnalyzer analyze=new TimelinessAnalyzer();
		analyze.timeliness();

		geoAnalyzer geo=new geoAnalyzer();
		//a.populateAirport_Codes();
		geo.geographicalAnalyzer();
		
		FlightNumberAnalyzer.TweetsWithFlight();
	
	}
}