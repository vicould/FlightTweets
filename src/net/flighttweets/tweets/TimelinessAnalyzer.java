package net.flighttweets.tweets;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.sql.Connection;
import java.sql.Date;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Calendar;
import java.util.GregorianCalendar;


public class TimelinessAnalyzer
{
	void timeliness() throws SQLException, IOException
	{
		
		java.sql.Timestamp [] first_weather_tweet=new java.sql.Timestamp [4]; //array to store the first tweet by a weather station
		java.sql.Timestamp [][] first_airline_tweet=new java.sql.Timestamp [4][6]; //array to store the first tweet by an airline
		long [][] airline_tweet_delay=new long[4][6]; //array to store the delay in weather station tweet vs airline tweet
		String [] airline={"JetBlue","Delta","united","AmericanAir","SouthwestAir","SpiritAirlines"};
		String [] station={"accuweather","usnoaagov","breakingweather","weatherchannel"};
		String [] eventname={"Tuscaloosa Tornado","Joplin Tornado","Hurricane Irene","October Snowstorm"};
		
		FileWriter outstream = new FileWriter("outTimelinessAnalyzer.txt");
		BufferedWriter output = new BufferedWriter(outstream);
		
		int [] event={1,2,3,4};
		 
		Connection con=StorageManager.getInstance().getConnection();
		
        Calendar initdate;
        initdate=GregorianCalendar.getInstance();
        initdate.set(2012, 0, 1, 5, 15, 15);
        java.util.Date date2=initdate.getTime();
        for(int k=0;k<4;k++)
        {
        	for(int j=0;j<=5;j++)
        	{
        		first_airline_tweet[k][j]=new java.sql.Timestamp(date2.getTime()); //initialize the timestamps
        	}
        }
        
        for(int i=0;i<4;i++)
        {
        Calendar mindate;
        mindate=GregorianCalendar.getInstance();
        mindate.set(2012, 0, 1, 5, 15, 15);
        java.util.Date date=mindate.getTime(); //default minimum date
        
        Calendar mindate1;
        mindate1=GregorianCalendar.getInstance();
        mindate1.set(2012, 0, 1, 5, 15, 15);
        java.util.Date date1=mindate1.getTime(); //default minimum date
        
        java.sql.Timestamp ts1 = new java.sql.Timestamp(date.getTime());
        
        PreparedStatement str=con.prepareStatement("select * from kw_tweet where event_id="+event[i]); //fetch all tweets for one event from table
        ResultSet rs=str.executeQuery();
        
        while(rs.next())
        {    
        	long tweet_id=rs.getLong(1); //tweet_id from intermediate table
        	//fetch the corresponding tweet from main tweets table
        	PreparedStatement st=con.prepareStatement("select * from tweets where tweet_id="+tweet_id+" and username in('accuweather','usnoaagov','breakingweather','weatherchannel')");
        	ResultSet set=st.executeQuery();
        	while(set.next())
        	{
        		java.sql.Timestamp ts2=new java.sql.Timestamp(date.getTime()); //latest minimum date
        		if((set.getTimestamp(5).compareTo(ts2)<0) && (set.getString(4).indexOf("@"+station[0])==-1) && (set.getString(4).indexOf("@"+station[1])==-1) && (set.getString(4).indexOf("@"+station[2])==-1) && (set.getString(4).indexOf("@"+station[3])==-1)) //check if timestamp of current row is earlier than current minimum
        		{
        			date=new Date(set.getTimestamp(5).getTime());
        			ts1 = new java.sql.Timestamp(date.getTime());
        		}
        	} 
        	
        	for(int j=0;j<=5;j++) //iterate over all 6 airlines
        	{
        		PreparedStatement st1=con.prepareStatement("select * from tweets where tweet_id="+tweet_id+" and username='"+airline[j]+"'");
        		ResultSet set1=st1.executeQuery();
        		while(set1.next())
        		{
        			// find the minimum value
        			if((set1.getTimestamp(5).compareTo(first_airline_tweet[i][j])<0)  && (set1.getString(4).indexOf("@"+airline[j])==-1))
        			{
        				date1=new Date(set1.getTimestamp(5).getTime());
        				first_airline_tweet[i][j] = new java.sql.Timestamp(date1.getTime());
        			}
        		}
        	}
        }
        first_weather_tweet[i]=ts1;	
        }
        
        // calculate the delay between weather station tweet and airline tweet
        for(int i=0;i<4;i++)
        {
        	for(int j=0;j<=5;j++)
        	{
        		long timetemp=first_airline_tweet[i][j].getTime()-first_weather_tweet[i].getTime();
        		airline_tweet_delay[i][j]=timetemp/(1000*60); // convert delay to minutes
        	}
        }
        
       long [][] delay_copy=new long[4][6]; // copy into a temporary array
        for(int i=0;i<4;i++)
        {
        	for(int j=0;j<=5;j++)
        	{
        		delay_copy[i][j]=airline_tweet_delay[i][j];
        	}
        }
        
        String [][] ranking={{"JetBlue","Delta","united","AmericanAir","SouthwestAir","SpiritAirlines"},{"JetBlue","Delta","united","AmericanAir","SouthwestAir","SpiritAirlines"},{"JetBlue","Delta","united","AmericanAir","SouthwestAir","SpiritAirlines"},{"JetBlue","Delta","united","AmericanAir","SouthwestAir","SpiritAirlines"}};
        // sort the delays in tweeting in ascending order for each event
        for(int i=0;i<4;i++)
        {
        	for(int j=0;j<=4;j++)
        	{
        		for(int k=j+1;k<=5;k++)
        		{
        			if(delay_copy[i][j]>delay_copy[i][k])
        			{
        				String tempname=ranking[i][j];
        				ranking[i][j]=ranking[i][k];
        				ranking[i][k]=tempname;
        				long tempdelay=delay_copy[i][j];
        				delay_copy[i][j]=delay_copy[i][k];
        				delay_copy[i][k]=tempdelay;
        			}
        		}
        	}
        }
        output.write("\nTimeliness of Airlines notifications \n");
        output.write("\nThe ranking according to events are: \n");
        for(int i=0;i<4;i++)
        {
        	output.write("\nRanking for "+eventname[i]+" is : \n");
        	for(int j=0;j<=5;j++)
        	{
        		output.write(ranking[i][j]+"\n");
        	}
        }
        
        long mean_delay[]=new long[6];
        
        //calculate the mean delay over all 4 events
        for(int i=0;i<=5;i++)
        {
        	long sum=0;
        	for(int j=0;j<4;j++)
        	{
        		sum=sum+airline_tweet_delay[j][i];
        	}
        	mean_delay[i]=sum/4;
        }
        
        String [] overall_ranking={"JetBlue","Delta","united","AmericanAir","SouthwestAir","SpiritAirlines"};
        
        output.write("\n \nOverall timeliness ranking across all weather events is : \n");
        // sort the airlines in ascending order of mean delays
        for(int j=0;j<5;j++)
    	{
    		for(int k=j+1;k<=5;k++)
    		{
    			if(mean_delay[j]>mean_delay[k])
    			{
    				long temp=mean_delay[j];
    				mean_delay[j]=mean_delay[k];
    				mean_delay[k]=temp;
    				String tempname=overall_ranking[j];
    				overall_ranking[j]=overall_ranking[k];
    				overall_ranking[k]=tempname;
    			}
    		}
    	}
        
        for(int i=0;i<=5;i++)
        {
        	output.write(overall_ranking[i]+"\n");
        }
        output.close();
    }
}