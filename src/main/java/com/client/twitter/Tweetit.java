package com.client.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Random;

import jxl.Sheet;
import jxl.Workbook;
import jxl.read.biff.BiffException;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;


/**
 * A simple twitter client to tweet , retweet and clone text and images
 * Search tweets with a specific hashtag from twitter stream and retweeting them back in your timeline
 * Searching tweets with a specific hashtag from twitter stream, clone such tweets and tweet them to your timeline as new tweets
 * Tweeting status messages and pictures that can be picked from a source of data, currently being a spreadsheet. The pictures need not be embedded in the spreadsheet but just an absolute path (reference) to pictures in your hard drive has to go in the spreadsheet
 * 
 * Supported options
 * --tweet
 *   --spreadsheet "C:\My Documents\tweetit\tweets.xls" (default "C:\tweetit\tweets.xls")
 *   --sleep 45 (default 30 seconds)
 * --retweet
 *   --count 200 (default 500)
 *   --search "H4EAD" (default "H4EAD OR H4EADJAN15")
 *   --until 01/22/2015 (default today)
 * --clone
 *   --count 250 (default 500)
 *   --search "H4EAD" (default "H4EAD OR H4EADJAN15")
 *   
 * @author skanniah
 * @see http://metallicatony.blogspot.com/2015/02/tweetit-simple-bulk-tweeter.html
 */
public class Tweetit {
	private static final Logger log = LoggerFactory.getLogger(Tweetit.class);
	private static Twitter twitter = TwitterFactory.getSingleton();
	private static String qw;
	
	private static int perQueryCount;
	private static int requestedTotalRTCount;
	private static String untilDate;
	private static Map<String, Tweet> tweets = null;
	private static String spreadsheetPath;
	
	public static void main(String[] args) throws TwitterException, BiffException, IOException, ParseException {
		// get supported command line options
		Options options = getSupportedCmdLineOptions();
		// get cmd line parser
		CommandLine cmd = getCmdLine(options, args);
		// load props from config file
		Properties props = loadProperties(options, cmd, args);
		// set oauth credentials 
		setOAuthCredentials(props);
		// execute app based on the given command line options
		tweetit(options, props, args);
	}
	
	private static CommandLine getCmdLine(Options options, String[] args)
			throws ParseException {
		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);
		log.info("given options: {}", cmd.getOptions());
		return cmd;
	}
	
	/**
	 * Loads tweetit properties from tweetit.properties config file
	 * Loads token and secret used for authenticating twitter from user provided config file
	 * @param options
	 * @param cmd
	 * @param args
	 * @return
	 * @throws IOException
	 * @throws ParseException
	 */
	private static Properties loadProperties(Options options, CommandLine cmd, String[] args) throws IOException, ParseException {
		String propsFile = "/tweetit.properties";
		//String keyFile = "src\\main\\resources\\keyfile.properties";
		String keyFile = null;
		
		if (cmd.hasOption("keyfile")) {
			keyFile = cmd.getOptionValue("keyfile");
		} else {
			throw new ParseException("keyfile path not provided as an argument");
		}
		
		log.info("keyfilePath={}", keyFile);
		
		if (cmd.hasOption("propfile")) {
			propsFile = cmd.getOptionValue("propfile");
		}
		log.info("propsFilePath={}", propsFile);
		
		Properties properties = new Properties();
		InputStream propsFileInput = ClassLoader.class.getResourceAsStream(propsFile);
		InputStream keyFileInput = new FileInputStream(keyFile);
		
		properties.load(propsFileInput);
		properties.load(keyFileInput);
		
		//log.info("Properties from config file={}", properties);
		return properties;
	}

	/**
	 * Get all supported options allowed
	 * @return Options
	 */
	private static Options getSupportedCmdLineOptions() {
		Options options = new Options();
		options.addOption("retweet", false, "retweets the latest tweets");
		options.addOption("tweet", false, "tweets the contents from spreadsheet");
		options.addOption("clone", false, "clones tweets under a hashtag");
		options.addOption("count", true, "count of tweets to retweet");
		options.addOption("until", true, "tweets until date in format YYYY-MM-DD");
		options.addOption("spreadsheet", true, "tweet using content from spreadsheet path");
		options.addOption("keyfile", true, "file containing twitter token keys");
		options.addOption("propfile", true, "file containing tweetit properties");
		options.addOption("search", true, "keyword to search tweets for");
		options.addOption("every", true, "interval between tweets in seconds");
		//log.info("supported options: {}", options);
		return options;
	}
	
	/**
	 * 
	 * @param options
	 * @param properties
	 * @param args
	 * @throws BiffException
	 * @throws IOException
	 * @throws TwitterException
	 * @throws ParseException 
	 */
	private static void tweetit(Options options, Properties properties, String[] args) throws BiffException, IOException, TwitterException, ParseException {

		CommandLineParser parser = new BasicParser();
		CommandLine cmd = parser.parse(options, args);

		Integer interval = getIntervalBetweenTweets(cmd, properties);
		requestedTotalRTCount = Integer.valueOf(properties.getProperty("retweet.count"));
		qw = properties.getProperty("search.hashtag");
		if (properties.containsKey("search.perquery.count")) {
			perQueryCount = Integer.valueOf(properties.getProperty("search.perquery.count"));
		}

		// search and retweet
		if (cmd.hasOption("retweet")) {

			if (cmd.hasOption("until")) {
				untilDate = cmd.getOptionValue("until");
			}

			if (cmd.hasOption("count")) {
				requestedTotalRTCount = Integer.valueOf(cmd.getOptionValue("count"));
			}

			if (cmd.hasOption("search")) {
				qw = cmd.getOptionValue("search");
			}

			log.info("retweet count={} untilDate={} searchKeyword={} perSearchCount={}", new Object[] { requestedTotalRTCount, untilDate, qw, perQueryCount });
			List<Status> status = searchTweets(qw, perQueryCount, requestedTotalRTCount, untilDate);
			log.info("total size of searched tweets={}", status.size());
			retweet(status, interval);
		} else if (cmd.hasOption("tweet")) {
			// tweet using a spreadsheet
			if (cmd.hasOption("spreadsheet")) {
				spreadsheetPath = cmd.getOptionValue("spreadsheet");
			} else {
				spreadsheetPath = properties.getProperty("spreadsheet.path");
			}

			// get status from spreadsheet and tweet
			tweets = getTweetsFromSpreadsheet(spreadsheetPath);
			// tweet given contents
			tweetAllTweets(tweets, interval);
		} else if (cmd.hasOption("clone")) {
			// clone tweets with a specific hashtag
			if (cmd.hasOption("count")) {
				requestedTotalRTCount = Integer.valueOf(cmd
						.getOptionValue("count"));
			}

			if (cmd.hasOption("search")) {
				qw = cmd.getOptionValue("search");
			}

			if (cmd.hasOption("until")) {
				untilDate = cmd.getOptionValue("until");
			}
			log.info("clone tweets count={} searchKeyword={} perSearchCount={}", new Object[] { requestedTotalRTCount, qw, perQueryCount });
			List<Status> status = searchTweets(qw, perQueryCount, requestedTotalRTCount, untilDate);
			log.info("total size of searched tweets={}", status.size());
			cloneTweet(status, interval);
		}

	}
	
	/**
	 * Clones tweets pulled from twitter's global stream
	 * @param statuses
	 * @param interval
	 * @throws MalformedURLException
	 * @throws IOException
	 */
	private static void cloneTweet(List<Status> statuses, Integer interval) throws MalformedURLException, IOException {
		int counter = 0;
		for (Status status : statuses) {
			try {
				String statusText;

				if (status.getRetweetedStatus() != null
						&& status.getRetweetedStatus().getText() != null
						&& status.getRetweetedStatus().getText().length() > 0) {
					statusText = status.getRetweetedStatus().getText();
				} else {
					statusText = status.getText();
				}

				StatusUpdate clonedStatus;
				clonedStatus = new StatusUpdate(statusText);
				twitter.updateStatus(clonedStatus);
				log.info("cloning #{} tweet#={} text={}", new Object[] {(counter + 1), status.getId(), statusText});

				// sleeps for the given interval
				if (++counter <= statuses.size()) {
					sleepForInterval(interval);
				}
			} catch (TwitterException e) {
				log.error("received twitter exception", e);
			}
		}
	}
	
	/**
	 * Returns time to wait between tweets
	 * @param cmd
	 * @param properties
	 * @return
	 */
	private static Integer getIntervalBetweenTweets(CommandLine cmd, Properties properties) {
		Integer interval = Integer.valueOf(properties.getProperty("tweet.interval"));
		if (cmd.hasOption("every")) {
			Integer givenInterval = Integer.valueOf(cmd.getOptionValue("every"));
			interval = givenInterval > interval ? givenInterval : interval;
		}
		return interval;
	}
	
	/**
	 * Tweets given set of messages
	 * @param tweetsFromSpreadsheet the tweets from spreadsheet
	 * @param interval time in seconds
	 */
	private static void tweetAllTweets(Map<String, Tweet> tweetsFromSpreadsheet, Integer interval) {
		int counter = 1;
		for (String s : tweetsFromSpreadsheet.keySet()) {
			// tweet the message
			tweet(s, tweetsFromSpreadsheet.get(s));

			// sleeps for the given interval
			if (++counter <= tweetsFromSpreadsheet.size()) {
				sleepForInterval(interval);
			}
		}
	}
	

	/**
	 * Sleeps for a given interval
	 * @param interval time in seconds
	 */
	private static void sleepForInterval(Integer interval) {
		try {
			Random random = new Random();
			int randomSleep = random.nextInt(5) + (interval);
			log.info("waiting {} seconds to tweet next one...", randomSleep);
			Thread.sleep(randomSleep * 1000);
		} catch (InterruptedException ie) {
			log.error("interrupted exception received during sleep...");
		}
	}
	
	/**
	 * Tweets given text and image
	 * @param index
	 * @param tweet
	 */
	private static void tweet(String index, Tweet tweet) {
		try {
			
			// get first 280 chars
			String statusText = (tweet.getStatusText() != null && tweet.getStatusText().length() > 280) ? tweet.getStatusText().substring(0, 280) : tweet.getStatusText();
			log.info("Status Text from spreadsheet....... statusText={}", tweet.getStatusText());
			StatusUpdate updateStatus = new StatusUpdate(statusText);
			
			// upload image
			if (tweet.getStatusImage() != null
					&& tweet.getStatusImage().length() > 0) {
				log.info("Uploading....... image={}", tweet.getStatusImage());
				updateStatus.setMedia(new File(tweet.getStatusImage()));
			}
			
			// tweet
			Status status = twitter.updateStatus(updateStatus);
			log.info("successfully tweeted status={} tweet#={} tweetText={} tweetImage={}",
					new Object[] { status.getId(), index,
							tweet.getStatusText(), tweet.getStatusImage() });
		} catch (TwitterException e) {
			log.error("twitter exception statusCode={}, exceptionCode={}, errorCode={} accessLevel={} rateLimitStatus={}",
					new Object[] { e.getStatusCode(), e.getExceptionCode(), e.getErrorCode(), e.getAccessLevel(), e.getRateLimitStatus(), e });
		}
	}
	
	/**
	 * Gets tweets from spreadsheet and returns a map
	 * @param spreadsheet name of spreadsheet
	 * @return Map<String, Tweet> a map with index and tweet
	 * @throws BiffException
	 * @throws IOException
	 */
	private static Map<String, Tweet> getTweetsFromSpreadsheet(String spreadsheet) throws BiffException, IOException {
		Map<String, Tweet> tweetContents = null;
		Workbook workbook = Workbook.getWorkbook(new File(spreadsheet));
		Sheet sheet = workbook.getSheet(0);
		
		// do not include header
		int numOfContentRows = sheet.getRows() > 0? (sheet.getRows() - 1) : 0;
		log.info("Spreadsheet rows={} columns={} numOfContentRows={}", new Object[] {sheet.getRows(), sheet.getColumns(), numOfContentRows});
		
		if (numOfContentRows > 0) {
			tweetContents = new HashMap<String, Tweet>();
			
			// get sheet contents
			for (int i = 1; i <= numOfContentRows; i++) {
				Tweet tweet = null;
				String sno = getContentsFromCell(sheet, 0, i);
				String text = getContentsFromCell(sheet, 1, i);
				String image = getContentsFromCell(sheet, 2, i);
				
				if (text != null || image != null) {
					tweet = new Tweet(text, image);
					tweetContents.put(sno, tweet);
				}
			}
		}
		return tweetContents;
	}
	
	
	/**
	 * Gets contents from a specified cell
	 * @param sheet
	 * @param column
	 * @param row
	 * @return string content
	 */
	private static String getContentsFromCell(Sheet sheet, int column, int row) {
		try {
			return sheet.getCell(column, row).getContents();
		} catch (ArrayIndexOutOfBoundsException e) {
			log.warn("no text provided {}", e.getMessage());
		}
		return null;
	}
	
	/**
	 * Retweets a given list of tweets
	 * @param statuses a list of status
	 */
	private static void retweet(List<Status> statuses, Integer interval) {
		log.info("retweeting latest {} tweets...", statuses.size());
		int counter = 0;
		for (Status status : statuses) {
			try {
				// call retweet
				Status retweetStatus = twitter.retweetStatus(status.getId());
				log.info("reweet #{} original_status={} retweet_status={}", new Object[]{(counter+1), status.getId(), retweetStatus.getId()});
				
				// sleeps for the given interval
				if (++counter <= statuses.size()) {
					sleepForInterval(interval);
				}
			} catch (TwitterException e) {
					log.error("received twitter exception", e);
			}
		}
	}
	
	/**
	 * Returns a specified number of tweets matching a given hashtag keyword
	 * @param qw
	 * @return List<Status> list of status
	 * @throws TwitterException
	 */
	private static List<Status> searchTweets(String qw, int perQueryCount, int requestedTotalRTCount, String untilDate) throws TwitterException {
		
		Query searchQuery = new Query(qw);
		searchQuery.setCount(perQueryCount);
		if (untilDate != null) {
			searchQuery.setUntil(untilDate);
		}
		
		int i = 0;
		//log.info("query={}", searchQuery);
		QueryResult result = twitter.search(searchQuery);
		List<Status> resultStatuses = result.getTweets();
		log.info("### retrieved tweets from search#{}={} ###", new Object[] {i, resultStatuses.size()});
		
		if (resultStatuses.size() > 0) {
			long maxId = resultStatuses.get(resultStatuses.size() - 1).getId();
			while (requestedTotalRTCount > (perQueryCount * ++i)) {
				int difference = requestedTotalRTCount - (perQueryCount * i);
				difference = (difference > perQueryCount) ? perQueryCount : difference;
				searchQuery.setCount(difference);
				searchQuery.setMaxId(maxId - 1);

				result = twitter.search(searchQuery);
				List<Status> status = result.getTweets();

				maxId = status.get((status.size() - 1)).getId();
				resultStatuses.addAll(status);
				log.info("### retrieved tweets from search#{}={} maxId={} ###", new Object[] { i, resultStatuses.size(), maxId });
			}
		}
		
		return resultStatuses;
	}
	

	/**
	 * Sets OAuth credentials using properties
	 * @param properties
	 */
	private static void setOAuthCredentials(Properties properties) {
		String CONSUMER_KEY = properties.getProperty("user.consumer_key");
		String CONSUMER_SECRET = properties.getProperty("user.consumer_secret");
		String ACCESS_TOKEN = properties.getProperty("user.access_token");
		String ACCESS_TOKEN_SECRET = properties.getProperty("user.access_token_secret");
		
		twitter.setOAuthConsumer(CONSUMER_KEY, CONSUMER_SECRET);
		AccessToken accessToken = new AccessToken(ACCESS_TOKEN, ACCESS_TOKEN_SECRET);
		twitter.setOAuthAccessToken(accessToken);
	}
	
}
