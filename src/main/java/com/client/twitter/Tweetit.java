package com.client.twitter;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.MalformedURLException;
import java.net.URL;
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

import twitter4j.HttpResponseCode;
import twitter4j.MediaEntity;
import twitter4j.Query;
import twitter4j.QueryResult;
import twitter4j.Status;
import twitter4j.StatusUpdate;
import twitter4j.Twitter;
import twitter4j.TwitterException;
import twitter4j.TwitterFactory;
import twitter4j.auth.AccessToken;


/**
 * A simple twitter client to tweet text and images from spreadsheet
 * Supported options
 * --tweet
 *   --spreadsheet "C:\My Documents\tweetit\tweets.xls" (default "C:\tweetit\tweets.xls")
 *   --sleep 45 (default 30 seconds)
 * --retweet
 *   --count 200 (default 500)
 *   --search "H4EAD" (default "H4EAD OR H4EADJAN15")
 *   --until 01/22/2015 (default today)
 * @author skanniah
 *
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
		options.addOption("search", true, "keyword to search tweets for");
		options.addOption("every", true, "interval between tweets in seconds");
		//log.info("supported options: {}", options);
		return options;
	}
	
	/**
	 * @param options
	 * @param properties
	 * @param args
	 * @throws BiffException
	 * @throws IOException
	 * @throws TwitterException
	 * @throws ParseException 
	 */
	private static void tweetit(Options options, Properties properties,
			String[] args) throws BiffException, IOException, TwitterException, ParseException {
		
		CommandLineParser parser = new BasicParser();
			CommandLine cmd = parser.parse(options, args);
			//log.info("given options: {}", cmd.getOptions());
			
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
				
				log.info("retweet count={} untilDate={} searchKeyword={} perSearchCount={}", new Object[] {requestedTotalRTCount, untilDate, qw, perQueryCount});
				List<Status> status = searchTweets(qw, perQueryCount, requestedTotalRTCount, untilDate);
				log.info("total size of searched tweets={}", status.size());
				retweet(status, interval);
			}
			
			if (cmd.hasOption("tweet")) {
				if (cmd.hasOption("spreadsheet")) {
					spreadsheetPath = cmd.getOptionValue("spreadsheet");
				} else {
					spreadsheetPath = properties.getProperty("spreadsheet.path");
				}
				
				log.info("tweets approximately every {} seconds", interval);
				// log.info("new tweets spreadsheet={}", spreadsheetPath);
				
				// get status from spreadsheet and tweet
				tweets = getTweetsFromSpreadsheet(spreadsheetPath);
				
				// tweet given contents
				tweetAllTweets(tweets, interval);
			}
			
			if (cmd.hasOption("clone")) {
				if (cmd.hasOption("count")) {
					requestedTotalRTCount = Integer.valueOf(cmd.getOptionValue("count"));
				}
				
				if (cmd.hasOption("search")) {
					qw = cmd.getOptionValue("search");
				}
				
				if (cmd.hasOption("until")) {
					untilDate = cmd.getOptionValue("until");
				}
				log.info("clone tweets count={} searchKeyword={} perSearchCount={}", new Object[] {requestedTotalRTCount, qw, perQueryCount});
				List<Status> status = searchTweets(qw, perQueryCount, requestedTotalRTCount, untilDate);
				log.info("total size of searched tweets={}", status.size());
				//retweet(status, interval);
				//Status status = getTweet();
				cloneTweet(status, interval);
			}
			
		}
	
	private static Status getTweet() throws TwitterException {
		Long i = new Long("565746001890127872");
		Status status = twitter.showStatus(i.longValue());
		return status;
	}
	
	private static void cloneTweet(List<Status> statuses, Integer interval)
			throws MalformedURLException, IOException {
		int counter=0;
		for (Status status : statuses) {
			try {
				String statusText = status.getText();
				StatusUpdate clonedStatus;

				if (status.getText().startsWith("RT")) {
					statusText = statusText
							.substring(statusText.indexOf(":") + 2);
				}

				clonedStatus = new StatusUpdate(statusText);
				/*
				 * if (status.getMediaEntities().length > 0) { statusText =
				 * statusText.substring(0, 110); MediaEntity[] media =
				 * status.getMediaEntities();
				 * log.info("cloning media text={} URL={}", statusText,
				 * media[0].getMediaURL()); InputStream is = new
				 * URL(media[0].getMediaURL()).openStream();
				 * clonedStatus.media("c", is); }
				 */
				twitter.updateStatus(clonedStatus);
				log.info("cloning #{} tweet#={} text={}", new Object[] {(counter+1), status.getId(), statusText});
				// sleeps for the given interval
				if (++counter <= statuses.size()) {
					sleepForInterval(interval);
				}
			} catch (TwitterException e) {
				// log.error(
				// "twitter exception statusCode={}, exceptionCode={}, errorCode={} accessLevel={} rateLimitStatus={}",
				// new Object[] { e.getStatusCode(), e.getExceptionCode(),
				// e.getErrorCode(), e.getAccessLevel(), e.getRateLimitStatus()
				// });
				if (e.getStatusCode() == HttpResponseCode.TOO_MANY_REQUESTS) {
					log.error(
							"TOO_MANY_REQUESTS RATE_LIMITED: received twitter exception={}",
							e.getMessage());
				}
			}
		}
	}
	
	private static Integer getIntervalBetweenTweets(CommandLine cmd, Properties properties) {
		Integer interval = Integer.valueOf(properties.getProperty("tweet.interval"));
		if (cmd.hasOption("every")) {
			Integer givenInterval = Integer.valueOf(cmd.getOptionValue("every"));
			interval = Integer.valueOf(cmd.getOptionValue("every")) > interval ? givenInterval : interval;
		}
		return interval;
	}
	
	/**
	 * Tweets the given set of data
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
	 * Sleeps for the given interval (intentionally approximate so that twitter
	 * folks cannot straight predict this app
	 * 
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
			
			// get first 140 chars
			String statusText = (tweet.getStatusText() != null && tweet
					.getStatusText().length() > 140) ? tweet.getStatusText().substring(0, 140) : tweet.getStatusText();
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
			log.error(
					"twitter exception statusCode={}, exceptionCode={}, errorCode={} accessLevel={} rateLimitStatus={}",
					new Object[] { e.getStatusCode(), e.getExceptionCode(), e.getErrorCode(), e.getAccessLevel(), e.getRateLimitStatus(), e });
			if (e.getStatusCode() == HttpResponseCode.FORBIDDEN) {
				log.error("received twitter exception={}", e.getMessage());
			}
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
				 
				//log.info("sno={} text={} image={}", new Object[] {sno, text, image});
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
//				log.error(
//						"twitter exception statusCode={}, exceptionCode={}, errorCode={} accessLevel={} rateLimitStatus={}",
//						new Object[] { e.getStatusCode(), e.getExceptionCode(), e.getErrorCode(), e.getAccessLevel(), e.getRateLimitStatus() });
				if (e.getStatusCode() == HttpResponseCode.TOO_MANY_REQUESTS) {
					log.error("TOO_MANY_REQUESTS RATE_LIMITED: received twitter exception={}", e.getMessage());
				}
			}
		}
	}
	
	/**
	 * Returns a specified number of tweets matching a given keyword
	 * @param qw
	 * @return List<Status> list of status
	 * @throws TwitterException
	 */
	private static List<Status> searchTweets(String qw, int perQueryCount,
			int requestedTotalRTCount, String untilDate)
			throws TwitterException {
		
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

//				log.info("query={}", searchQuery);
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
	 * Sets OAuth credentials
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
