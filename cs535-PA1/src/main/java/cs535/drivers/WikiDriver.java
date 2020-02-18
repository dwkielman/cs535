package cs535.drivers;

import java.text.DecimalFormat;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

/*
 * Hyperlink-Induced Topic Search (HITS) over Wikipedia Articles using Apache Spark
 * Daniel Kielman
 * CS 535
 */

public class WikiDriver extends Driver {
	
	private static String query;
	private static final String LINKS_DELIMITER = ":";
	private static final String LINKS_VALUES_DELIMITER = " ";
	private static final int PERCENTILE_CORRECT = 90;
	private static final double ACCURACY_PERCENT = 0.000001;
	
	
	public static void main(String[] args) {
		
		if (args.length != 1) {
			System.out.println("ERROR: One argument must be passed in order to query on dataset.");
			System.exit(0);
		} else {
			query = args[0];
		}
		
        new WikiDriver().run();
    }
	
	private void run() {
		
		// set up the spark configuration and context
        SparkConf conf = new SparkConf().setAppName("Wiki Analysis");

        JavaSparkContext sc = new JavaSparkContext(conf);

        String loweredQuery = query.toLowerCase();
        
        // Root set work
        
        // put the titles file in an RDD with the index to sort from
        JavaRDD<String> titlesRDD = sc.textFile(HDFS_TITLES_SORTED).filter(s -> isEmptyValue(s));
        JavaPairRDD<String, Long> titlesWithIndexRDD = titlesRDD.zipWithIndex().mapToPair(s -> new Tuple2<String, Long>(s._1.toLowerCase(), (s._2 + 1)));
        JavaPairRDD<String, Long> filteredTitlesWithIndex = titlesWithIndexRDD.filter(s -> s._1.contains(loweredQuery));
        JavaPairRDD<Long, String> rootSetRDD = filteredTitlesWithIndex.mapToPair(f -> new Tuple2<Long, String>(f._2, f._1)).cache();
        
        filteredTitlesWithIndex.saveAsTextFile(String.format("%s/RootSet", HDFS_OUTPUT_LOCATION));
        
        // Base Set work
        
        JavaRDD<String> linksRDD = sc.textFile(HDFS_LINKS_SIMPLE_SORTED).filter(s -> isEmptyValue(s));
        
        // builds an RDD with the index as the key and all links as the value in a single String
        JavaPairRDD<Long, String> linksPairRDD = linksRDD.mapToPair(f -> {
        	String[] split = f.split(LINKS_DELIMITER);
        	long key = Long.parseLong(split[0]);
        	return new Tuple2<Long, String>(key, split[1]);
        });
        
        // create an RDD of all of the links in the original file
        JavaPairRDD<Long, Long> allFlattenedLinks = linksPairRDD.flatMapToPair(f -> {
        	long key = f._1;
        	String[] values = f._2.trim().split(LINKS_VALUES_DELIMITER);
        	
        	List<Tuple2<Long, Long>> returnList = new ArrayList();
        	
        	for (String s : values) {
        		returnList.add(new Tuple2<>(key, Long.parseLong(s)));
        	}
        	return returnList.iterator();
        });

        // get half the base set by joining the roots with their links
        JavaPairRDD<Long, Long> rootLinkWithWholeSet = rootSetRDD.join(allFlattenedLinks).mapToPair(f -> new Tuple2<Long, Long>(f._1, f._2._2));

        // reverse the flattened links for joining
        JavaPairRDD<Long, Long> reversedFlattenedLinks = allFlattenedLinks.mapToPair(f -> f.swap());
        
        // get remaining half of base set by joining with the links that are referenced in all of the original links
        JavaPairRDD<Long, Long> reverseRootLinkWithWholeSet = rootSetRDD.join(reversedFlattenedLinks).mapToPair(f -> new Tuple2<Long, Long>(f._1, f._2._2));
        
        // reverse results to join correctly, otherwise joins with root set
        JavaPairRDD<Long, Long> otherHalfRootLinkWithWholeSet = reverseRootLinkWithWholeSet.mapToPair(f -> f.swap());
        
        // union together for base set
        JavaPairRDD<Long, Long> baseSetRDD = rootLinkWithWholeSet.union(otherHalfRootLinkWithWholeSet).cache();

        // Hub and Authority scores work
        
        // initialize hub and authority scores with the indices of the root set with a default of 1.0
        JavaRDD<Long> baseKeys = baseSetRDD.keys().distinct();
        JavaRDD<Long> baseValues = baseSetRDD.values().distinct();
        
        JavaPairRDD<Long, Double> hubScores = baseKeys.union(baseValues).distinct().mapToPair(f -> new Tuple2<Long, Double>(f, 1.0));
        JavaPairRDD<Long, Double> authorityScores = baseKeys.union(baseValues).distinct().mapToPair(f -> new Tuple2<Long, Double>(f, 1.0));
        
        JavaPairRDD<Long, String> prettyBasePrint = hubScores.join(titlesWithIndexRDD.mapToPair(f -> f.swap())).mapToPair(f -> new Tuple2<Long, String>(f._1, f._2._2));
        prettyBasePrint.saveAsTextFile(String.format("%s/BaseSet", HDFS_OUTPUT_LOCATION));

        //String debug = "BEFORE CONVERGIN: AUTHORITY SCORES = " + authorityScores.count() + "\n";
        long numberOfHubScores, numberOfAuthorityScores;
        numberOfHubScores = numberOfAuthorityScores = hubScores.count();

        boolean isScoreConverged = false;
        int numberOfConverganceLoops = 0;
        
        while (!isScoreConverged) {
        	numberOfConverganceLoops++;
        	
        	// authority scores
        	// raw authority score is the sum of all of the hub scores for the given index
        	JavaPairRDD<Long, Double> rawAuthorityScores = baseSetRDD.join(hubScores).mapToPair(f -> new Tuple2<>(f._2._1, f._2._2)).reduceByKey((x, y) -> x + y);

        	// for normalizing, use similar logic from above to sum all of the authority scores and divide each entry by the sum
        	double authrorityScoresSum = rawAuthorityScores.mapToPair(f -> new Tuple2<String, Double>("authoritySum", f._2)).reduceByKey((x, y) -> x + y).collect().get(0)._2;
        	JavaPairRDD<Long, Double> normalizedAuthorityScores = rawAuthorityScores.mapToPair(f -> new Tuple2<Long, Double>(f._1, (f._2 / authrorityScoresSum)));

        	// hub scores
        	// raw hub scores are calculated similarly, but this time we use the normalized authority scores and combine all authority score for a given hub index
        	JavaPairRDD<Long, Double> rawHubScores = baseSetRDD.mapToPair(f -> new Tuple2<Long, Long>(f._2, f._1)).join(normalizedAuthorityScores).mapToPair(f -> new Tuple2<Long, Double>(f._2._1, f._2._2)).reduceByKey((x, y)-> x + y);

        	// normalizing, same process as authority scores normalization
        	double hubScoresSum = rawHubScores.mapToPair(f -> new Tuple2<String, Double>("hubSum", f._2)).reduceByKey((x, y) -> x + y).collect().get(0)._2;
        	JavaPairRDD<Long, Double> normalizedHubScores = rawHubScores.mapToPair(f -> new Tuple2<Long, Double>(f._1, (f._2 / hubScoresSum)));

        	boolean isAuthorityConverged = hasScoreConverged(normalizedAuthorityScores, authorityScores, numberOfAuthorityScores);
        	boolean isHubConverged = hasScoreConverged(normalizedHubScores, hubScores, numberOfHubScores);
        	
        	// when both have converged then break, otherwise the normalized scores are the new scores
        	if (isAuthorityConverged && isHubConverged) {
        		isScoreConverged = true;
        	} else {
        		hubScores = normalizedHubScores;
        		authorityScores = normalizedAuthorityScores;
        	}
        	
        	// emergency break out of loop in case scores don't converge to proper percent accurate
        	if (numberOfConverganceLoops >= 25) {
        		isScoreConverged = true;
        		break;
        	}
        }
        
        // print authority and hub scores for at least top 50 in descending order
        JavaPairRDD<String, Double> joinedHubScores = hubScores.join(rootSetRDD).mapToPair(f -> new Tuple2<Double, String>(f._2._1, f._2._2)).sortByKey(false).mapToPair(f -> new Tuple2<String, Double>(f._2, f._1));
        
        JavaPairRDD<String, Double> joinedAuthorityScores = authorityScores.join(rootSetRDD).mapToPair(f -> new Tuple2<Double, String>(f._2._1, f._2._2)).sortByKey(false).mapToPair(f -> new Tuple2<String, Double>(f._2, f._1));
        
        List<String> hubKeys = joinedHubScores.keys().collect();
        List<Double> hubValues = joinedHubScores.values().collect();
        
        List<String> writeMeHub = new ArrayList<>();
        writeMeHub.add("Hub Set");
        writeMeHub.add("=======\n");
        writeMeHub.add("Number of Loops took to Converge to " + new DecimalFormat("#0.000000").format(ACCURACY_PERCENT) + ": " + numberOfConverganceLoops + "\n");
        writeMeHub.add("Hub total number of entries for " + loweredQuery + ": " + hubKeys.size() + "\n");
        
        int hubCount = 0;
        
        if (hubKeys.size() > 0) {
        	if (hubKeys.size() < 100) {
        		hubCount = hubKeys.size();
        	} else {
        		hubCount = 100;
        	}
        }
        
        for (int i = 0; i < hubCount; i++) {
        	writeMeHub.add("Wiki Page Title: " + hubKeys.get(i) +  "\tHub Score: " + hubValues.get(i));
        }
        
        sc.parallelize(writeMeHub, 1).saveAsTextFile(String.format("%s/HubScores", HDFS_OUTPUT_LOCATION));
        
        List<String> authorityKeys = joinedAuthorityScores.keys().collect();
        List<Double> authorityValues = joinedAuthorityScores.values().collect();

        List<String> writeMeAuthority = new ArrayList<>();
        writeMeAuthority.add("Authority Set");
        writeMeAuthority.add("=============\n");
        writeMeAuthority.add("Number of Loops took to Converge to " + new DecimalFormat("#0.000000").format(ACCURACY_PERCENT) + ": " + numberOfConverganceLoops + "\n");
        writeMeAuthority.add("Authority total number of entries for " + loweredQuery + ": " + authorityKeys.size() + "\n");
        
        int authorityCount = 0;
        
        if (authorityKeys.size() > 0) {
        	if (authorityKeys.size() < 100) {
        		authorityCount = authorityKeys.size();
        	} else {
        		authorityCount = 100;
        	}
        }
        
        for (int i = 0; i < authorityCount; i++) {
        	writeMeAuthority.add("Wiki Page Title: " + authorityKeys.get(i) +  "\tAuthority Score: " + authorityValues.get(i));
        }
        
        sc.parallelize(writeMeAuthority, 1).saveAsTextFile(String.format("%s/AuthorityScores", HDFS_OUTPUT_LOCATION));
	}
	
	// validate that a new score has converged with the old score
	private static boolean hasScoreConverged(JavaPairRDD<Long, Double> newScores, JavaPairRDD<Long, Double> oldScores, long numberOfScores) {
		
		long joinedScoresCount = newScores.join(oldScores).filter(f -> {
			if (Math.abs((f._2._1 - f._2._2)) < ACCURACY_PERCENT) {
				return true;
			}
			return false;
		}).count();

		float percent = (joinedScoresCount * 100.0f) / numberOfScores;
		
		return (percent > PERCENTILE_CORRECT);
	}
	
	// validate entries in the source files
	private static boolean isEmptyValue(String s) {
		
		if (s == null) {
    		return false;
    	} else if (s.trim().length() < 1) {
    		return false;
    	} else if (s != null) {
    		if (s.trim().length() > 0) {
    			return true;
    		}
    	}
    	return s.trim().length() < 1;
	}
	
}
