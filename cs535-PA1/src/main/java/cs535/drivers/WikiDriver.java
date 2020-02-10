package cs535.drivers;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;

import scala.Tuple2;

public class WikiDriver extends Driver {
	
	private static String query;
	private static final String FILE_DELIMITER = "_";
	private static final String LINKS_DELIMITER = ":";
	private static final String LINKS_VALUES_DELIMITER = " ";
	
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

        // CORRECT LATER: create the root set where the titles match the query that has been passed
        
        // put the titles file in an RDD with the index to sort from
        String loweredQuery = query.toLowerCase();
        JavaRDD<String> titlesRDD = sc.textFile(HDFS_TITLES_SORTED).filter(s -> isEmptyValue(s));
        JavaPairRDD<String, Long> titlesWithIndexRDD = titlesRDD.zipWithIndex().mapToPair(s -> new Tuple2<String, Long>(s._1.toLowerCase(), (s._2 + 1)));
        JavaPairRDD<String, Long> filteredTitlesWithIndex = titlesWithIndexRDD.filter(s -> s._1.contains(loweredQuery));
        JavaPairRDD<Long, String> rootSetRDD = filteredTitlesWithIndex.mapToPair(f -> new Tuple2<Long, String>(f._2, f._1)).cache();
        int size = rootSetRDD.values().collect().size();
        
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Root Set");
        writeMe.add("========\n");
        writeMe.add("Total in Root Set: " + size);
        sc.parallelize(writeMe, 1).saveAsTextFile(String.format("hdfs://%s/cs535/PA1/output/RootSet", HDFS_SERVER));
        
        
        
        // Base Set Time
        JavaRDD<String> linksRDD = sc.textFile(HDFS_LINKS_SIMPLE_SORTED).filter(s -> isEmptyValue(s));
        
        
        JavaPairRDD<Long, String> linksPairRDD = linksRDD.mapToPair(f -> {
        	String[] split = f.split(LINKS_DELIMITER);
        	long key = Long.parseLong(split[0]);
        	return new Tuple2<Long, String>(key, split[1]);
        });
       
        // Get the Links that the root set links to
        JavaPairRDD<Long, String> linkedToRootRDD = rootSetRDD.join(linksPairRDD).mapToPair(f -> {
        	return new Tuple2<Long, String>(f._1, f._2._2);
        });
        
        JavaPairRDD<Long, Long> flattenedLinks = linkedToRootRDD.flatMapToPair(f -> {
        	long key = f._1;
        	String[] values = f._2.trim().split(LINKS_VALUES_DELIMITER);
        	
        	List<Tuple2<Long, Long>> returnList = new ArrayList();
        	
        	for (String s : values) {
        		returnList.add(new Tuple2<>(key, Long.parseLong(s)));
        	}
        	return returnList.iterator();
        });
        
        JavaRDD<Long> distintRDDRootLinks = flattenedLinks.values();
        
        //List<Long> distinctRootLinks = flattenedLinks.values().distinct().collect();
        
        List<Long> rootSetKeys = rootSetRDD.keys().collect();
        
        // get the pages that link to the root set
        JavaRDD<Long> pagesThatLinkToRootSet = linksPairRDD.filter(f -> {
        	String[] values = f._2.trim().split(LINKS_VALUES_DELIMITER);
        	for (String s : values) {
        		if (rootSetKeys.contains(Long.parseLong(s))) {
        			return true;
        		}
        	}
        	return false;
        }).map(f -> {
        	return f._1;
        });
        
        JavaRDD<Long> rootRDDKeys = rootSetRDD.keys();
        
        JavaRDD<Long> baseSetMaybe = pagesThatLinkToRootSet.union(distintRDDRootLinks).union(rootRDDKeys);
        JavaRDD<Long> distinctBaseSetMaybe = baseSetMaybe.distinct();
        
        int baseSize = distinctBaseSetMaybe.collect().size();
        List<Long> sampleBaseKey = distinctBaseSetMaybe.collect();
        //List<String> sampleBaseValue = linkedToRootRDD.values().collect();
        List<String> writeMeBase = new ArrayList<>();
        writeMeBase.add("Base Set");
        writeMeBase.add("========\n");
        writeMeBase.add("Total in Base Set: " + baseSize);
        
        for (int i=1; i < 11; i++) {
        	writeMeBase.add("Key: " + sampleBaseKey.get(i));
        }
        
        writeMeBase.add("Flattened Links Sample Set");
        List<Long> flattenedKeys = flattenedLinks.keys().collect();
        List<Long> flattnedValues = flattenedLinks.values().collect();
        writeMeBase.add("========\n");
        writeMeBase.add("Total in Flattened Links Keys Set: " + flattenedKeys.size());
        writeMeBase.add("Total in Flattened Links Values Set: " + flattnedValues.size());
        
        for (int i=1; i < 11; i++) {
        	writeMeBase.add("Flattened Key: " + flattenedKeys.get(i));
        	writeMeBase.add("Flattened Value: " + flattnedValues.get(i));
        }
        
        sc.parallelize(writeMeBase, 1).saveAsTextFile(String.format("hdfs://%s/cs535/PA1/output/BaseSet", HDFS_SERVER));
        
        // hub and authority scores time
        JavaPairRDD<Long, Double> hubScores = linkedToRootRDD.mapToPair(f -> new Tuple2<Long, Double>(f._1, 1.0));
        JavaPairRDD<Long, Double> authorityScores = hubScores;
        
        boolean isScoreConverged = false;
        
        while (!isScoreConverged) {
        	JavaPairRDD<Long, Double> rawAuthorityScores = flattenedLinks.join(hubScores).mapToPair(f -> new Tuple2<>(f._2._1, f._2._2)).reduceByKey((x, y) -> x + y);
        	JavaPairRDD<Long, Double> rawHubScores = flattenedLinks.mapToPair(f -> new Tuple2<Long, Long>(f._2, f._1)).join(rawAuthorityScores).mapToPair(f -> new Tuple2<Long, Double>(f._2._1, f._2._2)).reduceByKey((x, y)-> x + y);
        	
        	double authrorityScoresSum = rawAuthorityScores.mapToPair(f -> new Tuple2<String, Double>("Current Sum", f._2)).reduceByKey((x, y) -> x + y).collect().get(0)._2;
        	JavaPairRDD<Long, Double> normalizedAuthorityScores = rawAuthorityScores.mapToPair(f -> new Tuple2<Long, Double>(f._1, (f._2 / authrorityScoresSum)));
        	
        	double hubScoresSum = rawHubScores.mapToPair(f -> new Tuple2<String, Double>("Current Sum", f._2)).reduceByKey((x, y) -> x + y).collect().get(0)._2;
        	JavaPairRDD<Long, Double> normalizedHubScores = rawAuthorityScores.mapToPair(f -> new Tuple2<Long, Double>(f._1, (f._2 / hubScoresSum)));
        	
        	boolean isAuthorityConverged = hasScoreConverged(normalizedAuthorityScores, authorityScores);
        	boolean isHubConverged = hasScoreConverged(normalizedHubScores, hubScores);
        	
        	if (isAuthorityConverged && isHubConverged) {
        		isScoreConverged = true;
        	} else {
        		hubScores = normalizedHubScores;
        		authorityScores = normalizedAuthorityScores;
        	}
        	
        }
        
        // print scores for top 50 in descending order
        JavaPairRDD<String, Double> joinedHubScores = hubScores.join(rootSetRDD).mapToPair(f -> new Tuple2<Double, String>(f._2._1, f._2._2))
	        	.sortByKey(false)
	        	.mapToPair(f -> new Tuple2<String, Double>(f._2, f._1));
        
        JavaPairRDD<String, Double> joinedAuthorityScores = authorityScores.join(rootSetRDD).mapToPair(f -> new Tuple2<Double, String>(f._2._1, f._2._2))
	        	.sortByKey(false)
	        	.mapToPair(f -> new Tuple2<String, Double>(f._2, f._1));
        
        List<String> hubKeys = joinedHubScores.keys().collect();
        List<Double> hubValues = joinedHubScores.values().collect();
        
        List<String> writeMeHub = new ArrayList<>();
        writeMeHub.add("Hub Set");
        writeMeHub.add("=======\n");
        
        int hubCount = 0;
        
        if (hubKeys.size() > 0) {
        	if (hubKeys.size() < 100) {
        		hubCount = hubKeys.size();
        	} else {
        		hubCount = 100;
        	}
        }
        
        for (int i = 0; i < hubCount; i++) {
        	writeMeHub.add("Key: " + hubKeys.get(i) +  "Hub Score: " + hubValues.get(i));
        }
        
        sc.parallelize(writeMeHub, 1).saveAsTextFile(String.format("hdfs://%s/cs535/PA1/output/HubScores", HDFS_SERVER));
        
        List<String> authorityKeys = joinedAuthorityScores.keys().collect();
        List<Double> authorityValues = joinedAuthorityScores.values().collect();

        List<String> writeMeAuthority = new ArrayList<>();
        writeMeAuthority.add("Authority Set");
        writeMeAuthority.add("=============\n");
        
        int authorityCount = 0;
        
        if (authorityKeys.size() > 0) {
        	if (authorityKeys.size() < 100) {
        		authorityCount = authorityKeys.size();
        	} else {
        		authorityCount = 100;
        	}
        }
        
        for (int i = 0; i < authorityCount; i++) {
        	writeMeAuthority.add("Key: " + authorityKeys.get(i) +  "Authority Score: " + authorityValues.get(i));
        }
        
        sc.parallelize(writeMeAuthority, 1).saveAsTextFile(String.format("hdfs://%s/cs535/PA1/output/AuthorityScores", HDFS_SERVER));
        
        /**
        int baseSize = linkedToRootRDD.values().collect().size();
        List<Long> sampleBaseKey = linkedToRootRDD.keys().collect();
        List<String> sampleBaseValue = linkedToRootRDD.values().collect();
        List<String> writeMeBase = new ArrayList<>();
        writeMeBase.add("Base Set");
        writeMeBase.add("========\n");
        writeMeBase.add("Total in Base Set: " + baseSize);
        
        for (int i=1; i < 11; i++) {
        	writeMeBase.add("Key: " + sampleBaseKey.get(i) + "Values:" + sampleBaseValue.get(i));
        }
        
        sc.parallelize(writeMeBase, 1).saveAsTextFile(String.format("hdfs://%s/cs535/PA1/output/BaseSet", HDFS_SERVER));

		**/
        /**
        JavaPairRDD<String, Long> links = linksPairRDD.join(rootSetRDD).mapToPair(s -> {
        	return new Tuple2<String, Long>(s._2._1, s._1);
        });
        **/
        
        //JavaPairRDD<Long, String> baseSetPart1 = rootSetRDD.join(links).mapToPair(s -> new Tuple2<Long, String>(s._1, s._2._1));
        //JavaPairRDD<Long, String> baseSetPart2 = 
        
        /**
        JavaPairRDD<Long, String> filteredLinksPairRDD = linksRDD.filter(f -> {
        	String[] split = f.split(LINKS_DELIMITER);
        	long key = Long.parseLong(split[0].trim()) - 1;
        	if (rootSetRDD.keys().collect().contains(key)) {
        		return true;
        		//return new Tuple2<Long, String>((Long.parseLong(split[0]) - 1), split[1]);
        	}
        	return false;

        	//return new Tuple2<Long, String>((Long.parseLong(split[0]) - 1), split[1]);
        }).mapToPair(f -> {
        	String[] split = f.split(LINKS_DELIMITER);
        	return new Tuple2<Long, String>((Long.parseLong(split[0]) - 1), split[1]);
        });
        **/
        
        /**
        JavaPairRDD<Long, List<Long>> linkysLong = linksRDD.mapToPair(f -> {
        	String[] fromAndTo = f.split(LINKS_DELIMITER);
        	String[] tos = fromAndTo[1].trim().split(LINKS_VALUES_DELIMITER);
        	List<Long> tosList = Arrays.asList(tos).stream().map(s -> Long.parseLong(s)).collect(Collectors.toList());
        	return new Tuple2<Long, List<Long>>(Long.parseLong(fromAndTo[0].trim()), tosList);
        });
        
        
        
        JavaPairRDD<List<Long>, Long> reallyLongLinks = linkysLong.join(rootSetRDD).mapToPair(s -> {
        	return new Tuple2<List<Long>, Long>(s._2._1, s._1);
        });
        
        
        JavaRDD<Long> rootsRDD = rootSetRDD.keys();
        
        JavaPairRDD<Long, String> filteredLinksPairRDD = linksRDD.filter(f -> {
        	String[] split = f.split(LINKS_DELIMITER);
        	String[] tos = split[1].trim().split(LINKS_VALUES_DELIMITER);
        	List<Long> tosList = Arrays.asList(tos).stream().map(s -> Long.parseLong(s)).collect(Collectors.toList());
        	
        	
        	
        	long key = Long.parseLong(split[0].trim()) - 1;
        	if (rootSetRDD.keys().collect().contains(key)) {
        		return true;
        		//return new Tuple2<Long, String>((Long.parseLong(split[0]) - 1), split[1]);
        	}
        	return false;

        	//return new Tuple2<Long, String>((Long.parseLong(split[0]) - 1), split[1]);
        }).mapToPair(f -> {
        	String[] split = f.split(LINKS_DELIMITER);
        	return new Tuple2<Long, String>((Long.parseLong(split[0]) - 1), split[1]);
        });
        
        
        
        
        
        //JavaRDD<Long, String> baseSet1 = rootSetRDD.joi
        
        
        /**
        JavaPairRDD<String, List<String>> linkys = linksRDD.mapToPair(f -> {
        	String[] fromAndTo = f.split(LINKS_DELIMITER);
        	String[] tos = fromAndTo[1].trim().split(LINKS_VALUES_DELIMITER);
        	List<String> tosList = Arrays.asList(tos);
        	return new Tuple2<String, List<String>>(fromAndTo[0].trim(), tosList);
        });
        **/
        
        
        
        //JavaPairRDD<String, Long> titlesWithIndexRDD = titlesRDD.zipWithIndex();
        
       // JavaRDD<String> titlesRDDTesting = sc.textFile(HDFS_TITLES_TESTING).filter(s -> isEmptyValue(s));
        
        /**
        JavaRDD<String> filtered = titlesRDDTesting.filter(s -> {
        	if (s == null) {
        		return false;
        	}
        	return s.trim().length() >= 1;
        });
        **/
        
        /**
        List<String> entries = titlesRDDTesting.collect();
       
        List<String> writeMe = new ArrayList<>();
        if (!entries.isEmpty()) {
            writeMe.add("Testing if values are written");
            writeMe.add("========\n");
            int count = 1;
            
            while (count < 6) {
            	writeMe.add(entries.get(count));
            	count++;
            }
        } else {
        	writeMe.add("Error, no values have been written.");
            writeMe.add("========\n");
        }
        **/
        /**
        JavaPairRDD<Long, String> newIndexWithTitlesRDD = titlesRDDTesting.zipWithIndex().filter(s -> {
        	String split = s._1;
        	return isRowValid(split, query);
        }).mapToPair(s -> new Tuple2<Long, String>(s._2, s._1)).cache();
        **/
        
        //JavaPairRDD<String, Long> titlesWithIndex = titlesRDDTesting.zipWithIndex().mapToPair(s -> new Tuple2<String, Long>(s._1.toLowerCase(), s._2));
        //String newQuery = query.toLowerCase();
        
        //JavaPairRDD<String, Long> filteredTitlesWithIndex = titlesWithIndex.filter(s -> s._1.contains(newQuery));
        
        //JavaPairRDD<String, Long> rootSet = titlesWithIndex.filter(s -> s._1.toLowerCase().contains(query.toLowerCase()));
        
        //titlesRDDTesting.filter(w -> (w._2().toLowerCase().contains("page")));
        
        /**
        JavaRDD<String> titlesNoNull = titlesRDDTesting.filter(x -> {
        	if (x != null) {
        		return true;
        	}
        	return false;
        });
        
        JavaPairRDD<Long, String> newIndexWithTitlesRDD2 = titlesNoNull.zipWithIndex().filter(s -> {
        	if (s._1 != null) {
        		if (s._1.contains(query)) {
            		return true;
            	}
        	}
        	return false;
        }).mapToPair(s -> new Tuple2<Long, String>(s._2, s._1)).cache();
        
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Testing if values are written");
        writeMe.add("========\n");
        //List<String> entries = rootSet.keys().collect();
        List<String> entries = filteredTitlesWithIndex.keys().collect();
        List<Long> values = filteredTitlesWithIndex.values().collect();
        int totalRoot = entries.size();
        writeMe.add("Total in Root Set: " + totalRoot);
        writeMe.add("Root Set Words: " + entries.toString());
        writeMe.add("Values: " + values.toString());
        writeMe.add("Query is: " + newQuery);
        **/
        /** END TESTING **/
        /**
        JavaPairRDD<Long, String> newIndexWithTitlesRDD = titlesRDD.zipWithIndex().filter(s -> {
        	String split = s._1;
        	return isRowValid(split, query);
        }).mapToPair(s -> new Tuple2<Long, String>(s._2, s._1)).cache();
        
        
        // CORRECT LATER: write the root set values to a file
        /**
        JavaPairRDD<String, Long> titles = titlesWithIndexRDD.filter(s -> {
        	String split = s._1;
        	
        	return isRowValid(split, query);
        });
        **/
        /**
        JavaPairRDD<Long, String> titles2 = titlesWithIndexRDD.filter(s -> {
        	String split = s._1;
        	
        	return isRowValid(split, query);
        }).map(f)
        **/
        
        /**
        List<String> writeMe = new ArrayList<>();
        writeMe.add("Root Set");
        writeMe.add("========\n");
        
        List<String> entries = newIndexWithTitlesRDD.values().collect();
        int totalRoot = entries.size();
        writeMe.add("Total in Root Set: " + totalRoot);
        sc.parallelize(writeMe, 1).saveAsTextFile(String.format("hdfs://%s/cs535/PA1/output/RootSet", HDFS_SERVER));
        **/
        //sc.parallelize(writeMe, 1).saveAsTextFile(String.format("hdfs://%s/cs535/PA1/output/Testing", HDFS_SERVER));
        
        
        //newIndexWithTitlesRDD.saveAsTextFile(String.format("hdfs://%s/cs535/PA1/output/RootSet", HDFS_SERVER));
        
        
        
        
        
	}
	
	private static boolean hasScoreConverged(JavaPairRDD<Long, Double> newScores, JavaPairRDD<Long, Double> oldScores) {
		JavaPairRDD<Long, Double> mergedScores = newScores.join(oldScores).filter(f -> {
			if (Math.abs((f._2._1 - f._2._2)) > 0.01) {
				return true;
			}
			return false;
			
		}).mapToPair(f -> new Tuple2<>(f._1, f._2._1));
		
		return mergedScores.count() < 10;
	}
	
	private static boolean isRowValid(String split, String query) {
		if (split != null) {
			
			String[] splitArray = split.trim().split(FILE_DELIMITER);

			for (String s : splitArray) {
				if (s != null) {
					if (s.trim().toLowerCase().contains(query.trim().toLowerCase())) {
						return true;
					}
				}
			}
		}
		return false;
		
	}
	
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
