package cs535.drivers;

import java.io.Serializable;

class Driver implements Serializable {
	
    static final String HDFS_SERVER = "boston:30451"; // replace with personal HDFS server here
    static final String HDFS_LINKS_SIMPLE_SORTED = String.format("hdfs://%s/cs535/data/links-simple-sorted.txt", HDFS_SERVER); // HDFS location of links-simple-sorted.txt file
    static final String HDFS_TITLES_SORTED = String.format("hdfs://%s/cs535/data/titles-sorted.txt", HDFS_SERVER); // HDFS location of titles-sorted.txt file
    static final String HDFS_OUTPUT_LOCATION = String.format("hdfs://%s/cs535/PA1/output", HDFS_SERVER); // HDFS location to write output files
    
}
