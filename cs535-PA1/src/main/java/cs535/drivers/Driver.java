package cs535.drivers;

import java.io.Serializable;

class Driver implements Serializable {
	
    static final String HDFS_SERVER = "boston:30451"; // replace with your personal HDFS server here
    static final String HDFS_LINKS_SIMPLE_SORTED = String.format("hdfs://%s/cs535/data/links-simple-sorted.txt", HDFS_SERVER);
    static final String HDFS_TITLES_SORTED = String.format("hdfs://%s/cs535/data/titles-sorted.txt", HDFS_SERVER);
    static final String HDFS_TITLES_TESTING = String.format("hdfs://%s/cs535/data/testing-titles.txt", HDFS_SERVER);
    
}
