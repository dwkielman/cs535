package cs535.drivers;

import java.io.Serializable;

class Driver implements Serializable {
    static final String HDFS_SERVER = "olympia:44001"; // replace with your personal HDFS server here
    static final String HDFS_MOVIES_METADATA = String.format("hdfs://%s/data/movies_metadata.csv", HDFS_SERVER);
    static final String HDFS_CREDITS = String.format("hdfs://%s/data/credits.csv", HDFS_SERVER);
    int numSuccessful;
    int numMovies;
    float populationMean;
    float stdDev;

    float calculateZ(float confidenceInterval) {
        return confidenceInterval / stdDev;
    }

}
