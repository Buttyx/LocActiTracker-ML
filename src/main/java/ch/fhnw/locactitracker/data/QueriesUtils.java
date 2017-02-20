package ch.fhnw.locactitracker.data;

import ch.fhnw.locactitracker.model.*;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;

/**
 * Utility functions to execute different queries through the Cassandra connector
 */
public class QueriesUtils {

    public static final String CASSANDRA_KEYSPACE = "locactitracker";
    public static final String CASSANDRA_TRAINING_TABLE = "training";
    public static final String CASSANDRA_RECOGNITON_TABLE = "recognition";
    public static final String CASSANDRA_RESULT_TABLE = "result";
    public static final String CASSANDRA_HOST = "127.0.0.1";

    public static final String FIELD_TIMESTAMP = "timestamp";
    public static final String FIELD_USER = "user";
    public static final String FIELD_DOMINANTHAND = "dominanthand";
    public static final String FIELD_ACC_X = "x";
    public static final String FIELD_ACC_Y = "y";
    public static final String FIELD_ACC_Z = "z";
    public static final String FIELD_LONGITUDE = "longitude";
    public static final String FIELD_LATITUDE = "latitude";
    public static final String FIELD_ACTIVITY = "activity";

    /**
     * Collect all the traces for the given user between the period defined by the start and end time
     * @param cassandraRowsRDD
     * @param user
     * @param dominantHand
     * @param startTime
     * @param endTime
     * @return the list of traces
     */
    public static JavaRDD<Trace> fetchTracesFromTo(CassandraJavaRDD<CassandraRow> cassandraRowsRDD,
                                                   String user,
                                                   boolean dominantHand,
                                                   long startTime,
                                                   long endTime){

        return  cassandraRowsRDD

                .select(FIELD_TIMESTAMP,
                        FIELD_DOMINANTHAND,
                        FIELD_USER,
                        FIELD_ACC_X, FIELD_ACC_Y, FIELD_ACC_Z,
                        FIELD_LATITUDE, FIELD_LONGITUDE)
                .where(FIELD_USER+"=? AND " +
                        FIELD_DOMINANTHAND + "=? AND "+
                        FIELD_TIMESTAMP + ">? AND " +
                        FIELD_TIMESTAMP + "<?" , user, dominantHand, startTime, endTime)
                .withDescOrder()
                .repartition(1)
                .map(row -> convertToTrace(row));
    }

    /**
     * Collect all the traces for the given user between the period defined by the start and end time
     * @param cassandraRowsRDD
     * @param user
     * @param dominantHand
     * @param limit
     * @return the list of traces
     */
    public static JavaRDD<Trace> fetchLatestTraces(CassandraJavaRDD<CassandraRow> cassandraRowsRDD,
                                                   String user,
                                                   boolean dominantHand,
                                                   long limit){

        return  cassandraRowsRDD

                .select(FIELD_TIMESTAMP,
                        FIELD_DOMINANTHAND,
                        FIELD_USER,
                        FIELD_ACC_X, FIELD_ACC_Y, FIELD_ACC_Z,
                        FIELD_LATITUDE, FIELD_LONGITUDE)
                .where(FIELD_USER+"=? AND " +
                        FIELD_DOMINANTHAND + "=?", user, dominantHand)
                .withDescOrder()
                .limit(limit)
                .repartition(1)
                .map(row -> convertToTrace(row));
    }


    /**
     * Fetch all the users from cassandra
     *
     * @param cassandraRowsRDD
     * @return The list of users
     */
    public static List<String> fetchUsers(CassandraJavaRDD<CassandraRow> cassandraRowsRDD){
        return cassandraRowsRDD
                .select(FIELD_USER).distinct()
                .map(CassandraRow::toMap)
                .map(row -> (String) row.get(FIELD_USER))
                .collect();
    }

    /**
     * Fetch all the records for the given user, activity and handednes  between the start (min) and end (max) timestamp values.
     *
     * @param cassandraRowsRDD
     * @param user
     * @param dominantHand
     * @param activity
     * @param min
     * @param max
     * @return A list of Trace
     */
    public static JavaRDD<TrainingTrace> fetchDataForUserAndActivity(CassandraJavaRDD<CassandraRow> cassandraRowsRDD, String user, boolean dominantHand, ActivityType activity, long min, long max){
        return cassandraRowsRDD
                .select(FIELD_TIMESTAMP,
                        FIELD_USER,
                        FIELD_ACC_X,
                        FIELD_ACC_Y,
                        FIELD_ACC_Z,
                        FIELD_LATITUDE,
                        FIELD_LONGITUDE)
                .where(FIELD_USER+"=? AND " +
                        FIELD_DOMINANTHAND + "=? AND " +
                        FIELD_ACTIVITY + "=? AND " +
                        FIELD_TIMESTAMP + " > ? AND " +
                        FIELD_TIMESTAMP + " < ?",
                        user, activity.getLabel(), dominantHand, min, max)
                .withAscOrder()
                .map(row -> convertToTrainingTrace(row));
    }

    /**
     * Fetch all the records for the given user, activity and handednes  between the start (min) and end (max) timestamp values.
     *
     * @param cassandraRowsRDD
     * @param user
     * @param dominantHand
     * @param activity
     * @return A list of Trace
     */
    public static JavaRDD<Trace> fetchAllDataForUserAndActivity(CassandraJavaRDD<CassandraRow> cassandraRowsRDD,
                                                                String user,
                                                                boolean dominantHand,
                                                                String activity,
                                                                long start,
                                                                long stop){
        return cassandraRowsRDD
                .select(FIELD_TIMESTAMP,
                        FIELD_USER,
                        FIELD_LONGITUDE,
                        FIELD_LATITUDE,
                        FIELD_DOMINANTHAND,
                        FIELD_ACC_X,
                        FIELD_ACC_Y,
                        FIELD_ACC_Z,
                        FIELD_LATITUDE,
                        FIELD_LONGITUDE)
                .where(FIELD_USER+"=? AND " +
                        FIELD_DOMINANTHAND + "=? AND " +
                        FIELD_ACTIVITY + "=? AND " +
                        FIELD_TIMESTAMP + " > ? AND " +
                        FIELD_TIMESTAMP + " < ?",
                        user, dominantHand, activity, start, stop)
                .withAscOrder()
                .map(row -> convertToTrace(row));
    }

    /**
     * Convert the cassandra row into trace
     *
     * @param row
     * @return Trace
     */
    private static Trace convertToTrace(CassandraRow row) {
        return new ActivityTrace(row.getLong(FIELD_TIMESTAMP), row.getString(FIELD_USER), row.getBoolean(FIELD_DOMINANTHAND),
                row.getDouble(FIELD_ACC_X), row.getDouble(FIELD_ACC_Y), row.getDouble(FIELD_ACC_Z),
                row.getDouble(FIELD_LATITUDE), row.getDouble(FIELD_LONGITUDE));
    }

    /**
     * Convert the cassandra row into training trace
     *
     * @param row
     * @return TrainingTrace
     */
    private static TrainingTrace convertToTrainingTrace(CassandraRow row) {
        return new TrainingTrace(convertToTrace(row), row.getString(FIELD_ACTIVITY));
    }
}
