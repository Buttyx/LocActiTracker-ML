package ch.fhnw.locactitracker.service;

import ch.fhnw.locactitracker.classifier.Classifier;
import ch.fhnw.locactitracker.classifier.RandomForestClassifier;
import ch.fhnw.locactitracker.data.LocationClustering;
import ch.fhnw.locactitracker.data.QueriesUtils;
import ch.fhnw.locactitracker.feature.FeatureUtils;
import ch.fhnw.locactitracker.feature.GooglePlacesPOIProvider;
import ch.fhnw.locactitracker.feature.TimeUtils;
import ch.fhnw.locactitracker.model.ActivityType;
import ch.fhnw.locactitracker.model.RecognitionResult;
import ch.fhnw.locactitracker.model.Trace;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import java.util.ArrayList;
import java.util.List;

import static ch.fhnw.locactitracker.data.QueriesUtils.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.mapToRow;

/**
 * The recognition service
 *
 * Recognise the activity based on the given data
 */
public class RecognitionService {

    private static final long TEN_SECONDS = 10000l;
    public static final long DURATION_WINDOW = 5000L;
    private static final String APPLICATION_NAME = "locactitracker recognition";

    private static String[][] modelNames = new String[2][2];
    private static RandomForestModel[][] models = new RandomForestModel[2][2];

    public static void main(String[] args) {
        SparkConf sparkConf = new SparkConf()
                .setAppName(APPLICATION_NAME)
                .set("spark.driver.host", "localhost")
                .set("spark.cassandra.connection.host", CASSANDRA_HOST)
                .setMaster("local[*]");

        JavaSparkContext context = new JavaSparkContext(sparkConf);

        // Prepare the inference models
        for (int i = 0; i < modelNames.length; i++) {
            for (int y = 0; y < modelNames.length; y++) {
                String name = Classifier.getModelName(i == 1, y == 1, RandomForestClassifier.NAME);
                modelNames[i][y] = name;
                models[i][y] = RandomForestModel.load(context.sc(), name);
            }
        }

        // Start the continuous recognition process
        recogniseLatestTraces(context);


    }

    /**
     * Recognise the current activity for all the users
     *
     * @param javaSparkContext
     * @param startTime
     */
    private static void recogniseActivityForAllUsers(JavaSparkContext javaSparkContext, long startTime) {

        CassandraJavaRDD<CassandraRow> cassandraRowsRDD = javaFunctions(javaSparkContext)
                            .cassandraTable(CASSANDRA_KEYSPACE, CASSANDRA_RECOGNITON_TABLE);

        // Get the list of users
        List<String> users = QueriesUtils.fetchUsers(cassandraRowsRDD);

        System.out.println("=== Number of users ====> " + users.size());


        // For each user, recognise the activity with the four variation of inference model
        for (String user:users) {
            recogniseActivityForUser(javaSparkContext, false, false, user, startTime-TEN_SECONDS, startTime);
            recogniseActivityForUser(javaSparkContext, false, true, user, startTime-TEN_SECONDS, startTime);
            recogniseActivityForUser(javaSparkContext, true, false, user, startTime-TEN_SECONDS, startTime);
            recogniseActivityForUser(javaSparkContext, true, true, user, startTime-TEN_SECONDS, startTime);
        }
    }

    /**
     * Recognise the activity for the given user for the interval defined by the given start and end time
     * and save the result in cassandra
     *
     * @param javaSparkContext
     * @param locationAware
     * @param dominantHand
     * @param user
     * @param startTime
     * @param endTime
     */
    private static void recogniseActivityForUser(JavaSparkContext javaSparkContext,
                                                 boolean locationAware,
                                                 boolean dominantHand,
                                                 String user,
                                                 long startTime,
                                                 long endTime) {

        // Get the list of recognised activities
        List<ActivityType> recognitionResult = recognise(javaSparkContext, user, locationAware, dominantHand, startTime, endTime);

        List<RecognitionResult> recognitions = new ArrayList();

        String modelName = modelNames[locationAware ? 1 : 0][dominantHand ? 1 : 0];

        recognitionResult.forEach(r -> {
            System.out.println("=== Recognition [ " + locationAware + " | " + dominantHand + "] ====> Activity: " + r.getLabel());
            recognitions.add(new RecognitionResult(user, startTime, r, modelName, locationAware, dominantHand));
        });

        JavaRDD<RecognitionResult> resultsRDD = javaSparkContext.parallelize(recognitions);

        //Write result into Cassandra
        javaFunctions(resultsRDD).writerBuilder(CASSANDRA_KEYSPACE, CASSANDRA_RESULT_TABLE,
                mapToRow(RecognitionResult.class)).saveToCassandra();
    }


    public static List<ActivityType> recognise(JavaSparkContext sc,
                                               String user,
                                               boolean locationAware,
                                               boolean dominantHand,
                                               long start,
                                               long end) {

        // retrieve latestAccelerations from Cassandra and create an CassandraRDD
        CassandraJavaRDD<CassandraRow> cassandraRowsRDD = javaFunctions(sc).cassandraTable(CASSANDRA_KEYSPACE, CASSANDRA_RECOGNITON_TABLE);
        //JavaRDD<Trace> latestTraces = QueriesUtils.fetchTracesFromTo(cassandraRowsRDD, user, dominantHand, start, end);
        JavaRDD<Trace> latestTraces = QueriesUtils.fetchLatestTraces(cassandraRowsRDD, user, dominantHand, 2500l);

        System.out.println("=== Number of elements ====> " + latestTraces.collect().size());
        System.out.println("=== Start time ====> " + start);
        System.out.println("=== End time ====> " + end);

        JavaPairRDD<Integer, Iterable<Trace>> regions;
        if (locationAware) {
            regions = LocationClustering.generateClusters(latestTraces);
        } else {
            // Only one region is created in case of location awareness
            regions = latestTraces.groupBy(t -> -1);
        }

        List<List<Trace>> stayRegionsWindows = new ArrayList<>();
        List<List<Trace>> tripsWindows = new ArrayList<>();

        /**========================================================
         * Extract segments with continuous records
         =========================================================*/
        regions.collect().forEach(pair -> {
            boolean isStayRegion = pair._1 != LocationClustering.NOISE_CLUSTER;

            List<List<Trace>> segments = TimeUtils.groupContinuousRecordsInSegments(pair._2, 800L);
            System.out.println("=== Number of segments ====> " + segments.size());

            /**========================================================
             * Generate uniform time-windows
             =========================================================*/
            segments.forEach(segment -> {
                if (isStayRegion)
                    stayRegionsWindows.addAll(TimeUtils.generateTimeWindows(segment, DURATION_WINDOW));
                else
                    tripsWindows.addAll(TimeUtils.generateTimeWindows(segment, DURATION_WINDOW));
            });

        });

        /**========================================================
         * Feature Extraction and recognition
         =========================================================*/

        System.out.println("=== Number of stay regions ====> " + stayRegionsWindows.size());
        System.out.println("=== Number of noise regions ====> " + tripsWindows.size());

        List<ActivityType> results = new ArrayList<>();

        // Prepare the POI Provider with all the stay regions
        GooglePlacesPOIProvider poiProvider = new GooglePlacesPOIProvider();
        stayRegionsWindows.forEach(window -> poiProvider.generatePOIMapsForRegion(window));

        final RandomForestModel model = models[locationAware ? 1 : 0][dominantHand ? 1 : 0];

        stayRegionsWindows.forEach(window -> {
            Vector feature = FeatureUtils.computeFeatures(sc.parallelize(window), true, locationAware, poiProvider);
            results.add(FeatureUtils.recognise(model,feature));
        });

        tripsWindows.forEach(window -> {
            Vector feature = FeatureUtils.computeFeatures(sc.parallelize(window), false, locationAware, poiProvider);
            results.add(FeatureUtils.recognise(model,feature));
        });

        System.out.println("=== Results size ====> " + results.size());

        return results;
    }

    /**
     * Get traces each 10 seconds for the last 10 seconds
     */
    private static void recogniseLatestTraces(JavaSparkContext context) {

        //Never stop until we kill the service
        while(true){
            try {
                long startTime = System.currentTimeMillis();
                recogniseActivityForAllUsers(context, startTime);
                Thread.sleep(TEN_SECONDS);
            } catch (InterruptedException e) {
                System.out.println("Prediction failed "+e.getMessage());
            }
        }
    }

}
