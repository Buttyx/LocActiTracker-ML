package ch.fhnw.locactitracker.service;

import ch.fhnw.locactitracker.classifier.DecisionTreeClassifier;
import ch.fhnw.locactitracker.classifier.RandomForestClassifier;
import ch.fhnw.locactitracker.data.LocationClustering;
import ch.fhnw.locactitracker.data.QueriesUtils;
import ch.fhnw.locactitracker.feature.FeatureUtils;
import ch.fhnw.locactitracker.feature.GooglePlacesPOIProvider;
import ch.fhnw.locactitracker.feature.TimeUtils;
import ch.fhnw.locactitracker.model.ActivityType;
import ch.fhnw.locactitracker.model.Trace;
import com.datastax.spark.connector.japi.CassandraRow;
import com.datastax.spark.connector.japi.rdd.CassandraJavaRDD;
import org.apache.commons.collections.CollectionUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.regression.LabeledPoint;

import java.util.ArrayList;
import java.util.List;

import static ch.fhnw.locactitracker.data.QueriesUtils.*;
import static com.datastax.spark.connector.japi.CassandraJavaUtil.javaFunctions;

/**
 * The training service
 *
 * Train the activity based on the given data
 */
public class TrainingService {

    private static final String APPLICATION_NAME = "locactitracker";

    public static final long DURATION_WINDOW = 5000L;
    public static final long ONE_SECOND = 800l;

    private JavaSparkContext sc;

    public static void main(String[] args){
        TrainingService trainingService = new TrainingService();
        trainingService.initSparkContext();
        trainingService.createInferenceModelsWithTrainingData(true, true);
    }

    /**
     * Init the spark context and configure the connexion to cassandra
     */
    public void initSparkContext(){
        SparkConf sparkConf = new SparkConf()
                .setAppName(APPLICATION_NAME)
                .set("spark.cassandra.connection.host", CASSANDRA_HOST)
                .set("spark.driver.host", "localhost")
                .setMaster("local[*]");
        sc = new JavaSparkContext(sparkConf);
    }

    /**
     * Generate Labeled points based on training data with the given configuration
     */
    public List<LabeledPoint> createLabeledPointsWithTrainingData(boolean locationAware, boolean withDominantHand) {
        List<LabeledPoint> labeledPoints = new ArrayList<>();

        CassandraJavaRDD<CassandraRow> cassandraRowsRDD =
                javaFunctions(sc).cassandraTable(CASSANDRA_KEYSPACE, CASSANDRA_TRAINING_TABLE);

        // Get all the users in the db
        List<String> users = QueriesUtils.fetchUsers(cassandraRowsRDD);

        // Go through all activities
        for (ActivityType activity : ActivityType.values()) {
            if (activity == ActivityType.OTHER)
                continue;

            List<List<Trace>> stayRegionsWindows = new ArrayList<>();
            List<List<Trace>> tripsWindows = new ArrayList<>();

            // for each user retrieved
            users.stream().forEach(user -> {

                JavaRDD<Trace> records = QueriesUtils.fetchAllDataForUserAndActivity(cassandraRowsRDD,user,withDominantHand,activity.getLabel(), Long.MIN_VALUE, Long.MAX_VALUE)
                                                    .sortBy(t -> t.getTimestamp(), true, 1);

                /**========================================================
                 * Extract the stay region (if location-awareness enabled
                 =========================================================*/
                JavaPairRDD<Integer, Iterable<Trace>> regions;
                if (locationAware) {
                    //regions = LocationClustering.generateClusters(records);
                    if (activity == ActivityType.BIKING || activity == ActivityType.TRANSPORTATION || activity == ActivityType.WALKING) {
                        regions = records.groupBy(t -> LocationClustering.NOISE_CLUSTER);
                    } else {
                        regions = records.groupBy(t -> 1);
                    }
                } else {
                    // Only one region is created in case of location awareness
                    regions = records.groupBy(t -> LocationClustering.NOISE_CLUSTER);
                }


                /**========================================================
                 * Extract segments with continuous records
                 =========================================================*/
                regions.collect().forEach(pair -> {
                    boolean isStayRegion = pair._1 != LocationClustering.NOISE_CLUSTER;

                    List<List<Trace>> segments = TimeUtils.groupContinuousRecordsInSegments(pair._2, ONE_SECOND);

                    /**========================================================
                     * Generate uniform time-windows
                     =========================================================*/
                    segments.forEach(segment -> {
                        long startTime = segment.get(0).getTimestamp();

                        List<Trace> currentWindows = new ArrayList<Trace>();
                        long endTimeWindow = startTime + DURATION_WINDOW;
                        for (Trace t:segment) {
                            if (t.getTimestamp() > endTimeWindow) {

                                if (currentWindows.size() >= 10) {
                                    if (isStayRegion)
                                        stayRegionsWindows.add(currentWindows);
                                    else
                                        tripsWindows.add(currentWindows);
                                }

                                currentWindows = new ArrayList<Trace>();
                                endTimeWindow += DURATION_WINDOW;
                            }
                            currentWindows.add(t);
                        }

                        if (currentWindows.size() >= 10) {
                            if (isStayRegion)
                                stayRegionsWindows.add(currentWindows);
                            else
                                tripsWindows.add(currentWindows);
                        }
                    });

                });
            });

            /**========================================================
             * Feature Extraction
             =========================================================*/

            // Prepare the POI Provider with all the stay regions
            GooglePlacesPOIProvider poiProvider = new GooglePlacesPOIProvider();
            stayRegionsWindows.forEach(window -> poiProvider.generatePOIMapsForRegion(window));

            stayRegionsWindows.forEach(window -> {
                Vector feature = FeatureUtils.computeFeatures(sc.parallelize(window), true, locationAware, poiProvider);
                labeledPoints.add(createLabeledPoint(activity, feature));
            });

            tripsWindows.forEach(window -> {
                Vector feature = FeatureUtils.computeFeatures(sc.parallelize(window), false, locationAware, poiProvider);
                labeledPoints.add(createLabeledPoint(activity, feature));
            });
        }

        return labeledPoints;
    }

    /**
     * Generate the inference models with the given configuration
     * @param locationAware
     * @param withDominantHand
     */
    public void createInferenceModelsWithTrainingData(boolean locationAware, boolean withDominantHand){
        List<LabeledPoint> labeledPoints = createLabeledPointsWithTrainingData(locationAware, withDominantHand);

        // create model prediction: decision tree
        if (CollectionUtils.isNotEmpty(labeledPoints)) {

            // transform label into RDD
            JavaRDD<LabeledPoint> labeledPointsRdd = sc.parallelize(labeledPoints);

            // Split labeledPointsRdd into 2 sets : training (60%) and test (40%).
            JavaRDD<LabeledPoint>[] splits = labeledPointsRdd.randomSplit(new double[]{0.7, 0.3});
            JavaRDD<LabeledPoint> trainingDataSet = splits[0].cache();
            JavaRDD<LabeledPoint> testDataSet = splits[1];

            // Create DecisionTree and calculate accuracy
            double decisionTreeAccuracy = new DecisionTreeClassifier(trainingDataSet, testDataSet, locationAware, withDominantHand).createModel(sc);

            // Create RandomForest and calculate accuracy
            double randomForestAccuracy = new RandomForestClassifier(trainingDataSet, testDataSet, locationAware, withDominantHand).createModel(sc);

            System.out.println("=== Locactitracker Training ========> Number of input vectors: " + labeledPointsRdd.count());
            System.out.println("=== Locactitracker Training ========> Decision Tree Accuracy: " + decisionTreeAccuracy);
            System.out.println("=== Locactitracker Training ========> Random Forest Accuracy: " + randomForestAccuracy);
        }
    }


    /**
     * Create labeled point based on computed features vector and selected activity.
     * @param activity
     * @param vector
     * @return labeled point
     */
    private static LabeledPoint createLabeledPoint(ActivityType activity, Vector vector) {
        return new LabeledPoint((double) activity.getId(), vector);
    }

}
