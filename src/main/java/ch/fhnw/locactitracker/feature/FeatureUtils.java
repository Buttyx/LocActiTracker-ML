package ch.fhnw.locactitracker.feature;


import ch.fhnw.locactitracker.model.ActivityType;
import ch.fhnw.locactitracker.model.Trace;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.tree.model.RandomForestModel;

import static ch.fhnw.locactitracker.feature.LocationFeature.*;

/**
 * Utility class for features computation and vectors generation
 */
public class FeatureUtils {

    /**
     * Generate the features vector
     */
    public static Vector computeFeatures(JavaRDD<Trace> data, boolean isStayRegion, boolean locationAware, POIProvider poiProvider) {

        double[] features = new double[locationAware ? 30 : 16];

        if (data.count() > 0) {

            JavaRDD<double[]> accelerationData = data.map(
                    trace -> new double[]{trace.getX(), trace.getY(), trace.getZ()}
            );

            JavaRDD<Vector> vectorsXYZ = accelerationData.map(Vectors::dense);


            /**========================================================
             * Extract the location feature
             =========================================================*/

            // Features related to the motions (not for stay regions)

            long totalTime = (locationAware && !isStayRegion) ? computeTotalTime(data) : 0l;

            double totalDistance = (locationAware && !isStayRegion) ? computeTotalDistance(data) : 0.0;

            double averageSpeed = (locationAware && !isStayRegion) ? computeAverageSpeed(totalDistance,totalTime) : 0.0;

            double standardDeviationSpeed = (locationAware && !isStayRegion) ? computeStandardDeviationSpeed(data) : 0.0;

            double maxSpeed = (locationAware && !isStayRegion) ? computeMaxSpeed(data) : 0.0;

            // Semantic categories
            double[] categoriesTDIDF = new double[8];

            if (locationAware && isStayRegion) {

                // Generate the TDIDF value for the stay region (only use the first record)
                categoriesTDIDF = poiProvider.generateTDIDFForRegion(data.first());
            }


            /**========================================================
             * Extract the accelerometer feature
             =========================================================*/

            //Extract features
            AccelerometerFeature feature = new AccelerometerFeature(vectorsXYZ);


            double[] averageAcc = feature.computeMean();

            double[] variance = feature.computeVariance();

            double[] standardDeviation = AccelerometerFeature.computeStandardDeviation(accelerationData, averageAcc);

            double[] averageAbsDiff = feature.computeAvgAbsDifference(accelerationData, averageAcc);

            double resultant = feature.computeResultantAcc(accelerationData);

            double[] averageTimePeak = feature.computeAvgTimeBetweenPeak(data);



            //Create feature
            if (locationAware) {
                features = new double[]{
                        categoriesTDIDF[0],
                        categoriesTDIDF[1],
                        categoriesTDIDF[2],
                        categoriesTDIDF[3],
                        categoriesTDIDF[4],
                        categoriesTDIDF[5],
                        categoriesTDIDF[6],
                        categoriesTDIDF[7],
                        averageSpeed,
                        maxSpeed,
                        standardDeviationSpeed,
                        totalDistance,
                        averageAcc[0],
                        averageAcc[1],
                        averageAcc[2],
                        standardDeviation[0],
                        standardDeviation[1],
                        standardDeviation[2],
                        variance[0],
                        variance[1],
                        variance[2],
                        averageAbsDiff[0],
                        averageAbsDiff[1],
                        averageAbsDiff[2],
                        resultant,
                        averageTimePeak[0],
                        averageTimePeak[1],
                        averageTimePeak[2]
                };
            } else {
                features = new double[]{
                        averageAcc[0],
                        averageAcc[1],
                        averageAcc[2],
                        standardDeviation[0],
                        standardDeviation[1],
                        standardDeviation[2],
                        variance[0],
                        variance[1],
                        variance[2],
                        averageAbsDiff[0],
                        averageAbsDiff[1],
                        averageAbsDiff[2],
                        resultant,
                        averageTimePeak[0],
                        averageTimePeak[1],
                        averageTimePeak[2]
                };
            }
        }

        return Vectors.dense(features);
    }

    /**
     * Recognise the activity based on the given model and features vector
     */
    public static ActivityType recognise(RandomForestModel model, Vector feature) {
        double prediction = model.predict(feature);
        return ActivityType.fromId((int) prediction);
    }
}