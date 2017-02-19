package ch.fhnw.locactitracker.feature;

import ch.fhnw.locactitracker.model.Trace;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.mllib.linalg.Vector;
import org.apache.spark.mllib.linalg.Vectors;
import org.apache.spark.mllib.stat.MultivariateStatisticalSummary;
import org.apache.spark.mllib.stat.Statistics;

import java.util.ArrayList;
import java.util.List;

/**
 * Accelerometer features
 */
public class AccelerometerFeature {

    private MultivariateStatisticalSummary summary;

    public AccelerometerFeature(JavaRDD<Vector> data) {
        this.summary = Statistics.colStats(data.rdd());
    }

    /**
     * Compute the mean of the absolute difference
     */
    public static double[] computeAvgAbsDifference(JavaRDD<double[]> data, double[] mean) {
        JavaRDD<Vector> abs = data.map(acceleration -> new double[]{Math.abs(acceleration[0] - mean[0]),
                Math.abs(acceleration[1] - mean[1]),
                Math.abs(acceleration[2] - mean[2])})
                .map(Vectors::dense);

        return Statistics.colStats(abs.rdd()).mean().toArray();

    }

    /**
     * Compute the resultant acceleration
     */
    public static double computeResultantAcc(JavaRDD<double[]> data) {
        JavaRDD<Vector> squared = data.map(acceleration -> Math.pow(acceleration[0], 2)
                + Math.pow(acceleration[1], 2)
                + Math.pow(acceleration[2], 2))
                .map(Math::sqrt)
                .map(sum -> Vectors.dense(new double[]{sum}));

        return Statistics.colStats(squared.rdd()).mean().toArray()[0];

    }

    /**
     * Compute the standard deviation
     */
    public static double[] computeStandardDeviation(JavaRDD<double[]> data, double[] mean){
        JavaRDD<Vector> squared = data.map(acceleration -> new double[]{
                Math.pow(acceleration[0] - mean[0],2),
                Math.pow(acceleration[1] - mean[1],2),
                Math.pow(acceleration[2] - mean[2],2)})
                .map(sum -> Vectors.dense(sum));

        double[] meanDiff = Statistics.colStats(squared.rdd()).mean().toArray();

        if(meanDiff.length>0){
            return new double[]{Math.sqrt(meanDiff[0]), Math.sqrt(meanDiff[1]), Math.sqrt(meanDiff[2])};
        }
        return new double[]{0.0,0.0,0.0};
    }

    /**
     * Compute the mean
     */
    public double[] computeMean() {
        return this.summary.mean().toArray();
    }

    /**
     * Compute the variance
     */
    public double[] computeVariance() {
        return this.summary.variance().toArray();
    }

    /**
     * compute average time between peaks.
     */
    public double[] computeAvgTimeBetweenPeak(JavaRDD<Trace> data) {
        // define the maximum
        double[] max = this.summary.max().toArray();

        double[] results = new double[3];


        JavaRDD<Long> topX = data.filter(t -> t.getX() > 0.9 * max[0])
                .map(t -> t.getTimestamp())
                .sortBy(time -> time, true, 1);

        JavaRDD<Long> topY = data.filter(t -> t.getY() > 0.9 * max[1])
                .map(t -> t.getTimestamp())
                .sortBy(time -> time, true, 1);

        JavaRDD<Long> topZ = data.filter(t -> t.getZ() > 0.9 * max[2])
                .map(t -> t.getTimestamp())
                .sortBy(time -> time, true, 1);

        List<JavaRDD<Long>> values = new ArrayList<JavaRDD<Long>>();
        values.add(topX);
        values.add(topY);
        values.add(topZ);

        int i = 0;
        for(JavaRDD<Long> topValues:values) {
            if (topValues.count() > 1) {
                Long firstElement = topValues.first();
                Long lastElement = topValues.sortBy(time -> time, false, 1).first();

                // compute the delta between each tick
                JavaRDD<Long> firstRDD = topValues.filter(record -> record > firstElement);
                JavaRDD<Long> secondRDD = topValues.filter(record -> record < lastElement);

                JavaRDD<Vector> product = firstRDD.zip(secondRDD)
                        .map(pair -> pair._1() - pair._2())
                        // and keep it if the delta is != 0
                        .filter(value -> value > 0)
                        .map(line -> Vectors.dense(line));

                // compute the average
                results[i++] = Statistics.colStats(product.rdd()).mean().toArray()[0];
            } else {
                results[i++] = 0.0;
            }
        }
        return results;
    }
}
