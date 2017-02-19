package ch.fhnw.locactitracker.classifier;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.classification.NaiveBayes;
import org.apache.spark.mllib.classification.NaiveBayesModel;
import org.apache.spark.mllib.regression.LabeledPoint;
import scala.Tuple2;

/**
 * Naive Bayes Classifier
 * https://spark.apache.org/docs/2.0.2/mllib-naive-bayes.html
 */
public class NaiveBayesClassifier extends Classifier {

    public static String NAME = "naivebayes";

    public NaiveBayesClassifier(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData, boolean locationAware, boolean withDominantHand) {
        super(trainingData, testData, locationAware, withDominantHand);
    }

    @Override
    public Double createModel(JavaSparkContext sc) {
        /**========================================================
         * Parameters
         =========================================================*/
        double smoothing = 1.0;

        /**========================================================
         * Creation of the inference model
         =========================================================*/
        // create model
        final NaiveBayesModel model = NaiveBayes.train(trainingData.rdd(), smoothing);
        model.save(sc.sc(), getModelName());

        // Compute classification accuracy on test data
        final long correctPredictionCount = testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()))
                .filter(pl -> pl._1().equals(pl._2()))
                .count();
        Double classificationAccuracy = 1.0 * correctPredictionCount / testData.count();

        return classificationAccuracy;
    }

    @Override
    public String getModelNamePrefix() {
        return NAME;
    }
}
