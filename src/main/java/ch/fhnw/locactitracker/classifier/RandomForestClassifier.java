package ch.fhnw.locactitracker.classifier;

import ch.fhnw.locactitracker.model.ActivityType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.RandomForest;
import org.apache.spark.mllib.tree.model.RandomForestModel;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Random forest Classifier
 * https://spark.apache.org/docs/latest/ml-classification-regression.html#random-forest-classifier
 */
public class RandomForestClassifier extends Classifier {

    public static String NAME = "randomforest";

    public RandomForestClassifier(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData, boolean locationAware, boolean withDominantHand) {
        super(trainingData, testData, locationAware, withDominantHand);
    }

    public Double createModel(JavaSparkContext sc) {
        /**========================================================
         * Parameters
         =========================================================*/
        // parameters
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<Integer, Integer>();
        int numTrees = 10;
        int numClasses = ActivityType.values().length - 1;
        String featureSubsetStrategy = "auto";
        String impurity = "gini";
        int maxDepth = 20;
        int maxBins = 32;
        int randomSeeds = 12345;

        /**========================================================
         * Creation of the inference model
         =========================================================*/
        // create model
        RandomForestModel model = RandomForest.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, numTrees, featureSubsetStrategy, impurity, maxDepth, maxBins, randomSeeds);
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
