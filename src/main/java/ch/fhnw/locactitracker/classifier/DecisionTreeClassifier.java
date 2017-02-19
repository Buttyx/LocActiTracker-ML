package ch.fhnw.locactitracker.classifier;

import ch.fhnw.locactitracker.model.ActivityType;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;
import org.apache.spark.mllib.tree.DecisionTree;
import org.apache.spark.mllib.tree.model.DecisionTreeModel;
import scala.Tuple2;

import java.util.HashMap;
import java.util.Map;

/**
 * Decision Tree Classifier
 * http://spark.apache.org/docs/2.1.0/mllib-decision-tree.html
 */
public class DecisionTreeClassifier extends Classifier {

    public static String NAME = "decisiontree";

    public DecisionTreeClassifier(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData, boolean locationAware, boolean withDominantHand) {
        super(trainingData, testData, locationAware, withDominantHand);
    }

    public Double createModel(JavaSparkContext sc) {
        /**========================================================
         * Parameters
         =========================================================*/
        Map<Integer, Integer> categoricalFeaturesInfo = new HashMap<>();
        int numClasses = ActivityType.values().length -1 ; //num of classes = num of activity to recognise
        String impurity = "gini";
        int maxDepth = 10;
        int maxBins = 32;

        /**========================================================
         * Creation of the inference model
         =========================================================*/
        // create model
        final DecisionTreeModel model = DecisionTree.trainClassifier(trainingData, numClasses, categoricalFeaturesInfo, impurity, maxDepth, maxBins);
        model.save(sc.sc(), getModelName());

        // Compute classification accuracy on test data
        final long correctPredictionCount = testData.mapToPair(p -> new Tuple2<>(model.predict(p.features()), p.label()))
                .filter(pl -> pl._1().equals(pl._2()))
                .count();

        Double accuracy = 1.0 * correctPredictionCount / testData.count();

        return accuracy;
    }

    @Override
    public String getModelNamePrefix() {
        return NAME;
    }


}
