package ch.fhnw.locactitracker.classifier;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.regression.LabeledPoint;

/**
 * Abstract classifier
 */
public abstract class Classifier {

    public final static String NAME_PREFIX = "recognitionModel/";

    // Name with location-awareness
    public final static String NAME_DOMINANT_HAND_LOCAWARE = "/training_locactitracker_dhand_locaware";
    public final static String NAME_NOT_DOMINANT_HAND_LOCAWARE = "/training_locactitracker_locaware";

    // Name without location-awareness
    public final static String NAME_DOMINANT_HAND = "/training_locactitracker_dhand";
    public final static String NAME_NOT_DOMINANT_HAND = "/training_locactitracker";

    public boolean locationAware;
    public boolean withDominantHand;
    JavaRDD<LabeledPoint> trainingData;
    JavaRDD<LabeledPoint> testData;

    public Classifier(JavaRDD<LabeledPoint> trainingData, JavaRDD<LabeledPoint> testData, boolean locationAware, boolean withDominantHand) {
        this.trainingData = trainingData;
        this.testData = testData;
        this.withDominantHand = withDominantHand;
        this.locationAware = locationAware;
    }

    /**
     * Create the model with the given spark context
     */
    public abstract Double createModel(JavaSparkContext sc);

    /**
     * Return the model name prefix
     */
    public abstract String getModelNamePrefix();

    /**
     * Get the model name
     */
    public String getModelName() {
        String name = NAME_PREFIX + getModelNamePrefix();

        if (locationAware) {
            if (withDominantHand)
                name += NAME_DOMINANT_HAND_LOCAWARE;
            else
                name += NAME_NOT_DOMINANT_HAND_LOCAWARE;
        } else {
            if (withDominantHand)
                name +=  NAME_DOMINANT_HAND;
            else
                name +=  NAME_NOT_DOMINANT_HAND;
        }

        return name;
    }

    /**
     * Get the model name
     */
    public static String getModelName(boolean locationAware, boolean withDominantHand, String model) {
        String name = NAME_PREFIX + model;

        if (locationAware) {
            if (withDominantHand)
                name += NAME_DOMINANT_HAND_LOCAWARE;
            else
                name += NAME_NOT_DOMINANT_HAND_LOCAWARE;
        } else {
            if (withDominantHand)
                name +=  NAME_DOMINANT_HAND;
            else
                name +=  NAME_NOT_DOMINANT_HAND;
        }

        return name;
    }
}
