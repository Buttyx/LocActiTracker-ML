package ch.fhnw.locactitracker.data;

import ch.fhnw.locactitracker.feature.FeatureUtils;
import ch.fhnw.locactitracker.feature.GooglePlacesPOIProvider;
import ch.fhnw.locactitracker.model.Trace;
import ch.fhnw.locactitracker.model.TrainingTrace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.mllib.linalg.Vector;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;


public class FeatureUtilsTest {

    private JavaSparkContext sc;
    private JavaRDD<Trace> data;

    @Before
    public void init() {
        SparkConf conf = new SparkConf().setAppName("test features")
                .set("spark.driver.host", "localhost")
                .setMaster("local[*]");

        sc = new JavaSparkContext(conf);

        data = sc.textFile("data/test_poi.csv")
                .map(line -> line.split(","))
                .map(line -> new TrainingTrace(
                        Double.valueOf(line[6]),
                        Double.valueOf(line[7]),
                        Double.valueOf(line[8]),
                        Long.valueOf(line[1]),
                        Double.valueOf(line[4]),
                        Double.valueOf(line[5]),
                        line[0],
                        line[2],
                        Boolean.valueOf(line[3])));
    }

    @After
    public void tearDown() {
        if (sc != null)
            sc.stop();
    }

    @Test
    public void featureUtil() {
        GooglePlacesPOIProvider poiProvider = new GooglePlacesPOIProvider();
        poiProvider.generatePOIMapsForRegion(data);
        Vector feature = FeatureUtils.computeFeatures(data, false, true, poiProvider);
        double[] results = feature.toArray();

        Assert.assertEquals("Check the received feature vector", 28, feature.size());
        Assert.assertTrue(0.0 == results[0]);
        Assert.assertTrue(0.0 == results[1]);
        Assert.assertTrue(0.0 == results[2]);
        Assert.assertTrue(0.0 == results[3]);
        Assert.assertTrue(0.0 == results[4]);
        Assert.assertTrue(0.0 == results[5]);
        Assert.assertTrue(0.0 == results[6]);
        Assert.assertTrue(0.0 == results[7]);


        feature = FeatureUtils.computeFeatures(data, true, true, poiProvider);
        results = feature.toArray();

        Assert.assertEquals("Check the received feature vector", 28, feature.size());
        Assert.assertTrue(0.0 == results[0]);
        Assert.assertTrue(0.0 == results[1]);
        Assert.assertTrue(0.0 == results[2]);
        Assert.assertTrue(0.0 == results[3]);
        Assert.assertTrue(0.0 < results[4]);
        Assert.assertTrue(0.0 == results[5]);
        Assert.assertTrue(0.0 == results[6]);
        Assert.assertTrue(0.0 == results[7]);
    }
}
