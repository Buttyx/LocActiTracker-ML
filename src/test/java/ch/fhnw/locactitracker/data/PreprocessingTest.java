package ch.fhnw.locactitracker.data;

import ch.fhnw.locactitracker.feature.TimeUtils;
import ch.fhnw.locactitracker.model.Trace;
import ch.fhnw.locactitracker.model.TrainingTrace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.List;


public class PreprocessingTest {

    private JavaSparkContext sc;
    private JavaRDD<Trace> data;

    @Before
    public void init() {
        SparkConf conf = new SparkConf().setAppName("test preprocessing")
            .set("spark.driver.host", "localhost")
            .setMaster("local[*]");

            sc = new JavaSparkContext(conf);

            data = sc.textFile("data/test_location_clustering.csv")
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
    public void generateTimeWindows() {
        long startTime = data.first().getTimestamp();
        long endTime = data.sortBy(t -> t.getTimestamp(), false, 1).first().getTimestamp();

        long totalDuration = endTime - startTime;

        long[] lengthOfWindow = {5000, 10000, 20000};

        for (int i = 0; i < lengthOfWindow.length; i++) {
            List<List<Trace>> lists = TimeUtils.generateTimeWindows(data.collect(), lengthOfWindow[i]);
            Assert.assertEquals("Test the number of windows", (int)Math.ceil(totalDuration/(double)lengthOfWindow[i]), lists.size());
        }
    }

    @Test
    public void groupContinuousRecordsInSegments() {
        List<List<Trace>> listOfContinuousSegment = TimeUtils.groupContinuousRecordsInSegments(data.collect(), 1000);
        Assert.assertEquals("Test the number of interval", 1, listOfContinuousSegment.size());

        listOfContinuousSegment = TimeUtils.groupContinuousRecordsInSegments(data.collect(), 500);
        Assert.assertEquals("Test the number of interval", 14, listOfContinuousSegment.size());
    }
}
