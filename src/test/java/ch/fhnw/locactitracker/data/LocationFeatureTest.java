package ch.fhnw.locactitracker.data;

import ch.fhnw.locactitracker.feature.GooglePlacesPOIProvider;
import ch.fhnw.locactitracker.feature.LocationFeature;
import ch.fhnw.locactitracker.model.ActivityType;
import ch.fhnw.locactitracker.model.Trace;
import ch.fhnw.locactitracker.model.TrainingTrace;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.*;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


public class LocationFeatureTest {

    private JavaRDD<Trace> data;
    private JavaSparkContext sc;
    GooglePlacesPOIProvider locationFeature;

    @Before
    public void init() {
        SparkConf conf = new SparkConf().setAppName("test location feature extraction")
                .set("spark.driver.host", "localhost")
                .setMaster("local[*]");

        sc = new JavaSparkContext(conf);

        data = sc.textFile("data/test_location_feature.csv")
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

    /**
     * -------------------------------
     * START POI feature tests
     * -------------------------------
     */

    @Test
    public void searchAndMapPOINearby() {
        locationFeature = new GooglePlacesPOIProvider();

        double fhnwLat = 47.348098;
        double fhnwLng = 7.907894;

        // run
        Integer[] values = locationFeature.searchAndMapPOINearby(fhnwLat, fhnwLng);
        // assert
        Assert.assertTrue(values.length > 0);

        Assert.assertTrue(values[ActivityType.STUDYING.getId()]>0);
    }

    @Test
    public void getTDIFForRegion() {
        locationFeature = new GooglePlacesPOIProvider();

        double fhnwLat = 47.348098;
        double fhnwLng = 7.907894;

        Integer[] values = locationFeature.searchAndMapPOINearby(fhnwLat, fhnwLng);
        double[] tdifForRegion = locationFeature.getTDIFForRegion(ArrayUtils.toPrimitive(values));

        System.out.println(Arrays.toString(tdifForRegion));

        // assert
        Assert.assertTrue(values.length > 0);
        Assert.assertTrue(tdifForRegion.length>0);
    }

    @Test
    public void generatePOIMapsForRegion() {
        locationFeature = new GooglePlacesPOIProvider();

        JavaRDD<Trace> traces = sc.textFile("data/test_poi.csv")
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

        Map<Long, Integer[]> poiMap = locationFeature.generatePOIMapsForRegion(traces);

        // assert
        Assert.assertTrue(poiMap.size() == 11);
        Assert.assertTrue(poiMap.get(1L)[ActivityType.STUDYING.getId()] > 2);
    }

    @Test
    @Ignore
    public void testTDIDFForAllRegion() {
        locationFeature = new GooglePlacesPOIProvider();

        JavaRDD<String[]> tracesStr = sc.textFile("data/test_poi.csv")
                .map(line -> line.split(","));

        List<String> locations =  tracesStr.map(line -> line[9]).collect();
        List<String> activity =  tracesStr.map(line -> line[2]).collect();

        JavaRDD<Trace> traces = tracesStr.map(line -> new TrainingTrace(
                Double.valueOf(line[6]),
                Double.valueOf(line[7]),
                Double.valueOf(line[8]),
                Long.valueOf(line[1]),
                Double.valueOf(line[4]),
                Double.valueOf(line[5]),
                line[0],
                line[2],
                Boolean.valueOf(line[3])));

        List<Trace> tracesList = traces.collect();

        List<Map<Long, Integer[]>> results = new ArrayList<>();

        boolean[] other = {false, true , false, true, false, true, false, true};
        int[] radius = {5, 5, 10, 10, 50, 50, 100, 100};
        for (int i=0; i<radius.length; i++) {
            locationFeature.setRadius(radius[i]);
            locationFeature.enableOther(other[i]);
            Map<Long, Integer[]> map = locationFeature.generatePOIMapsForRegion(traces);
            results.add(i,map);
            // assert
            Assert.assertTrue(map.size() == 11);
            Assert.assertTrue(map.get(1L)[ActivityType.STUDYING.getId()] > 1);
        }

        try {
            File file = new File("poi_" + System.currentTimeMillis() + ".csv");
            FileWriter writer = new FileWriter(file, true);
            PrintWriter outputfile = new PrintWriter(writer);
            outputfile.println("radius,other,regionId,A1,A2,A3,A4,A5,A6,A7,A8,A9");

            for (int i=0; i<radius.length; i++) {
                //outputfile.println("=================================");
                //outputfile.println("POI Parameters - Radius: " + radius[i] + " - isOtherEnabled: " + other[i]);
                //outputfile.println("=================================");
                Map<Long, Integer[]> map = results.get(i);
                for (long id:map.keySet()) {
                    outputfile.println(radius[i] + "," + other[i] + "," + (id) + "," +
                            ArrayUtils.toString(locationFeature.getTDIFForRegion(ArrayUtils.toPrimitive(map.get(id))))
                                    .replace('}',' ')
                                    .replace('{',' ')
                                    .trim() + "," + activity.get((int)id-1) + "," + locations.get((int)id-1)
                    );
                }
                //outputfile.println("=================================");
            }

            outputfile.close();
        } catch (IOException e){
            e.printStackTrace();
        }



    }

    /**
     * -------------------------------
     * START Motion feature tests
     * -------------------------------
     */

    @Test
    public void maximumSpeed() {
        double maxSpeed = LocationFeature.computeMaxSpeed(data);

        Assert.assertTrue(maxSpeed > 0.0);
    }

    @Test
    public void computeAllSpeed() {
        JavaRDD<Double> allSpeed = LocationFeature.computeAllSpeed(data);

        allSpeed.collect().forEach(t -> Assert.assertTrue(t >= 0.0));
    }

    @Test
    public void totalDistance() {
        double totalDistance = LocationFeature.computeTotalDistance(data);

        Assert.assertTrue(965 == Math.round(totalDistance));
    }

    @Test
    public void totalTime() {
        long totalTime = LocationFeature.computeTotalTime(data);

        Assert.assertTrue(161817 == totalTime);
    }

    @Test
    public void averageSpeed() {
        double totalDistance = LocationFeature.computeTotalDistance(data);
        long totalTime = LocationFeature.computeTotalTime(data) / 1000; // convert millis to seconds

        double averageSpeed = LocationFeature.computeAverageSpeed(totalDistance, totalTime);

        Assert.assertTrue(22 == Math.round(averageSpeed));
    }

    @Test
    @Ignore
    public void standardDeviation() {
        double sd = LocationFeature.computeStandardDeviationSpeed(data);
        Assert.assertTrue(70 == Math.round(sd));
    }



}
