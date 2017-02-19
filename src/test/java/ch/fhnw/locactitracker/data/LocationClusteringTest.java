package ch.fhnw.locactitracker.data;

import ch.fhnw.locactitracker.model.Trace;
import ch.fhnw.locactitracker.model.TrainingTrace;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.*;
import scala.Tuple2;

import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.ArrayList;
import java.util.List;

public class LocationClusteringTest {

    private JavaSparkContext sc;
    private JavaRDD<Trace> data;

    @Before
    public void init() {
        SparkConf conf = new SparkConf().setAppName("test location clustering")
                .set("spark.driver.host", "localhost")
                .setMaster("local[*]");

        sc = new JavaSparkContext(conf);

        data = sc.textFile("data/test_location_clustering_large.csv")
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
    @Ignore
    public void locationClustering() {

        List<List<Tuple2<Integer, Iterable<Trace>>>> results = new ArrayList<>();

        int[] _tDistance = {50};
        long[] _tTime = {1500};
        int[] _mPoints = {85, 90};

        int[] tDistance = new int[_tDistance.length * _tTime.length * _mPoints.length];
        long[] tTime = new long[_tDistance.length * _tTime.length * _mPoints.length];
        int[] mPoints = new int[_tDistance.length * _tTime.length * _mPoints.length];

        int idx = 0;
        for (int d=0;d < _tDistance.length; d++) {
            for (int t=0; t < _tTime.length; t++) {
                for (int m=0; m<_mPoints.length; m++) {
                    tDistance[idx] = _tDistance[d];
                    tTime[idx] = _tTime[t];
                    mPoints[idx] = _mPoints[m];
                    idx++;
                }
            }
        }

        for (int i = 0; i < tDistance.length; i++) {
            JavaPairRDD<Integer, Iterable<Trace>> clusters = LocationClustering.generateClusters(data, mPoints[i], tDistance[i], tTime[i]);
            results.add(i, clusters.collect());
        }

        try {
            File file = new File("clustering_" + System.currentTimeMillis() + ".csv");
            FileWriter writer = new FileWriter(file, true);
            PrintWriter outputfile = new PrintWriter(writer);

            outputfile.println("minPoints,tDistance,tTime,clusterId,size,duration,startTime,startLat,startLng,endTime,endLat,endLng");
            for (int i = 0; i < tDistance.length; i++) {
                outputfile.println("=================================");
                outputfile.println("Clustering with minPoints = " + mPoints[i] + ", tDistance = " + tDistance[i] + ", tTime = " + tTime[i]);
                outputfile.println("=================================");
                List<Tuple2<Integer, Iterable<Trace>>> clusters = results.get(i);
                Assert.assertTrue(clusters.size() > 0);
                outputfile.println("Number of clusters (include noise cluster): " + clusters.size());
                for (Tuple2<Integer, Iterable<Trace>> c : clusters) {
                    Iterable<Trace> traces = c._2;
                    Trace first = null, last = null;
                    int total = 0;
                    String name = (c._1 == LocationClustering.NOISE_CLUSTER) ? "noise" : ""+c._1;
                    for (Trace t : traces) {
                        if (first == null)
                            first = t;
                        last = t;
                        total++;
                    }
                    outputfile.println(mPoints[i] + "," + tDistance[i] + "," + tTime[i] + ","
                            + name + "," + total + "," + (last.getTimestamp() - first.getTimestamp()) + ","
                            + first.getTimestamp() + ","
                            + first.getLatitude() + ","
                            + first.getLongitude() + ","
                            + last.getTimestamp() + ","
                            + last.getLatitude() + ","
                            + last.getLongitude()
                    );
                }
                outputfile.println("=================================");
            }

            outputfile.close();
        } catch (IOException e){
            e.printStackTrace();
        }
    }

    @Test
    @Ignore
    public void retrievedNeighbours() {
        JavaRDD<Trace> neighbours = LocationClustering.retrievedNeighbours(data.first(),
                data, LocationClustering.MAX_DISTANCE, LocationClustering.MAX_TIME_DIFFERENCE);

        List<Trace> neighboursList = neighbours.collect();

        Assert.assertTrue(neighboursList.size() == 1);
    }
}
