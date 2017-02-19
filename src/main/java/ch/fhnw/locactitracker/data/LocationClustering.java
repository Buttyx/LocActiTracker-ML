package ch.fhnw.locactitracker.data;

import ch.fhnw.locactitracker.model.Trace;
import com.grum.geocalc.DegreeCoordinate;
import com.grum.geocalc.EarthCalc;
import com.grum.geocalc.Point;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

import java.util.*;


/**
 * ST-DBSCAN implementation for locactitracker
 */
public class LocationClustering {

    public static int MAX_DISTANCE = 40;            // Maximal geographical distance in meters
    public static long MAX_TIME_DIFFERENCE = 800;   // Maximal time difference in milliseconds
    public static int  MIN_POINTS = 35;             // The minimal number of points per cluster

    public static int NOISE_CLUSTER = -1;

    /**
     * Generate clusters with default values
     * @param traces the records to clusterize
     * @return the list of clusters
     */
    public static JavaPairRDD<Integer, Iterable<Trace>> generateClusters(JavaRDD<Trace> traces) {
        return generateClusters(traces, MIN_POINTS, MAX_DISTANCE, MAX_TIME_DIFFERENCE);
    }

    /**
     * Generate the clusters with the given parameters
     * @param traces the records to clusterize
     * @param minPoints The minimal number of point per cluster
     * @param distanceThreshold The distance threshold
     * @param timeThreshold The time threshold
     * @return
     */
    public static JavaPairRDD<Integer, Iterable<Trace>> generateClusters(JavaRDD<Trace> traces, int minPoints, int distanceThreshold, long timeThreshold) {
        final Map<Long, Integer> assignedToCluster = new HashMap<Long, Integer>();
        final List<Trace> tracesList = traces.sortBy(t -> t.getTimestamp(), true, 1).collect();

        int clusterIndex = 0;
        for (Trace t:tracesList) {
            // If not visited
            if (!assignedToCluster.containsKey(t.getTimestamp())) {

                List<Trace> neighbourhoodCenterTrace = retrievedNeighbours(t, traces, distanceThreshold, timeThreshold).collect();

                if (neighbourhoodCenterTrace.size() < minPoints) {
                    // If not enough point assigned to noise cluster
                    assignedToCluster.put(t.getTimestamp(), NOISE_CLUSTER);
                } else {
                    // Otherwise to the a new cluster
                    assignedToCluster.put(t.getTimestamp(), clusterIndex);

                    // Create a queue with the direct neighbours of the center
                    Queue<Trace> stack = new LinkedList<Trace>();

                    for (Trace n:neighbourhoodCenterTrace) {
                        assignedToCluster.put(n.getTimestamp(), clusterIndex);
                        stack.add(n);
                    }

                    while(!stack.isEmpty()) {
                        // Get the neighbours from current trace
                        List<Trace> neighbours = retrievedNeighbours(stack.poll(), traces, distanceThreshold, timeThreshold).collect();

                        if (neighbours.size() >= minPoints) {
                            for (Trace n:neighbours) {
                                long index = n.getTimestamp();

                                if (!assignedToCluster.containsKey(index)) {
                                        stack.add(n);
                                }
                                assignedToCluster.put(index, clusterIndex);
                            }
                        }
                    }

                    clusterIndex++;
                }
            }
        }
        return traces.groupBy(t -> assignedToCluster.get(t.getTimestamp()));
    }

    /**
     * Get all the neighbour for the given trace based on the distance and time threshold
     * @param currentTrace
     * @param traceSet
     * @param distanceThreshold
     * @param timeThreshold
     * @return the list of neighbours
     */
    protected static JavaRDD<Trace> retrievedNeighbours(Trace currentTrace, JavaRDD<Trace> traceSet, int distanceThreshold, long timeThreshold) {
        return traceSet.filter(t -> (
                !t.equals(currentTrace) && // Avoid considering current trace as neighbour
                        (computeDistance(currentTrace, t) < distanceThreshold) && (getTimeDifference(currentTrace, t) < timeThreshold))
        );
    }

    /**
     * Compute distance between the record t1 and t2 with the geocalc library
     * @param t1
     * @param t2
     * @return the distance between the two trace
     */
    protected static double getDistance(Trace t1, Trace t2) {
        Point p1 = new Point(new DegreeCoordinate(t1.getLatitude()), new DegreeCoordinate(t1.getLongitude()));
        Point p2 = new Point(new DegreeCoordinate(t2.getLatitude()), new DegreeCoordinate(t2.getLongitude()));
        return EarthCalc.getDistance(p1, p2);
    }

    /**
     * Compute the distance only with commons Math library
     * @param t1
     * @param t2
     * @return the distance between the two trace
     */
    protected static double computeDistance(Trace t1, Trace t2) {
        double lat1 = t1.getLatitude();
        double lng1 = t1.getLongitude();
        double lat2 = t2.getLatitude();
        double lng2 = t2.getLongitude();

        double earthRadius = 6371;
        double dLat = Math.toRadians(lat2 - lat1);
        double dLng = Math.toRadians(lng2 - lng1);
        double sindLat = Math.sin(dLat / 2);
        double sindLng = Math.sin(dLng / 2);
        double a = Math.pow(sindLat, 2) + Math.pow(sindLng, 2)
                * Math.cos(Math.toRadians(lat1))
                * Math.cos(Math.toRadians(lat2));

        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
        double dist = earthRadius * c;

        return dist;
    }

    protected static double getTimeDifference(Trace t1, Trace t2) {
        return Math.abs(t2.getTimestamp() - t1.getTimestamp());
    }
}
