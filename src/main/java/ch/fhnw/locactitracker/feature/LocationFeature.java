package ch.fhnw.locactitracker.feature;

import ch.fhnw.locactitracker.model.Trace;
import com.grum.geocalc.DegreeCoordinate;
import com.grum.geocalc.EarthCalc;
import com.grum.geocalc.Point;
import org.apache.spark.api.java.JavaRDD;
import scala.Tuple2;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Location features
 */
public class LocationFeature {


    /**
     * Compute the total distance of a trip in meters
     */
    public static double computeTotalDistance(JavaRDD<Trace> traces) {

        JavaRDD<Point> points = traces.sortBy(t -> t.getTimestamp(), true, 1).map(t -> getPointfromTrace(t));

        double d = 0.0;
        Point previousPoint = null;
        List<Point> pointsList = points.collect();
        for (Point p:pointsList) {
            if (!(previousPoint == null)) {
                d += getDistanceBetweenPoint(previousPoint, p);
            }
            previousPoint = p;
        }

        return d;
    }

    /**
     * Compute the total duration of a trip in seconds
     */
    public static long computeTotalTime(JavaRDD<Trace> traces) {
        JavaRDD<Long> tracesTime = traces.map(t -> t.getTimestamp());
        Long first = tracesTime.first();
        Long last = tracesTime.sortBy(t -> t, false, 1).first();
        return  last - first;
    }

    /**
     * Compute the distance between two points in meters
     */
    public static double getDistanceBetweenPoint(Point p1, Point p2) {
        if (p1.equals(p2))
            return 0.0;
        else
            return EarthCalc.getDistance(p1,p2);
    }

    /**
     * Compute the average speed (km/h)
     */
    public static double computeAverageSpeed(JavaRDD<Trace> data) {
        return computeAverageSpeed(computeTotalDistance(data),computeTotalTime(data));
    }

    /**
     * Compute the average speed (km/h)
     */
    public static double computeAverageSpeed(double totalDistanceMeter, long totalTimeSeconds) {
        return (totalDistanceMeter/totalTimeSeconds)*3.6;
    }

    /**
     * Compute the maximal speed (km/h)
     */
    public static double computeMaxSpeed(JavaRDD<Trace> traces) {
        return traces.map(t -> new Tuple2<>(t,0.0))
                .reduce((max, p) -> new Tuple2<>(p._1, Math.max(max._2 == null ? 0.0 : max._2, computeSpeed(max._1,p._1))))
                ._2;
    }

    /**
     * Compute the speed between all the points in km/h
     */
    public static JavaRDD<Double> computeAllSpeed(JavaRDD<Trace> data) {
        JavaRDD<Trace> sorted = data.sortBy(t -> t, true, 1);
        List<Trace> sortedList = sorted.collect();
        final Map<Long,Trace> previousMap = new HashMap<Long,Trace>();

        Trace previousTrace = null;
        for (Trace t:sortedList) {
            if (!(previousTrace == null))
                previousMap.put(t.getTimestamp(),previousTrace);
            previousTrace = t;
        }

        return sorted.map(t -> computeSpeed(previousMap.get(t.getTimestamp()),t));
    }

    /**
     * Compute the speed in km/h
     */
    public static double computeSpeed(Trace t1, Trace t2) {
        if (t1 == null)
            return 0.0;
        double distanceBetweenPoint = getDistanceBetweenPoint(getPointfromTrace(t1), getPointfromTrace(t2));
        double time = (t2.getTimestamp() - t1.getTimestamp()) / 1000.0;
        return (distanceBetweenPoint/time)*3.6;
    }

    /**
     * Compute standard deviation of the speed (km/h)
     */
    public static double computeStandardDeviationSpeed(JavaRDD<Trace> data){
        JavaRDD<Double> speeds = computeAllSpeed(data);

        double mean = computeAverageSpeed(data);
        Double reduce = speeds.reduce((sum, speed) -> sum + ((speed - mean) * (speed - mean)));
        return Math.sqrt(reduce/data.count());
    }

    /**
     * Return the geographical point of a trace
     */
    public static Point getPointfromTrace(Trace trace) {
        return new Point(new DegreeCoordinate(trace.getLatitude()), new DegreeCoordinate(trace.getLongitude()));
    }
}
