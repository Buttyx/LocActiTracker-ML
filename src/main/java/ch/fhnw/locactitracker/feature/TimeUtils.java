package ch.fhnw.locactitracker.feature;

import ch.fhnw.locactitracker.model.Trace;

import java.util.ArrayList;
import java.util.List;

/**
 * Utility class for time related operation on series
 */
public class TimeUtils {

    /**
     * Group records in continuous segments
     * @param traces
     * @param maxDifference Maximal difference between two records on the same segment in ms
     * @return
     */
    public static List<List<Trace>> groupContinuousRecordsInSegments(Iterable<Trace> traces, long maxDifference) {

        long previousTrace = -1;
        List<Trace> currentSegment = null;
        List<List<Trace>> segments = new ArrayList<>();

        for (Trace t:traces) {
            // If the difference is too large between the two records, a new segment is created
            if ((t.getTimestamp() - previousTrace) > maxDifference) {
                if (currentSegment != null)
                    segments.add(currentSegment);
                currentSegment = new ArrayList<>();
            }

            // Assign records to segment
            currentSegment.add(t);
            previousTrace = t.getTimestamp();
        }
        segments.add(currentSegment);

        return segments;
    }

    /**
     * Split the segment in windows with the given duration
     * @param segment
     * @param duration in ms
     * @return
     */
    public static List<List<Trace>> generateTimeWindows(List<Trace> segment, long duration) {
        long startTime = segment.get(0).getTimestamp();

        List<List<Trace>> windows = new ArrayList<>();
        List<Trace> currentWindow = new ArrayList<Trace>();
        long endTimeWindow = startTime + duration;
        for (Trace t:segment) {
            if (t.getTimestamp() > endTimeWindow) {

                // Be sure that there are enough element in the window
                if (currentWindow.size() >= 10) {
                    windows.add(currentWindow);
                }

                currentWindow = new ArrayList<Trace>();
                endTimeWindow += duration;
            }
            currentWindow.add(t);
        };

        if (currentWindow.size() >= 10) {
            windows.add(currentWindow);
        }

        return windows;
    }


}
