package ch.fhnw.locactitracker.model;


/**
 * Training trace
 * Extend activity trace with the activity label
 */
public class TrainingTrace extends ActivityTrace {

    private String userID;
    private String activity;
    private ActivityTrace activityTrace;

    public TrainingTrace(long timestamp){
        super(timestamp);
    };


    public TrainingTrace(double x_value, double y_value, double z_value, long timestamp,
                         double latitude, double longitude, String user, String activity, boolean dominantHand) {

        super(timestamp, user, dominantHand, x_value, y_value, z_value, latitude, longitude);

        this.activity = activity;
    }

    public TrainingTrace(Trace trace, String activity) {

        super(trace.getTimestamp(), trace.getUser(), trace.isDominantHand(),
                trace.getX(), trace.getY(), trace.getZ(),
                trace.getLatitude(), trace.getLongitude());

        this.activity = activity;
    }


    public String getActivity() {
        return activity;
    }

    public void setActivity(String activity) {
        this.activity = activity;
    }
}

