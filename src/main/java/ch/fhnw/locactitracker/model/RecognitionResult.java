package ch.fhnw.locactitracker.model;

import java.io.Serializable;

/**
 * Model for the recognition result
 */
public class RecognitionResult implements Serializable{

    private String user;
    private long timestamp;
    private String activity;
    private String model;
    private boolean dominanthand;
    private boolean locationaware;

    public RecognitionResult(String user, long timestamp, ActivityType type, String model,
                             boolean locationAware, boolean dominantHand) {
        this.user = user;
        this.timestamp = timestamp;
        this.activity = type.getLabel();
        this.model = model;
        this.locationaware = locationAware;
        this.dominanthand = dominantHand;
    }

    public String getUser() {
        return user;
    }
    public void setUser(String user) {
        this.user = user;
    }

    public long getTimestamp() {
        return timestamp;
    }
    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }

    public String getActivity() {return activity;}
    public void setActivity(String type) {
        this.activity = type;
    }

    public boolean getLocationaware() {return locationaware;}
    public void setLocationaware(boolean locationaware) {
        this.locationaware = locationaware;
    }

    public boolean getDominanthand() {return dominanthand;}
    public void setDominanthand(boolean dominanthand) {
        this.dominanthand = dominanthand;
    }

    public String getModel() {return model;}
    public void setModel(String model) {
        this.model = model;
    }
}
