package ch.fhnw.locactitracker.model;


import java.io.Serializable;

/**
 * Activity trace containing
 * - the user
 * - timestamp
 * - values of the 3 accelerometer axis
 * - location coordinates
 * - if the data have been collected on dominant hand
 */
public class ActivityTrace implements Trace, Comparable<Trace>, Serializable{

    private long timestamp;
    private String user;
    private double x;
    private double y;
    private double z;
    private double longitude;
    private double latitude;
    private boolean dominantHand;

    public ActivityTrace(long timestamp){
        this.timestamp = timestamp;
    }

    public ActivityTrace(long timestamp, String user, boolean dominantHand,
                         double x_value, double y_value, double z_value,
                         double latitude, double longitude) {
        x= new Double(""+x_value);
        y= new Double(""+y_value);
        z= new Double(""+z_value);
        this.user = user;
        this.timestamp = timestamp;
        this.latitude = latitude;
        this.longitude = longitude;
        this.dominantHand = dominantHand;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public String getUser() { return user; }
    public void setUser(String user){this.user = user;}

    public boolean isDominantHand() { return dominantHand; }
    public void setDominantHand(boolean dominantHand){this.dominantHand = dominantHand;}

    public double getX() {return x;}
    public void setX(double x){this.x = x;}

    public double getY() {return y;}
    public void setY(double y){this.y = y;}

    public double getZ() {return z;}
    public void setZ(double z){this.z = z;}

    public double getLongitude() { return longitude; }
    public void setLongitude(double longitude) {this.longitude = longitude;}

    public double getLatitude() { return latitude; }
    public void setLatitude(double latitude) {this.latitude = latitude;}

    @Override
    public boolean equals(Object obj) {
        if (obj == null) {
            return false;
        }
        if (!Trace.class.isAssignableFrom(obj.getClass())) {
            return false;
        }
        final Trace t = (Trace) obj;

        return t.getTimestamp() == this.getTimestamp();
    }

    @Override
    public int compareTo(Trace t) {
        if (t.getTimestamp() == this.getTimestamp())
            return 0;
        else if (t.getTimestamp() > this.getTimestamp())
            return -1;
        else
            return 1;
    }
}
