package ch.fhnw.locactitracker.model;

/**
 * Interface for the activity trace
 */
public interface Trace {
    public long getTimestamp();

    public String getUser();
    public void setUser(String user);

    public boolean isDominantHand();
    public void setDominantHand(boolean dominantHand);

    public double getX();
    public void setX(double x);

    public double getY();
    public void setY(double y);

    public double getZ();
    public void setZ(double z);

    public double getLongitude();
    public void setLongitude(double longitude);

    public double getLatitude();
    public void setLatitude(double latitude);
}
