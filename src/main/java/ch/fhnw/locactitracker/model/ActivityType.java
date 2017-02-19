package ch.fhnw.locactitracker.model;

/**
 * List of supported activity
 */
public enum ActivityType {

    COOKING("Cooking", 0),
    DINNING("Dinning", 1),
    SHOPPING("Shopping",2 ),
    ENTERTAINMENT("Entertainment",3),
    STUDYING("Studying",4),
    TRANSPORTATION("Transportation",5),
    WALKING("Walking",6),
    BIKING("Biking", 7),
    OTHER("Other", 8);

    private String label;
    private int id;

    ActivityType(String label, int id) {
        this.id = id;
        this.label = label;
    }

    public String getLabel(){
        return label;
    }

    public int getId() {
        return id;
    }

    public static ActivityType fromId(int activityID) throws IllegalArgumentException {
        try{
            return ActivityType.values()[activityID];
        }catch( ArrayIndexOutOfBoundsException e ) {
            throw new IllegalArgumentException("Does not match any Id");
        }
    }

    @Override
    public String toString() {
        return this.getLabel();
    }

}
