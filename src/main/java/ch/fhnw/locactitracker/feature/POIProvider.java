package ch.fhnw.locactitracker.feature;

import ch.fhnw.locactitracker.model.ActivityType;
import ch.fhnw.locactitracker.model.Trace;
import org.apache.commons.lang.ArrayUtils;
import org.apache.spark.api.java.JavaRDD;

import java.util.List;
import java.util.Map;

/**
 * Abstract POI provider
 */
public abstract class POIProvider {

    protected Map<Long, Integer[]> regions;
    protected boolean isOtherEnabled = false; // Define if the OTHER type is enabled (default false)

    /**
     * Set the radius in meter
     * @param radius
     */
    public abstract void setRadius(int radius);

    /**
     * Enable an additional category for the location type not mapped to any activity
     * @param enable
     */
    public abstract void enableOther(boolean enable);

    /**
     * Search for the places around the given point
     * @param latitude
     * @param longitude
     */
    public abstract Integer[] searchAndMapPOINearby(double latitude, double longitude);

    /**
     * Return the activity id based on the POI type
     * @param poiType the POI type as String
     * @return activity identifier
     */
    public abstract int mapPOITypeToActivityType(String poiType);


    public Map<Long, Integer[]> generatePOIMapsForRegion(JavaRDD<Trace> traces) {
        return  generatePOIMapsForRegion(traces.collect());
    }

    public Map<Long, Integer[]> generatePOIMapsForRegion(List<Trace> traces) {
        double[] values = new double[ActivityType.values().length-1];

        int i = 0;
        for (Trace r:traces) {
            regions.put(r.getTimestamp(), searchAndMapPOINearby(r.getLatitude(), r.getLongitude()));
        }

        return regions;
    }

    public double[] generateTDIDFForRegion(Trace trace) {
        if (regions.containsKey(trace.getTimestamp()))
            return getTDIFForRegion(trace.getTimestamp());

        Integer[] region = searchAndMapPOINearby(trace.getLatitude(), trace.getLongitude());
        regions.put(trace.getTimestamp(), region);

        return getTDIFForRegion(ArrayUtils.toPrimitive(region));
    }


    public double[] getTDIFForRegion(int[] region) {
        // ActivityTypes - 1 (Others is not included)
        double[] values = new double[ActivityType.values().length - (isOtherEnabled ? 0 : 1)];

        for (ActivityType type:ActivityType.values()) {
            if (type.equals(ActivityType.OTHER) && !isOtherEnabled)
                continue;
            values[type.getId()] = getTDIDFcategory(type, (region == null) ? new int[]{} : region);
        }

        return values;
    }

    public double[] getTDIFForRegion(long regionId) {
        if (!regions.containsKey(regionId))
            return null;

        return getTDIFForRegion(ArrayUtils.toPrimitive(regions.get(regionId)));
    }

    public double getTDIDFcategory(ActivityType category, int[] region) {
        int sumCategoryRegion = 0;
        int sumCategoriesRegion = 0;

        int nbRegions = regions.size();
        int nbRegionsWithCategory = 0;
        int tmpSum;

        for (int i=0; i<region.length; i++){
            tmpSum = region[i];
            if (i == category.getId()) {
                sumCategoryRegion = tmpSum;
            }

            sumCategoriesRegion += tmpSum;
        }

        double IDF = 1.0;
        if (regions.size() > 1) {
            for (Integer[] r:regions.values()) {
                if(r[category.getId()]>0)
                    nbRegionsWithCategory ++;
            }
            nbRegionsWithCategory = ((nbRegionsWithCategory == 0) ? 1 : nbRegionsWithCategory);
            IDF = Math.log((double)nbRegions/nbRegionsWithCategory);
            if (IDF == 0.0) IDF = 1.0;
        }

        if (sumCategoriesRegion == 0)
            sumCategoriesRegion = 1;

        //return ((double)sumCategoryRegion/sumCategoriesRegion)*IDF;
        return ((double)sumCategoryRegion/sumCategoriesRegion);
    }
}
