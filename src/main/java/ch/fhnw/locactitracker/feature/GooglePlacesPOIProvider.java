package ch.fhnw.locactitracker.feature;


import ch.fhnw.locactitracker.model.ActivityType;
import com.google.maps.GeoApiContext;
import com.google.maps.NearbySearchRequest;
import com.google.maps.PlacesApi;
import com.google.maps.model.*;
import org.apache.commons.lang.ArrayUtils;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

import static ch.fhnw.locactitracker.model.ActivityType.*;

/**
 * Provides POI Feature implementation based on the Google Places API
 */
public class GooglePlacesPOIProvider extends POIProvider {

    private final static int SEARCH_RADIUS = 20;

    private static final Map<PlaceType, ActivityType> POI_TYPES_CATEGORIES;
    static {
        Map<PlaceType, ActivityType> poiTypes = new HashMap<>();
        // ENTERTAINMENT
        poiTypes.put(PlaceType.MOVIE_THEATER, ENTERTAINMENT);
        poiTypes.put(PlaceType.AMUSEMENT_PARK, ENTERTAINMENT);
        poiTypes.put(PlaceType.ART_GALLERY, ENTERTAINMENT);
        poiTypes.put(PlaceType.CASINO, ENTERTAINMENT);
        poiTypes.put(PlaceType.MUSEUM, ENTERTAINMENT);
        poiTypes.put(PlaceType.STADIUM, ENTERTAINMENT);
        poiTypes.put(PlaceType.AQUARIUM, ENTERTAINMENT);

        // DINNING
        poiTypes.put(PlaceType.RESTAURANT, DINNING);
        poiTypes.put(PlaceType.CAFE, DINNING);
        poiTypes.put(PlaceType.BAR, DINNING);

        // STUDYING
        poiTypes.put(PlaceType.UNIVERSITY, STUDYING);
        poiTypes.put(PlaceType.SCHOOL, STUDYING);
        poiTypes.put(PlaceType.LIBRARY, STUDYING);

        // SHOPPING
        poiTypes.put(PlaceType.SHOPPING_MALL, SHOPPING);
        poiTypes.put(PlaceType.SHOE_STORE, SHOPPING);
        poiTypes.put(PlaceType.LIQUOR_STORE, SHOPPING);
        poiTypes.put(PlaceType.STORE, SHOPPING);
        poiTypes.put(PlaceType.PHARMACY, SHOPPING);
        poiTypes.put(PlaceType.PET_STORE, SHOPPING);
        poiTypes.put(PlaceType.JEWELRY_STORE, SHOPPING);
        poiTypes.put(PlaceType.BAKERY, SHOPPING);

        POI_TYPES_CATEGORIES = Collections.unmodifiableMap(poiTypes);
    }

    protected Map<LatLng,Integer[]> locationCaches;
    protected GeoApiContext geoContext;
    protected int radius;

    public GooglePlacesPOIProvider() {
        geoContext = new GeoApiContext();
        geoContext.setApiKey("AIzaSyAGVSspxnRkYuMqwd8EnOijla-U_kfwTpQ");
        locationCaches = new HashMap<>();
        regions = new HashMap<>();
        radius = SEARCH_RADIUS;
    }

    @Override
    public void setRadius(int radius) {
        // Reset cache
        this.locationCaches.clear();
        this.regions = new HashMap<>();
        this.radius = radius;
    }

    @Override
    public void enableOther(boolean enable) {
        this.isOtherEnabled = enable;
    }


    /**
     * Generate a map<ActivityId,numberOfPOI> for the given region
     */
    @Override
    public Integer[] searchAndMapPOINearby(double lat, double lng) {
        // Set the other activity id for check if not enabled
        int otherActiviyId = (isOtherEnabled ? -1 : ActivityType.OTHER.getId());

        for (LatLng p:locationCaches.keySet()) {
            if (p.lat == lat && p.lng == lng)
                return locationCaches.get(p);
        }

        LatLng point = new LatLng(lat, lng);

        NearbySearchRequest request = PlacesApi.nearbySearchQuery(geoContext, point)
                                        .radius(radius);

        int[] poiCategories = new int[ActivityType.values().length - (isOtherEnabled ? 0 : 1)];

        try {
            PlacesSearchResponse response = request.await();
            PlacesSearchResult[] results = response.results;

            for(PlacesSearchResult place:results){
                for(String type:place.types){
                    int activityId = mapPOITypeToActivityType(type);
                    if (activityId == otherActiviyId)
                        continue;
                    Integer nb = poiCategories[activityId];
                    poiCategories[activityId] = (nb == null ? 1 : ++nb);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }

        // Start with primitive to avoid null pointer
        Integer[] poiCategoriesObjects = ArrayUtils.toObject(poiCategories);

        locationCaches.put(point, poiCategoriesObjects);

        return poiCategoriesObjects;
    }

    @Override
    public int mapPOITypeToActivityType(String typeStr) {
        if (typeStr == null)
            return OTHER.getId();

        PlaceType type;

        try {
            type = PlaceType.valueOf(typeStr.trim().toUpperCase());
        } catch (IllegalArgumentException e) {
            return OTHER.getId();
        }

        if (POI_TYPES_CATEGORIES.containsKey(type)) {
            return POI_TYPES_CATEGORIES.get(type).getId();
        } else {
            return OTHER.getId();
        }
    }
}
