package edu.umass.cs.reconfiguration.reconfigurationutils;

import com.javadocmd.simplelatlng.LatLng;
import org.junit.Test;

import static org.junit.Assert.*;

public class GeoIpUtilsTest {

    @Test
    public void getApproximateLocation() {
        final LatLng approximateLocation = GeoIpUtils.getApproximateLocation("13.209.66.6");
        assertEquals(37.4, approximateLocation.getLatitude(), 0.1);
        assertEquals(126.7, approximateLocation.getLongitude(), 0.1);
    }
}