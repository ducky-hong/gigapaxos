package edu.umass.cs.reconfiguration.reconfigurationutils;

import com.javadocmd.simplelatlng.LatLng;
import com.maxmind.geoip2.DatabaseReader;
import com.maxmind.geoip2.exception.GeoIp2Exception;
import com.maxmind.geoip2.model.CityResponse;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;

public class GeoIpUtils {

    private static DatabaseReader reader;

    public static LatLng getApproximateLocation(String ipAddress) {
        if (reader == null) {
            final File database = new File("GeoLite2-City.mmdb");
            try {
                reader = new DatabaseReader.Builder(database).build();
            } catch (IOException e) {
                throw new UncheckedIOException(e);
            }
        }
        try {
            InetAddress address = InetAddress.getByName(ipAddress);
            final CityResponse city = reader.city(address);
            return new LatLng(city.getLocation().getLatitude(),
                    city.getLocation().getLongitude());
        } catch (IOException e) {
            throw new UncheckedIOException(e);
        } catch (GeoIp2Exception e) {
            throw new RuntimeException(e);
        }
    }
}
