package edu.umass.cs.reconfiguration.reconfigurationutils;

import com.google.common.collect.Sets;
import com.google.gson.Gson;
import com.javadocmd.simplelatlng.LatLng;
import com.javadocmd.simplelatlng.LatLngTool;
import com.javadocmd.simplelatlng.util.LengthUnit;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableAppInfo;
import org.json.JSONException;
import org.json.JSONObject;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.*;
import java.util.stream.Collectors;

public class GeoIpDemandProfile extends DemandProfile {

    private enum Keys {
        DIST
    }

    private Map<String, Integer> demandDistribution = new HashMap<>();

    public GeoIpDemandProfile(String name) {
        super(name);
    }

    public GeoIpDemandProfile(DemandProfile dp) {
        super(dp);
    }

    public GeoIpDemandProfile(JSONObject json) throws JSONException {
        super(json);
        if (json != null) {
            if (json.has(Keys.DIST.toString())) {
                final JSONObject dist = json.getJSONObject(Keys.DIST.toString());
                demandDistribution = new HashMap(new Gson().fromJson(dist.toString(), HashMap.class));
                System.out.println(demandDistribution);
            }
        }
    }

    @Override
    public boolean shouldReportDemandStats(Request request, InetAddress sender, ReconfigurableAppInfo nodeConfig) {
        synchronized (demandDistribution) {
            super.shouldReportDemandStats(request, sender, nodeConfig);
            final Integer numRequests = demandDistribution.getOrDefault(sender.getHostAddress(), 0);
            System.out.println(demandDistribution.containsKey(sender.getHostAddress()));
            demandDistribution.put(sender.getHostAddress(), numRequests + 1);
            System.out.println(demandDistribution);
        }
        return true;
    }

    @Override
    public JSONObject getDemandStats() {
        final JSONObject json = super.getDemandStats();
        try {
            json.put(Keys.DIST.toString(), demandDistribution);
        } catch (JSONException e) {
            throw new RuntimeException(e);
        }
        return json;
    }

    @Override
    public Set<String> reconfigure(Set<String> curActives, ReconfigurableAppInfo nodeConfig) {
        final List<Map.Entry<String, Integer>> sortedDemandDistribution = demandDistribution.entrySet().stream()
                .sorted(Comparator.comparingInt(Map.Entry::getValue))
                .collect(Collectors.toList());
        if (!sortedDemandDistribution.isEmpty() && getNumRequests() > 5) {
            final Map.Entry<String, Integer> highestDemand =
                    sortedDemandDistribution.get(sortedDemandDistribution.size() - 1);
            final String highestDemandAddress = highestDemand.getKey();
            final LatLng highestDemandLocation = GeoIpUtils.getApproximateLocation(highestDemandAddress);
            if (nodeConfig.getAllActiveReplicas() != null) {
                String closest = null;
                double closestDistance = Integer.MAX_VALUE;
                for (Map.Entry<String, InetSocketAddress> entry : nodeConfig.getAllActiveReplicas().entrySet()) {
                    String acName = entry.getKey();
                    InetSocketAddress acAddress = entry.getValue();
                    if (curActives.contains(acName)) {
                        continue;
                    }
                    final LatLng acLocation = GeoIpUtils.getApproximateLocation(acAddress.getHostString());
                    final double distance = LatLngTool.distance(highestDemandLocation, acLocation, LengthUnit.KILOMETER);
                    System.out.println(acName + " distance: " + distance);
                    if (distance < closestDistance) {
                        closest = acName;
                        closestDistance = distance;
                    }
                }
                if (closest != null) {
                    System.out.println("moves to " + closest);
                    return Sets.newHashSet(closest);
                }
            }
        }
        return super.reconfigure(curActives, nodeConfig);
    }
}
