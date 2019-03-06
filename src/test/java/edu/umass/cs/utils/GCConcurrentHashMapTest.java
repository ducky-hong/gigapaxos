package edu.umass.cs.utils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 *
 */
public class GCConcurrentHashMapTest extends DefaultTest {
    /**
     * @throws InterruptedException
     */
    @Test
    public void testMain() throws InterruptedException {
        Util.assertAssertionsEnabled();
        GCConcurrentHashMap<String, Integer> map1 = new GCConcurrentHashMap<String, Integer>(
                new GCConcurrentHashMapCallback() {

                    @Override
                    public void callbackGC(Object key, Object value) {
                        // System.out.println("GC: " + key + ", " + value);
                    }

                }, 100);
        map1.setGCThresholdSize(1000);
        ConcurrentHashMap<String, Integer> map2 = new ConcurrentHashMap<String, Integer>();
        Map<String, Integer> map = map1;
        HashMap<String, Integer> hmap = new HashMap<String, Integer>();
        int n = 1000 * 1000;
        String prefix = "random";
        long t = System.currentTimeMillis();
        for (int i = 0; i < n; i++) {
            (map != null ? map : hmap).put(prefix + i, i);
            assert ((map != null ? map : hmap).containsKey(prefix + i));
            int sizeThreshold = 8000;
            if (i >= sizeThreshold)
                map.remove(prefix + (i - sizeThreshold));
        }
        long t2 = System.currentTimeMillis();
        System.out.println("delay = " + (t2 - t) + "; rate = "
                + (n / (t2 - t) + "K/s") + "; numGC = " + map1.numGC
                + "; numGCAttempts = " + map1.numGCAttempts);
        System.out.println("size = " + map.size());
        Thread.sleep(1500);
        (map != null ? map : hmap).put(prefix + (n + 1),
                (int) (Math.random() * Integer.MAX_VALUE));
        for (int i = 0; i < n; i++)
            assert (!map1.containsKey(prefix + i)
                    || !map1.putTimes.containsKey(prefix + i) || (t2
                    - map1.putTimes.get(prefix + i) < map1.gcTimeout)) : prefix
                    + i;
        assert (map1 != null && map2 != null);
    }
}
