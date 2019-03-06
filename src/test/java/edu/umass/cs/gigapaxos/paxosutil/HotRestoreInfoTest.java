package edu.umass.cs.gigapaxos.paxosutil;

import edu.umass.cs.utils.DefaultTest;
import org.junit.Assert;
import org.junit.Test;

public class HotRestoreInfoTest extends DefaultTest {
    @Test
    public void testToStringAndBack() {
        int[] members = { 1, 4, 67 };
        int[] nodeSlots = { 1, 3, 5 };
        HotRestoreInfo hri1 = new HotRestoreInfo("paxos0", 2, members, 5,
                new Ballot(3, 4), 3, new Ballot(45, 67), 34, nodeSlots);
        System.out.println(hri1.toString());
        String str1 = hri1.toString();
        HotRestoreInfo hri2 = new HotRestoreInfo(str1);
        String str2 = hri2.toString();
        System.out.println(str2);
        Assert.assertEquals(str1, str2);
    }
}
