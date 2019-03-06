package edu.umass.cs.gigapaxos.paxosutil;

import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PrepareReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.utils.DefaultTest;
import edu.umass.cs.utils.Util;
import org.junit.Test;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Set;

/**
 * Test class for PreprareReplyAssembler
 */
public class PrepareReplyAssemblerTest extends DefaultTest {

    /**
     * Unit testing code.
     *
     * @throws InterruptedException
     */
    @Test
    public void testFragmentAndReassembly() throws InterruptedException {
        Util.assertAssertionsEnabled();
        String paxosID1 = "paxos1", paxosID2 = "paxos2";
        int version1 = 0, version2 = 3;
        int first1 = 0, max1 = 2;
        Ballot ballot1 = new Ballot(43, 578);
        int acceptor1 = 23;
        PrepareReplyPacket preply1 = new PrepareReplyPacket(acceptor1,
                ballot1, new HashMap<Integer, PValuePacket>(), first1 - 1,
                max1);
        preply1.putPaxosID(paxosID1, version1);
        assert (!preply1.isComplete());
        preply1.accepted.put(0, RequestPacket.getRandomPValue(paxosID1,
                version1, 0, ballot1));
        assert (!preply1.isComplete());
        preply1.accepted.put(1, RequestPacket.getRandomPValue(paxosID1,
                version1, 1, ballot1));
        assert (!preply1.isComplete());
        preply1.accepted.put(2, RequestPacket.getRandomPValue(paxosID1,
                version1, 2, ballot1));
        assert (preply1.isComplete());

        assert (PrepareReplyAssembler.processIncoming(preply1) == preply1);

        int acceptor2 = 25;
        Ballot ballot2 = new Ballot(35, 67);
        int first2 = 20, max2 = 40;
        PrepareReplyPacket preply2 = new PrepareReplyPacket(acceptor2,
                ballot2, new HashMap<Integer, PValuePacket>(), first2 - 1,
                max2);
        preply2.putPaxosID(paxosID2, version2);
        assert (!preply2.isComplete());
        preply2.accepted.put(first2, RequestPacket.getRandomPValue(
                paxosID2, version2, first2, ballot2));
        assert (PrepareReplyAssembler.processIncoming(preply2) == null);
        System.out
                .println("preplies after processing preply2: " + PrepareReplyAssembler.preplies);

        PrepareReplyPacket tmp2 = new PrepareReplyPacket(acceptor2,
                ballot2, new HashMap<Integer, PValuePacket>(), first2 - 1,
                max2);
        for (int i = first2 + 1; i <= max2; i++) {
            tmp2.putPaxosID(paxosID2, version2);
            tmp2.accepted.put(i, RequestPacket.getRandomPValue(paxosID2,
                    version2, i, ballot2));
        }
        assert (!tmp2.isComplete());
        // used to manually test GC with a wait time of 1 sec
        Thread.sleep(100);

        assert (PrepareReplyAssembler.processIncoming(tmp2) == preply2);
        System.out.println("preplies after processing tmp2: " + PrepareReplyAssembler.preplies);
        assert (PrepareReplyAssembler.processIncoming(tmp2) == null);

        assert (preply2.isComplete());

        // preply3 is same ballot and acceptor as preply2
        Ballot ballot3 = new Ballot(36, 67);
        PrepareReplyPacket preply3 = new PrepareReplyPacket(acceptor2,
                ballot3, new HashMap<Integer, PValuePacket>(), first2 - 1,
                max2);
        preply3.putPaxosID(paxosID2, version2);
        preply3.accepted.put(first2, RequestPacket.getRandomPValue(
                paxosID2, version2, first2, ballot2));
        preply3.accepted.put(first2 + 1, RequestPacket.getRandomPValue(
                paxosID2, version2, first2 + 1, ballot2));
        assert (PrepareReplyAssembler.processIncoming(preply3) == null);
        System.out
                .println("preplies slots after processing preply3: "
                        + PrepareReplyAssembler.preplies.get(preply3.getPaxosIDVersion()).get(
                                acceptor2).accepted.keySet());
        assert (PrepareReplyAssembler.preplies.get(preply3.getPaxosIDVersion()).get(acceptor2).accepted
                .size() == 2);
        System.out
                .println("preplies slots after processing preply3: "
                        + PrepareReplyAssembler.preplies.get(preply3.getPaxosIDVersion()).get(
                                acceptor2).accepted.keySet());
        // tmp2 is lower ballot
        assert (PrepareReplyAssembler.processIncoming(tmp2) != preply3);

        Set<Integer> before = new HashSet<Integer>(tmp2.accepted.keySet());
        PrepareReplyPacket[] fragments = PrepareReplyAssembler.fragment(tmp2, 500);
        System.out.println("Created " + fragments.length + " fragments");
        Set<Integer> after = new HashSet<Integer>();
        for (PrepareReplyPacket fragment : fragments)
            after.addAll(fragment.accepted.keySet());
        assert (after.equals(before));
    }
}
