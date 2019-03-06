package edu.umass.cs.gigapaxos.paxosutil;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;


import edu.umass.cs.gigapaxos.paxospackets.PValuePacket;
import edu.umass.cs.gigapaxos.paxospackets.PrepareReplyPacket;
import edu.umass.cs.gigapaxos.paxospackets.RequestPacket;
import edu.umass.cs.nio.NIOTransport;
import edu.umass.cs.utils.Util;

/**
 * @author arun
 *
 *         This class is needed because we need to fragment and reassemble large
 *         prepare replies so that each fragment respects NIO's MTU
 *         {@link NIOTransport#MAX_PAYLOAD_SIZE}. We currently limit the size of
 *         batched requests so that it is at most MTU bytes, but that still
 *         means that prepare replies could be much larger as they consist of
 *         all requests from the garbage-collected slot (typically the most
 *         recent checkpoint slot) up until the highest accepted slot. It is
 *         unclear how to precisely limit the size of prepare replies without
 *         significantly hurting performance or liveness. So we don't attempt to
 *         ensure an explicit limit on prepare reply sizes but instead just
 *         fragment and reassemble them if they are too big.
 */
public class PrepareReplyAssembler {

	static final ConcurrentHashMap<String, ConcurrentHashMap<Integer, PrepareReplyPacket>> preplies = new ConcurrentHashMap<String, ConcurrentHashMap<Integer, PrepareReplyPacket>>();

	/**
	 * @param incoming
	 * @return Reassembled {@link PrepareReplyPacket} if any.
	 */
	public synchronized static PrepareReplyPacket processIncoming(
			PrepareReplyPacket incoming) {

		if (incoming.isComplete())
			return incoming;

		preplies.putIfAbsent(incoming.getPaxosIDVersion(),
				new ConcurrentHashMap<Integer, PrepareReplyPacket>());
		PrepareReplyPacket existing = preplies
				.get(incoming.getPaxosIDVersion()).putIfAbsent(
						incoming.acceptor, incoming);
		// first fragment inserted, wait for more
		if (existing == null)
			return null;

		GC();

		// combine returns true if existing becomes complete
		PrepareReplyPacket retval = (existing.combine(incoming) ? preplies.get(
				incoming.getPaxosIDVersion()).remove(incoming.acceptor) : null);

		// always retain the highest ballot ack'ed by the acceptor
		if (incoming.ballot.compareTo(existing.ballot) > 0)
			preplies.get(incoming.getPaxosIDVersion()).put(incoming.acceptor,
					incoming);

		// remove empty state for paxosID
		if (preplies.get(incoming.getPaxosIDVersion()).isEmpty())
			preplies.remove(incoming.getPaxosIDVersion());

		return retval;
	}

	private static final long MAX_WAITING_TIME = 60000;

	private static void GC() {
		Set<PrepareReplyPacket> removals = new HashSet<PrepareReplyPacket>();
		for (Map<Integer, PrepareReplyPacket> map : preplies.values())
			for (PrepareReplyPacket preply : map.values())
				if (System.currentTimeMillis() - preply.getCreateTime() > MAX_WAITING_TIME)
					removals.add(preply);

		for (PrepareReplyPacket preply : removals) {
			Map<Integer, PrepareReplyPacket> map = preplies.get(preply
					.getPaxosIDVersion());
			if (map != null && map.containsValue(preply))
				map.remove(preply.acceptor);
		}

	}

	/**
	 * @param preply
	 * @return Array of fragment prepare replies.
	 */
	public static PrepareReplyPacket[] fragment(PrepareReplyPacket preply) {
		return fragment(preply, NIOTransport.MAX_PAYLOAD_SIZE);
	}

	static PrepareReplyPacket[] fragment(PrepareReplyPacket preply,
										 int fragmentSize) {
		Set<PrepareReplyPacket> fragments = new HashSet<PrepareReplyPacket>();
		if (preply.getLengthEstimate() <= fragmentSize) {
			fragments.add(preply);
			return fragments.toArray(new PrepareReplyPacket[0]);
		}

		while (!preply.accepted.isEmpty())
			fragments.add(preply.fragment(fragmentSize));
		return fragments.toArray(new PrepareReplyPacket[0]);
	}

}
