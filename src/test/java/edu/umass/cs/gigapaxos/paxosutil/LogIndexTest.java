package edu.umass.cs.gigapaxos.paxosutil;

import edu.umass.cs.gigapaxos.SQLPaxosLogger;
import edu.umass.cs.utils.DefaultTest;
import org.json.JSONArray;
import org.json.JSONException;
import org.junit.Test;

import java.io.IOException;

/**
 *
 */
public class LogIndexTest extends DefaultTest {
    /**
     * @throws JSONException
     * @throws IOException
     */
    @Test
    public void testRestore() throws JSONException, IOException {
        LogIndex logIndex = new LogIndex("paxos0", 3);
        System.out.println(logIndex.toString());
        LogIndex restored = new LogIndex(new JSONArray(logIndex.toString()));
        System.out.println(restored);
        assert (logIndex.toString().equals(restored.toString()));
        System.out.println(new String(SQLPaxosLogger.inflate(SQLPaxosLogger
                .deflate(logIndex.toString().getBytes("ISO-8859-1"))),
                "ISO-8859-1"));
    }

}
