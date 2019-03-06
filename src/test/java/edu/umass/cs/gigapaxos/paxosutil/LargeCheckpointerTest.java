package edu.umass.cs.gigapaxos.paxosutil;

import edu.umass.cs.utils.DefaultTest;
import org.json.JSONException;
import org.json.JSONObject;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;

/**
 */
public class LargeCheckpointerTest extends DefaultTest {

    private static final String NAME = "name";

    /**
     * @throws JSONException
     * @throws IOException
     *
     */
    @Test
    public void test_checkpoint() throws JSONException, IOException {
        LargeCheckpointer.TestReplicable app = new LargeCheckpointer.TestReplicable();
        String name = NAME;
        String state = app.setRandomState(name);
        String handle = (app.checkpoint(name));
        String filename = new JSONObject(handle).getString(LargeCheckpointer.Keys.FNAME2178
                .toString());
        String content = new String(Files.readAllBytes(Paths.get(filename)));
        assert (state.equals(content));
        new File(filename).delete();
    }

    /**
     * @throws JSONException
     * @throws IOException
     */
    @Test
    public void test_restore() throws JSONException, IOException {
        LargeCheckpointer.TestReplicable app1 = new LargeCheckpointer.TestReplicable();
        LargeCheckpointer.TestReplicable app2 = new LargeCheckpointer.TestReplicable();
        LargeCheckpointer lcp1 = new LargeCheckpointer(".", "123");

        String name = NAME;
        String state = app1.setRandomState(name);
        String handle = (app1.checkpoint(name));
        handle = lcp1.stowAwayCheckpoint(name, handle);

        app2.restore(name, handle);
        assert (app2.states.get(name).equals(state));

        LargeCheckpointer.deleteHandleFile(handle);
        lcp1.deleteAllCheckpointsAndClose();
    }

    /**
     * @throws JSONException
     * @throws IOException
     */
    @Test
    public void test_remoteCheckpointTransfer() throws JSONException,
            IOException {
        LargeCheckpointer.TestReplicable app1 = new LargeCheckpointer.TestReplicable();
        LargeCheckpointer.TestReplicable app2 = new LargeCheckpointer.TestReplicable();
        LargeCheckpointer lcp1 = new LargeCheckpointer(".", "123");

        String name = NAME;
        String state = app1.setRandomState(name);
        String handle = (app1.checkpoint(name));

        handle = lcp1.stowAwayCheckpoint(name, handle);

        app2.restore(name, handle);

        assert (app2.states.get(name).equals(state));

        LargeCheckpointer.deleteHandleFile(handle);
        // lcp1.deleteAllCheckpointsAndClose();
        lcp1.close();
    }
}
