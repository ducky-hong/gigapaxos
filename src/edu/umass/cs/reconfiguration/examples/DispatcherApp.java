package edu.umass.cs.reconfiguration.examples;

import com.google.api.core.ApiFuture;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firebase.FirebaseApp;
import com.google.firebase.FirebaseOptions;
import com.google.firebase.cloud.FirestoreClient;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.gigapaxos.paxosutil.LargeCheckpointer;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import edu.umass.cs.utils.ContainerUtils;
import edu.umass.cs.utils.FullHttpRequestSerializer;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import okhttp3.*;
import okhttp3.internal.http.HttpMethod;
import org.apache.commons.io.FileUtils;
import org.json.JSONException;
import org.json.JSONObject;
import org.zeroturnaround.zip.ZipUtil;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.concurrent.ExecutionException;
import java.util.logging.Logger;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class DispatcherApp extends AbstractReconfigurablePaxosApp implements Replicable, Reconfigurable {

    private static Logger log = Logger.getLogger(DispatcherApp.class.getName());


    private OkHttpClient httpClient = new OkHttpClient();

    public static final Map<Long, ChannelHandlerContext> channelMap = new HashMap<>();

    public DispatcherApp() {
        try (InputStream is = new FileInputStream("dk-store-firebase-adminsdk-87kqy-c3cdac3b43.json")) {
            FirebaseOptions options = new FirebaseOptions.Builder()
                    .setCredentials(GoogleCredentials.fromStream(is))
                    .setDatabaseUrl("https://dk-store.firebaseio.com")
                    .build();
            FirebaseApp.initializeApp(options);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public boolean execute(Request _request, boolean doNotReplyToClient) {
        System.out.println("DispatcherApp.execute");
        System.out.println("_request = [" + _request + "], doNotReplyToClient = [" + doNotReplyToClient + "]");
        if (!(_request instanceof AppRequest)) {
            return true;
        }

        final AppRequest request = (AppRequest) _request;
        if (request.isStop()) {
            // TODO: handle the stop request
            return true;
        }

        final String serviceName = request.getServiceName();

        Integer port = ContainerUtils.getPort(serviceName);
        if (port == null) {
            log.warning("cannot find a service port for " + serviceName);
            return false;
        }

        FullHttpRequest httpRequest = FullHttpRequestSerializer.decode(request.getValue());
        HttpUrl url = HttpUrl.parse("http://localhost" + httpRequest.uri()).newBuilder()
                .scheme("http")
                .host("localhost")
                .port(port)
                .build();

        okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder();
        requestBuilder.url(url);
        RequestBody body = null;
        String contentType = httpRequest.headers().get("Content-Type", "text/html");
        if (httpRequest.content().isReadable()) {
            body = RequestBody.create(MediaType.get(contentType), Unpooled.copiedBuffer(httpRequest.content()).array());
        }
        if (body == null && HttpMethod.permitsRequestBody(httpRequest.method().toString())) {
            body = RequestBody.create(MediaType.get(contentType), "");
        }
        requestBuilder.method(httpRequest.method().toString(), body);
        for (Map.Entry<String, String> entry : httpRequest.headers().entries()) {
            requestBuilder.addHeader(entry.getKey(), entry.getValue());
        }
        okhttp3.Request httpClientRequest = requestBuilder.build();

        try {
            Response httpResponse = httpClient.newCall(httpClientRequest).execute();
            if (channelMap.containsKey(request.getRequestID())) {
                HttpResponseStatus status = HttpResponseStatus.valueOf(httpResponse.code());
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HTTP_1_1, status, Unpooled.copiedBuffer(httpResponse.body().bytes()));
                for (String name : httpResponse.headers().names()) {
                    response.headers().set(name, httpResponse.headers().get(name));
                }
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
                HttpUtil.setContentLength(response, response.content().readableBytes());
                ChannelFuture flushPromise = channelMap.get(request.getRequestID()).writeAndFlush(response);
                flushPromise.addListener(ChannelFutureListener.CLOSE);
                httpResponse.close();
            }
        } catch (IOException e) {
            e.printStackTrace();
        }

        return true;
    }

    private String getContainerImage(String serviceName) {
        final Firestore db = FirestoreClient.getFirestore();
        final Query query = db.collection("services").whereEqualTo("serviceName", serviceName);
        final ApiFuture<QuerySnapshot> querySnapshot = query.get();
        final List<QueryDocumentSnapshot> documents;
        try {
            documents = querySnapshot.get().getDocuments();
        } catch (InterruptedException | ExecutionException e) {
            e.printStackTrace();
            return null;
        }
        if (documents.isEmpty()) {
            return null;
        }
        return documents.get(0).getData().get("image").toString();
    }

    @Override
    public String checkpoint(String name) {
        System.out.println("DispatcherApp.checkpoint");
        System.out.println("name = [" + name + "]");

        ContainerUtils.checkpoint(name, "ckpt");
        ContainerUtils.startContainer(name, "ckpt");

        final String containerId = ContainerUtils.getRunningContainerId(name);
        if (containerId == null) {
            return null;
        }

        try {
            final File checkpointDir = new File(String.format("/var/lib/docker/containers/%s/checkpoints/", containerId));
            final File checkpointZip = File.createTempFile("gigapaxos", "ckpt");
            ZipUtil.pack(checkpointDir, checkpointZip);
            return LargeCheckpointer.createCheckpointHandle(checkpointZip.getAbsolutePath());
        } catch (IOException e) {
            e.printStackTrace();
            return null;
        }
    }

    @Override
    public boolean restore(String name, String state) {
        System.out.println("DispatcherApp.restore");
        System.out.println("name = [" + name + "], state = [" + state + "]");

        if (name.equals(DispatcherApp.class.getSimpleName() + "0")) {
            return true;
        }

        String containerId = ContainerUtils.getRunningContainerId(name);
        System.out.println("container id: " + containerId + "(" + name + ")");
        if (containerId != null && state == null) {
            deleteService(name);
            return true;
        }

        if (containerId == null) {
            createService(name, state);
        }

        return true;
    }

    private String createService(String serviceName, String state) {
        final String image = getContainerImage(serviceName);
        String containerId = ContainerUtils.createContainer(serviceName, image);

        String checkpoint = null;
        if (state != null && !state.isEmpty()) {
            try {
                final File checkpointDir = new File(String.format("/var/lib/docker/containers/%s/checkpoints/", containerId));
                if (checkpointDir.exists()) {
                    FileUtils.deleteDirectory(checkpointDir);
                }
                checkpointDir.mkdir();

                final File checkpointZip = File.createTempFile("gigapaxos", "ckpt");
                LargeCheckpointer.restoreCheckpointHandle(state, checkpointZip.getAbsolutePath());
                System.out.println(checkpointZip);

                ZipUtil.unpack(checkpointZip, checkpointDir);
                checkpoint = "ckpt";
            } catch (Exception e) {
                e.printStackTrace();
            }
        }
        ContainerUtils.startContainer(serviceName, checkpoint);

        return containerId;
    }

    private void deleteService(String serviceName) {
        ContainerUtils.stopContainer(serviceName);
    }

    @Override
    public boolean execute(Request request) {
        return execute(request, false);
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        System.out.println("DispatcherApp.getRequest");
        System.out.println("stringified = [" + stringified + "]");
        try {
            return new AppRequest(new JSONObject(stringified));
        } catch (JSONException e) {
            e.printStackTrace();
            throw new RequestParseException(e);
        }
    }

    @Override
    public Request getRequest(byte[] message, NIOHeader header) throws RequestParseException {
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        return new HashSet<>(Arrays.asList(AppRequest.PacketType.values()));
    }
}
