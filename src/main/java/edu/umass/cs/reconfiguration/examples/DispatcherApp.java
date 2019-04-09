package edu.umass.cs.reconfiguration.examples;

import com.github.dockerjava.api.DockerClient;
import com.github.dockerjava.api.command.CreateContainerResponse;
import com.github.dockerjava.api.command.InspectContainerResponse;
import com.github.dockerjava.api.model.ExposedPort;
import com.github.dockerjava.api.model.HostConfig;
import com.github.dockerjava.api.model.Ports;
import com.github.dockerjava.core.DockerClientBuilder;
import edu.umass.cs.gigapaxos.interfaces.Replicable;
import edu.umass.cs.gigapaxos.interfaces.Request;
import edu.umass.cs.nio.interfaces.IntegerPacketType;
import edu.umass.cs.nio.nioutils.NIOHeader;
import edu.umass.cs.reconfiguration.HttpActiveReplica;
import edu.umass.cs.reconfiguration.interfaces.Reconfigurable;
import edu.umass.cs.reconfiguration.interfaces.ReconfigurableRequest;
import edu.umass.cs.reconfiguration.interfaces.ReplicableRequest;
import edu.umass.cs.reconfiguration.reconfigurationutils.RequestParseException;
import io.netty.buffer.Unpooled;
import io.netty.channel.ChannelFuture;
import io.netty.channel.ChannelFutureListener;
import io.netty.channel.ChannelHandlerContext;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;
import okhttp3.*;
import okhttp3.internal.http.HttpMethod;
import org.json.JSONException;
import org.json.JSONObject;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.WeakHashMap;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class DispatcherApp extends AbstractReconfigurablePaxosApp implements Replicable, Reconfigurable {

    private Map<String, String> serviceStates = new HashMap<>();

    private Map<String, Integer> servicePorts = new HashMap<>();
    private Map<String, String> serviceContainerIds = new HashMap<>();

    private OkHttpClient httpClient = new OkHttpClient();

    public static final Map<Long, ChannelHandlerContext> channelMap = new HashMap<>();

    @Override
    public boolean execute(Request _request, boolean doNotReplyToClient) {
        System.out.println("execute 1 ");

        // stop request
        if (_request instanceof AppRequest) {
            return true;
        }

        HttpReplicableRequest request = (HttpReplicableRequest) _request;

        String serviceName = request.getServiceName();
        Integer port = servicePorts.get(serviceName);

        HttpUrl url = HttpUrl.parse("http://localhost" + request.httpRequest.uri()).newBuilder()
                .scheme("http")
                .host("localhost")
                .port(port)
                .build();

        okhttp3.Request.Builder requestBuilder = new okhttp3.Request.Builder();
        requestBuilder.url(url);
        RequestBody body = null;
        String contentType = request.httpRequest.headers().get("Content-Type", "text/html");
        if (request.httpRequest.content().isReadable()) {
            body = RequestBody.create(MediaType.get(contentType), request.httpRequest.content().array());
        }
        if (body == null && HttpMethod.permitsRequestBody(request.httpRequest.method().toString())) {
            body = RequestBody.create(MediaType.get(contentType), "");
        }
        requestBuilder.method(request.httpRequest.method().toString(), body);
        for (Map.Entry<String, String> entry : request.httpRequest.headers().entries()) {
            requestBuilder.addHeader(entry.getKey(), entry.getValue());
        }
        okhttp3.Request httpClientRequest = requestBuilder.build();


        try {
            Response httpResponse = httpClient.newCall(httpClientRequest).execute();
            System.out.println("response = " + httpResponse);
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

        String state = serviceStates.get(serviceName);

        Integer integer = null;
        try {
            integer = Integer.valueOf(state);
        } catch (NumberFormatException e) {
            integer = 0;
        }

        serviceStates.put(serviceName, String.valueOf(integer + 1));

        return true;
    }

    @Override
    public String checkpoint(String name) {
        System.out.println("checkpoint: " + name + " " + serviceStates.get(name));
        return serviceStates.get(name);
    }

    @Override
    public boolean restore(String name, String state) {
        System.out.println("restore: " + name + " " + state);

        if (state == null) {
            deleteService(name);
            serviceStates.remove(name);
            return true;
        }

        String previousState = serviceStates.get(name);
        if (previousState == null) {
            createService(name, state);
            serviceStates.put(name, state);
        } else {
            System.out.println("====== Updating");
        }
        return true;
    }

    private void createService(String name, String state) {
        DockerClient client = DockerClientBuilder.getInstance().build();

        Ports ports = new Ports();
        ExposedPort tcp = ExposedPort.tcp(80);
        ports.bind(tcp, Ports.Binding.empty());
        CreateContainerResponse container = client
                .createContainerCmd("nginx")
//                .withName(name)
                .withHostConfig(HostConfig.newHostConfig().withPortBindings(ports))
                .exec();
        client.startContainerCmd(container.getId()).exec();

        InspectContainerResponse inspect = client.inspectContainerCmd(container.getId()).exec();

        Ports.Binding binding = inspect.getNetworkSettings().getPorts().getBindings().get(tcp)[0];
        Integer port = Integer.valueOf(binding.getHostPortSpec());

        servicePorts.put(name, port);
        serviceContainerIds.put(name, container.getId());
    }

    private void deleteService(String name) {
        String containerId = serviceContainerIds.get(name);
        if (containerId == null) {
            return;
        }
        DockerClient client = DockerClientBuilder.getInstance().build();
        client.removeContainerCmd(containerId).withForce(true).exec();

        serviceContainerIds.remove(name);
        servicePorts.remove(name);
    }

    @Override
    public boolean execute(Request request) {
        System.out.println("execute 2");
        return execute(request, false);
    }

    @Override
    public Request getRequest(String stringified) throws RequestParseException {
        System.out.println("getRequest json: " + stringified);

        if (stringified.contains("STOP") && stringified.contains("NO_OP")) {
            try {
                JSONObject json = new JSONObject(stringified);
                return new AppRequest(json);
            } catch (JSONException e) {
                e.printStackTrace();
            }
        }
        FullHttpRequest httpRequest = HttpActiveReplica.FullHttpRequestSerializer.fromJson(stringified);
        HttpReplicableRequest replicableRequest = new HttpReplicableRequest(httpRequest);
        try {
            Integer requestId = new JSONObject(stringified).getInt("rid");
            replicableRequest.requestId = requestId;
        } catch (JSONException e) {
            e.printStackTrace();
        }
        return replicableRequest;
    }

    @Override
    public Request getRequest(byte[] message, NIOHeader header) throws RequestParseException {
        System.out.println("getRequest: " + message.length);
        return null;
    }

    @Override
    public Set<IntegerPacketType> getRequestTypes() {
        System.out.println("getRequestTypes");
        return null;
    }


    @Override
    public ReconfigurableRequest getStopRequest(String name, int epoch) {
        System.out.println("getStopRequest: " + name + ", " + epoch);
        return AppRequest.getObliviousPaxosStopRequest(name, epoch);
    }

    public static class HttpReplicableRequest implements ReplicableRequest {

        public final FullHttpRequest httpRequest;

        public Integer requestId;

        public HttpReplicableRequest(FullHttpRequest httpRequest) {
            this.httpRequest = httpRequest;
        }

        @Override
        public boolean needsCoordination() {
            return HttpMethod.permitsRequestBody(httpRequest.method().toString());
        }

        @Override
        public IntegerPacketType getRequestType() {
            return null;
        }

        @Override
        public String getServiceName() {
            // TODO: hostname to service name
            String host = httpRequest.headers().get("Host");
            System.out.println("getServiceName: " + host);
            return host;
        }

        @Override
        public long getRequestID() {
            if (requestId == null) {
                requestId = (int) (Math.random() * Integer.MAX_VALUE);
            }
            return requestId;
        }

        @Override
        public String toString() {
            return HttpActiveReplica.FullHttpRequestSerializer.toJsonString(httpRequest, getRequestID());
        }
    }
}
