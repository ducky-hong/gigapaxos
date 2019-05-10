package edu.umass.cs.reconfiguration;

import com.google.api.core.ApiFuture;
import com.google.cloud.firestore.Firestore;
import com.google.cloud.firestore.Query;
import com.google.cloud.firestore.QueryDocumentSnapshot;
import com.google.cloud.firestore.QuerySnapshot;
import com.google.firebase.cloud.FirestoreClient;
import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.DispatcherApp;
import edu.umass.cs.utils.FullHttpRequestSerializer;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import okhttp3.internal.http.HttpMethod;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.concurrent.ExecutionException;

import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

class HttpActiveReplica {

    HttpActiveReplica(AbstractReplicaCoordinator appCoordinator, ActiveReplica activeReplica) {
        NioEventLoopGroup bossGroup = new NioEventLoopGroup(1);
        NioEventLoopGroup workerGroup = new NioEventLoopGroup();
        ServerBootstrap b = new ServerBootstrap();
        b.group(bossGroup, workerGroup)
                .channel(NioServerSocketChannel.class)
                .handler(new LoggingHandler(LogLevel.INFO))
                .childHandler(new ChannelInitializer<SocketChannel>() {
                    @Override
                    protected void initChannel(SocketChannel ch) throws Exception {
                        ChannelPipeline p = ch.pipeline();
                        p.addLast(new HttpServerCodec());
                        p.addLast(new HttpObjectAggregator(65536));
                        p.addLast(new ActiveReplicaHttpHandler(appCoordinator, activeReplica));
                    }
                });

        try {
            int port = appCoordinator.messenger.getListeningSocketAddress().getPort() + 300;
            String portGiven = System.getProperty("http.activeReplica.port");
            if (portGiven != null) {
                port = Integer.parseInt(portGiven);
            }
            b.bind(port).sync().channel();
        } catch (InterruptedException e) {
            e.printStackTrace();
            bossGroup.shutdownGracefully();
            workerGroup.shutdownGracefully();
        }
    }

    private static class ActiveReplicaHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private AbstractReplicaCoordinator appCoordinator;

        private ActiveReplica activeReplica;

        ActiveReplicaHttpHandler(AbstractReplicaCoordinator appCoordinator, ActiveReplica activeReplica) {
            this.appCoordinator = appCoordinator;
            this.activeReplica = activeReplica;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
            String virtualHost = request.headers().get("Host");
            String value = FullHttpRequestSerializer.encode(request);

            String serviceName = getServiceName(virtualHost);
            if (serviceName == null) {
                HttpResponseStatus status = HttpResponseStatus.valueOf(404);
                FullHttpResponse response = new DefaultFullHttpResponse(
                        HTTP_1_1, status);
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
                HttpUtil.setContentLength(response, response.content().readableBytes());
                ChannelFuture flushPromise = ctx.writeAndFlush(response);
                flushPromise.addListener(ChannelFutureListener.CLOSE);
                return;
            }

            AppRequest appRequest = new AppRequest(serviceName, value, AppRequest.PacketType.DEFAULT_APP_REQUEST, false);
            appRequest.setNeedsCoordination(HttpMethod.permitsRequestBody(request.method().toString()));
            InetAddress sender = ((InetSocketAddress) ctx.channel().remoteAddress()).getAddress();

            DispatcherApp.channelMap.put(appRequest.getRequestID(), ctx);
            appCoordinator.handleIncoming(appRequest, (_request, handled) -> {
                activeReplica.updateDemandStats(appRequest, sender);
                DispatcherApp.channelMap.remove(appRequest.getRequestID());
            });
        }

        private String getServiceName(String virtualHost) {
            final Firestore db = FirestoreClient.getFirestore();
            final Query query = db.collection("services").whereEqualTo("virtualHost", virtualHost);
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
            return documents.get(0).getData().get("serviceName").toString();
        }
    }
}
