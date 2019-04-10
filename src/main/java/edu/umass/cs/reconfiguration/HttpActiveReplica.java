package edu.umass.cs.reconfiguration;

import edu.umass.cs.reconfiguration.examples.AppRequest;
import edu.umass.cs.reconfiguration.examples.DispatcherApp;
import io.netty.bootstrap.ServerBootstrap;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.channel.*;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.SocketChannel;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.handler.codec.http.*;
import io.netty.handler.logging.LogLevel;
import io.netty.handler.logging.LoggingHandler;
import io.netty.util.CharsetUtil;
import net.minidev.json.JSONObject;
import net.minidev.json.JSONValue;

import java.net.InetSocketAddress;
import java.util.*;

import static io.netty.handler.codec.http.HttpResponseStatus.OK;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_0;
import static io.netty.handler.codec.http.HttpVersion.HTTP_1_1;

public class HttpActiveReplica {

    public HttpActiveReplica(AbstractReplicaCoordinator appCoordinator, ActiveReplica activeReplica) {
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


    public static class FullHttpRequestSerializer {

        public static FullHttpRequest fromJson(String json) {
            Map<String, String> parsed = (Map<String, String>) JSONValue.parse(json);
            byte[] bytes = Base64.getDecoder().decode(parsed.get("data"));
            ByteBuf buf = Unpooled.wrappedBuffer(bytes);

            int size = 0;

            size = buf.readInt();
            String protocolVersion = buf.readCharSequence(size, CharsetUtil.UTF_8).toString();

            size = buf.readInt();
            String method = buf.readCharSequence(size, CharsetUtil.UTF_8).toString();

            size = buf.readInt();
            String uri = buf.readCharSequence(size, CharsetUtil.UTF_8).toString();

            DefaultHttpHeaders headers = new DefaultHttpHeaders();
            int numHeaders = buf.readInt();
            for (int i = 0; i < numHeaders; i++) {
                size = buf.readInt();
                String sequence = buf.readCharSequence(size, CharsetUtil.UTF_8).toString();
                String[] tokens = sequence.split(":");
                headers.add(tokens[0], tokens[1]);
            }

            size = buf.readInt();
            ByteBuf content = buf.readBytes(size);

            return new DefaultFullHttpRequest(
                    HttpVersion.valueOf(protocolVersion),
                    HttpMethod.valueOf(method),
                    uri,
                    content,
                    headers,
                    new DefaultHttpHeaders()
            );
        }

        public static String toJsonString(FullHttpRequest request, long requestId) {
            ByteBuf buf = Unpooled.buffer();

            String protocolVersion = request.protocolVersion().text();
            buf.writeInt(protocolVersion.length());
            buf.writeCharSequence(protocolVersion, CharsetUtil.UTF_8);

            String method = request.method().toString();
            buf.writeInt(method.length());
            buf.writeCharSequence(method, CharsetUtil.UTF_8);

            String uri = request.uri();
            buf.writeInt(uri.length());
            buf.writeCharSequence(uri, CharsetUtil.UTF_8);

            List<Map.Entry<String, String>> entries = request.headers().entries();
            buf.writeInt(entries.size());
            for (Map.Entry<String, String> entry : entries) {
                String sequence = entry.getKey() + ":" + entry.getValue();
                buf.writeInt(sequence.length());
                buf.writeCharSequence(sequence, CharsetUtil.UTF_8);
            }

            ByteBuf content = request.content();
            buf.writeInt(content.readableBytes());
            buf.writeBytes(content);

            String encoded = Base64.getEncoder().encodeToString(buf.array());
            HashMap<String, Object> map = new HashMap<>();
            map.put("data", encoded);
            map.put("type", AppRequest.PacketType.DEFAULT_APP_REQUEST);
            map.put("rid", requestId);
            return JSONObject.toJSONString(map);
        }
    }

    public static class ActiveReplicaHttpHandler extends SimpleChannelInboundHandler<FullHttpRequest> {

        private AbstractReplicaCoordinator appCoordinator;

        private ActiveReplica activeReplica;

        public ActiveReplicaHttpHandler(AbstractReplicaCoordinator appCoordinator, ActiveReplica activeReplica) {
            this.appCoordinator = appCoordinator;
            this.activeReplica = activeReplica;
        }

        @Override
        protected void channelRead0(ChannelHandlerContext ctx, FullHttpRequest request) throws Exception {
            if (isCORSPreflightRequest(request.method())) {
                handleCORSPreflightRequest(ctx, false);
                return;
            }


            String json = FullHttpRequestSerializer.toJsonString(request, (int) (Math.random() * Integer.MAX_VALUE));
            DispatcherApp.HttpReplicableRequest replicableRequest = (DispatcherApp.HttpReplicableRequest) appCoordinator.getRequest(json);

            InetSocketAddress sender = (InetSocketAddress) ctx.channel().remoteAddress();

            DispatcherApp.channelMap.put(replicableRequest.getRequestID(), ctx);
            appCoordinator.handleIncoming(replicableRequest, (_request, handled) -> {
                activeReplica.updateDemandStats(replicableRequest, sender.getAddress());
                DispatcherApp.channelMap.remove(replicableRequest.getRequestID());

//                HttpResponseStatus status = HttpResponseStatus.OK;
//                FullHttpResponse response = new DefaultFullHttpResponse(
//                        HTTP_1_1, status, Unpooled.copiedBuffer("Failure: " + status + "\r\n", CharsetUtil.UTF_8));
//                response.headers().set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");
//                this.sendAndCleanupConnection(ctx, request, response);
            });
        }

        private void sendAndCleanupConnection(ChannelHandlerContext ctx, FullHttpRequest request, FullHttpResponse response) {
            final boolean keepAlive = HttpUtil.isKeepAlive(request);
            HttpUtil.setContentLength(response, response.content().readableBytes());
            if (!keepAlive) {
                // We're going to close the connection as soon as the response is sent,
                // so we should also make it clear for the client.
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.CLOSE);
            } else if (request.protocolVersion().equals(HTTP_1_0)) {
                response.headers().set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }


            ChannelFuture flushPromise = ctx.writeAndFlush(response);

            if (!keepAlive) {
                // Close the connection as soon as the response is sent.
                flushPromise.addListener(ChannelFutureListener.CLOSE);
            }
        }


        public static void setHeaders(FullHttpResponse response, boolean keepAlive) {
            HttpHeaders headers = response.headers();

            // CORS headers, required to access resource in browser from different origin
            headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_ORIGIN, "*"); // Allowing all origins
            headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_METHODS, "GET, POST, OPTIONS"); // Can also add PUT,DELETE,OPTIONS
            headers.set(HttpHeaderNames.ACCESS_CONTROL_ALLOW_HEADERS, "Accept, Accept-CH, Accept-Charset, Accept-Datetime, Accept-Encoding, Accept-Ext, Accept-Features, Accept-Language, Accept-Params, Accept-Ranges, Access-Control-Allow-Credentials, Access-Control-Allow-Headers, Access-Control-Allow-Methods, Access-Control-Allow-Origin, Access-Control-Expose-Headers, Access-Control-Max-Age, Access-Control-Request-Headers, Access-Control-Request-Method, Age, Allow, Alternates, Authentication-Info, Authorization, C-Ext, C-Man, C-Opt, C-PEP, C-PEP-Info, CONNECT, Cache-Control, Compliance, Connection, Content-Base, Content-Disposition, Content-Encoding, Content-ID, Content-Language, Content-Length, Content-Location, Content-MD5, Content-Range, Content-Script-Type, Content-Security-Policy, Content-Style-Type, Content-Transfer-Encoding, Content-Type, Content-Version, Cookie, Cost, DAV, DELETE, DNT, DPR, Date, Default-Style, Delta-Base, Depth, Derived-From, Destination, Differential-ID, Digest, ETag, Expect, Expires, Ext, From, GET, GetProfile, HEAD, HTTP-date, Host, IM, If, If-Match, If-Modified-Since, If-None-Match, If-Range, If-Unmodified-Since, Keep-Alive, Label, Last-Event-ID, Last-Modified, Link, Location, Lock-Token, MIME-Version, Man, Max-Forwards, Media-Range, Message-ID, Meter, Negotiate, Non-Compliance, OPTION, OPTIONS, OWS, Opt, Optional, Ordering-Type, Origin, Overwrite, P3P, PEP, PICS-Label, POST, PUT, Pep-Info, Permanent, Position, Pragma, ProfileObject, Protocol, Protocol-Query, Protocol-Request, Proxy-Authenticate, Proxy-Authentication-Info, Proxy-Authorization, Proxy-Features, Proxy-Instruction, Public, RWS, Range, Referer, Refresh, Resolution-Hint, Resolver-Location, Retry-After, Safe, Sec-Websocket-Extensions, Sec-Websocket-Key, Sec-Websocket-Origin, Sec-Websocket-Protocol, Sec-Websocket-Version, Security-Scheme, Server, Set-Cookie, Set-Cookie2, SetProfile, SoapAction, Status, Status-URI, Strict-Transport-Security, SubOK, Subst, Surrogate-Capability, Surrogate-Control, TCN, TE, TRACE, Timeout, Title, Trailer, Transfer-Encoding, UA-Color, UA-Media, UA-Pixels, UA-Resolution, UA-Windowpixels, URI, Upgrade, User-Agent, Variant-Vary, Vary, Version, Via, Viewport-Width, WWW-Authenticate, Want-Digest, Warning, Width, X-Content-Duration, X-Content-Security-Policy, X-Content-Type-Options, X-CustomHeader, X-DNSPrefetch-Control, X-Forwarded-For, X-Forwarded-Port, X-Forwarded-Proto, X-Frame-Options, X-Modified, X-OTHER, X-PING, X-PINGOTHER, X-Powered-By, X-Requested-With, X-SERVICE-NAME");

            headers.set(HttpHeaderNames.CONTENT_TYPE, "text/plain; charset=UTF-8");

            if (keepAlive) {
                // Add 'Content-Length' header only for a keep-alive connection.
                headers.setInt(HttpHeaderNames.CONTENT_LENGTH, response.content().readableBytes());
                // Add keep alive header as per:
                // -
                // http://www.w3.org/Protocols/HTTP/1.1/draft-ietf-http-v11-spec-01.html#Connection
                headers.set(HttpHeaderNames.CONNECTION, HttpHeaderValues.KEEP_ALIVE);
            }
        }

        /**
         * This method checks if a request is a CORS preflight request by checking if the request method
         * is {@link HttpMethod#OPTIONS}, although that is not the sole property of a CORS preflight request.
         *
         * @param httpMethod {@link HttpMethod} of the request
         * @return True - request is a CORS preflight request, false otherwise
         */
        public static boolean isCORSPreflightRequest(HttpMethod httpMethod) {
            return httpMethod == HttpMethod.OPTIONS;
        }

        /**
         * Sends an empty response containing C
         * @param ctx {@link ChannelHandlerContext} for the request
         * @param keepAlive If the connection is keep alive (true) or not
         */
        public static void handleCORSPreflightRequest(ChannelHandlerContext ctx, boolean keepAlive) {
            FullHttpResponse httpResponse = new DefaultFullHttpResponse(HTTP_1_1, OK);
            setHeaders(httpResponse, keepAlive);

            ChannelFuture channelFuture = ctx.writeAndFlush(httpResponse);
            if (!keepAlive) {
                channelFuture.addListener(ChannelFutureListener.CLOSE);
            }
        }
    }
}
