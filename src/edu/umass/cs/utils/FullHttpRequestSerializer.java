package edu.umass.cs.utils;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.handler.codec.http.*;
import io.netty.util.CharsetUtil;

import java.util.Base64;
import java.util.List;
import java.util.Map;

public class FullHttpRequestSerializer {

    public static FullHttpRequest decode(String encoded) {
        byte[] bytes = Base64.getDecoder().decode(encoded);
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

    public static String encode(FullHttpRequest request) {
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

        return Base64.getEncoder().encodeToString(buf.array());
    }
}
