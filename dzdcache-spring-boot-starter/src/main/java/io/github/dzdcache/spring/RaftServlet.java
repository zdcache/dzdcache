package io.github.dzdcache.spring;

import io.github.dzdcache.cluster.Protocol;
import io.github.dzdcache.cluster.RaftEndpoint;
import jakarta.servlet.AsyncContext;
import jakarta.servlet.http.HttpServlet;
import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import java.io.IOException;

final class RaftServlet extends HttpServlet {
    private final transient RaftEndpoint endpoint;
    private final long timeoutMillis;

    RaftServlet(RaftEndpoint endpoint, long timeoutMillis) { this.endpoint = endpoint; this.timeoutMillis = timeoutMillis; }

    @Override protected void service(HttpServletRequest request, HttpServletResponse response) throws IOException {
        if (request.getMethod().equals("OPTIONS")) { response.setStatus(204); return; }
        byte[] body = request.getMethod().equals("POST") ? request.getInputStream().readNBytes(endpoint.maxBytes() + 1) : new byte[0];
        if (body.length > endpoint.maxBytes()) body = null;
        AsyncContext async = request.startAsync();
        async.setTimeout(timeoutMillis);
        endpoint.handle(request.getMethod(), request.getHeader("Authorization"), request.getHeader(Protocol.HEADER),
                request.getHeader(io.github.dzdcache.cluster.ClusterCodec.HEADER), body).whenComplete((reply, error) -> {
            HttpServletResponse out = (HttpServletResponse) async.getResponse();
            try {
                if (error != null) { out.setStatus(503); return; }
                out.setStatus(reply.status());
                if (reply.contentType() != null) out.setContentType(reply.contentType());
                out.setContentLength(reply.body().length);
                out.getOutputStream().write(reply.body());
            } catch (IOException ignored) {
            } finally {
                async.complete();
            }
        });
    }
}
