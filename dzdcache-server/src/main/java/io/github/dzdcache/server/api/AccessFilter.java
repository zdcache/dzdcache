package io.github.dzdcache.server.api;

import io.github.dzdcache.config.ServiceProperties;
import jakarta.servlet.*;
import jakarta.servlet.http.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import org.springframework.stereotype.Component;
import org.springframework.web.filter.OncePerRequestFilter;

@Component
public class AccessFilter extends OncePerRequestFilter {
    private final ServiceProperties properties;
    public AccessFilter(ServiceProperties properties) { this.properties = properties; }
    @Override protected void doFilterInternal(HttpServletRequest request, HttpServletResponse response, FilterChain chain)
            throws ServletException, IOException {
        // MVC ignores matrix parameters for route matching; protect the same normalized paths.
        String path = request.getServletPath().replaceAll(";[^/]*", "");
        if (path.startsWith("/cache/")) {
            String token = properties.getApiToken();
            if (!token.isBlank() && !matches(request.getHeader("Authorization"), token)) {
                response.sendError(401, "A valid bearer token is required"); return;
            }
            if (!request.getMethod().equals("GET") && !request.getMethod().equals("HEAD")) {
                int limit = properties.getMaxRequestBytes();
                byte[] body = request.getInputStream().readNBytes(limit + 1);
                if (body.length > limit) { response.sendError(413, "Request body exceeds limit"); return; }
                chain.doFilter(new BufferedRequest(request, body), response); return;
            }
        }
        chain.doFilter(request, response);
    }
    private static boolean matches(String header, String token) {
        return header != null && MessageDigest.isEqual(header.getBytes(StandardCharsets.UTF_8),
            ("Bearer " + token).getBytes(StandardCharsets.UTF_8));
    }
    private static final class BufferedRequest extends HttpServletRequestWrapper {
        private final byte[] bytes;
        BufferedRequest(HttpServletRequest request, byte[] bytes) { super(request); this.bytes = bytes; }
        @Override public ServletInputStream getInputStream() {
            var input = new ByteArrayInputStream(bytes);
            return new ServletInputStream() {
                @Override public int read() { return input.read(); }
                @Override public int read(byte[] b, int offset, int length) { return input.read(b, offset, length); }
                @Override public boolean isFinished() { return input.available() == 0; }
                @Override public boolean isReady() { return true; }
                @Override public void setReadListener(ReadListener listener) { throw new UnsupportedOperationException(); }
            };
        }
        @Override public BufferedReader getReader() { return new BufferedReader(new InputStreamReader(getInputStream(), StandardCharsets.UTF_8)); }
    }
}
