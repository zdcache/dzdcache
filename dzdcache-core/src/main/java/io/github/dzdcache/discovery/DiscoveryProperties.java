package io.github.dzdcache.discovery;

import java.net.URI;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.UUID;

public class DiscoveryProperties {
    private String mode = "standalone";
    private String nodeId;
    private final String generatedId = UUID.randomUUID().toString();
    private URI selfUrl = URI.create("http://localhost:7070");
    private List<URI> staticPeers = List.of();
    private String namespace = "default";
    private String serviceName = "dzdcache-peers";
    private String dnsName;
    private String podUid = System.getenv().getOrDefault("POD_UID", "");
    private String podIp = System.getenv().getOrDefault("POD_IP", "");
    private String detectedIp;
    private String hostname = System.getenv().getOrDefault("HOSTNAME", "");
    private URI apiServer = URI.create("https://kubernetes.default.svc");
    private Path serviceAccountDirectory = Path.of("/var/run/secrets/kubernetes.io/serviceaccount");
    private Integer peerPort;
    private Duration retryDelay = Duration.ofSeconds(1);
    private String scheme = "http";
    public URI advertisedUrl() {
        String ip = getPodIp();
        return ip.isBlank() ? selfUrl : URI.create(scheme + "://" + (ip.contains(":") ? "[" + ip + "]" : ip) + ":" + getPeerPort());
    }
    public String getMode() { return mode; } public void setMode(String v) { mode = v; }
    public String getNodeId() {
        if (nodeId != null && !nodeId.isBlank()) return nodeId;
        if (mode.equals("static")) return selfUrl.getHost() + ":" + selfUrl.getPort();
        return hostname.isBlank() ? generatedId : hostname;
    }
    public String getHostname() { return hostname; } public void setHostname(String v) { hostname = v == null ? "" : v; }
    public void setNodeId(String v) { nodeId = v; }
    public URI getSelfUrl() { return selfUrl; } public void setSelfUrl(URI v) { selfUrl = v; }
    public List<URI> getStaticPeers() { return staticPeers; } public void setStaticPeers(List<URI> v) { staticPeers = List.copyOf(v); }
    public String getNamespace() { return namespace; } public void setNamespace(String v) { namespace = v; }
    public String getServiceName() { return serviceName; } public void setServiceName(String v) { serviceName = v; }
    public String getPodUid() { return podUid; } public void setPodUid(String v) { podUid = v; }
    public String getPodIp() {
        if (!podIp.isBlank() || !(mode.equals("dns") || mode.equals("kubernetes"))) return podIp;
        if (detectedIp == null) detectedIp = HostAddress.detect();
        return detectedIp;
    }
    public void setPodIp(String v) { podIp = v == null ? "" : v; }
    public URI getApiServer() { return apiServer; } public void setApiServer(URI v) { apiServer = v; }
    public Path getServiceAccountDirectory() { return serviceAccountDirectory; } public void setServiceAccountDirectory(Path v) { serviceAccountDirectory = v; }
    public int getPeerPort() { return peerPort == null ? 7070 : peerPort; } public void setPeerPort(int v) { peerPort = v; }
    public boolean hasPeerPort() { return peerPort != null; }
    public String getDnsName() { return dnsName == null || dnsName.isBlank() ? serviceName + "." + namespace + ".svc.cluster.local" : dnsName; }
    public void setDnsName(String v) { dnsName = v; }
    public void requireScheme(URI uri, String setting) {
        if (!scheme.equals(uri.getScheme()))
            throw new IllegalArgumentException(setting + " " + uri + " must use " + scheme + ":// because dzdcache.cluster-tls.enabled is "
                + scheme.equals("https"));
    }
    public String getScheme() { return scheme; }
    public void setScheme(String v) {
        if (!"http".equals(v) && !"https".equals(v)) throw new IllegalArgumentException("Discovery scheme must be http or https");
        scheme = v;
    }
    public Duration getRetryDelay() { return retryDelay; } public void setRetryDelay(Duration v) { retryDelay = v; }
}
