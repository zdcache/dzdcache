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
    private String podUid = System.getenv().getOrDefault("POD_UID", "");
    private String podIp = System.getenv().getOrDefault("POD_IP", "");
    private URI apiServer = URI.create("https://kubernetes.default.svc");
    private Path serviceAccountDirectory = Path.of("/var/run/secrets/kubernetes.io/serviceaccount");
    private int peerPort = 7070;
    private Duration retryDelay = Duration.ofSeconds(1);
    public URI advertisedUrl() { return podIp.isBlank() ? selfUrl : URI.create("http://" + (podIp.contains(":") ? "[" + podIp + "]" : podIp) + ":" + peerPort); }
    public String getMode() { return mode; } public void setMode(String v) { mode = v; }
    public String getNodeId() {
        if (nodeId != null && !nodeId.isBlank()) return nodeId;
        return mode.equals("static") ? selfUrl.getHost() + ":" + selfUrl.getPort() : generatedId;
    }
    public void setNodeId(String v) { nodeId = v; }
    public URI getSelfUrl() { return selfUrl; } public void setSelfUrl(URI v) { selfUrl = v; }
    public List<URI> getStaticPeers() { return staticPeers; } public void setStaticPeers(List<URI> v) { staticPeers = List.copyOf(v); }
    public String getNamespace() { return namespace; } public void setNamespace(String v) { namespace = v; }
    public String getServiceName() { return serviceName; } public void setServiceName(String v) { serviceName = v; }
    public String getPodUid() { return podUid; } public void setPodUid(String v) { podUid = v; }
    public String getPodIp() { return podIp; } public void setPodIp(String v) { podIp = v; }
    public URI getApiServer() { return apiServer; } public void setApiServer(URI v) { apiServer = v; }
    public Path getServiceAccountDirectory() { return serviceAccountDirectory; } public void setServiceAccountDirectory(Path v) { serviceAccountDirectory = v; }
    public int getPeerPort() { return peerPort; } public void setPeerPort(int v) { peerPort = v; }
    public Duration getRetryDelay() { return retryDelay; } public void setRetryDelay(Duration v) { retryDelay = v; }
}
