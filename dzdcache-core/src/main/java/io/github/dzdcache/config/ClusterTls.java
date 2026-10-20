package io.github.dzdcache.config;

public class ClusterTls {
    private boolean enabled;
    private String bundle = "";
    private String clientAuth = "none";
    private boolean verifyHostname = true;
    public void validate() {
        if (!"none".equals(clientAuth) && !"need".equals(clientAuth))
            throw new IllegalArgumentException("dzdcache.cluster-tls.client-auth must be none or need");
        if (enabled && (bundle == null || bundle.isBlank()))
            throw new IllegalArgumentException("dzdcache.cluster-tls.bundle must name a spring.ssl.bundle when dzdcache.cluster-tls.enabled is true");
    }
    public String scheme() { return enabled ? "https" : "http"; }
    public boolean needClientAuth() { return enabled && "need".equals(clientAuth); }
    public boolean isEnabled() { return enabled; } public void setEnabled(boolean v) { enabled = v; }
    public String getBundle() { return bundle; } public void setBundle(String v) { bundle = v; }
    public String getClientAuth() { return clientAuth; } public void setClientAuth(String v) { clientAuth = v; }
    public boolean isVerifyHostname() { return verifyHostname; } public void setVerifyHostname(boolean v) { verifyHostname = v; }
}
