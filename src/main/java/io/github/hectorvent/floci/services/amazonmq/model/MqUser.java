package io.github.hectorvent.floci.services.amazonmq.model;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.quarkus.runtime.annotations.RegisterForReflection;

import java.util.List;

/**
 * A broker user. Amazon MQ never returns the password in any response
 * (DescribeUser / ListUsers omit it), so the password is stored for later
 * projection into the real broker but is never serialized back to the client.
 */
@RegisterForReflection
public class MqUser {

    @JsonProperty("username")
    private String username;

    // Stored so the broker's admin user can be seeded into the RabbitMQ container
    // (via RABBITMQ_DEFAULT_USER/PASS); never serialized in a response.
    @JsonIgnore
    private String password;

    @JsonProperty("consoleAccess")
    private boolean consoleAccess;

    @JsonProperty("groups")
    private List<String> groups;

    public MqUser() {}

    public MqUser(String username, String password, boolean consoleAccess, List<String> groups) {
        this.username = username;
        this.password = password;
        this.consoleAccess = consoleAccess;
        this.groups = groups;
    }

    public String getUsername() { return username; }
    public void setUsername(String username) { this.username = username; }

    @JsonIgnore
    public String getPassword() { return password; }
    public void setPassword(String password) { this.password = password; }

    public boolean isConsoleAccess() { return consoleAccess; }
    public void setConsoleAccess(boolean consoleAccess) { this.consoleAccess = consoleAccess; }

    public List<String> getGroups() { return groups; }
    public void setGroups(List<String> groups) { this.groups = groups; }
}
