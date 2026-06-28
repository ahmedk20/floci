package io.github.hectorvent.floci.services.amazonmq;

import com.fasterxml.jackson.core.type.TypeReference;
import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsArnUtils;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.StorageBackend;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerInstance;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import io.github.hectorvent.floci.services.amazonmq.model.MqUser;
import jakarta.enterprise.context.ApplicationScoped;
import jakarta.inject.Inject;
import org.jboss.logging.Logger;

import java.security.SecureRandom;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;

@ApplicationScoped
public class AmazonMqService {

    private static final Logger LOG = Logger.getLogger(AmazonMqService.class);
    private static final String ENGINE_RABBITMQ = "RABBITMQ";
    private static final String DEFAULT_ENGINE_VERSION = "3.13";
    private static final String DEPLOYMENT_SINGLE_INSTANCE = "SINGLE_INSTANCE";

    private final StorageBackend<String, Broker> storage;
    private final EmulatorConfig config;
    private final RegionResolver regionResolver;

    @Inject
    public AmazonMqService(StorageFactory storageFactory, EmulatorConfig config,
                           RegionResolver regionResolver) {
        this.storage = storageFactory.create("amazonmq", "amazonmq-brokers.json",
                new TypeReference<Map<String, Broker>>() {});
        this.config = config;
        this.regionResolver = regionResolver;
    }

    public Broker createBroker(CreateBrokerParams params) {
        String name = params.brokerName();
        if (name == null || name.isBlank()) {
            throw new AwsException("BadRequestException", "BrokerName is required", 400);
        }
        if (!ENGINE_RABBITMQ.equals(params.engineType())) {
            throw new AwsException("BadRequestException",
                    "Only RABBITMQ EngineType is supported", 400);
        }
        String deploymentMode = params.deploymentMode() == null
                ? DEPLOYMENT_SINGLE_INSTANCE : params.deploymentMode();
        if (!DEPLOYMENT_SINGLE_INSTANCE.equals(deploymentMode)) {
            throw new AwsException("BadRequestException",
                    "Only SINGLE_INSTANCE DeploymentMode is supported", 400);
        }
        if (storage.scan(k -> true).stream().anyMatch(b -> name.equals(b.getBrokerName()))) {
            throw new AwsException("ConflictException", "Broker already exists: " + name, 409);
        }

        String brokerId = "b-" + UUID.randomUUID();
        String accountId = regionResolver.getAccountId();
        String brokerArn = AwsArnUtils.Arn.of("mq", config.defaultRegion(), accountId,
                "broker:" + name + ":" + brokerId).toString();
        String engineVersion = (params.engineVersion() == null || params.engineVersion().isBlank())
                ? DEFAULT_ENGINE_VERSION : params.engineVersion();

        Broker broker = new Broker(brokerId, brokerArn, name, ENGINE_RABBITMQ,
                engineVersion, deploymentMode, params.hostInstanceType());
        broker.setAccountId(accountId);
        broker.setVolumeId(String.format("%06x", new SecureRandom().nextInt(0xFFFFFF)));
        broker.setPubliclyAccessible(params.publiclyAccessible());
        broker.setAutoMinorVersionUpgrade(params.autoMinorVersionUpgrade());
        if (params.users() != null) {
            broker.setUsers(new ArrayList<>(params.users()));
        }
        if (params.tags() != null) {
            broker.setTags(new HashMap<>(params.tags()));
        }

        // commit 1: no backing container yet. Bring the broker up immediately
        // with synthetic local endpoints; commit 2 replaces this with a real
        // RabbitMQ container started behind a readiness poller.
        applyLocalEndpoints(broker);
        broker.setBrokerState(BrokerState.RUNNING);

        storage.put(brokerId, broker);
        LOG.infov("Created Amazon MQ broker {0} ({1})", name, brokerId);
        return broker;
    }

    public Broker describeBroker(String brokerId) {
        return storage.get(brokerId)
                .orElseThrow(() -> new AwsException("NotFoundException",
                        "Broker not found: " + brokerId, 404));
    }

    public List<Broker> listBrokers() {
        return storage.scan(k -> true);
    }

    public void deleteBroker(String brokerId) {
        Broker broker = describeBroker(brokerId);
        broker.setBrokerState(BrokerState.DELETION_IN_PROGRESS);
        storage.delete(brokerId);
        LOG.infov("Deleted Amazon MQ broker {0}", brokerId);
    }

    public Broker rebootBroker(String brokerId) {
        Broker broker = describeBroker(brokerId);
        // Synchronous in mock mode: a real reboot would cycle the container and
        // flip back to RUNNING via the readiness poller (commit 2).
        broker.setBrokerState(BrokerState.RUNNING);
        storage.put(brokerId, broker);
        return broker;
    }

    private void applyLocalEndpoints(Broker broker) {
        BrokerInstance instance = new BrokerInstance(
                "http://localhost:15672",
                List.of("amqp://localhost:5672"),
                "localhost");
        broker.setBrokerInstances(new ArrayList<>(List.of(instance)));
    }

    // --- Users (in-memory; projected into the real broker in commit 3) ---

    public MqUser createUser(String brokerId, MqUser user) {
        Broker broker = describeBroker(brokerId);
        if (broker.getUsers().stream().anyMatch(u -> u.getUsername().equals(user.getUsername()))) {
            throw new AwsException("ConflictException",
                    "User already exists: " + user.getUsername(), 409);
        }
        broker.getUsers().add(user);
        storage.put(brokerId, broker);
        return user;
    }

    public MqUser describeUser(String brokerId, String username) {
        Broker broker = describeBroker(brokerId);
        return broker.getUsers().stream()
                .filter(u -> u.getUsername().equals(username))
                .findFirst()
                .orElseThrow(() -> new AwsException("NotFoundException",
                        "User not found: " + username, 404));
    }

    public List<MqUser> listUsers(String brokerId) {
        return describeBroker(brokerId).getUsers();
    }

    public void deleteUser(String brokerId, String username) {
        Broker broker = describeBroker(brokerId);
        boolean removed = broker.getUsers().removeIf(u -> u.getUsername().equals(username));
        if (!removed) {
            throw new AwsException("NotFoundException", "User not found: " + username, 404);
        }
        storage.put(brokerId, broker);
    }
}
