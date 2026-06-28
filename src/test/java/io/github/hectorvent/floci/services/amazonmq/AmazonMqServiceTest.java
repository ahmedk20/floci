package io.github.hectorvent.floci.services.amazonmq;

import io.github.hectorvent.floci.config.EmulatorConfig;
import io.github.hectorvent.floci.core.common.AwsException;
import io.github.hectorvent.floci.core.common.RegionResolver;
import io.github.hectorvent.floci.core.storage.InMemoryStorage;
import io.github.hectorvent.floci.core.storage.StorageFactory;
import io.github.hectorvent.floci.services.amazonmq.container.RabbitMqManager;
import io.github.hectorvent.floci.services.amazonmq.model.Broker;
import io.github.hectorvent.floci.services.amazonmq.model.BrokerState;
import io.github.hectorvent.floci.services.amazonmq.model.MqUser;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.when;

class AmazonMqServiceTest {

    private AmazonMqService service;

    @BeforeEach
    void setUp() {
        StorageFactory storageFactory = Mockito.mock(StorageFactory.class);
        when(storageFactory.create(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenReturn(new InMemoryStorage<>());

        EmulatorConfig config = Mockito.mock(EmulatorConfig.class);
        var servicesConfig = Mockito.mock(EmulatorConfig.ServicesConfig.class);
        var mqConfig = Mockito.mock(EmulatorConfig.AmazonMqServiceConfig.class);
        when(config.services()).thenReturn(servicesConfig);
        when(servicesConfig.amazonmq()).thenReturn(mqConfig);
        when(mqConfig.mock()).thenReturn(true);
        when(config.defaultRegion()).thenReturn("us-east-1");

        RegionResolver regionResolver = new RegionResolver("us-east-1", "000000000000");
        RabbitMqManager rabbitMqManager = Mockito.mock(RabbitMqManager.class);
        service = new AmazonMqService(storageFactory, config, regionResolver, rabbitMqManager);
    }

    private CreateBrokerParams rabbitParams(String name) {
        return new CreateBrokerParams(name, "RABBITMQ", null, "SINGLE_INSTANCE",
                "mq.t3.micro", false, false, null, null);
    }

    @Test
    void createBrokerComesUpRunningWithEndpoints() {
        Broker broker = service.createBroker(rabbitParams("orders"));

        assertEquals("orders", broker.getBrokerName());
        assertEquals("RABBITMQ", broker.getEngineType());
        assertEquals(BrokerState.RUNNING, broker.getBrokerState());
        assertTrue(broker.getBrokerId().startsWith("b-"));
        assertTrue(broker.getBrokerArn().contains(":mq:"));
        assertTrue(broker.getBrokerArn().contains("orders"));
        assertFalse(broker.getBrokerInstances().isEmpty());
        assertFalse(broker.getBrokerInstances().get(0).getEndpoints().isEmpty());
    }

    @Test
    void createBrokerDefaultsEngineVersionWhenAbsent() {
        Broker broker = service.createBroker(rabbitParams("orders"));
        assertEquals("3.13", broker.getEngineVersion());
    }

    @Test
    void createBrokerRejectsNonRabbitEngine() {
        CreateBrokerParams activeMq = new CreateBrokerParams("legacy", "ACTIVEMQ", null,
                "SINGLE_INSTANCE", "mq.t3.micro", false, false, null, null);
        assertThrows(AwsException.class, () -> service.createBroker(activeMq));
    }

    @Test
    void createBrokerRejectsNonSingleInstanceDeployment() {
        CreateBrokerParams cluster = new CreateBrokerParams("ha", "RABBITMQ", null,
                "CLUSTER_MULTI_AZ", "mq.t3.micro", false, false, null, null);
        assertThrows(AwsException.class, () -> service.createBroker(cluster));
    }

    @Test
    void createBrokerRejectsDuplicateName() {
        service.createBroker(rabbitParams("orders"));
        assertThrows(AwsException.class, () -> service.createBroker(rabbitParams("orders")));
    }

    @Test
    void describeBrokerThrowsWhenMissing() {
        assertThrows(AwsException.class, () -> service.describeBroker("b-does-not-exist"));
    }

    @Test
    void deleteBrokerRemovesIt() {
        Broker broker = service.createBroker(rabbitParams("orders"));
        service.deleteBroker(broker.getBrokerId());
        assertTrue(service.listBrokers().isEmpty());
    }

    @Test
    void userRoundTrip() {
        Broker broker = service.createBroker(rabbitParams("orders"));
        String id = broker.getBrokerId();

        service.createUser(id, new MqUser("alice", "s3cret-pass", false, List.of("admin")));
        assertEquals(1, service.listUsers(id).size());
        assertEquals("alice", service.describeUser(id, "alice").getUsername());

        service.deleteUser(id, "alice");
        assertTrue(service.listUsers(id).isEmpty());
    }

    @Test
    void createDuplicateUserThrows() {
        Broker broker = service.createBroker(rabbitParams("orders"));
        String id = broker.getBrokerId();
        service.createUser(id, new MqUser("alice", "p1", false, null));
        assertThrows(AwsException.class,
                () -> service.createUser(id, new MqUser("alice", "p2", false, null)));
    }

    @Test
    void createBrokerMarksFailedWhenProvisioningThrows() {
        // Real (non-mock) mode with a container manager that fails to start.
        StorageFactory storageFactory = Mockito.mock(StorageFactory.class);
        when(storageFactory.create(Mockito.anyString(), Mockito.anyString(), Mockito.any()))
                .thenReturn(new InMemoryStorage<>());

        EmulatorConfig config = Mockito.mock(EmulatorConfig.class);
        var servicesConfig = Mockito.mock(EmulatorConfig.ServicesConfig.class);
        var mqConfig = Mockito.mock(EmulatorConfig.AmazonMqServiceConfig.class);
        when(config.services()).thenReturn(servicesConfig);
        when(servicesConfig.amazonmq()).thenReturn(mqConfig);
        when(mqConfig.mock()).thenReturn(false);
        when(config.defaultRegion()).thenReturn("us-east-1");

        RegionResolver regionResolver = new RegionResolver("us-east-1", "000000000000");
        RabbitMqManager failingManager = Mockito.mock(RabbitMqManager.class);
        Mockito.doThrow(new RuntimeException("docker unavailable"))
                .when(failingManager).startContainer(Mockito.any());
        AmazonMqService realModeService =
                new AmazonMqService(storageFactory, config, regionResolver, failingManager);

        assertThrows(AwsException.class,
                () -> realModeService.createBroker(rabbitParams("orders")));

        // The failed broker is persisted as CREATION_FAILED, not left dangling.
        List<Broker> brokers = realModeService.listBrokers();
        assertEquals(1, brokers.size());
        assertEquals(BrokerState.CREATION_FAILED, brokers.get(0).getBrokerState());
    }
}
