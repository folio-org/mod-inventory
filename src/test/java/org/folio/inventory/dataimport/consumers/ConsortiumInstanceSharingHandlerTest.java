package org.folio.inventory.dataimport.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.consumers.ConsortiumInstanceSharingHandler;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.KafkaConfig;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.folio.inventory.consortium.entities.SharingStatus.IN_PROGRESS;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class ConsortiumInstanceSharingHandlerTest {

  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static Vertx vertx;
  @Mock
  private Storage storage;
  @Mock
  private InstanceCollection mockedTargetInstanceCollection;
  @Mock
  private InstanceCollection mockedSourceInstanceCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private static KafkaConfig kafkaConfig;
  private Instance existingInstance;
  private ConsortiumInstanceSharingHandler consortiumInstanceSharingHandler;

  @BeforeClass
  public static void setUpClass() {
    vertx = Vertx.vertx();
    kafkaConfig = KafkaConfig.builder()
      .envId("env")
      .maxRequestSize(1048576)
      .build();
  }

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
  }

  @Test
  public void shouldShareInstanceWithFOLIOSource(TestContext context) throws IOException {

    // given
    Async async = context.async();
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonInstance.put("source", "FOLIO");
    existingInstance = Instance.fromJson(jsonInstance);

    String shareId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";
    String instanceId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";

    SharingInstance sharingInstance = new SharingInstance()
      .withId(UUID.fromString(shareId))
      .withInstanceIdentifier(UUID.fromString(instanceId))
      .withSourceTenantId("consortium")
      .withTargetTenantId("university")
      .withStatus(IN_PROGRESS);

    when(kafkaRecord.key()).thenReturn(shareId);
    when(kafkaRecord.value()).thenReturn(Json.encode(sharingInstance));
    when(kafkaRecord.headers()).thenReturn(
      List.of(KafkaHeader.header(OKAPI_TOKEN_HEADER, "token"),
        KafkaHeader.header(OKAPI_URL_HEADER, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedTargetInstanceCollection)
      .thenReturn(mockedSourceInstanceCollection)
      .thenReturn(mockedTargetInstanceCollection)
      .thenReturn(mockedSourceInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedSourceInstanceCollection).findById(eq(instanceId), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedTargetInstanceCollection).add(any(Instance.class), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedSourceInstanceCollection).update(any(Instance.class), any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertx, storage, kafkaConfig);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });
  }

  @Test
  public void shouldNotShareInstanceWhenInstanceExistsOnTargetTenant(TestContext context) throws IOException {

    // given
    Async async = context.async();
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonInstance.put("source", "FOLIO");
    existingInstance = Instance.fromJson(jsonInstance);

    String shareId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";
    String instanceId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";

    SharingInstance sharingInstance = new SharingInstance()
      .withId(UUID.fromString(shareId))
      .withInstanceIdentifier(UUID.fromString(instanceId))
      .withSourceTenantId("consortium")
      .withTargetTenantId("university")
      .withStatus(IN_PROGRESS);

    when(kafkaRecord.key()).thenReturn(shareId);
    when(kafkaRecord.value()).thenReturn(Json.encode(sharingInstance));
    when(kafkaRecord.headers()).thenReturn(
      List.of(KafkaHeader.header(OKAPI_TOKEN_HEADER, "token"),
        KafkaHeader.header(OKAPI_URL_HEADER, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedTargetInstanceCollection)
      .thenReturn(mockedSourceInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedSourceInstanceCollection).findById(eq(instanceId), any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertx, storage, kafkaConfig);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage()
        .contains("Instance with InstanceId=" + instanceId + " is present on target tenant: university"));
      async.complete();
    });
  }

  @Test
  public void shouldNotShareInstanceWhenInstanceNotExistsOnSourceTenant(TestContext context) throws IOException {

    // given
    Async async = context.async();
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonInstance.put("source", "FOLIO");
    existingInstance = Instance.fromJson(jsonInstance);

    String shareId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";
    String instanceId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";

    SharingInstance sharingInstance = new SharingInstance()
      .withId(UUID.fromString(shareId))
      .withInstanceIdentifier(UUID.fromString(instanceId))
      .withSourceTenantId("consortium")
      .withTargetTenantId("university")
      .withStatus(IN_PROGRESS);

    when(kafkaRecord.key()).thenReturn(shareId);
    when(kafkaRecord.value()).thenReturn(Json.encode(sharingInstance));
    when(kafkaRecord.headers()).thenReturn(
      List.of(KafkaHeader.header(OKAPI_TOKEN_HEADER, "token"),
        KafkaHeader.header(OKAPI_URL_HEADER, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedTargetInstanceCollection)
      .thenReturn(mockedSourceInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(mockedSourceInstanceCollection).findById(eq(instanceId), any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertx, storage, kafkaConfig);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage()
        .contains("Error retrieving Instance by InstanceId=" + instanceId
          + " from source tenant consortium. Error: org.folio.inventory.exceptions.NotFoundException: Can't find Instance by InstanceId=" + instanceId + " on tenant: consortium"));
      async.complete();
    });
  }

  @Test
  public void shouldNotShareInstanceWithMARCSource(TestContext context) throws IOException {

    // given
    Async async = context.async();
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    String shareId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";
    String instanceId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";

    SharingInstance sharingInstance = new SharingInstance()
      .withId(UUID.fromString(shareId))
      .withInstanceIdentifier(UUID.fromString(instanceId))
      .withSourceTenantId("consortium")
      .withTargetTenantId("university")
      .withStatus(IN_PROGRESS);

    when(kafkaRecord.key()).thenReturn(shareId);
    when(kafkaRecord.value()).thenReturn(Json.encode(sharingInstance));
    when(kafkaRecord.headers()).thenReturn(
      List.of(KafkaHeader.header(OKAPI_TOKEN_HEADER, "token"),
        KafkaHeader.header(OKAPI_URL_HEADER, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedTargetInstanceCollection).thenReturn(mockedSourceInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedSourceInstanceCollection).findById(eq(instanceId), any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertx, storage, kafkaConfig);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage()
        .contains("Error sharing Instance with InstanceId=" + instanceId +
          " to the target tenant university. Because source is MARC"));
      async.complete();
    });
  }

  @Test
  public void shouldNotShareInstanceWithNotFOLIOAndMARCSource(TestContext context) throws IOException {

    // given
    Async async = context.async();
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonInstance.put("source", "SOURCE");
    existingInstance = Instance.fromJson(jsonInstance);

    String shareId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";
    String instanceId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";

    SharingInstance sharingInstance = new SharingInstance()
      .withId(UUID.fromString(shareId))
      .withInstanceIdentifier(UUID.fromString(instanceId))
      .withSourceTenantId("consortium")
      .withTargetTenantId("university")
      .withStatus(IN_PROGRESS);

    when(kafkaRecord.key()).thenReturn(shareId);
    when(kafkaRecord.value()).thenReturn(Json.encode(sharingInstance));
    when(kafkaRecord.headers()).thenReturn(
      List.of(KafkaHeader.header(OKAPI_TOKEN_HEADER, "token"),
        KafkaHeader.header(OKAPI_URL_HEADER, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedTargetInstanceCollection).thenReturn(mockedSourceInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedSourceInstanceCollection).findById(eq(instanceId), any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertx, storage, kafkaConfig);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage()
        .contains("Error sharing Instance with InstanceId=" + instanceId
          + " to the target tenant university. Because source is SOURCE"));
      async.complete();
    });
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close(ar -> async.complete());
  }

}
