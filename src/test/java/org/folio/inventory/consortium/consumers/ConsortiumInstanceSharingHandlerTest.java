package org.folio.inventory.consortium.consumers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.inventory.KafkaTest;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.handlers.InstanceSharingHandler;
import org.folio.inventory.consortium.handlers.InstanceSharingHandlerFactory;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;

import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TENANT_HEADER;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.times;
import org.folio.inventory.services.EventIdStorageService;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.inventory.storage.Storage;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.Mockito;
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
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(VertxUnitRunner.class)
public class ConsortiumInstanceSharingHandlerTest extends KafkaTest {
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static HttpClient httpClient;

  @Mock
  private Storage storage;
  @Mock
  private InstanceCollection mockedTargetInstanceCollection;
  @Mock
  private InstanceCollection mockedSourceInstanceCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private EventIdStorageService eventIdStorageService;
  private Instance existingInstance;
  private ConsortiumInstanceSharingHandler consortiumInstanceSharingHandler;
  private MockedStatic<InstanceSharingHandlerFactory> mockedInstanceSharingHandler;

  @BeforeClass
  public static void setUpClass() {
    httpClient = vertxAssistant.getVertx().createHttpClient();
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
    jsonInstance.put("subjects", new JsonArray().add(new JsonObject().put("authorityId", "null").put("value", "\\\"Test subject\\\"")));
    existingInstance = Instance.fromJson(jsonInstance);

    String targetInstanceHrid = "consin0000000000123";

    JsonObject jsonTargetInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonTargetInstance.put("hrid", targetInstanceHrid);
    Instance targetInstance = Instance.fromJson(jsonTargetInstance);

    String shareId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";
    String instanceId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";

    SharingInstance sharingInstance = new SharingInstance()
      .withId(UUID.fromString(shareId))
      .withInstanceIdentifier(UUID.fromString(instanceId))
      .withSourceTenantId("university")
      .withTargetTenantId("consortium")
      .withStatus(IN_PROGRESS);

    when(kafkaRecord.key()).thenReturn(shareId);
    when(kafkaRecord.value()).thenReturn(Json.encode(sharingInstance));
    when(kafkaRecord.headers()).thenReturn(
      List.of(KafkaHeader.header(OKAPI_TOKEN_HEADER, "token"),
        KafkaHeader.header(OKAPI_URL_HEADER, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedSourceInstanceCollection)
      .thenReturn(mockedTargetInstanceCollection);

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
      successHandler.accept(new Success<>(targetInstance));
      return null;
    }).when(mockedTargetInstanceCollection).add(any(Instance.class), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedSourceInstanceCollection).update(any(Instance.class), any(), any());

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService).store(any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig, eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertTrue(ar.result()
        .contains("Instance with InstanceId=" + instanceId + " has been shared to the target tenant consortium"));

      ArgumentCaptor<Instance> updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(mockedSourceInstanceCollection, times(1)).update(updatedInstanceCaptor.capture(), any(), any());
      verify(mockedTargetInstanceCollection, times(1)).add(argThat(instance -> !instance.getSubjects().isEmpty()), any(), any());
      Instance updatedInstance = updatedInstanceCaptor.getValue();
      context.assertEquals("CONSORTIUM-FOLIO", updatedInstance.getSource());
      context.assertEquals(targetInstanceHrid, updatedInstance.getHrid());
      context.assertFalse(updatedInstance.getSubjects().isEmpty());

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
      .withSourceTenantId("university")
      .withTargetTenantId("consortium")
      .withStatus(IN_PROGRESS);

    when(kafkaRecord.key()).thenReturn(shareId);
    when(kafkaRecord.value()).thenReturn(Json.encode(sharingInstance));
    when(kafkaRecord.headers()).thenReturn(
      List.of(KafkaHeader.header(OKAPI_TOKEN_HEADER, "token"),
        KafkaHeader.header(OKAPI_URL_HEADER, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedSourceInstanceCollection)
      .thenReturn(mockedTargetInstanceCollection);

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

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService).store(any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig, eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage()
        .contains("Error sharing Instance with InstanceId=" + instanceId
          + " to the target tenant consortium. Error: Unsupported source type: SOURCE"));
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
      .thenReturn(mockedTargetInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService).store(any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig, eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.result().equals("Instance with InstanceId=" + instanceId +
        " is present on target tenant: university"));
      async.complete();
    });
  }

  @Test
  public void shouldNotShareInstanceWhenTargetReturns500Error(TestContext context) throws IOException {

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
        KafkaHeader.header(OKAPI_URL_HEADER, "url"),
        KafkaHeader.header(OKAPI_TENANT_HEADER, "consortium")
      ));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedTargetInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal server error.", 500));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService).store(any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig, eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().equals("Internal server error."));
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
      .withSourceTenantId("university")
      .withTargetTenantId("consortium")
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

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService).store(any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig, eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage()
        .contains("Error sharing Instance with InstanceId=" + instanceId + " to the target tenant consortium. " +
          "Because the instance is not found on the source tenant university"));
      async.complete();
    });
  }

  @Test
  public void shouldShareInstanceWithMARCSource(TestContext context) throws IOException {

    // given
    Async async = context.async();
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonInstance.put("source", "MARC");
    existingInstance = Instance.fromJson(jsonInstance);

    String shareId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";
    String instanceId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";

    SharingInstance sharingInstance = new SharingInstance()
      .withId(UUID.fromString(shareId))
      .withInstanceIdentifier(UUID.fromString(instanceId))
      .withSourceTenantId("university")
      .withTargetTenantId("consortium")
      .withStatus(IN_PROGRESS);

    when(kafkaRecord.key()).thenReturn(shareId);
    when(kafkaRecord.value()).thenReturn(Json.encode(sharingInstance));
    when(kafkaRecord.headers()).thenReturn(
      List.of(KafkaHeader.header(OKAPI_TOKEN_HEADER, "token"),
        KafkaHeader.header(OKAPI_URL_HEADER, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedSourceInstanceCollection)
      .thenReturn(mockedTargetInstanceCollection);

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

    mockedInstanceSharingHandler = Mockito.mockStatic(InstanceSharingHandlerFactory.class);

    InstanceSharingHandler sharingHandler = mock(InstanceSharingHandler.class);

    mockedInstanceSharingHandler.when(InstanceSharingHandlerFactory :: values)
      .thenReturn(new InstanceSharingHandlerFactory[]{InstanceSharingHandlerFactory.MARC});

    mockedInstanceSharingHandler.when(() ->
        InstanceSharingHandlerFactory.getInstanceSharingHandler(eq(InstanceSharingHandlerFactory.MARC),
          any(InstanceOperationsHelper.class), any(Storage.class), any(Vertx.class), any(HttpClient.class)))
      .thenReturn(sharingHandler);

    when(sharingHandler.publishInstance(any(), any(), any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService).store(any(), any());

    //when
    consortiumInstanceSharingHandler = spy(new ConsortiumInstanceSharingHandler(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig, eventIdStorageService));

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertFalse(ar.failed());
      context.assertTrue(ar.result()
        .contains("Instance with InstanceId=" + instanceId + " has been shared to the target tenant consortium"));
      async.complete();
    });
  }

  @Test
  public void shouldNotShareInstanceWithMARCSourceBecauseDIFailed(TestContext context) throws IOException {

    // given
    Async async = context.async();
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonInstance.put("source", "MARC");
    existingInstance = Instance.fromJson(jsonInstance);

    String shareId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";
    String instanceId = "8673c2b0-dfe6-447b-bb6e-a1d7eb2e3572";

    SharingInstance sharingInstance = new SharingInstance()
      .withId(UUID.fromString(shareId))
      .withInstanceIdentifier(UUID.fromString(instanceId))
      .withSourceTenantId("university")
      .withTargetTenantId("consortium")
      .withStatus(IN_PROGRESS);

    when(kafkaRecord.key()).thenReturn(shareId);
    when(kafkaRecord.value()).thenReturn(Json.encode(sharingInstance));
    when(kafkaRecord.headers()).thenReturn(
      List.of(KafkaHeader.header(OKAPI_TOKEN_HEADER, "token"),
        KafkaHeader.header(OKAPI_URL_HEADER, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedSourceInstanceCollection)
      .thenReturn(mockedTargetInstanceCollection);

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

    mockedInstanceSharingHandler = Mockito.mockStatic(InstanceSharingHandlerFactory.class);

    InstanceSharingHandler sharingHandler = mock(InstanceSharingHandler.class);

    mockedInstanceSharingHandler.when(InstanceSharingHandlerFactory :: values)
      .thenReturn(new InstanceSharingHandlerFactory[]{InstanceSharingHandlerFactory.MARC});

    mockedInstanceSharingHandler.when(() ->
        InstanceSharingHandlerFactory.getInstanceSharingHandler(eq(InstanceSharingHandlerFactory.MARC),
          any(InstanceOperationsHelper.class), any(Storage.class), any(Vertx.class), any(HttpClient.class)))
      .thenReturn(sharingHandler);

    when(sharingHandler.publishInstance(any(), any(), any(), any(), any()))
      .thenReturn(Future.failedFuture("ERROR"));

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService).store(any(), any());

    //when
    consortiumInstanceSharingHandler = spy(new ConsortiumInstanceSharingHandler(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig, eventIdStorageService));

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage()
        .contains("Sharing instance with InstanceId=" + instanceId + " to the target tenant consortium. Error: ERROR"));
      async.complete();
    });
  }

  @Test
  public void shouldNotProcessIfDuplicatedEventReceived(TestContext context) throws IOException {

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

    when(eventIdStorageService.store(any(), any())).thenReturn(Future.failedFuture(new DuplicateEventException("SQL Unique constraint violation prevented repeatedly saving the record")));

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig, eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      async.complete();
    });

    Mockito.verify(mockedSourceInstanceCollection, times(0)).add(any(), any(), any());
    Mockito.verify(storage, times(0)).getInstanceCollection(any());
  }

  @After
  public void tearDown() {
    if (mockedInstanceSharingHandler != null)
      mockedInstanceSharingHandler.close();
  }

  @AfterClass
  public static void tearDownClass() {
    httpClient.close();
  }
}
