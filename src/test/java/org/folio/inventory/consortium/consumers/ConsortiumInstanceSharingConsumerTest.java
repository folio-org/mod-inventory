package org.folio.inventory.consortium.consumers;

import static org.folio.inventory.consortium.entities.SharingStatus.IN_PROGRESS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.handlers.InstanceSharingHandler;
import org.folio.inventory.consortium.handlers.InstanceSharingHandlerFactory;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.services.EventIdStorageService;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.exception.DuplicateEventException;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.MockedStatic;
import org.mockito.junit.jupiter.MockitoExtension;
import support.KafkaTest;
import support.TestUtil;

@ExtendWith({VertxExtension.class, MockitoExtension.class})
class ConsortiumInstanceSharingConsumerTest extends KafkaTest {
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
  private ConsortiumInstanceSharingConsumer consortiumInstanceSharingConsumer;
  private MockedStatic<InstanceSharingHandlerFactory> mockedInstanceSharingHandler;

  @BeforeAll
  static void setUpClass() {
    httpClient = vertxAssistant.getVertx().createHttpClient();
  }

  @AfterAll
  static void tearDownClass() {
    httpClient.close();
  }

  @AfterEach
  void tearDown() {
    if (mockedInstanceSharingHandler != null) {
      mockedInstanceSharingHandler.close();
    }
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldShareInstanceWithFolioSource(VertxTestContext testContext) {
    // given
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonInstance.put("source", "FOLIO");
    jsonInstance.put("subjects",
      new JsonArray().add(new JsonObject().put("authorityId", "null").put("value", "\\\"Test subject\\\"")));
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
      List.of(KafkaHeader.header(XOkapiHeaders.TOKEN, "token"),
        KafkaHeader.header(XOkapiHeaders.URL, "url")));

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

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService)
      .store(any(), any());

    // when
    consortiumInstanceSharingConsumer =
      new ConsortiumInstanceSharingConsumer(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig,
        eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingConsumer.handle(kafkaRecord);
    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(instanceId, result);

      ArgumentCaptor<Instance> updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(mockedSourceInstanceCollection, times(1)).update(updatedInstanceCaptor.capture(), any(), any());
      verify(mockedTargetInstanceCollection, times(1)).add(argThat(instance -> !instance.getSubjects().isEmpty()),
        any(), any());
      Instance updatedInstance = updatedInstanceCaptor.getValue();
      assertEquals("CONSORTIUM-FOLIO", updatedInstance.getSource());
      assertEquals(targetInstanceHrid, updatedInstance.getHrid());
      assertFalse(updatedInstance.getSubjects().isEmpty());

      testContext.completeNow();
    })));
  }

  @Test
  void shouldNotShareInstanceWithNotFolioAndMarcSource(VertxTestContext testContext) {
    // given
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
      List.of(KafkaHeader.header(XOkapiHeaders.TOKEN, "token"),
        KafkaHeader.header(XOkapiHeaders.URL, "url")));

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

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService)
      .store(any(), any());

    // when
    consortiumInstanceSharingConsumer =
      new ConsortiumInstanceSharingConsumer(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig,
        eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingConsumer.handle(kafkaRecord);
    future.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertTrue(err.getMessage()
        .contains("Error sharing Instance with InstanceId=" + instanceId
                  + " to the target tenant consortium. Error: Unsupported source type: SOURCE"));
      testContext.completeNow();
    })));
  }

  @Test
  void shouldNotShareInstanceWhenInstanceExistsOnTargetTenant(VertxTestContext testContext) {

    // given
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
      List.of(KafkaHeader.header(XOkapiHeaders.TOKEN, "token"),
        KafkaHeader.header(XOkapiHeaders.URL, "url")));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedTargetInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService)
      .store(any(), any());

    // when
    consortiumInstanceSharingConsumer =
      new ConsortiumInstanceSharingConsumer(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig,
        eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingConsumer.handle(kafkaRecord);
    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(instanceId, result);
      testContext.completeNow();
    })));
  }

  @Test
  void shouldNotShareInstanceWhenTargetReturns500Error(VertxTestContext testContext) {

    // given
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
      List.of(KafkaHeader.header(XOkapiHeaders.TOKEN, "token"),
        KafkaHeader.header(XOkapiHeaders.URL, "url"),
        KafkaHeader.header(XOkapiHeaders.TENANT, "consortium")
      ));

    when(storage.getInstanceCollection(any(Context.class)))
      .thenReturn(mockedTargetInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Internal server error.", 500));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService)
      .store(any(), any());

    // when
    consortiumInstanceSharingConsumer =
      new ConsortiumInstanceSharingConsumer(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig,
        eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingConsumer.handle(kafkaRecord);
    future.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertEquals("Internal server error.", err.getMessage());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldNotShareInstanceWhenInstanceNotExistsOnSourceTenant(VertxTestContext testContext) {

    // given
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
      List.of(KafkaHeader.header(XOkapiHeaders.TOKEN, "token"),
        KafkaHeader.header(XOkapiHeaders.URL, "url")));

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

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService)
      .store(any(), any());

    // when
    consortiumInstanceSharingConsumer =
      new ConsortiumInstanceSharingConsumer(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig,
        eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingConsumer.handle(kafkaRecord);
    future.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertTrue(err.getMessage()
        .contains("Error sharing Instance with InstanceId=" + instanceId + " to the target tenant consortium. "
                  + "Because the instance is not found on the source tenant university"));
      testContext.completeNow();
    })));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldShareInstanceWithMarcSource(VertxTestContext testContext) {
    // given
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
      List.of(KafkaHeader.header(XOkapiHeaders.TOKEN, "token"),
        KafkaHeader.header(XOkapiHeaders.URL, "url")));

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

    mockedInstanceSharingHandler = mockStatic(InstanceSharingHandlerFactory.class);

    InstanceSharingHandler sharingHandler = mock(InstanceSharingHandler.class);

    mockedInstanceSharingHandler.when(InstanceSharingHandlerFactory::values)
      .thenReturn(new InstanceSharingHandlerFactory[] {InstanceSharingHandlerFactory.MARC});

    mockedInstanceSharingHandler.when(() ->
        InstanceSharingHandlerFactory.getInstanceSharingHandler(eq(InstanceSharingHandlerFactory.MARC),
          any(InstanceOperationsHelper.class), any(Storage.class), any(Vertx.class), any(HttpClient.class)))
      .thenReturn(sharingHandler);

    when(sharingHandler.publishInstance(any(), any(), any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString())).when(eventIdStorageService)
      .store(any(), any());

    //when
    consortiumInstanceSharingConsumer = spy(
      new ConsortiumInstanceSharingConsumer(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig,
        eventIdStorageService));

    //then
    Future<String> future = consortiumInstanceSharingConsumer.handle(kafkaRecord);
    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(instanceId, result);
      testContext.completeNow();
    })));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldNotShareInstanceWithMarcSourceBecauseDiFailed(VertxTestContext testContext) {
    // given
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
      List.of(KafkaHeader.header(XOkapiHeaders.TOKEN, "token"),
        KafkaHeader.header(XOkapiHeaders.URL, "url")));

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

    mockedInstanceSharingHandler = mockStatic(InstanceSharingHandlerFactory.class);

    InstanceSharingHandler sharingHandler = mock(InstanceSharingHandler.class);

    mockedInstanceSharingHandler.when(InstanceSharingHandlerFactory::values)
      .thenReturn(new InstanceSharingHandlerFactory[] {InstanceSharingHandlerFactory.MARC});

    mockedInstanceSharingHandler.when(() ->
        InstanceSharingHandlerFactory.getInstanceSharingHandler(eq(InstanceSharingHandlerFactory.MARC),
          any(InstanceOperationsHelper.class), any(Storage.class), any(Vertx.class), any(HttpClient.class)))
      .thenReturn(sharingHandler);

    when(sharingHandler.publishInstance(any(), any(), any(), any(), any()))
      .thenReturn(Future.failedFuture("ERROR"));

    doAnswer(invocationOnMock -> Future.succeededFuture(UUID.randomUUID().toString()))
      .when(eventIdStorageService).store(any(), any());

    //when
    consortiumInstanceSharingConsumer = spy(
      new ConsortiumInstanceSharingConsumer(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig,
        eventIdStorageService));

    //then
    Future<String> future = consortiumInstanceSharingConsumer.handle(kafkaRecord);
    future.onComplete(testContext.failing(err -> testContext.verify(() -> {
      assertTrue(err.getMessage()
        .contains("Sharing instance with InstanceId=" + instanceId + " to the target tenant consortium. Error: ERROR"));
      testContext.completeNow();
    })));
  }

  @Test
  void shouldNotProcessIfDuplicatedEventReceived(VertxTestContext testContext) {

    // given
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
      List.of(KafkaHeader.header(XOkapiHeaders.TOKEN, "token"),
        KafkaHeader.header(XOkapiHeaders.URL, "url")));

    when(eventIdStorageService.store(any(), any())).thenReturn(Future.failedFuture(
      new DuplicateEventException("SQL Unique constraint violation prevented repeatedly saving the record")));

    // when
    consortiumInstanceSharingConsumer =
      new ConsortiumInstanceSharingConsumer(vertxAssistant.getVertx(), httpClient, storage, kafkaConfig,
        eventIdStorageService);

    //then
    Future<String> future = consortiumInstanceSharingConsumer.handle(kafkaRecord);
    future.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      verify(mockedSourceInstanceCollection, times(0)).add(any(), any(), any());
      verify(storage, times(0)).getInstanceCollection(any());
      testContext.completeNow();
    })));
  }
}
