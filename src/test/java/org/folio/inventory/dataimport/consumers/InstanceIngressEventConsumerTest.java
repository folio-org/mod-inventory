package org.folio.inventory.dataimport.consumers;

import static net.mguenther.kafka.junit.EmbeddedKafkaCluster.provisionWith;
import static net.mguenther.kafka.junit.EmbeddedKafkaClusterConfig.defaultClusterConfig;
import static org.folio.inventory.instanceingress.InstanceIngressEventConsumer.CACHE_KEY;
import static org.mockito.AdditionalMatchers.not;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.eq;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import net.mguenther.kafka.junit.EmbeddedKafkaCluster;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.instanceingress.InstanceIngressEventConsumer;
import org.folio.inventory.resources.TenantApi;
import org.folio.inventory.rest.impl.PgPoolContainer;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.KafkaConfig;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.InstanceIngressEvent;
import org.folio.rest.jaxrs.model.InstanceIngressPayload;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.MarcBibUpdate;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

@RunWith(VertxUnitRunner.class)
public class InstanceIngressEventConsumerTest {

  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String RECORD_PATH = "src/test/resources/handlers/bib-record.json";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";
  private static final String INVALID_INSTANCE_ID = "02e54bce-9588-11ed-a1eb-0242ac120002";
  private static final String TENANT_ID = "test";
  private static final Vertx vertx = Vertx.vertx();
  private static EmbeddedKafkaCluster cluster;
  private static KafkaConfig kafkaConfig;

  @Mock
  private Storage mockedStorage;
  @Mock
  private InstanceCollection mockedInstanceCollection;
  @Mock
  private KafkaConsumerRecord<String, String> kafkaRecord;
  @Mock
  private MappingMetadataCache mappingMetadataCache;
  @Mock
  private ProfileSnapshotCache profileSnapshotCache;
  @Mock
  private ProfileSnapshotWrapper profileSnapshotWrapper;
  private String record;
  private Instance existingInstance;
  private InstanceIngressEventConsumer instanceIngressEventConsumer;
  private AutoCloseable mocks;
  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"))));

  @BeforeClass
  public static void beforeClass() {
    if (!PgPoolContainer.isRunning()) {
      PgPoolContainer.create();
    }
    cluster = provisionWith(defaultClusterConfig());
    cluster.start();
    String[] hostAndPort = cluster.getBrokerList().split(":");
    kafkaConfig = KafkaConfig.builder()
      .envId("env")
      .kafkaHost(hostAndPort[0])
      .kafkaPort(hostAndPort[1])
      .maxRequestSize(1048576)
      .build();
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    if (PgPoolContainer.isRunning()) {
      PgPoolContainer.stop();
    }
    Async async = context.async();
    vertx.close(ar -> {
      cluster.stop();
      async.complete();
    });
  }

  @Before
  public void setUp() throws IOException {
    PgPoolContainer.setEmbeddedPostgresOptions();
    TenantApi tenantApi = new TenantApi();
    tenantApi.initializeSchemaForTenant(TENANT_ID);

    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
    record = TestUtil.readFileFromPath(RECORD_PATH);

    mocks = MockitoAnnotations.openMocks(this);
    when(mockedStorage.getInstanceCollection(any(Context.class))).thenReturn(mockedInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(null));
      return null;
    }).when(mockedInstanceCollection).findById(eq(INVALID_INSTANCE_ID), any(), any());

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedInstanceCollection).findById(not(eq(INVALID_INSTANCE_ID)), any(), any());

    doAnswer(invocationOnMock -> {
      Instance instance = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instance));
      return null;
    }).when(mockedInstanceCollection).update(any(Instance.class), any(), any());

    when(mappingMetadataCache.get(eq(CACHE_KEY), any(Context.class)))
      .thenReturn(Future.succeededFuture(Optional.of(new MappingMetadataDto()
        .withMappingRules(mappingRules.encode())
        .withMappingParams(Json.encode(new MappingParameters())))));

    var parentProfileSnapshotWrapper = new ProfileSnapshotWrapper();
    parentProfileSnapshotWrapper.setChildSnapshotWrappers(List.of(profileSnapshotWrapper));
    when(profileSnapshotWrapper.getContentType()).thenReturn(ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE);
    when(profileSnapshotWrapper.getContent()).thenReturn(JsonObject.mapFrom(mappingProfile).getMap());
    when(profileSnapshotWrapper.getProfileId()).thenReturn(mappingProfile.getId());
    when(profileSnapshotCache.get(anyString(), any(Context.class))).thenReturn(Future.succeededFuture(Optional.of(parentProfileSnapshotWrapper)));

    instanceIngressEventConsumer = new InstanceIngressEventConsumer(vertx, mockedStorage, vertx.createHttpClient(), mappingMetadataCache, profileSnapshotCache);
  }

  @After
  public void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  public void shouldReturnSucceededFutureWithObtainedRecordKey(TestContext context) {
    // given
    var async = context.async();

    var payload = new InstanceIngressPayload()
      .withSourceType(InstanceIngressPayload.SourceType.BIBFRAME)
      .withSourceRecordIdentifier(UUID.randomUUID().toString())
      .withSourceRecordObject(record);
    var event = new InstanceIngressEvent()
      .withId("1")
      .withEventType(InstanceIngressEvent.EventType.CREATE_INSTANCE)
      .withEventPayload(payload);

    String expectedKafkaRecordKey = "test_key";
    when(kafkaRecord.key()).thenReturn(expectedKafkaRecordKey);
    when(kafkaRecord.value()).thenReturn(Json.encode(event));
    when(kafkaRecord.headers()).thenReturn(List.of(
        KafkaHeader.header(XOkapiHeaders.TENANT.toLowerCase(), TENANT_ID)
      )
    );

    // when
    Future<String> future = instanceIngressEventConsumer.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(expectedKafkaRecordKey, ar.result());
      verify(mappingMetadataCache, times(1)).get(eq(CACHE_KEY), any(Context.class));
      async.complete();
    });
  }


  //@Test temp copy-paste
  public void shouldReturnFailedFutureWhenMappingRulesNotFound(TestContext context) {
    // given
    Async async = context.async();
    when(mappingMetadataCache.getByRecordType(anyString(), any(Context.class), anyString()))
      .thenReturn(Future.succeededFuture(Optional.empty()));


    when(kafkaRecord.value()).thenReturn(Json.encode(null));

    // when
    Future<String> future = instanceIngressEventConsumer.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("MappingParameters and mapping rules snapshots were not found by jobId"));
      async.complete();
    });

    verify(mockedInstanceCollection, times(0)).findById(anyString(), any(), any());
    verify(mockedInstanceCollection, times(0)).update(any(Instance.class), any(), any());
    verify(mappingMetadataCache, times(1)).getByRecordType(anyString(), any(Context.class), anyString());
  }

  //@Test temp copy-paste
  public void shouldReturnFailedFutureWhenPayloadCanNotBeMapped(TestContext context) {
    // given
    Async async = context.async();
    MarcBibUpdate payload = new MarcBibUpdate()
      .withRecord(null)
      .withType(MarcBibUpdate.Type.UPDATE)
      .withTenant(TENANT_ID)
      .withJobId(UUID.randomUUID().toString());
    when(kafkaRecord.value()).thenReturn(Json.encode(payload));

    // when
    Future<String> future = instanceIngressEventConsumer.handle(kafkaRecord);

    // then
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage().contains("Event message does not contain required data to update Instance by jobId"));
      async.complete();
    });

    verify(mockedInstanceCollection, times(0)).findById(anyString(), any(), any());
    verify(mockedInstanceCollection, times(0)).update(any(Instance.class), any(), any());
    verify(mappingMetadataCache, times(0)).getByRecordType(anyString(), any(Context.class), anyString());
  }

}
