package org.folio.inventory.dataimport.services;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static java.lang.Boolean.parseBoolean;
import static org.folio.ActionProfile.Action.CREATE;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_ORDER_CREATED_READY_FOR_POST_PROCESSING;
import static org.folio.processing.events.EventManager.POST_PROCESSING_INDICATOR;
import static org.folio.rest.jaxrs.model.EntityType.HOLDINGS;
import static org.folio.rest.jaxrs.model.EntityType.INSTANCE;
import static org.folio.rest.jaxrs.model.EntityType.MARC_BIBLIOGRAPHIC;
import static org.folio.rest.jaxrs.model.EntityType.ORDER;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.Collections;
import java.util.HashMap;
import java.util.UUID;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingProfile;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.inventory.common.Context;
import org.folio.inventory.dataimport.cache.ProfileSnapshotCache;
import org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.MockitoAnnotations;

@ExtendWith(VertxExtension.class)
class OrderHelperServiceTest extends BaseWireMockTest {

  private static final String JOB_PROFILE_URL = "/data-import-profiles/jobProfileSnapshots";
  private static final String JOB_PROFILE_SNAPSHOT_ID = "JOB_PROFILE_SNAPSHOT_ID";
  private static final String TENANT_ID = "diku";
  private static Vertx vertx;
  private OrderHelperService orderHelperService;
  private AutoCloseable mocks;
  private HttpClient client;

  @BeforeAll
  static void setUpClass() {
    vertx = Vertx.vertx();
  }

  @BeforeEach
  void setUp() {
    mocks = MockitoAnnotations.openMocks(this);

    client = vertx.createHttpClient();
  }

  @AfterEach
  void tearDown() throws Exception {
    mocks.close();
  }

  @Test
  void shouldFillPayloadIfOrderActionProfileExistsAndCurrentProfileIsTheLastOne(VertxTestContext testContext) {
    JobProfile jobProfile = new JobProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create Instance and Order")
      .withDataType(org.folio.JobProfile.DataType.MARC);

    ActionProfile instanceActionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withAction(CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    MappingProfile instanceMappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE);

    ActionProfile orderActionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create order")
      .withAction(CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.ORDER);

    MappingProfile orderMappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create order")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ORDER);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(jobProfile).getMap())
      .withChildSnapshotWrappers(Lists.newArrayList(
        new ProfileSnapshotWrapper()
          .withId(UUID.randomUUID().toString())
          .withProfileId(orderActionProfile.getId())
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(orderActionProfile).getMap())
          .withChildSnapshotWrappers(Collections.singletonList(
            new ProfileSnapshotWrapper()
              .withProfileId(orderMappingProfile.getId())
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(orderMappingProfile).getMap()))),
        new ProfileSnapshotWrapper()
          .withId(UUID.randomUUID().toString())
          .withProfileId(instanceActionProfile.getId())
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(instanceActionProfile).getMap())
          .withChildSnapshotWrappers(Collections.singletonList(
            new ProfileSnapshotWrapper()
              .withProfileId(instanceMappingProfile.getId())
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(instanceMappingProfile).getMap())))
      ));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(JOB_PROFILE_SNAPSHOT_ID, profileSnapshotWrapper.getProfileId());
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType("TEST_EVENT")
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken("test-token")
      .withCurrentNode(new ProfileSnapshotWrapper().withProfileId(instanceMappingProfile.getId()));
    Context context =
      EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(),
        dataImportEventPayload.getOkapiUrl());

    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(JOB_PROFILE_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapper))));

    orderHelperService = new OrderHelperServiceImpl(new ProfileSnapshotCache(vertx, client, 3600));
    //when
    Future<Void> future = orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(dataImportEventPayload,
      DI_INVENTORY_INSTANCE_CREATED, context);

    // then
    future.onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.succeeded());
      assertEquals(dataImportEventPayload.getEventType(), DI_ORDER_CREATED_READY_FOR_POST_PROCESSING.value());
      assertEquals(dataImportEventPayload.getEventsChain().getFirst(), DI_INVENTORY_INSTANCE_CREATED.value());
      assertTrue(parseBoolean(dataImportEventPayload.getContext().get(POST_PROCESSING_INDICATOR)));
      testContext.completeNow();
    }));
  }

  @Test
  void shouldNotFillPayloadEventIfOrderActionProfileNotExistsAndCurrentProfileIsTheLastOne(
    VertxTestContext testContext) {
    JobProfile jobProfile = new JobProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create Instance and Order")
      .withDataType(org.folio.JobProfile.DataType.MARC);

    ActionProfile instanceActionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withAction(CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    MappingProfile instanceMappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE);

    ActionProfile holdingsActionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create holdings")
      .withAction(CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.HOLDINGS);

    MappingProfile holdingsMappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create holdings")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(HOLDINGS);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(jobProfile).getMap())
      .withChildSnapshotWrappers(Lists.newArrayList(
        new ProfileSnapshotWrapper()
          .withId(UUID.randomUUID().toString())
          .withProfileId(holdingsActionProfile.getId())
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(holdingsActionProfile).getMap())
          .withChildSnapshotWrappers(Collections.singletonList(
            new ProfileSnapshotWrapper()
              .withProfileId(holdingsMappingProfile.getId())
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(holdingsMappingProfile).getMap()))),
        new ProfileSnapshotWrapper()
          .withId(UUID.randomUUID().toString())
          .withProfileId(instanceActionProfile.getId())
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(instanceActionProfile).getMap())
          .withChildSnapshotWrappers(Collections.singletonList(
            new ProfileSnapshotWrapper()
              .withProfileId(instanceMappingProfile.getId())
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(instanceMappingProfile).getMap())))
      ));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(JOB_PROFILE_SNAPSHOT_ID, profileSnapshotWrapper.getProfileId());
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType("TEST_EVENT")
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken("test-token")
      .withCurrentNode(new ProfileSnapshotWrapper().withProfileId(instanceMappingProfile.getId()));
    Context context =
      EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(),
        dataImportEventPayload.getOkapiUrl());

    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(JOB_PROFILE_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapper))));

    orderHelperService = new OrderHelperServiceImpl(new ProfileSnapshotCache(vertx, client, 3600));
    //when
    Future<Void> future = orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(dataImportEventPayload,
      DI_INVENTORY_INSTANCE_CREATED, context);

    // then
    future.onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.succeeded());
      assertEquals("TEST_EVENT", dataImportEventPayload.getEventType());
      assertEquals(0, dataImportEventPayload.getEventsChain().size());
      testContext.completeNow();
    }));
  }

  @Test
  void shouldNotFillPayloadIfOrderActionProfileExistsAndCurrentProfileIsNotTheLastOne(VertxTestContext testContext) {
    JobProfile jobProfile = new JobProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create Instance and Order")
      .withDataType(org.folio.JobProfile.DataType.MARC);

    ActionProfile instanceActionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withAction(CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.INSTANCE);

    MappingProfile instanceMappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create instance")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(INSTANCE);

    ActionProfile orderActionProfile = new ActionProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create order")
      .withAction(CREATE)
      .withFolioRecord(ActionProfile.FolioRecord.ORDER);

    MappingProfile orderMappingProfile = new MappingProfile()
      .withId(UUID.randomUUID().toString())
      .withName("Create order")
      .withIncomingRecordType(MARC_BIBLIOGRAPHIC)
      .withExistingRecordType(ORDER);

    ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
      .withId(UUID.randomUUID().toString())
      .withProfileId(jobProfile.getId())
      .withContentType(JOB_PROFILE)
      .withContent(JsonObject.mapFrom(jobProfile).getMap())
      .withChildSnapshotWrappers(Lists.newArrayList(
        new ProfileSnapshotWrapper()
          .withId(UUID.randomUUID().toString())
          .withProfileId(orderActionProfile.getId())
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(orderActionProfile).getMap())
          .withChildSnapshotWrappers(Collections.singletonList(
            new ProfileSnapshotWrapper()
              .withProfileId(orderMappingProfile.getId())
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(orderMappingProfile).getMap()))),
        new ProfileSnapshotWrapper()
          .withId(UUID.randomUUID().toString())
          .withProfileId(instanceActionProfile.getId())
          .withContentType(ACTION_PROFILE)
          .withContent(JsonObject.mapFrom(instanceActionProfile).getMap())
          .withChildSnapshotWrappers(Collections.singletonList(
            new ProfileSnapshotWrapper()
              .withProfileId(instanceMappingProfile.getId())
              .withContentType(MAPPING_PROFILE)
              .withContent(JsonObject.mapFrom(instanceMappingProfile).getMap())))
      ));

    HashMap<String, String> payloadContext = new HashMap<>();
    payloadContext.put(EntityType.MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record()));
    payloadContext.put(JOB_PROFILE_SNAPSHOT_ID, profileSnapshotWrapper.getProfileId());
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType("TEST_EVENT")
      .withJobExecutionId(UUID.randomUUID().toString())
      .withContext(payloadContext)
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken("test-token")
      .withCurrentNode(new ProfileSnapshotWrapper().withProfileId(orderMappingProfile.getId()));
    Context context =
      EventHandlingUtil.constructContext(dataImportEventPayload.getTenant(), dataImportEventPayload.getToken(),
        dataImportEventPayload.getOkapiUrl());

    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(JOB_PROFILE_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(profileSnapshotWrapper))));

    orderHelperService = new OrderHelperServiceImpl(new ProfileSnapshotCache(vertx, client, 3600));

    //when
    Future<Void> future = orderHelperService.fillPayloadForOrderPostProcessingIfNeeded(dataImportEventPayload,
      DI_INVENTORY_INSTANCE_CREATED, context);

    // then
    future.onComplete(ar -> testContext.verify(() -> {
      assertTrue(ar.succeeded());
      assertEquals("TEST_EVENT", dataImportEventPayload.getEventType());
      assertEquals(0, dataImportEventPayload.getEventsChain().size());
      testContext.completeNow();
    }));
  }
}
