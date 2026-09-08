package org.folio.inventory.dataimport.handlers.actions;

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static io.vertx.core.buffer.Buffer.buffer;
import static java.util.concurrent.CompletableFuture.completedFuture;
import static java.util.concurrent.CompletableFuture.completedStage;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.CENTRAL_RECORD_UPDATE_PERMISSION;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.MARC_BIB_RECORD_CREATED;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.USER_HAS_NO_PERMISSION_MSG;
import static org.folio.inventory.dataimport.util.ParsedRecordUtil.LEADER_STATUS_DELETED;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.MARC;
import static org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle.TITLE_KEY;
import static org.folio.okapi.common.XOkapiHeaders.PERMISSIONS;
import static org.folio.okapi.common.XOkapiHeaders.REQUEST_ID;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static support.TestUtil.buildHttpResponseWithBuffer;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.junit5.VertxExtension;
import java.io.File;
import java.io.IOException;
import java.net.URL;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import lombok.SneakyThrows;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.InstanceLinkDtoCollection;
import org.folio.JobProfile;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.dataimport.util.DataImportHeaders;
import org.folio.dataimport.util.marc.MarcContentCodec;
import org.folio.inventory.client.InstanceLinkClient;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.SharingStatus;
import org.folio.inventory.consortium.services.ConsortiumServiceImpl;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
import org.folio.inventory.dataimport.services.SnapshotService;
import org.folio.inventory.dataimport.util.ParsedRecordUtil;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.BooleanValue;
import org.folio.processing.value.ListValue;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.client.SourceStorageSnapshotsClient;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.folio.rest.jaxrs.model.Snapshot;
import org.folio.rest.tools.ClientHelpers;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;
import support.TestUtil;
import support.builders.MarcRecordBuilder;

@ExtendWith({MockitoExtension.class, VertxExtension.class})
@MockitoSettings(strictness = Strictness.LENIENT)
class ReplaceInstanceEventHandlerTest extends BaseWireMockTest {

  private static final String PARSED_CONTENT = MarcRecordBuilder.newBibRecord()
    .build();

  private static final String PARSED_CONTENT_WITH_DELETED_05 = MarcRecordBuilder.newBibRecord()
    .with003("in001")
    .deleted()
    .build();
  private static final String RESPONSE_CONTENT = """
    {
      "id": "%s",
      "matchedId": "%s",
      "generation": 1,
      "parsedRecord": {
        "content": {
          "leader": "00574nam  22001211a 4500",
          "fields": [
            {
              "035": {
                "subfields": [
                  {
                    "a": "(in001)ybp7406411"
                  }
                ],
                "ind1": " ",
                "ind2": " "
              }
            },
            {
              "245": {
                "subfields": [
                  {
                    "a": "titleValue"
                  }
                ],
                "ind1": "1",
                "ind2": "0"
              }
            },
            {
              "336": {
                "subfields": [
                  {
                    "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                  }
                ],
                "ind1": "1",
                "ind2": "0"
              }
            },
            {
              "780": {
                "subfields": [
                  {
                    "t": "Houston oil directory"
                  }
                ],
                "ind1": "0",
                "ind2": "0"
              }
            },
            {
              "785": {
                "subfields": [
                  {
                    "t": "SAIS review of international affairs"
                  },
                  {
                    "x": "1945-4724"
                  }
                ],
                "ind1": "0",
                "ind2": "0"
              }
            },
            {
              "500": {
                "subfields": [
                  {
                    "a": "Adaptation of Xi xiang ji by Wang Shifu."
                  }
                ],
                "ind1": " ",
                "ind2": " "
              }
            },
            {
              "520": {
                "subfields": [
                  {
                    "a": "Ben shu miao shu."
                  }
                ],
                "ind1": " ",
                "ind2": " "
              }
            },
            {
              "999": {
                "subfields": [
                  {
                    "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                  }
                ],
                "ind1": "f",
                "ind2": "f"
              }
            }
          ]
        }
      }
    }
    """;
  private static final String EXISTING_SRS_CONTENT = """
    {
       "id": "%s",
       "matchedId": "%s",
       "generation": %d,
       "parsedRecord": {
         "content": {
           "leader": "00574nam  22001211a 4500",
           "fields": [
             {
               "035": {
                 "subfields": [
                   {
                     "a": "(in001)ybp7406411"
                   }
                 ],
                 "ind1": "",
                 "ind2": ""
               }
             },
             {
               "245": {
                 "subfields": [
                   {
                     "a": "titleValue"
                   }
                 ],
                 "ind1": "1",
                 "ind2": "0"
               }
             },
             {
               "336": {
                 "subfields": [
                   {
                     "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                   }
                 ],
                 "ind1": "1",
                 "ind2": "0"
               }
             },
             {
               "780": {
                 "subfields": [
                   {
                     "t": "Houstonoildirectory"
                   }
                 ],
                 "ind1": "0",
                 "ind2": "0"
               }
             },
             {
               "785": {
                 "subfields": [
                   {
                     "t": "SAISreviewofinternationalaffairs"
                   },
                   {
                     "x": "1945-4724"
                   }
                 ],
                 "ind1": "0",
                 "ind2": "0"
               }
             },
             {
               "500": {
                 "subfields": [
                   {
                     "a": "AdaptationofXixiangjibyWangShifu."
                   }
                 ],
                 "ind1": "",
                 "ind2": ""
               }
             },
             {
               "520": {
                 "subfields": [
                   {
                     "a": "Benshumiaoshulecuiyingyinghezhangshengweizhengquhunyinziyoulijinquzhejianxinzhihou."
                   }
                 ],
                 "ind1": "",
                 "ind2": ""
               }
             },
             {
               "999": {
                 "subfields": [
                   {
                     "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                   }
                 ],
                 "ind1": "f",
                 "ind2": "f"
               }
             }
           ]
         }
       }
     }
    """;
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String SOURCE_RECORDS_PATH = "/source-storage/records";
  private static final String PRECEDING_SUCCEEDING_TITLES_KEY = "precedingSucceedingTitles";
  private static final String TENANT_ID = "test-tenant";
  private static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";
  private static final String CENTRAL_TENANT_INSTANCE_UPDATED_KEY = "CENTRAL_TENANT_INSTANCE_UPDATED";
  private static final String TOKEN = "dummy-token";
  private static final String USER_ID = "123567";
  private static final Integer INSTANCE_VERSION = 1;
  private static final String INSTANCE_VERSION_AS_STRING = "1";
  private static final String MARC_INSTANCE_SOURCE = "MARC";
  private static final String LINKED_DATA_INSTANCE_SOURCE = "LINKED_DATA";

  private final String localTenant = "tenant";
  private final String consortiumTenant = "consortiumTenant";
  private final UUID instanceId = UUID.randomUUID();
  private final String instanceHrid = "in0001";
  private final String consortiumId = UUID.randomUUID().toString();
  private final String jobExecutionId = UUID.randomUUID().toString();

  private final JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private final ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Replace preliminary Item")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(INSTANCE);

  private final MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"")
          .withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"))));

  private final ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile)
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfile.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfile).getMap())))));

  private final JobProfile jobProfileWithSuppressFromDiscovery = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private final ActionProfile actionProfileWithSuppressFromDiscovery = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update Instance with suppress from discovery")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(INSTANCE);

  private final MappingProfile mappingProfileWithSuppressFromDiscovery = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"")
          .withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.discoverySuppress").withValue("true").withEnabled("true")
      )));

  private final ProfileSnapshotWrapper profileSnapshotWrapperWithSuppressFromDiscovery = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfileWithSuppressFromDiscovery.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfileWithSuppressFromDiscovery)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfileWithSuppressFromDiscovery.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfileWithSuppressFromDiscovery)
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfileWithSuppressFromDiscovery.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfileWithSuppressFromDiscovery).getMap())))));

  private final JobProfile jobProfileWithNatureOfContentTerm = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs with NatureOfContentTerm")
    .withDataType(JobProfile.DataType.MARC);

  private final ActionProfile actionProfileWithNatureOfContentTerm = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Replace preliminary Item with NatureOfContentTerm")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(INSTANCE);

  private final MappingProfile mappingProfileWithNatureOfContentTerm = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC with NatureOfContentTerm")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"")
          .withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.natureOfContentTermIds[]").withValue("\"not uuid\"").withEnabled("true")
          .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING))));

  private final ProfileSnapshotWrapper profileSnapshotWrapperWithNatureOfContentTerm = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfileWithNatureOfContentTerm.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfileWithNatureOfContentTerm)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfileWithNatureOfContentTerm.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfileWithNatureOfContentTerm)
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfileWithNatureOfContentTerm.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfileWithNatureOfContentTerm).getMap())))));

  private final MappingProfile mappingProfileWithStatisticalCode = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC with invalid StatisticalCode")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"")
          .withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.statisticalCodeIds[]").withValue("\"ebookss\"").withEnabled("true")
          .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING))));

  private final ProfileSnapshotWrapper profileSnapshotWrapperWithStatisticalCode = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile)
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfileWithStatisticalCode.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfileWithStatisticalCode).getMap())))));

  private final MappingProfile mappingProfileWithDeleteAdministrativeNote = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Instance repeatable 2-Find & remove")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"")
          .withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.administrativeNotes[]").withValue("\"Withdrawn as part of workflow\"")
          .withEnabled("true")
          .withRepeatableFieldAction(MappingRule.RepeatableFieldAction.DELETE_INCOMING))));

  private final ProfileSnapshotWrapper profileSnapshotWrapperWithDeleteAdministrativeNote = new ProfileSnapshotWrapper()
    .withId(UUID.randomUUID().toString())
    .withProfileId(jobProfile.getId())
    .withContentType(JOB_PROFILE)
    .withContent(jobProfile)
    .withChildSnapshotWrappers(Collections.singletonList(
      new ProfileSnapshotWrapper()
        .withProfileId(actionProfile.getId())
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile)
        .withChildSnapshotWrappers(Collections.singletonList(
          new ProfileSnapshotWrapper()
            .withProfileId(mappingProfileWithDeleteAdministrativeNote.getId())
            .withContentType(MAPPING_PROFILE)
            .withContent(JsonObject.mapFrom(mappingProfileWithDeleteAdministrativeNote).getMap())))));

  @Mock
  private Storage storage;
  @Mock
  private InstanceCollection instanceRecordCollection;
  @Mock
  private OkapiHttpClient mockedClient;
  @Mock
  private ConsortiumServiceImpl consortiumServiceImpl;
  @Mock
  private InstanceLinkClient instanceLinkClient;
  @Spy
  private MarcBibReaderFactory fakeReaderFactory = new MarcBibReaderFactory();
  @Mock
  private SourceStorageRecordsClient sourceStorageClient;
  @Mock
  private SnapshotService snapshotService;
  @Mock
  private Reader fakeReader;
  @Mock
  private SourceStorageSnapshotsClient sourceStorageSnapshotsClient;
  @Captor
  private ArgumentCaptor<Record> recordCaptor;

  private ReplaceInstanceEventHandler replaceInstanceEventHandler;
  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;

  @BeforeEach
  void setUp(Vertx vertx) {
    MappingManager.clearReaderFactories();

    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()
          .withLinkingRules(List.of(new LinkingRuleDto()
            .withId(1)
            .withBibField("100")
            .withAuthorityField("100")))))
        .withMappingRules(new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH)).toString())))));

    precedingSucceedingTitlesHelper = spy(new PrecedingSucceedingTitlesHelper(ctxt -> mockedClient));

    var metadataCache = MappingMetadataCache.getInstance(vertx, true);
    replaceInstanceEventHandler = spy(new ReplaceInstanceEventHandler(storage, precedingSucceedingTitlesHelper,
      metadataCache, vertx.createHttpClient(), consortiumServiceImpl, instanceLinkClient, snapshotService));

    var recordId = UUID.randomUUID().toString();
    HttpResponse<Buffer> recordHttpResponse =
      buildHttpResponseWithBuffer(Buffer.buffer(String.format(EXISTING_SRS_CONTENT, recordId, recordId, 0)),
        HttpStatus.SC_OK);
    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.succeededFuture(recordHttpResponse));

    HttpResponse<Buffer> snapshotHttpResponse =
      buildHttpResponseWithBuffer(buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    when(sourceStorageSnapshotsClient.postSourceStorageSnapshots(any())).thenReturn(
      Future.succeededFuture(snapshotHttpResponse));

    doAnswer(invocationOnMock -> {
      Instance instanceRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instanceRecord));
      return null;
    }).when(instanceRecordCollection).update(any(), any(), any());

    doReturn(sourceStorageClient).when(replaceInstanceEventHandler)
      .getSourceStorageClient(any(), any(), any(), any(), any());

    doAnswer(invocationOnMock -> completedStage(createResponse(201, null)))
      .when(mockedClient).post(any(URL.class), any(JsonObject.class));
    doAnswer(invocationOnMock -> completedStage(createResponse(200, new JsonObject().encode())))
      .when(mockedClient).get(anyString());
    doAnswer(invocationOnMock -> completedStage(createResponse(204, null)))
      .when(mockedClient).delete(anyString());

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldProcessEvent() throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = Buffer.buffer("""
      {
        "parsedRecord": {
          "id": "990fad8b-64ec-4de4-978c-9f8bbed4c6d3",
          "content": {
            "leader": "00574nam  22001211a 4500",
            "fields": [
              {
                "035": {
                  "subfields": [
                    {
                      "a": "(in001)ybp7406411"
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "245": {
                  "subfields": [
                    {
                      "a": "titleValue"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "336": {
                  "subfields": [
                    {
                      "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "780": {
                  "subfields": [
                    {
                      "t": "Houston oil directory"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "785": {
                  "subfields": [
                    {
                      "t": "SAIS review of international affairs"
                    },
                    {
                      "x": "1945-4724"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "500": {
                  "subfields": [
                    {
                      "a": "Adaptation of Xi xiang ji by Wang Shifu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "520": {
                  "subfields": [
                    {
                      "a": "Ben shu miao shu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "999": {
                  "subfields": [
                    {
                      "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                    }
                  ],
                  "ind1": "f",
                  "ind2": "f"
                }
              }
            ]
          }
        }
      }
      """);
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    WIRE_MOCK.verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldProcessEventAndMarkInstanceAndRecordAsDeletedIfLeaderIsDeleted()
    throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record srsRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_DELETED_05))
      .withExternalIdsHolder(
        new ExternalIdsHolder().withInstanceId(instanceId.toString()).withInstanceHrid(instanceHrid));

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(srsRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid)
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = Buffer.buffer("""
      {
        "parsedRecord": {
          "id": "990fad8b-64ec-4de4-978c-9f8bbed4c6d3",
          "content": {
            "leader": "00574nam  22001211a 4500",
            "fields": [
              {
                "035": {
                  "subfields": [
                    {
                      "a": "(in001)ybp7406411"
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "245": {
                  "subfields": [
                    {
                      "a": "titleValue"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "336": {
                  "subfields": [
                    {
                      "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "780": {
                  "subfields": [
                    {
                      "t": "Houston oil directory"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "785": {
                  "subfields": [
                    {
                      "t": "SAIS review of international affairs"
                    },
                    {
                      "x": "1945-4724"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "500": {
                  "subfields": [
                    {
                      "a": "Adaptation of Xi xiang ji by Wang Shifu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "520": {
                  "subfields": [
                    {
                      "a": "Ben shu miao shu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "999": {
                  "subfields": [
                    {
                      "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                    }
                  ],
                  "ind1": "f",
                  "ind2": "f"
                }
              }
            ]
          }
        }
      }
      """);
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertEquals("true", createdInstance.getString("staffSuppress"));
    assertEquals("true", createdInstance.getString("discoverySuppress"));
    assertEquals("true", createdInstance.getString("deleted"));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    verify(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), argThat(r -> {
      Optional<Character> leader = ParsedRecordUtil.getLeaderStatus(r.getParsedRecord());
      return r.getState() == Record.State.DELETED && r.getAdditionalInfo().getSuppressDiscovery()
             && r.getDeleted() && leader.isPresent() && leader.get().equals(LEADER_STATUS_DELETED);
    }));
    verify(sourceStorageClient).putSourceStorageRecordsGenerationById(any(),
      argThat(this::verifyParsedContentSerialization));
    WIRE_MOCK.verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldProcessEventAndUnMarkInstanceAndRecordDeleted()
    throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record srsRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(srsRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", true)
      .put("staffSuppress", true)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE, true);

    Buffer buffer = Buffer.buffer("""
      {
         "parsedRecord": {
           "id": "990fad8b-64ec-4de4-978c-9f8bbed4c6d3",
           "content": {
             "leader": "00574nam  22001211a 4500",
             "fields": [
               {
                 "035": {
                   "subfields": [
                     {
                       "a": "(in001)ybp7406411"
                     }
                   ],
                   "ind1": " ",
                   "ind2": " "
                 }
               },
               {
                 "245": {
                   "subfields": [
                     {
                       "a": "titleValue"
                     }
                   ],
                   "ind1": "1",
                   "ind2": "0"
                 }
               },
               {
                 "336": {
                   "subfields": [
                     {
                       "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                     }
                   ],
                   "ind1": "1",
                   "ind2": "0"
                 }
               },
               {
                 "780": {
                   "subfields": [
                     {
                       "t": "Houston oil directory"
                     }
                   ],
                   "ind1": "0",
                   "ind2": "0"
                 }
               },
               {
                 "785": {
                   "subfields": [
                     {
                       "t": "SAIS review of international affairs"
                     },
                     {
                       "x": "1945-4724"
                     }
                   ],
                   "ind1": "0",
                   "ind2": "0"
                 }
               },
               {
                 "500": {
                   "subfields": [
                     {
                       "a": "Adaptation of Xi xiang ji by Wang Shifu."
                     }
                   ],
                   "ind1": " ",
                   "ind2": " "
                 }
               },
               {
                 "520": {
                   "subfields": [
                     {
                       "a": "Ben shu miao shu."
                     }
                   ],
                   "ind1": " ",
                   "ind2": " "
                 }
               },
               {
                 "999": {
                   "subfields": [
                     {
                       "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                     }
                   ],
                   "ind1": "f",
                   "ind2": "f"
                 }
               }
             ]
           }
         }
       }
      """);
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertEquals("true", createdInstance.getString("staffSuppress"));
    assertEquals("true", createdInstance.getString("discoverySuppress"));
    assertEquals("false", createdInstance.getString("deleted"));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    verify(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), argThat(r -> {
      Optional<Character> leader = ParsedRecordUtil.getLeaderStatus(r.getParsedRecord());
      return r.getState() == Record.State.ACTUAL && r.getAdditionalInfo().getSuppressDiscovery()
             && !r.getDeleted() && leader.isPresent() && !leader.get().equals(LEADER_STATUS_DELETED);
    }));
    WIRE_MOCK.verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldReplaceExistingPrecedingTitleOnInstanceUpdate() throws InterruptedException, ExecutionException {
    JsonObject existingPrecedingTitle = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put(TITLE_KEY, "Butterflies in the snow");

    JsonObject precedingSucceedingTitles =
      new JsonObject().put(PRECEDING_SUCCEEDING_TITLES_KEY, new JsonArray().add(existingPrecedingTitle));
    when(mockedClient.get(anyString()))
      .thenReturn(completedFuture(createResponse(HttpStatus.SC_OK, precedingSucceedingTitles.encode())));

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = Buffer.buffer(String.format(RESPONSE_CONTENT, UUID.randomUUID(), UUID.randomUUID()));
    HttpResponse<Buffer> resp = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(resp));

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    marcRecord.withGeneration(0);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get();

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject updatedInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));

    assertEquals(title, updatedInstance.getString("title"));
    assertThat(updatedInstance.getJsonArray("precedingTitles").size(), is(1));
    assertNotEquals(existingPrecedingTitle.getString(TITLE_KEY),
      updatedInstance.getJsonArray("precedingTitles").getJsonObject(0).getString(TITLE_KEY));
    assertThat(updatedInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    JsonObject updatedSrsMarc =
      new JsonObject(actualDataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertEquals(Integer.valueOf(1), updatedSrsMarc.getInteger("generation"));

    ArgumentCaptor<Set<String>> titleIdCaptor = ArgumentCaptor.forClass(Set.class);
    verify(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(titleIdCaptor.capture(),
      any(Context.class));
    assertTrue(titleIdCaptor.getValue().contains(existingPrecedingTitle.getString("id")));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldProcessEventIfPayloadHasShadowInstance()
    throws InterruptedException, ExecutionException, TimeoutException {
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    mockInstance(CONSORTIUM_MARC.getValue());

    JsonObject centralTenantIdResponse = new JsonObject()
      .put("userTenants", new JsonArray().add(new JsonObject().put("centralTenantId", consortiumTenant)));

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(centralTenantIdResponse))));

    JsonObject consortiumIdResponse = new JsonObject()
      .put("consortia", new JsonArray().add(new JsonObject().put("id", consortiumId)));

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/consortia"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(consortiumIdResponse))));

    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setId(UUID.randomUUID());
    sharingInstance.setSourceTenantId(consortiumTenant);
    sharingInstance.setInstanceIdentifier(instanceId);
    sharingInstance.setTargetTenantId(localTenant);
    sharingInstance.setStatus(SharingStatus.COMPLETE);

    WIRE_MOCK.stubFor(
      post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
        .willReturn(WireMock.ok().withBody(Json.encode(sharingInstance))));

    doAnswer(invocationOnMock -> Future.succeededFuture(
      Optional.of(new ConsortiumConfiguration(consortiumTenant, consortiumId)))).when(consortiumServiceImpl)
      .getConsortiumConfiguration(any());

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord =
      new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withSnapshotId(jobExecutionId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(PERMISSIONS, JsonArray.of(CENTRAL_RECORD_UPDATE_PERMISSION).encode());
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", CONSORTIUM_MARC.getValue())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    doReturn(Future.succeededFuture(new Snapshot().withJobExecutionId("someJobExecutionId")))
      .when(snapshotService).postSnapshotInSrsAndHandleResponse(any(), any());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(replaceInstanceEventHandler).getSourceStorageClient(any(), any(),
      argThat(tenantId -> tenantId.equals(consortiumTenant)), any(), any());
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    ArgumentCaptor<Context> contextCaptorForSnapshot = ArgumentCaptor.forClass(Context.class);
    ArgumentCaptor<Snapshot> snapshotCaptor = ArgumentCaptor.forClass(Snapshot.class);
    verify(snapshotService).postSnapshotInSrsAndHandleResponse(contextCaptorForSnapshot.capture(),
      snapshotCaptor.capture());
    assertEquals(consortiumTenant, contextCaptorForSnapshot.getValue().getTenantId());
    assertEquals(marcRecord.getSnapshotId(), snapshotCaptor.getValue().getJobExecutionId());
    WIRE_MOCK.verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldFailIfErrorDuringCreatingOfSnapshotForConsortiumInstance() {
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    mockInstance(CONSORTIUM_MARC.getValue());

    JsonObject centralTenantIdResponse = new JsonObject()
      .put("userTenants", new JsonArray().add(new JsonObject().put("centralTenantId", consortiumTenant)));

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(centralTenantIdResponse))));

    JsonObject consortiumIdResponse = new JsonObject()
      .put("consortia", new JsonArray().add(new JsonObject().put("id", consortiumId)));

    WIRE_MOCK.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/consortia"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(consortiumIdResponse))));

    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setId(UUID.randomUUID());
    sharingInstance.setSourceTenantId(consortiumTenant);
    sharingInstance.setInstanceIdentifier(instanceId);
    sharingInstance.setTargetTenantId(localTenant);
    sharingInstance.setStatus(SharingStatus.COMPLETE);

    WIRE_MOCK.stubFor(
      post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
        .willReturn(WireMock.ok().withBody(Json.encode(sharingInstance))));

    doAnswer(invocationOnMock -> Future.succeededFuture(
      Optional.of(new ConsortiumConfiguration(consortiumTenant, consortiumId)))).when(consortiumServiceImpl)
      .getConsortiumConfiguration(any());

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    HttpResponse<Buffer> snapshotHttpResponse =
      buildHttpResponseWithBuffer(Buffer.buffer("{}"), HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(sourceStorageSnapshotsClient.postSourceStorageSnapshots(any())).thenReturn(
      Future.succeededFuture(snapshotHttpResponse));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord =
      new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withSnapshotId(jobExecutionId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", CONSORTIUM_MARC.getValue())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(20, TimeUnit.SECONDS));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldUpdateSharedFolioInstanceOnCentralTenantIfPayloadContainsCentralTenantIdAndSharedInstance()
    throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    String recordId = UUID.randomUUID().toString();
    Record marcRecord =
      new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withSnapshotId(jobExecutionId);
    marcRecord.setId(recordId);

    HashMap<String, String> context = new HashMap<>();
    context.put(CENTRAL_TENANT_ID_KEY, consortiumTenant);
    context.put(DataImportHeaders.USER_ID, USER_ID);
    context.put(REQUEST_ID.toLowerCase(), REQUEST_ID);
    context.put(PERMISSIONS, JsonArray.of(CENTRAL_RECORD_UPDATE_PERMISSION).encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", FOLIO.toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    mockInstance(FOLIO.getValue());

    Buffer buffer = Buffer.buffer(String.format(RESPONSE_CONTENT, recordId, recordId));
    HttpResponse<Buffer> respForCreated = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_CREATED);

    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(respForCreated));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(context)
      .withJobExecutionId(UUID.randomUUID().toString());

    doReturn(Future.succeededFuture(new Snapshot().withJobExecutionId("someJobExecutionId")))
      .when(snapshotService).postSnapshotInSrsAndHandleResponse(any(), any());

    assertEquals(consortiumTenant, dataImportEventPayload.getContext().get(CENTRAL_TENANT_ID_KEY));

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertTrue(
      Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(CENTRAL_TENANT_INSTANCE_UPDATED_KEY)));
    JsonObject updatedInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));

    assertEquals(title, updatedInstance.getString("title"));
    assertEquals(MARC_INSTANCE_SOURCE, updatedInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertTrue(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));

    ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
    verify(storage).getInstanceCollection(contextCaptor.capture());
    assertEquals(consortiumTenant, contextCaptor.getValue().getTenantId());

    verify(sourceStorageClient).postSourceStorageRecords(recordCaptor.capture());
    verify(replaceInstanceEventHandler).getSourceStorageClient(any(), any(),
      argThat(tenantId -> tenantId.equals(consortiumTenant)), argThat(USER_ID::equals), argThat(REQUEST_ID::equals));

    ArgumentCaptor<Context> contextCaptorForSnapshot = ArgumentCaptor.forClass(Context.class);
    ArgumentCaptor<Snapshot> snapshotCaptor = ArgumentCaptor.forClass(Snapshot.class);
    verify(snapshotService).postSnapshotInSrsAndHandleResponse(contextCaptorForSnapshot.capture(),
      snapshotCaptor.capture());
    assertEquals(consortiumTenant, contextCaptorForSnapshot.getValue().getTenantId());
    assertEquals(marcRecord.getSnapshotId(), snapshotCaptor.getValue().getJobExecutionId());
    assertNotNull(recordCaptor.getValue().getMatchedId());
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldUpdateSharedMarcInstanceOnCentralTenantIfPayloadContainsCentralTenantIdAndSharedInstance()
    throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    String recordId = UUID.randomUUID().toString();
    Record marcRecord =
      new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withSnapshotId(jobExecutionId);
    marcRecord.setId(recordId);

    HashMap<String, String> context = new HashMap<>();
    context.put(CENTRAL_TENANT_ID_KEY, consortiumTenant);
    context.put(PERMISSIONS, JsonArray.of(CENTRAL_RECORD_UPDATE_PERMISSION).encode());
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(DataImportHeaders.USER_ID, USER_ID);
    context.put(REQUEST_ID.toLowerCase(), REQUEST_ID);
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC.toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    mockInstance(MARC.getValue());

    Buffer buffer = Buffer.buffer(String.format(RESPONSE_CONTENT, recordId, recordId));
    HttpResponse<Buffer> respForCreated = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);

    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any()))
      .thenReturn(Future.succeededFuture(respForCreated));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(context)
      .withJobExecutionId(UUID.randomUUID().toString());

    doReturn(Future.succeededFuture(new Snapshot().withJobExecutionId("someJobExecutionId")))
      .when(snapshotService).postSnapshotInSrsAndHandleResponse(any(), any());

    assertEquals(consortiumTenant, dataImportEventPayload.getContext().get(CENTRAL_TENANT_ID_KEY));

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertTrue(
      Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(CENTRAL_TENANT_INSTANCE_UPDATED_KEY)));
    JsonObject updatedInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));

    assertEquals(title, updatedInstance.getString("title"));
    assertEquals(MARC_INSTANCE_SOURCE, updatedInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));

    ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
    verify(storage).getInstanceCollection(contextCaptor.capture());
    assertEquals(consortiumTenant, contextCaptor.getValue().getTenantId());

    verify(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), recordCaptor.capture());
    verify(replaceInstanceEventHandler, times(2)).getSourceStorageClient(any(), any(),
      argThat(tenantId -> tenantId.equals(consortiumTenant)), argThat(USER_ID::equals), argThat(REQUEST_ID::equals));

    ArgumentCaptor<Context> contextCaptorForSnapshot = ArgumentCaptor.forClass(Context.class);
    ArgumentCaptor<Snapshot> snapshotCaptor = ArgumentCaptor.forClass(Snapshot.class);

    verify(snapshotService).postSnapshotInSrsAndHandleResponse(contextCaptorForSnapshot.capture(),
      snapshotCaptor.capture());
    assertEquals(consortiumTenant, contextCaptorForSnapshot.getValue().getTenantId());
    assertEquals(marcRecord.getSnapshotId(), snapshotCaptor.getValue().getJobExecutionId());
  }

  @Test
  void shouldNotUpdateInstanceWhenSrsUpdateFails() {
    // Test that verifies SRS-first approach: if SRS update fails, instance should NOT be updated
    Record incomingRecord = new Record()
      .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))
      .withMatchedId(UUID.randomUUID().toString())
      .withSnapshotId(jobExecutionId);

    HashMap<String, String> context = new HashMap<>();
    context.put(DataImportHeaders.USER_ID, USER_ID);
    context.put(REQUEST_ID.toLowerCase(), REQUEST_ID);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", FOLIO.getValue()) // FOLIO instance being converted to MARC
      .put("_version", INSTANCE_VERSION)
      .encode());

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "Test Title";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    mockInstance(FOLIO.getValue());

    // Mock SRS client to return FAILED response (simulating SRS failure)
    Buffer errorBuffer = Buffer.buffer("SRS update failed");
    HttpResponse<Buffer> failedResponse = buildHttpResponseWithBuffer(errorBuffer, HttpStatus.SC_BAD_REQUEST);
    when(sourceStorageClient.postSourceStorageRecords(any()))
      .thenReturn(Future.succeededFuture(failedResponse));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(context)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);

    // Expect ExecutionException because SRS update failed
    ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(20, TimeUnit.SECONDS));

    // Verify the exception message contains expected error
    assertTrue(exception.getMessage().contains("Failed to create MARC record in SRS"));

    // MOST IMPORTANT: Verify that instance update was NOT called (0 times)
    // because SRS operation failed before reaching instance update
    verify(instanceRecordCollection, times(0)).update(any(Instance.class), any(), any());
  }

  @Test
  void shouldFailIfPayloadContainsCentralTenantIdAndSharedInstanceButHasNoPermissionForSharedInstanceUpdate() {
    Record incomingRecord = new Record().withSnapshotId(jobExecutionId)
      .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));

    HashMap<String, String> context = new HashMap<>();
    context.put(CENTRAL_TENANT_ID_KEY, consortiumTenant);
    context.put(DataImportHeaders.USER_ID, USER_ID);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC.toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_MATCHED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(context)
      .withJobExecutionId(UUID.randomUUID().toString());
    assertEquals(consortiumTenant, dataImportEventPayload.getContext().get(CENTRAL_TENANT_ID_KEY));

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertEquals(USER_HAS_NO_PERMISSION_MSG, exception.getCause().getMessage());
  }

  @Test
  void shouldFailIfPayloadHasShadowInstanceButHasNoPermissionForSharedInstanceUpdate() {
    Record incomingRecord = new Record().withSnapshotId(jobExecutionId)
      .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", CONSORTIUM_MARC.getValue())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_MATCHED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertEquals(USER_HAS_NO_PERMISSION_MSG, exception.getCause().getMessage());
  }

  @Test
  void shouldNotProcessEventIfContextIsNull() {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(null)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.MILLISECONDS));
  }

  @Test
  void shouldNotProcessEventIfContextIsEmpty() {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.MILLISECONDS));
  }

  @Test
  void shouldNotProcessEventIfMarcBibliographicIsNotExistsInContext() {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put("InvalidField", Json.encode(new Record()));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.MILLISECONDS));
  }

  @Test
  void shouldNotProcessEventIfMarcBibliographicIsEmptyInContext() {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.MILLISECONDS));
  }

  @Test
  void shouldNotProcessEventIfRequiredFieldIsEmpty() {
    String instanceTypeId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId),
      MissingValue.getInstance());
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(),
      JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(new JsonObject()))).encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.MILLISECONDS));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldNotProcessEventIfNatureContentFieldIsNotUuid() {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title),
      ListValue.of(Lists.newArrayList("not uuid")));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = Buffer.buffer("""
      {
        "parsedRecord": {
          "id": "990fad8b-64ec-4de4-978c-9f8bbed4c6d3",
          "content": {
            "leader": "00574nam  22001211a 4500",
            "fields": [
              {
                "035": {
                  "subfields": [
                    {
                      "a": "(in001)ybp7406411"
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "245": {
                  "subfields": [
                    {
                      "a": "titleValue"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "336": {
                  "subfields": [
                    {
                      "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "780": {
                  "subfields": [
                    {
                      "t": "Houston oil directory"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "785": {
                  "subfields": [
                    {
                      "t": "SAIS review of international affairs"
                    },
                    {
                      "x": "1945-4724"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "500": {
                  "subfields": [
                    {
                      "a": "Adaptation of Xi xiang ji by Wang Shifu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "520": {
                  "subfields": [
                    {
                      "a": "Ben shu miao shu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "999": {
                  "subfields": [
                    {
                      "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                    }
                  ],
                  "ind1": "f",
                  "ind2": "f"
                }
              }
            ]
          }
        }
      }
      """);
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapperWithNatureOfContentTerm.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
  }

  @Test()
  void shouldNotUpdatedInstanceIfStatisticalCodeIdIsInvalid() {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";
    mockInstance(MARC_INSTANCE_SOURCE);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(
      StringValue.of(instanceTypeId),
      StringValue.of(title),
      ListValue.of(Lists.newArrayList("ebookss"))
    );

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record incomingRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withJobExecutionId(UUID.randomUUID().toString())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapperWithStatisticalCode.getChildSnapshotWrappers().getFirst());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
    assertThat(exception.getMessage(),
      containsString("Provided Statistical code(s) are not a valid values: 'ebookss'."));
  }

  @Test
  void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() {
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(),
      Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))));
    context.put(INSTANCE.value(), new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_MATCHED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile))
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = assertThrows(ExecutionException.class, future::get);
    assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test
  void shouldNotProcessEventIfOlErrorExists() {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    Instance returnedInstance =
      new Instance(UUID.randomUUID().toString(), INSTANCE_VERSION, UUID.randomUUID().toString(), "source", "title",
        instanceTypeId);
    returnedInstance.setTags(List.of("firstTag"));

    mockInstance(MARC_INSTANCE_SOURCE);

    when(instanceRecordCollection.findById(anyString())).thenReturn(completedFuture(returnedInstance));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure(
        "Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed "
        + "(optimistic locking): Stored _version is 2, _version of request is 1",
        409));
      return null;
    }).when(instanceRecordCollection).update(any(), any(), any());

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(20, TimeUnit.SECONDS));
  }

  @Test
  void shouldNotRequestMarcRecordIfInstanceSourceIsNotMarc()
    throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String newTitle = "test title";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(newTitle));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    mockInstance(FOLIO.getValue());

    String recordId = UUID.randomUUID().toString();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    marcRecord.setId(recordId);

    Buffer buffer = Buffer.buffer(String.format(RESPONSE_CONTENT, recordId, recordId));
    HttpResponse<Buffer> respForCreated = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_CREATED);

    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(respForCreated));

    JsonObject instanceJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", "FOLIO")
      .put("_version", INSTANCE_VERSION);

    var context = new HashMap<String, String>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), instanceJson.encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withContext(context);

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertEquals(newTitle, createdInstance.getString("title"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertTrue(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    WIRE_MOCK.verify(0, getRequestedFor(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/.{36}"), true)));

    verify(sourceStorageClient).postSourceStorageRecords(recordCaptor.capture());
    assertNotNull(recordId, recordCaptor.getValue().getMatchedId());
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldProcessEventEvenIfRecordIsNotExistsInSrs()
    throws InterruptedException, ExecutionException, TimeoutException {
    WIRE_MOCK.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/.{36}" + "/formatted"), true))
      .willReturn(WireMock.notFound()));

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = Buffer.buffer("""
      {
        "parsedRecord": {
          "id": "990fad8b-64ec-4de4-978c-9f8bbed4c6d3",
          "content": {
            "leader": "00574nam  22001211a 4500",
            "fields": [
              {
                "035": {
                  "subfields": [
                    {
                      "a": "(in001)ybp7406411"
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "245": {
                  "subfields": [
                    {
                      "a": "titleValue"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "336": {
                  "subfields": [
                    {
                      "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "780": {
                  "subfields": [
                    {
                      "t": "Houston oil directory"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "785": {
                  "subfields": [
                    {
                      "t": "SAIS review of international affairs"
                    },
                    {
                      "x": "1945-4724"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "500": {
                  "subfields": [
                    {
                      "a": "Adaptation of Xi xiang ji by Wang Shifu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "520": {
                  "subfields": [
                    {
                      "a": "Ben shu miao shu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "999": {
                  "subfields": [
                    {
                      "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                    }
                  ],
                  "ind1": "f",
                  "ind2": "f"
                }
              }
            ]
          }
        }
      }
      """);
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);

    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    verify(sourceStorageClient, times(0)).postSourceStorageRecords(any(), any());
    WIRE_MOCK.verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldUpdateInstanceWithoutRelatedMarcRecord()
    throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";
    HttpResponse<Buffer> recordHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_NOT_FOUND);

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any())).thenReturn(
      Future.succeededFuture(recordHttpResponse));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record srsRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(srsRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = Buffer.buffer("""
      {
        "parsedRecord": {
          "id": "990fad8b-64ec-4de4-978c-9f8bbed4c6d3",
          "content": {
            "leader": "00574nam  22001211a 4500",
            "fields": [
              {
                "035": {
                  "subfields": [
                    {
                      "a": "(in001)ybp7406411"
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "245": {
                  "subfields": [
                    {
                      "a": "titleValue"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "336": {
                  "subfields": [
                    {
                      "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "780": {
                  "subfields": [
                    {
                      "t": "Houston oil directory"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "785": {
                  "subfields": [
                    {
                      "t": "SAIS review of international affairs"
                    },
                    {
                      "x": "1945-4724"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "500": {
                  "subfields": [
                    {
                      "a": "Adaptation of Xi xiang ji by Wang Shifu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "520": {
                  "subfields": [
                    {
                      "a": "Ben shu miao shu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "999": {
                  "subfields": [
                    {
                      "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                    }
                  ],
                  "ind1": "f",
                  "ind2": "f"
                }
              }
            ]
          }
        }
      }
      """);
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_CREATED);
    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    verify(sourceStorageClient, times(1))
      .postSourceStorageRecords(argThat(r -> r.getMatchedId() != null && r.getId() != null));
    verify(sourceStorageClient, times(0)).putSourceStorageRecordsGenerationById(any(), any());
    WIRE_MOCK.verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldProcessEventWithExternalEntity() throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());
    String newInstanceId = UUID.randomUUID().toString();
    String newInstanceHrid = UUID.randomUUID().toString();

    String recordId = UUID.randomUUID().toString();
    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(newInstanceId).withInstanceHrid(newInstanceHrid));
    marcRecord.setMatchedId(recordId);

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", newInstanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    Instance returnedInstance = new Instance(newInstanceId, INSTANCE_VERSION,
      UUID.randomUUID().toString(), MARC_INSTANCE_SOURCE, "title", "instanceTypeId")
      .setDiscoverySuppress(false);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(), any());

    Buffer buffer = Buffer.buffer(String.format(RESPONSE_CONTENT, recordId, recordId));
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    WIRE_MOCK.verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldProcessEventAndUpdateSuppressFromDiscovery()
    throws InterruptedException, ExecutionException, TimeoutException {
    final String instanceTypeId = UUID.randomUUID().toString();
    final String recordId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId),
      BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withId(recordId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = Buffer.buffer("""
      {
        "parsedRecord": {
          "id": "990fad8b-64ec-4de4-978c-9f8bbed4c6d3",
          "content": {
            "leader": "00574nam  22001211a 4500",
            "fields": [
              {
                "035": {
                  "subfields": [
                    {
                      "a": "(in001)ybp7406411"
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "245": {
                  "subfields": [
                    {
                      "a": "titleValue"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "336": {
                  "subfields": [
                    {
                      "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "780": {
                  "subfields": [
                    {
                      "t": "Houston oil directory"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "785": {
                  "subfields": [
                    {
                      "t": "SAIS review of international affairs"
                    },
                    {
                      "x": "1945-4724"
                    }
                  ],
                  "ind1": "0",
                  "ind2": "0"
                }
              },
              {
                "500": {
                  "subfields": [
                    {
                      "a": "Adaptation of Xi xiang ji by Wang Shifu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "520": {
                  "subfields": [
                    {
                      "a": "Ben shu miao shu."
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "999": {
                  "subfields": [
                    {
                      "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                    }
                  ],
                  "ind1": "f",
                  "ind2": "f"
                }
              }
            ]
          }
        }
      }
      """);
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    Instance returnedInstance = new Instance(instanceTypeId, INSTANCE_VERSION,
      UUID.randomUUID().toString(), MARC_INSTANCE_SOURCE, "title", "instanceTypeId");

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(), any());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapperWithSuppressFromDiscovery.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    verify(sourceStorageClient).putSourceStorageRecordsGenerationById(anyString(), recordCaptor.capture());
    Record capturedRecord = recordCaptor.getValue();
    assertTrue(capturedRecord.getAdditionalInfo().getSuppressDiscovery());

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    assertThat(createdInstance.getString("discoverySuppress"), is("true"));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    WIRE_MOCK.verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldRemove035FieldWhenRecordContainsHrId() throws Exception {
    final String hrId = "in00000000052";
    final String instanceTypeId = UUID.randomUUID().toString();
    final String recordId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId),
      BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord()
        .withContent(readFileFromPath("src/test/resources/marc/record_with_001_in_035.json")))
      .withRecordType(Record.RecordType.MARC_BIB)
      .withId(recordId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", hrId)
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = Buffer.buffer(JsonObject.mapFrom(marcRecord).encode());
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    Instance returnedInstance = new Instance(instanceTypeId, INSTANCE_VERSION,
      hrId, MARC_INSTANCE_SOURCE, "title", "instanceTypeId");

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(), any());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapperWithSuppressFromDiscovery.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    verify(sourceStorageClient).putSourceStorageRecordsGenerationById(anyString(), recordCaptor.capture());
    Record capturedRecord = recordCaptor.getValue();
    // check that only one hrid is presented in marc file, only in hrid field but not in 035
    assertEquals(1, StringUtils.countMatches(capturedRecord.getParsedRecord().getContent().toString(), hrId));
    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject updatedInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(updatedInstance.getString("id"));
    assertEquals(MARC_INSTANCE_SOURCE, updatedInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertThat(updatedInstance.getString("hrid"), is(hrId));
    // incoming marc file had 2 035 fields, check that only 1 identifier remains without hrid identifier
    JsonArray identifiers = updatedInstance.getJsonArray("identifiers");
    assertThat(identifiers.size(), is(1));
    assertThat(identifiers.getJsonObject(0).getString("value"), is("393893"));
  }

  @Test
  void shouldProcessEventAnd() {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(Buffer.buffer("{}"), HttpStatus.SC_BAD_REQUEST);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(5, TimeUnit.SECONDS));
  }

  @Test
  void shouldNotProcessEventIfSourceLinkedData() {
    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", LINKED_DATA_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    assertThrows(ExecutionException.class, () -> future.get(10, TimeUnit.SECONDS));
  }

  @Test
  void isEligibleShouldReturnTrue() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_UPDATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst());
    assertTrue(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  void isPostProcessingNeededShouldReturnTrue() {
    assertFalse(replaceInstanceEventHandler.isPostProcessingNeeded());
  }

  @Test
  void shouldReturnPostProcessingInitializationEventType() {
    assertEquals(DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value(),
      replaceInstanceEventHandler.getPostProcessingInitializationEventType());
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  @SneakyThrows
  void shouldNotUpdateLinksWhenIncomingZeroSubfieldIsSameAsExisting() {
    // given
    final var incomingParsedContent = """
      {
        "leader": "02340cam a2200301Ki 4500",
        "fields": [
          {
            "001": "ybp7406411"
          },
          {
            "100": {
              "subfields": [
                {
                  "a": "Chin, Staceyann Test,"
                },
                {
                  "e": "author updated."
                },
                {
                  "0": "http://id.loc.gov/authorities/names/n2008052404"
                }
              ],
              "ind1": "1",
              "ind2": " "
            }
          }
        ]
      }
      """;
    final var expectedParsedContent = """
      {
        "leader": "00220cam a2200061Ki 4500",
        "fields": [
          {
            "001": "ybp7406411"
          },
          {
            "100": {
              "subfields": [
                {
                  "a": "Chin, Staceyann Test,"
                },
                {
                  "e": "author updated."
                },
                {
                  "0": "http://id.loc.gov/authorities/names/n2008052404"
                },
                {
                  "9": "5a56ffa8-e274-40ca-8620-34a23b5b45dd"
                }
              ],
              "ind1": "1",
              "ind2": " "
            }
          }
        ]
      }
      """;

    var instanceTypeId = UUID.randomUUID().toString();
    var title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    var context = new HashMap<String, String>();
    var recordId = UUID.randomUUID().toString();
    var incomingRecord = new Record().withId(recordId).withMatchedId(recordId)
      .withParsedRecord(new ParsedRecord().withContent(incomingParsedContent));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(incomingRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    var expectedRecord = new Record()
      .withId(recordId)
      .withMatchedId(recordId)
      .withParsedRecord(new ParsedRecord()
        .withId(recordId)
        .withContent(new JsonObject(expectedParsedContent)));
    var buffer = Buffer.buffer(JsonObject.mapFrom(expectedRecord).encode());
    var respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.succeededFuture(respForPass));
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    var authorityId = "5a56ffa8-e274-40ca-8620-34a23b5b45dd";
    var links = new InstanceLinkDtoCollection()
      .withLinks(List.of(new Link()
        .withInstanceId(instanceId.toString())
        .withId(2)
        .withLinkingRuleId(1)
        .withAuthorityId(authorityId)
        .withAuthorityNaturalId("n2008052404")));
    when(instanceLinkClient.getLinksByInstanceId(eq(instanceId.toString()), any())).thenReturn(
      completedFuture(Optional.of(links)));

    var dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    var future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    var actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    var createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertNotNull(createdInstance.getString("id"));
    assertEquals(title, createdInstance.getString("title"));
    assertEquals(instanceTypeId, createdInstance.getString("instanceTypeId"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    var updatedBib = actualDataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value());
    var updatedBibContent = new JsonObject(updatedBib).getJsonObject("parsedRecord").getString("content");
    assertThat(new JsonObject(updatedBibContent), is(new JsonObject(expectedParsedContent)));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    WIRE_MOCK.verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));

    verify(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), recordCaptor.capture());
    assertThat(recordCaptor.getValue().getParsedRecord().getContent().toString(), containsString(authorityId));
    verify(instanceLinkClient, times(0)).updateInstanceLinks(any(), any(), any());
  }

  @SuppressWarnings("checkstyle:MethodLength")
  @Test
  void shouldDeleteAdministrativeNote_whenDeleteIncomingActionIsApplied()
    throws InterruptedException, ExecutionException, TimeoutException {
    // arrange
    final String instanceTypeId = UUID.randomUUID().toString();
    final String title = "titleValue";
    final String noteToDelete = "Withdrawn as part of workflow";
    final String noteToKeep1 = "Catalogued by staff";
    final String noteToKeep2 = "Source: OCLC";
    final String noteToKeep3 = "Review pending";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(
      StringValue.of(instanceTypeId),
      StringValue.of(title),
      ListValue.of(Lists.newArrayList(noteToDelete), MappingRule.RepeatableFieldAction.DELETE_INCOMING));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    Instance existingInstance = new Instance(instanceId.toString(), INSTANCE_VERSION,
      instanceHrid, MARC_INSTANCE_SOURCE, "title", "instanceTypeId");
    existingInstance.setAdministrativeNotes(Lists.newArrayList(
      noteToKeep1, noteToDelete, noteToKeep2, noteToDelete, noteToDelete, noteToKeep3, noteToDelete));

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(), any());

    HashMap<String, String> context = new HashMap<>();
    Record marcRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(marcRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", instanceHrid)
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    Buffer buffer = Buffer.buffer("""
      {
        "parsedRecord": {
          "id": "990fad8b-64ec-4de4-978c-9f8bbed4c6d3",
          "content": {
            "leader": "00574nam  22001211a 4500",
            "fields": [
              {
                "035": {
                  "subfields": [
                    {
                      "a": "(in001)ybp7406411"
                    }
                  ],
                  "ind1": " ",
                  "ind2": " "
                }
              },
              {
                "245": {
                  "subfields": [
                    {
                      "a": "titleValue"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "336": {
                  "subfields": [
                    {
                      "b": "b6698d38-149f-11ec-82a8-0242ac130003"
                    }
                  ],
                  "ind1": "1",
                  "ind2": "0"
                }
              },
              {
                "999": {
                  "subfields": [
                    {
                      "i": "4d4545df-b5ba-4031-a031-70b1c1b2fc5d"
                    }
                  ],
                  "ind1": "f",
                  "ind2": "f"
                }
              }
            ]
          }
        }
      }
      """);
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(
      Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapperWithDeleteAdministrativeNote.getChildSnapshotWrappers().getFirst())
      .withTenant(TENANT_ID)
      .withOkapiUrl(WIRE_MOCK.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    // act
    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    // assert
    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject updatedInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonArray administrativeNotes = updatedInstance.getJsonArray("administrativeNotes");
    assertThat(administrativeNotes.size(), is(3));
    assertThat(administrativeNotes.contains(noteToDelete), is(false));
    assertThat(administrativeNotes.contains(noteToKeep1), is(true));
    assertThat(administrativeNotes.contains(noteToKeep2), is(true));
    assertThat(administrativeNotes.contains(noteToKeep3), is(true));
  }

  private Response createResponse(int statusCode, String body) {
    return new Response(statusCode, body, null, null);
  }

  private void mockInstance(String sourceType) {
    mockInstance(sourceType, false);
  }

  private void mockInstance(String sourceType, boolean deleted) {
    Instance returnedInstance = new Instance(instanceId.toString(), INSTANCE_VERSION,
      instanceHrid, sourceType, "title", "instanceTypeId");
    returnedInstance.setDeleted(deleted);
    returnedInstance.setDiscoverySuppress(deleted);
    returnedInstance.setStaffSuppress(deleted);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(), any());
  }

  private static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }

  private boolean verifyParsedContentSerialization(Record sourceRecord) {
    String serializedParsedRecord = ClientHelpers.pojo2json(sourceRecord.getParsedRecord());
    JsonObject contentJson =
      MarcContentCodec.canonicalizeJson(Json.decodeValue(serializedParsedRecord, ParsedRecord.class).getContent());
    return contentJson.fieldNames().size() == 2 && contentJson.fieldNames().containsAll(List.of("fields", "leader"));
  }
}
