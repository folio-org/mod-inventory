package org.folio.inventory.dataimport.handlers.actions;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import com.google.common.collect.Lists;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.web.client.HttpResponse;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.http.HttpStatus;
import org.folio.ActionProfile;
import org.folio.DataImportEventPayload;
import org.folio.JobProfile;
import org.folio.MappingMetadataDto;
import org.folio.MappingProfile;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.entities.ConsortiumConfiguration;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.consortium.entities.SharingStatus;
import org.folio.inventory.consortium.services.ConsortiumServiceImpl;
import org.folio.inventory.dataimport.InstanceWriterFactory;
import org.folio.inventory.dataimport.cache.MappingMetadataCache;
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
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Captor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

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

import static com.github.tomakehurst.wiremock.client.WireMock.get;
import static com.github.tomakehurst.wiremock.client.WireMock.getRequestedFor;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.concurrent.CompletableFuture.completedStage;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.TestUtil.buildHttpResponseWithBuffer;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.MARC_BIB_RECORD_CREATED;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.PAYLOAD_USER_ID;
import static org.folio.inventory.dataimport.util.ParsedRecordUtil.LEADER_STATUS_DELETED;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.domain.instances.InstanceSource.MARC;
import static org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle.TITLE_KEY;
import static org.folio.rest.jaxrs.model.ProfileType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileType.MAPPING_PROFILE;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
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

public class ReplaceInstanceEventHandlerTest {

  private static final String PARSED_CONTENT = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"titleValue\"}]}},{\"336\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"b\":\"b6698d38-149f-11ec-82a8-0242ac130003\"}]}},{\"780\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"Houston oil directory\"}]}},{\"785\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"SAIS review of international affairs\"},{\"x\":\"1945-4724\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Adaptation of Xi xiang ji by Wang Shifu.\"}]}},{\"520\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\"}]}}]}";
  private static final String PARSED_CONTENT_WITH_DELETED_05 = "{\"leader\":\"01314dam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"003\":\"in001\"},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"titleValue\"}]}},{\"336\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"b\":\"b6698d38-149f-11ec-82a8-0242ac130003\"}]}},{\"780\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"Houston oil directory\"}]}},{\"785\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"SAIS review of international affairs\"},{\"x\":\"1945-4724\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Adaptation of Xi xiang ji by Wang Shifu.\"}]}},{\"520\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\"}]}}]}";
  private static final String RESPONSE_CONTENT = "{\"id\":\"%s\",\"matchedId\":\"%s\",\"generation\":1,\"parsedRecord\":{" + "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" + "}}";
  private static final String EXISTING_SRS_CONTENT = "{\"id\":\"%s\",\"matchedId\":\"%s\",\"generation\":%d,\"parsedRecord\":{" + "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" + "}}";
  private static final String MAPPING_RULES_PATH = "src/test/resources/handlers/bib-rules.json";
  private static final String MAPPING_METADATA_URL = "/mapping-metadata";
  private static final String SOURCE_RECORDS_PATH = "/source-storage/records";
  private static final String PRECEDING_SUCCEEDING_TITLES_KEY = "precedingSucceedingTitles";
  private static final String TENANT_ID = "diku";
  private static final String CENTRAL_TENANT_ID_KEY = "CENTRAL_TENANT_ID";
  private static final String CENTRAL_TENANT_INSTANCE_UPDATED_KEY = "CENTRAL_TENANT_INSTANCE_UPDATED";
  private static final String TOKEN = "dummy";
  private static final Integer INSTANCE_VERSION = 1;
  private static final String INSTANCE_VERSION_AS_STRING = "1";
  private static final String MARC_INSTANCE_SOURCE = "MARC";
  private static final String LINKED_DATA_INSTANCE_SOURCE = "LINKED_DATA";
  public static final String USER_ID = "userId";
  private final String localTenant = "tenant";
  private final String consortiumTenant = "consortiumTenant";
  private final UUID instanceId = UUID.randomUUID();
  private final String consortiumId = UUID.randomUUID().toString();
  private final String jobExecutionId = UUID.randomUUID().toString();

  @Mock
  private Storage storage;
  @Mock
  private InstanceCollection instanceRecordCollection;
  @Mock
  private OkapiHttpClient mockedClient;
  @Mock
  private ConsortiumServiceImpl consortiumServiceImpl;
  @Spy
  private MarcBibReaderFactory fakeReaderFactory = new MarcBibReaderFactory();
  @Mock
  private SourceStorageRecordsClient sourceStorageClient;

  @Mock
  private SourceStorageSnapshotsClient sourceStorageSnapshotsClient;

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Captor
  private ArgumentCaptor<Record> recordCaptor;

  private JobProfile jobProfile = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfile = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Replace preliminary Item")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(INSTANCE);

  private MappingProfile mappingProfile = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"))));

  private ProfileSnapshotWrapper profileSnapshotWrapper = new ProfileSnapshotWrapper()
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

  private JobProfile jobProfileWithSuppressFromDiscovery = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfileWithSuppressFromDiscovery = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Update Instance with suppress from discovery")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(INSTANCE);

  private MappingProfile mappingProfileWithSuppressFromDiscovery = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.discoverySuppress").withValue("true").withEnabled("true")
      )));

  private ProfileSnapshotWrapper profileSnapshotWrapperWithSuppressFromDiscovery = new ProfileSnapshotWrapper()
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

  private JobProfile jobProfileWithNatureOfContentTerm = new JobProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Create MARC Bibs with NatureOfContentTerm")
    .withDataType(JobProfile.DataType.MARC);

  private ActionProfile actionProfileWithNatureOfContentTerm = new ActionProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Replace preliminary Item with NatureOfContentTerm")
    .withAction(ActionProfile.Action.UPDATE)
    .withFolioRecord(INSTANCE);

  private MappingProfile mappingProfileWithNatureOfContentTerm = new MappingProfile()
    .withId(UUID.randomUUID().toString())
    .withName("Prelim item from MARC with NatureOfContentTerm")
    .withIncomingRecordType(EntityType.MARC_BIBLIOGRAPHIC)
    .withExistingRecordType(EntityType.INSTANCE)
    .withMappingDetails(new MappingDetail()
      .withMappingFields(Lists.newArrayList(
        new MappingRule().withPath("instance.instanceTypeId").withValue("\"instanceTypeIdExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.title").withValue("\"titleExpression\"").withEnabled("true"),
        new MappingRule().withPath("instance.natureOfContentTermIds[]").withValue("\"not uuid\"").withEnabled("true").withRepeatableFieldAction(MappingRule.RepeatableFieldAction.EXTEND_EXISTING))));

  private ProfileSnapshotWrapper profileSnapshotWrapperWithNatureOfContentTerm = new ProfileSnapshotWrapper()
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

  private ReplaceInstanceEventHandler replaceInstanceEventHandler;
  private PrecedingSucceedingTitlesHelper precedingSucceedingTitlesHelper;

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);
    MappingManager.clearReaderFactories();

    JsonObject mappingRules = new JsonObject(TestUtil.readFileFromPath(MAPPING_RULES_PATH));

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(new MappingMetadataDto()
        .withMappingParams(Json.encode(new MappingParameters()))
        .withMappingRules(mappingRules.toString())))));

    precedingSucceedingTitlesHelper = spy(new PrecedingSucceedingTitlesHelper(ctxt -> mockedClient));

    Vertx vertx = Vertx.vertx();
    replaceInstanceEventHandler = spy(new ReplaceInstanceEventHandler(storage, precedingSucceedingTitlesHelper, MappingMetadataCache.getInstance(vertx,
      vertx.createHttpClient(), true), vertx.createHttpClient(), consortiumServiceImpl));

    var recordUUID = UUID.randomUUID().toString();
    HttpResponse<Buffer> recordHttpResponse = buildHttpResponseWithBuffer(BufferImpl.buffer(String.format(EXISTING_SRS_CONTENT, recordUUID, recordUUID, 0)), HttpStatus.SC_OK);
    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.succeededFuture(recordHttpResponse));

    HttpResponse<Buffer> snapshotHttpResponse = buildHttpResponseWithBuffer(BufferImpl.buffer(Json.encode(new Snapshot())), HttpStatus.SC_CREATED);
    when(sourceStorageSnapshotsClient.postSourceStorageSnapshots(any())).thenReturn(Future.succeededFuture(snapshotHttpResponse));

    doAnswer(invocationOnMock -> {
      Instance instanceRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instanceRecord));
      return null;
    }).when(instanceRecordCollection).update(any(), any(Consumer.class), any(Consumer.class));

    doReturn(sourceStorageClient).when(replaceInstanceEventHandler).getSourceStorageRecordsClient(any(), any(), any(), any());
    doReturn(sourceStorageSnapshotsClient).when(replaceInstanceEventHandler).getSourceStorageSnapshotsClient(any(), any(), any(), any());

    doAnswer(invocationOnMock -> completedStage(createResponse(201, null)))
      .when(mockedClient).post(any(URL.class), any(JsonObject.class));
    doAnswer(invocationOnMock -> completedStage(createResponse(200, new JsonObject().encode())))
      .when(mockedClient).get(anyString());
    doAnswer(invocationOnMock -> completedStage(createResponse(204, null)))
      .when(mockedClient).delete(anyString());
  }

  @Test
  public void shouldProcessEvent() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = BufferImpl.buffer("{\"parsedRecord\":{" +
      "\"id\":\"990fad8b-64ec-4de4-978c-9f8bbed4c6d3\"," +
      "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" +
      "}}");
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
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
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @Test
  public void shouldProcessEventAndMarkInstanceAndRecordAsDeletedIfLeaderIsDeleted() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record srsRecord = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT_WITH_DELETED_05));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(srsRecord));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = BufferImpl.buffer("{\"parsedRecord\":{" +
      "\"id\":\"990fad8b-64ec-4de4-978c-9f8bbed4c6d3\"," +
      "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" +
      "}}");
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
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
      return r.getState() == Record.State.DELETED && r.getAdditionalInfo().getSuppressDiscovery() &&
        r.getDeleted() && leader.isPresent() && leader.get().equals(LEADER_STATUS_DELETED);
    }));
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @Test
  public void shouldProcessEventAndUnMarkInstanceAndRecordDeleted() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

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

    Buffer buffer = BufferImpl.buffer("{\"parsedRecord\":{" +
      "\"id\":\"990fad8b-64ec-4de4-978c-9f8bbed4c6d3\"," +
      "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" +
      "}}");
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
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
      return r.getState() == Record.State.ACTUAL && r.getAdditionalInfo().getSuppressDiscovery() &&
        !r.getDeleted() && leader.isPresent() && !leader.get().equals(LEADER_STATUS_DELETED);
    }));
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @Test
  public void shouldReplaceExistingPrecedingTitleOnInstanceUpdate() throws InterruptedException, ExecutionException {
    JsonObject existingPrecedingTitle = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put(TITLE_KEY, "Butterflies in the snow");

    JsonObject precedingSucceedingTitles = new JsonObject().put(PRECEDING_SUCCEEDING_TITLES_KEY, new JsonArray().add(existingPrecedingTitle));
    when(mockedClient.get(anyString()))
      .thenReturn(CompletableFuture.completedFuture(createResponse(HttpStatus.SC_OK, precedingSucceedingTitles.encode())));

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = BufferImpl.buffer(String.format(RESPONSE_CONTENT, UUID.randomUUID(), UUID.randomUUID()));
    HttpResponse<Buffer> resp = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(resp));

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    record.withGeneration(0);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
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
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get();

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject updatedInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));

    assertEquals(title, updatedInstance.getString("title"));
    assertThat(updatedInstance.getJsonArray("precedingTitles").size(), is(1));
    assertNotEquals(existingPrecedingTitle.getString(TITLE_KEY), updatedInstance.getJsonArray("precedingTitles").getJsonObject(0).getString(TITLE_KEY));
    assertThat(updatedInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertFalse(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    JsonObject updatedSrsMarc = new JsonObject(actualDataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));
    assertEquals(Integer.valueOf(1), updatedSrsMarc.getInteger("generation"));

    ArgumentCaptor<Set<String>> titleIdCaptor = ArgumentCaptor.forClass(Set.class);
    verify(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(titleIdCaptor.capture(), any(Context.class));
    assertTrue(titleIdCaptor.getValue().contains(existingPrecedingTitle.getString("id")));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
  }

  @Test
  public void shouldProcessEventIfConsortiumInstance() throws InterruptedException, ExecutionException, TimeoutException {
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    mockInstance(CONSORTIUM_MARC.getValue());

    JsonObject centralTenantIdResponse = new JsonObject()
      .put("userTenants", new JsonArray().add(new JsonObject().put("centralTenantId", consortiumTenant)));

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(centralTenantIdResponse))));

    JsonObject consortiumIdResponse = new JsonObject()
      .put("consortia", new JsonArray().add(new JsonObject().put("id", consortiumId)));

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/consortia"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(consortiumIdResponse))));

    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setId(UUID.randomUUID());
    sharingInstance.setSourceTenantId(consortiumTenant);
    sharingInstance.setInstanceIdentifier(instanceId);
    sharingInstance.setTargetTenantId(localTenant);
    sharingInstance.setStatus(SharingStatus.COMPLETE);

    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(sharingInstance))));

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(new ConsortiumConfiguration(consortiumTenant, consortiumId)))).when(consortiumServiceImpl).getConsortiumConfiguration(any());

    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withSnapshotId(jobExecutionId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", CONSORTIUM_MARC.getValue())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
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
    assertThat(createdInstance.getJsonArray("precedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("succeedingTitles").size(), is(1));
    assertThat(createdInstance.getJsonArray("notes").size(), is(2));
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(0).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getJsonArray("notes").getJsonObject(1).getString("instanceNoteTypeId"), notNullValue());
    assertThat(createdInstance.getString("_version"), is(INSTANCE_VERSION_AS_STRING));
    verify(mockedClient, times(2)).post(any(URL.class), any(JsonObject.class));
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(),eq(INSTANCE.value()));
    verify(replaceInstanceEventHandler).getSourceStorageSnapshotsClient(any(), any(), argThat(tenantId -> tenantId.equals(consortiumTenant)), any());
    verify(sourceStorageSnapshotsClient).postSourceStorageSnapshots(argThat(snapshot -> snapshot.getJobExecutionId().equals(record.getSnapshotId())));
    verify(replaceInstanceEventHandler).getSourceStorageRecordsClient(any(), any(), argThat(tenantId -> tenantId.equals(consortiumTenant)), any());
    verify(sourceStorageClient).getSourceStorageRecordsFormattedById(anyString(), eq(INSTANCE.value()));
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @Test(expected = ExecutionException.class)
  public void shouldShouldFailIfErrorDuringCreatingOfSnapshotForConsortiumInstance() throws InterruptedException, ExecutionException, TimeoutException {
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    mockInstance(CONSORTIUM_MARC.getValue());

    JsonObject centralTenantIdResponse = new JsonObject()
      .put("userTenants", new JsonArray().add(new JsonObject().put("centralTenantId", consortiumTenant)));

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/user-tenants"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(centralTenantIdResponse))));

    JsonObject consortiumIdResponse = new JsonObject()
      .put("consortia", new JsonArray().add(new JsonObject().put("id", consortiumId)));

    WireMock.stubFor(WireMock.get(new UrlPathPattern(new RegexPattern("/consortia"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(consortiumIdResponse))));

    SharingInstance sharingInstance = new SharingInstance();
    sharingInstance.setId(UUID.randomUUID());
    sharingInstance.setSourceTenantId(consortiumTenant);
    sharingInstance.setInstanceIdentifier(instanceId);
    sharingInstance.setTargetTenantId(localTenant);
    sharingInstance.setStatus(SharingStatus.COMPLETE);

    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(sharingInstance))));

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(new ConsortiumConfiguration(consortiumTenant, consortiumId)))).when(consortiumServiceImpl).getConsortiumConfiguration(any());

    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    HttpResponse<Buffer> snapshotHttpResponse = buildHttpResponseWithBuffer(BufferImpl.buffer("{}"), HttpStatus.SC_INTERNAL_SERVER_ERROR);
    when(sourceStorageSnapshotsClient.postSourceStorageSnapshots(any())).thenReturn(Future.succeededFuture(snapshotHttpResponse));

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withSnapshotId(jobExecutionId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", CONSORTIUM_MARC.getValue())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);
  }

  @Test
  public void shouldUpdateSharedFolioInstanceOnCentralTenantIfPayloadContainsCentralTenantIdAndSharedInstance() throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    String recordId = UUID.randomUUID().toString();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withSnapshotId(jobExecutionId);
    record.setId(recordId);

    HashMap<String, String> context = new HashMap<>();
    context.put(CENTRAL_TENANT_ID_KEY, consortiumTenant);
    context.put(PAYLOAD_USER_ID, USER_ID);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", FOLIO.toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    mockInstance(FOLIO.getValue());

    Buffer buffer = BufferImpl.buffer(String.format(RESPONSE_CONTENT, recordId, recordId));
    HttpResponse<Buffer> respForCreated = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_CREATED);

    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(respForCreated));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withOkapiUrl(mockServer.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(context)
      .withJobExecutionId(UUID.randomUUID().toString());

    assertEquals(consortiumTenant, dataImportEventPayload.getContext().get(CENTRAL_TENANT_ID_KEY));

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertTrue(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(CENTRAL_TENANT_INSTANCE_UPDATED_KEY)));
    JsonObject updatedInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));

    assertEquals(title, updatedInstance.getString("title"));
    assertEquals(MARC_INSTANCE_SOURCE, updatedInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertTrue(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));

    ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
    verify(storage).getInstanceCollection(contextCaptor.capture());
    assertEquals(consortiumTenant, contextCaptor.getValue().getTenantId());

    ArgumentCaptor<Record> recordCaptor = ArgumentCaptor.forClass(Record.class);
    verify(sourceStorageClient).postSourceStorageRecords(recordCaptor.capture());
    verify(replaceInstanceEventHandler).getSourceStorageRecordsClient(any(), any(), argThat(tenantId -> tenantId.equals(consortiumTenant)), argThat(USER_ID::equals));
    verify(replaceInstanceEventHandler).getSourceStorageSnapshotsClient(any(), any(), argThat(tenantId -> tenantId.equals(consortiumTenant)), argThat(USER_ID::equals));
    verify(sourceStorageSnapshotsClient).postSourceStorageSnapshots(argThat(snapshot -> snapshot.getJobExecutionId().equals(record.getSnapshotId())));
    assertNotNull(recordId, recordCaptor.getValue().getMatchedId());
  }

  @Test
  public void shouldUpdateSharedMarcInstanceOnCentralTenantIfPayloadContainsCentralTenantIdAndSharedInstance() throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    String recordId = UUID.randomUUID().toString();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withSnapshotId(jobExecutionId);
    record.setId(recordId);

    HashMap<String, String> context = new HashMap<>();
    context.put(CENTRAL_TENANT_ID_KEY, consortiumTenant);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC.toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    mockInstance(MARC.getValue());

    Buffer buffer = BufferImpl.buffer(String.format(RESPONSE_CONTENT, recordId, recordId));
    HttpResponse<Buffer> respForCreated = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);

    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForCreated));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withOkapiUrl(mockServer.baseUrl())
      .withTenant(TENANT_ID)
      .withToken(TOKEN)
      .withContext(context)
      .withJobExecutionId(UUID.randomUUID().toString());

    assertEquals(consortiumTenant, dataImportEventPayload.getContext().get(CENTRAL_TENANT_ID_KEY));

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertTrue(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(CENTRAL_TENANT_INSTANCE_UPDATED_KEY)));
    JsonObject updatedInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));

    assertEquals(title, updatedInstance.getString("title"));
    assertEquals(MARC_INSTANCE_SOURCE, updatedInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));

    ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
    verify(storage).getInstanceCollection(contextCaptor.capture());
    assertEquals(consortiumTenant, contextCaptor.getValue().getTenantId());

    ArgumentCaptor<Record> recordCaptor = ArgumentCaptor.forClass(Record.class);
    verify(sourceStorageClient).putSourceStorageRecordsGenerationById(any(), recordCaptor.capture());
    verify(replaceInstanceEventHandler, times(2)).getSourceStorageRecordsClient(any(), any(), argThat(tenantId -> tenantId.equals(consortiumTenant)), any());
    verify(replaceInstanceEventHandler).getSourceStorageSnapshotsClient(any(), any(), argThat(tenantId -> tenantId.equals(consortiumTenant)), any());
    verify(sourceStorageSnapshotsClient).postSourceStorageSnapshots(argThat(snapshot -> snapshot.getJobExecutionId().equals(record.getSnapshotId())));
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsNull() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(null)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfContextIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfMArcBibliographicIsNotExistsInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put("InvalidField", Json.encode(new Record()));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfMarcBibliographicIsEmptyInContext() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), "");

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfRequiredFieldIsEmpty() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), MissingValue.getInstance());

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), JsonObject.mapFrom(new Record().withParsedRecord(new ParsedRecord().withContent(new JsonObject()))).encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.MILLISECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfNatureContentFieldIsNotUUID() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title), ListValue.of(Lists.newArrayList("not uuid")));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = BufferImpl.buffer("{\"parsedRecord\":{" +
      "\"id\":\"990fad8b-64ec-4de4-978c-9f8bbed4c6d3\"," +
      "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" +
      "}}");
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapperWithNatureOfContentTerm.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(10, TimeUnit.SECONDS);
  }


  @Test
  public void shouldReturnFailedFutureIfCurrentActionProfileHasNoMappingProfile() {
    HashMap<String, String> context = new HashMap<>();
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))));
    context.put(INSTANCE.value(), new JsonObject().encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_MATCHED.value())
      .withContext(context)
      .withCurrentNode(new ProfileSnapshotWrapper()
        .withContentType(ACTION_PROFILE)
        .withContent(actionProfile))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);

    ExecutionException exception = Assert.assertThrows(ExecutionException.class, future::get);
    Assert.assertEquals(ACTION_HAS_NO_MAPPING_MSG, exception.getCause().getMessage());
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfOLErrorExists() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    Instance returnedInstance = new Instance(UUID.randomUUID().toString(), String.valueOf(INSTANCE_VERSION), UUID.randomUUID().toString(), "source", "title", instanceTypeId);
    returnedInstance.setTags(List.of("firstTag"));

    mockInstance(MARC_INSTANCE_SOURCE);

    when(instanceRecordCollection.findById(anyString())).thenReturn(CompletableFuture.completedFuture(returnedInstance));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(2);
      failureHandler.accept(new Failure("Cannot update record 601a8dc4-dee7-48eb-b03f-d02fdf0debd0 because it has been changed (optimistic locking): Stored _version is 2, _version of request is 1", 409));
      return null;
    }).when(instanceRecordCollection).update(any(), any(), any());

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(20, TimeUnit.SECONDS);
  }

  @Test
  public void shouldNotRequestMarcRecordIfInstanceSourceIsNotMarc() throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String newTitle = "test title";

    Reader mockedReader = Mockito.mock(Reader.class);
    when(mockedReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(newTitle));

    when(fakeReaderFactory.createReader()).thenReturn(mockedReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    mockInstance(FOLIO.getValue());

    String recordId = UUID.randomUUID().toString();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    record.setId(recordId);

    Buffer buffer = BufferImpl.buffer(String.format(RESPONSE_CONTENT, recordId, recordId));
    HttpResponse<Buffer> respForCreated = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_CREATED);

    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(respForCreated));

    JsonObject instanceJson = new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", "FOLIO")
      .put("_version", INSTANCE_VERSION);

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withJobExecutionId(UUID.randomUUID().toString())
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withContext(new HashMap<>() {{
        put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
        put(INSTANCE.value(), instanceJson.encode());
      }});

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    DataImportEventPayload actualDataImportEventPayload = future.get(20, TimeUnit.SECONDS);

    assertEquals(DI_INVENTORY_INSTANCE_UPDATED.value(), actualDataImportEventPayload.getEventType());
    assertNotNull(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    JsonObject createdInstance = new JsonObject(actualDataImportEventPayload.getContext().get(INSTANCE.value()));
    assertEquals(newTitle, createdInstance.getString("title"));
    assertEquals(MARC_INSTANCE_SOURCE, createdInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertTrue(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    verify(0, getRequestedFor(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/.{36}"), true)));

    ArgumentCaptor<Record> recordCaptor = ArgumentCaptor.forClass(Record.class);
    verify(sourceStorageClient).postSourceStorageRecords(recordCaptor.capture());
    assertNotNull(recordId, recordCaptor.getValue().getMatchedId());
  }

  @Test
  public void shouldProcessEventEvenIfRecordIsNotExistsInSRS() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/.{36}" + "/formatted"), true))
      .willReturn(WireMock.notFound()));

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = BufferImpl.buffer("{\"parsedRecord\":{" +
      "\"id\":\"990fad8b-64ec-4de4-978c-9f8bbed4c6d3\"," +
      "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" +
      "}}");
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);

    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForPass));

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
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
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
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
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @Test
  public void shouldUpdateInstanceWithoutRelatedMarcRecord() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";
    HttpResponse<Buffer> recordHttpResponse = buildHttpResponseWithBuffer(HttpStatus.SC_NOT_FOUND);

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);
    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any())).thenReturn(Future.succeededFuture(recordHttpResponse));

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

    Buffer buffer = BufferImpl.buffer("{\"parsedRecord\":{" +
      "\"id\":\"990fad8b-64ec-4de4-978c-9f8bbed4c6d3\"," +
      "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" +
      "}}");
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_CREATED);
    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
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
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @Test
  public void shouldProcessEventWithExternalEntity() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());
    String instanceId = UUID.randomUUID().toString();
    String instanceHrid = UUID.randomUUID().toString();

    String recordId = UUID.randomUUID().toString();
    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(instanceHrid));
    record.setMatchedId(recordId);

    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    Instance returnedInstance = new Instance(instanceId, String.valueOf(INSTANCE_VERSION),
      UUID.randomUUID().toString(), MARC_INSTANCE_SOURCE, "title", "instanceTypeId")
      .setDiscoverySuppress(false);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));

    Buffer buffer = BufferImpl.buffer(String.format(RESPONSE_CONTENT, recordId, recordId));
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
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
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @Test
  public void shouldProcessEventAndUpdateSuppressFromDiscovery() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String recordId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT)).withId(recordId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = BufferImpl.buffer("{\"parsedRecord\":{" +
      "\"id\":\"990fad8b-64ec-4de4-978c-9f8bbed4c6d3\"," +
      "\"content\":\"{\\\"leader\\\":\\\"00574nam  22001211a 4500\\\",\\\"fields\\\":[{\\\"035\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"(in001)ybp7406411\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"245\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"titleValue\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"336\\\":{\\\"subfields\\\":[{\\\"b\\\":\\\"b6698d38-149f-11ec-82a8-0242ac130003\\\"}],\\\"ind1\\\":\\\"1\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"780\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"Houston oil directory\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"785\\\":{\\\"subfields\\\":[{\\\"t\\\":\\\"SAIS review of international affairs\\\"},{\\\"x\\\":\\\"1945-4724\\\"}],\\\"ind1\\\":\\\"0\\\",\\\"ind2\\\":\\\"0\\\"}},{\\\"500\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Adaptation of Xi xiang ji by Wang Shifu.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"520\\\":{\\\"subfields\\\":[{\\\"a\\\":\\\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\\\"}],\\\"ind1\\\":\\\" \\\",\\\"ind2\\\":\\\" \\\"}},{\\\"999\\\":{\\\"subfields\\\":[{\\\"i\\\":\\\"4d4545df-b5ba-4031-a031-70b1c1b2fc5d\\\"}],\\\"ind1\\\":\\\"f\\\",\\\"ind2\\\":\\\"f\\\"}}]}\"" +
      "}}");
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForPass));

    Instance returnedInstance = new Instance(instanceTypeId, String.valueOf(INSTANCE_VERSION),
      UUID.randomUUID().toString(), MARC_INSTANCE_SOURCE, "title", "instanceTypeId");

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapperWithSuppressFromDiscovery.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
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
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @Test
  public void shouldRemove035FieldWhenRecordContainsHrId() throws Exception {
    String hrId = "in00000000052";
    String marcRecord = readFileFromPath("src/test/resources/marc/record_with_001_in_035.json");

    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String recordId = UUID.randomUUID().toString();

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), BooleanValue.of(MappingRule.BooleanFieldAction.ALL_TRUE));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord()
      .withContent(marcRecord))
      .withRecordType(Record.RecordType.MARC_BIB)
      .withId(recordId);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", hrId)
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    Buffer buffer = BufferImpl.buffer(JsonObject.mapFrom(record).encode());
    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForPass));

    Instance returnedInstance = new Instance(instanceTypeId, String.valueOf(INSTANCE_VERSION),
      hrId, MARC_INSTANCE_SOURCE, "title", "instanceTypeId");

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapperWithSuppressFromDiscovery.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
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

  @Test(expected = ExecutionException.class)
  public void shouldProcessEventAnd() throws InterruptedException, ExecutionException, TimeoutException {
    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", instanceId)
      .put("hrid", UUID.randomUUID().toString())
      .put("source", MARC_INSTANCE_SOURCE)
      .put("_version", INSTANCE_VERSION)
      .put("discoverySuppress", false)
      .encode());

    mockInstance(MARC_INSTANCE_SOURCE);

    HttpResponse<Buffer> respForPass = buildHttpResponseWithBuffer(BufferImpl.buffer("{}"), HttpStatus.SC_BAD_REQUEST);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(respForPass));

    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(context)
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
      .withTenant(TENANT_ID)
      .withOkapiUrl(mockServer.baseUrl())
      .withToken(TOKEN)
      .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(5, TimeUnit.SECONDS);
  }

  @Test(expected = ExecutionException.class)
  public void shouldNotProcessEventIfSourceLinkedData() throws InterruptedException, ExecutionException, TimeoutException {
    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
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
            .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0))
            .withTenant(TENANT_ID)
            .withOkapiUrl(mockServer.baseUrl())
            .withToken(TOKEN)
            .withJobExecutionId(UUID.randomUUID().toString());

    CompletableFuture<DataImportEventPayload> future = replaceInstanceEventHandler.handle(dataImportEventPayload);
    future.get(10, TimeUnit.SECONDS);
  }

  @Test
  public void isEligibleShouldReturnTrue() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_UPDATED.value())
      .withContext(new HashMap<>())
      .withCurrentNode(profileSnapshotWrapper.getChildSnapshotWrappers().get(0));
    assertTrue(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsEmpty() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfCurrentNodeIsNotActionProfile() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfActionIsNotCreate() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isEligibleShouldReturnFalseIfRecordIsNotInstance() {
    DataImportEventPayload dataImportEventPayload = new DataImportEventPayload()
      .withEventType(DI_INVENTORY_INSTANCE_CREATED.value())
      .withContext(new HashMap<>());
    assertFalse(replaceInstanceEventHandler.isEligible(dataImportEventPayload));
  }

  @Test
  public void isPostProcessingNeededShouldReturnTrue() {
    assertFalse(replaceInstanceEventHandler.isPostProcessingNeeded());
  }

  @Test
  public void shouldReturnPostProcessingInitializationEventType() {
    assertEquals(DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING.value(), replaceInstanceEventHandler.getPostProcessingInitializationEventType());
  }

  private Response createResponse(int statusCode, String body) {
    return new Response(statusCode, body, null, null);
  }

  private void mockInstance(String sourceType) {
    mockInstance(sourceType, false);
  }

  private void mockInstance(String sourceType, boolean deleted) {
    Instance returnedInstance = new Instance(UUID.randomUUID().toString(), String.valueOf(INSTANCE_VERSION),
      UUID.randomUUID().toString(), sourceType, "title", "instanceTypeId");
    returnedInstance.setDeleted(deleted);
    returnedInstance.setDiscoverySuppress(deleted);
    returnedInstance.setStaffSuppress(deleted);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));
  }

  private static String readFileFromPath(String path) throws IOException {
    return new String(FileUtils.readFileToByteArray(new File(path)));
  }
}
