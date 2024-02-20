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
import io.vertx.ext.web.client.impl.HttpResponseImpl;
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
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.inventory.support.http.client.OkapiHttpClient;
import org.folio.inventory.support.http.client.Response;
import org.folio.processing.mapping.MappingManager;
import org.folio.processing.mapping.defaultmapper.processor.parameters.MappingParameters;
import org.folio.processing.mapping.mapper.reader.Reader;
import org.folio.processing.mapping.mapper.reader.record.marc.MarcBibReaderFactory;
import org.folio.processing.value.MissingValue;
import org.folio.processing.value.StringValue;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.EntityType;
import org.folio.rest.jaxrs.model.ExternalIdsHolder;
import org.folio.rest.jaxrs.model.MappingDetail;
import org.folio.rest.jaxrs.model.MappingRule;
import org.folio.rest.jaxrs.model.ParsedRecord;
import org.folio.rest.jaxrs.model.ProfileSnapshotWrapper;
import org.folio.rest.jaxrs.model.Record;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;
import org.mockito.Spy;

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
import static com.github.tomakehurst.wiremock.client.WireMock.verify;
import static java.util.concurrent.CompletableFuture.completedStage;
import static org.folio.ActionProfile.FolioRecord.INSTANCE;
import static org.folio.ActionProfile.FolioRecord.MARC_BIBLIOGRAPHIC;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_CREATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_MATCHED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED;
import static org.folio.DataImportEventTypes.DI_INVENTORY_INSTANCE_UPDATED_READY_FOR_POST_PROCESSING;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.ACTION_HAS_NO_MAPPING_MSG;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.MARC_BIB_RECORD_CREATED;
import static org.folio.inventory.domain.instances.InstanceSource.CONSORTIUM_MARC;
import static org.folio.inventory.domain.instances.InstanceSource.FOLIO;
import static org.folio.inventory.domain.instances.titles.PrecedingSucceedingTitle.TITLE_KEY;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.ACTION_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.JOB_PROFILE;
import static org.folio.rest.jaxrs.model.ProfileSnapshotWrapper.ContentType.MAPPING_PROFILE;
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
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

public class ReplaceInstanceEventHandlerTest {

  private static final String PARSED_CONTENT = "{\"leader\":\"01314nam  22003851a 4500\",\"fields\":[{\"001\":\"ybp7406411\"},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"titleValue\"}]}},{\"336\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"b\":\"b6698d38-149f-11ec-82a8-0242ac130003\"}]}},{\"780\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"Houston oil directory\"}]}},{\"785\":{\"ind1\":\"0\",\"ind2\":\"0\",\"subfields\":[{\"t\":\"SAIS review of international affairs\"},{\"x\":\"1945-4724\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Adaptation of Xi xiang ji by Wang Shifu.\"}]}},{\"520\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Ben shu miao shu le cui ying ying he zhang sheng wei zheng qu hun yin zi you li jin qu zhe jian xin zhi hou, zhong cheng juan shu de ai qing gu shi. jie lu le bao ban hun yin he feng jian li jiao de zui e.\"}]}}]}";
  private final static String recordJsonResponse = "{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"snapshotId\":\"7376bb73-845e-44ce-ade4-53394f7526a6\",\"matchedId\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\",\"generation\":1,\"recordType\":\"MARC_BIB\"," +
    "\"parsedRecord\":{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"content\":{\"fields\":[{\"001\":\"in00000000001\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"  2006214613\"}]}},{\"019\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"1058285745\"}]}},{\"022\":{\"ind1\":\"0\",\"ind2\":\" \"," +
    "\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}]}},{\"035\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}]}},{\"040\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"ILGA\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"ISSN RECORD\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"4\",\"subfields\":[{\"a\":\"QL640\"}]}},{\"082\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}]}},{\"130\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}]}},{\"210\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}]}},{\"222\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}]}},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Biology!!!!!\"}]}},{\"246\":{\"ind1\":\"1\",\"ind2\":\"3\",\"subfields\":[{\"a\":\"HCBBBB\"}]}},{\"260\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}]}},{\"310\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Semiannual\"}]}},{\"336\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}]}},{\"337\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}]}},{\"338\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}]}},{\"362\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"655\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Electronic journals.\"}]}},{\"710\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}]}},{\"711\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}]}},{\"776\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}]}},{\"841\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"v.1- (1992-)\"}]}},{\"856\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\"}]}}],\"leader\":\"01819cas a2200469 a 4500\"},\"formattedContent\":\"LEADER 01819cas a2200469 a 4500\\n001 in00000000001\\n006 m     o  d        \\n007 cr cnu||||||||\\n008 060504c20069999txufr pso     0   a0eng c\\n005 20230915131710.4\\n010   $a  2006214613\\n019   $a1058285745\\n022 0 $a1931-7603$l1931-7603$21\\n035   $a(OCoLC)68188263$z(OCoLC)1058285745\\n040   $aNSD$beng$cNSD$dWAU$dDLC$dHUL$dOCLCQ$dOCLCF$dOCL$dAU@\\n042   $apcc$ansdp\\n049   $aILGA\\n050 10$aISSN RECORD\\n050 14$aQL640\\n082 10$a598.1$214\\n130 0 $aHerpetological conservation and biology (Online)\\n210 0 $aHerpetol. conserv. biol.$b(Online)\\n222  0$aHerpetological conservation and biology$b(Online)\\n245 10$aBiology!!!!!\\n246 13$aHCBBBB\\n260   $a[Texarkana, Tex.] :$b[publisher not identified],$c[2006]\\n310   $aSemiannual\\n336   $atext$btxt$2rdacontent\\n337   $acomputer$bc$2rdamedia\\n338   $aonline resource$bcr$2rdacarrier\\n362 1 $aBegan with: Vol. 1, issue 1 (Sept. 2006).\\n500   $aPublished in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\\n650  0$aHerpetology$xConservation$vPeriodicals.\\n650  0$aAmphibians$xConservation$vPeriodicals.\\n650  0$aReptiles$xConservation$vPeriodicals.\\n655  0$aElectronic journals.\\n710 2 $aPartners in Amphibian and Reptile Conservation.\\n711 2 $aWorld Congress of Herpetology.\\n776 1 $tHerpetological conservation and biology (Print)$x2151-0733$w(DLC)  2009202029$w(OCoLC)427887140\\n841   $av.1- (1992-)\\n856 40$uhttp://www.herpconbio.org$zAvailable to Lehigh users\\n999 ff$s0ecd6e9f-f02f-47b7-8326-2743bfa3fc43$ifea6477b-d8f5-4d22-9e86-6218407c780b\\n\\n\"}," +
    "\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\",\"instanceHrid\":\"in00000000001\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\",\"metadata\":{\"createdDate\":\"2023-09-15T13:17:10.571+00:00\",\"updatedDate\":\"2023-09-15T13:17:10.571+00:00\"}}";
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
  private final String localTenant = "tenant";
  private final String consortiumTenant = "consortiumTenant";
  private final UUID instanceId = UUID.randomUUID();
  private final String consortiumId = UUID.randomUUID().toString();

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

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

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

    WireMock.stubFor(get(new UrlPathPattern(new RegexPattern(SOURCE_RECORDS_PATH + "/.{36}" + "/formatted"), true))
      .willReturn(WireMock.ok(Json.encode(new Record()
        .withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))))));

    precedingSucceedingTitlesHelper = spy(new PrecedingSucceedingTitlesHelper(ctxt -> mockedClient));

    Vertx vertx = Vertx.vertx();
    replaceInstanceEventHandler = spy(new ReplaceInstanceEventHandler(storage, precedingSucceedingTitlesHelper, new MappingMetadataCache(vertx,
      vertx.createHttpClient(), 3600), vertx.createHttpClient(), consortiumServiceImpl));

    HttpResponse<Buffer> recordHttpResponse = mock(HttpResponse.class);
    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.succeededFuture(recordHttpResponse));

    doAnswer(invocationOnMock -> {
      Instance instanceRecord = invocationOnMock.getArgument(0);
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(instanceRecord));
      return null;
    }).when(instanceRecordCollection).update(any(), any(Consumer.class), any(Consumer.class));

    doReturn(sourceStorageClient).when(replaceInstanceEventHandler).getSourceStorageRecordsClient(any());

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

    Buffer buffer = BufferImpl.buffer(recordJsonResponse);
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

    Buffer buffer = BufferImpl.buffer(recordJsonResponse);
    HttpResponse<Buffer> resp = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_OK);
    when(sourceStorageClient.putSourceStorageRecordsGenerationById(any(), any())).thenReturn(Future.succeededFuture(resp));

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

    ArgumentCaptor<Set<String>> titleIdCaptor = ArgumentCaptor.forClass(Set.class);
    verify(precedingSucceedingTitlesHelper).deletePrecedingSucceedingTitles(titleIdCaptor.capture(), any(Context.class));
    assertTrue(titleIdCaptor.getValue().contains(existingPrecedingTitle.getString("id")));
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

    WireMock.stubFor(WireMock.post(new UrlPathPattern(new RegexPattern("/consortia/" + consortiumId + "/sharing/instances"), true))
      .willReturn(WireMock.ok().withBody(Json.encode(sharingInstance))));

    doAnswer(invocationOnMock -> Future.succeededFuture(Optional.of(new ConsortiumConfiguration(consortiumId, consortiumId)))).when(consortiumServiceImpl).getConsortiumConfiguration(any());

    Reader fakeReader = Mockito.mock(Reader.class);

    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));

    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
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
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
  }

  @Test
  public void shouldUpdateSharedInstanceOnCentralTenantIfPayloadContainsCentralTenantIdAndSharedInstance() throws InterruptedException, ExecutionException, TimeoutException {
    String instanceTypeId = UUID.randomUUID().toString();
    String title = "titleValue";

    Reader fakeReader = Mockito.mock(Reader.class);
    when(fakeReaderFactory.createReader()).thenReturn(fakeReader);
    when(fakeReader.read(any(MappingRule.class))).thenReturn(StringValue.of(instanceTypeId), StringValue.of(title));
    when(storage.getInstanceCollection(any())).thenReturn(instanceRecordCollection);

    MappingManager.registerReaderFactory(fakeReaderFactory);
    MappingManager.registerWriterFactory(new InstanceWriterFactory());

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
    context.put(CENTRAL_TENANT_ID_KEY, consortiumTenant);
    context.put(MARC_BIBLIOGRAPHIC.value(), Json.encode(record));
    context.put(INSTANCE.value(), new JsonObject()
      .put("id", UUID.randomUUID().toString())
      .put("hrid", UUID.randomUUID().toString())
      .put("source", FOLIO.toString())
      .put("_version", INSTANCE_VERSION)
      .encode());

    mockInstance(FOLIO.getValue());

    Buffer buffer = BufferImpl.buffer(recordJsonResponse);
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
    JsonObject createdSrsMarc = new JsonObject(actualDataImportEventPayload.getContext().get(MARC_BIBLIOGRAPHIC.value()));

    assertEquals(title, updatedInstance.getString("title"));
    assertEquals(MARC_INSTANCE_SOURCE, updatedInstance.getString("source"));
    assertTrue(actualDataImportEventPayload.getContext().containsKey(MARC_BIB_RECORD_CREATED));
    assertTrue(Boolean.parseBoolean(actualDataImportEventPayload.getContext().get(MARC_BIB_RECORD_CREATED)));
    assertNotNull(createdSrsMarc.getString("matchedId"));

    ArgumentCaptor<Context> contextCaptor = ArgumentCaptor.forClass(Context.class);
    verify(storage).getInstanceCollection(contextCaptor.capture());
    assertEquals(consortiumTenant, contextCaptor.getValue().getTenantId());
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

    Buffer buffer = BufferImpl.buffer(recordJsonResponse);
    HttpResponse<Buffer> respForCreated = buildHttpResponseWithBuffer(buffer, HttpStatus.SC_CREATED);

    when(sourceStorageClient.postSourceStorageRecords(any())).thenReturn(Future.succeededFuture(respForCreated));

    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT));
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

    Buffer buffer = BufferImpl.buffer(recordJsonResponse);
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

    HashMap<String, String> context = new HashMap<>();
    Record record = new Record().withParsedRecord(new ParsedRecord().withContent(PARSED_CONTENT))
      .withExternalIdsHolder(new ExternalIdsHolder().withInstanceId(instanceId).withInstanceHrid(instanceHrid));

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

    Buffer buffer = BufferImpl.buffer(recordJsonResponse);
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
    verify(1, getRequestedFor(new UrlPathPattern(new RegexPattern(MAPPING_METADATA_URL + "/.*"), true)));
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

  private void mockInstance(String sourceType){
    Instance returnedInstance = new Instance(UUID.randomUUID().toString(), String.valueOf(INSTANCE_VERSION),
      UUID.randomUUID().toString(), sourceType, "title", "instanceTypeId")
      .setDiscoverySuppress(false);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(returnedInstance));
      return null;
    }).when(instanceRecordCollection).findById(anyString(), any(Consumer.class), any(Consumer.class));
  }

  private static HttpResponseImpl<Buffer> buildHttpResponseWithBuffer(Buffer buffer, int httpStatus) {
    return new HttpResponseImpl<>(null, httpStatus, "",
      null, null, null, buffer, null);
  }
}
