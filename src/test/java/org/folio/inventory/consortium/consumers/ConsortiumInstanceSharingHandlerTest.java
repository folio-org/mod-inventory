package org.folio.inventory.consortium.consumers;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import io.vertx.kafka.client.consumer.KafkaConsumerRecord;
import io.vertx.kafka.client.producer.KafkaHeader;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.instances.InstanceCollection;
import org.folio.inventory.storage.Storage;
import org.folio.kafka.KafkaConfig;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.function.Consumer;

import static org.folio.inventory.consortium.consumers.ConsortiumInstanceSharingHandler.COMMITTED;
import static org.folio.inventory.consortium.consumers.ConsortiumInstanceSharingHandler.JOB_PROFILE_INFO;
import static org.folio.inventory.consortium.entities.SharingStatus.IN_PROGRESS;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_TOKEN_HEADER;
import static org.folio.rest.util.OkapiConnectionParams.OKAPI_URL_HEADER;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyLong;
import static org.mockito.ArgumentMatchers.anyMap;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.spy;
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
  private SourceStorageRecordsClient mockedSourceStorageRecordsClient;
  @Mock
  private ChangeManagerClient mockedTargetManagerClient;
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
      .thenReturn(mockedTargetInstanceCollection);

    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedTargetInstanceCollection).findById(any(String.class), any(), any());

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertx, storage, kafkaConfig);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.result().equals("Instance with InstanceId=" + instanceId +
        " is present on target tenant: university"));
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

    // when
    consortiumInstanceSharingHandler = new ConsortiumInstanceSharingHandler(vertx, storage, kafkaConfig);

    //then
    Future<String> future = consortiumInstanceSharingHandler.handle(kafkaRecord);
    future.onComplete(ar -> {
      context.assertTrue(ar.failed());
      context.assertTrue(ar.cause().getMessage()
        .contains("Error sharing Instance with InstanceId=" + instanceId + " to the target tenant=consortium. " +
          "Because the instance is not found on the source tenant=university"));
      async.complete();
    });
  }

  @Test
  public void shouldShareInstanceWithMARCSource(TestContext context) throws IOException {

    // given
    Async async = context.async();
    existingInstance = Instance.fromJson(new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH)));
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
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedSourceInstanceCollection).findById(eq(instanceId), any(), any());

    HttpResponse<Buffer> sourceStorageRecordsClientResponseBuffer =
      new HttpResponseImpl(
        null,
        200,
        "Ok",
        null,
        null,
        null,
        BufferImpl.buffer(recordJson),
        new ArrayList<String>());

    HttpResponse<Buffer> targetManagerClientJobExecutionResponseBuffer =
      new HttpResponseImpl(
        null,
        201,
        "Ok",
        null,
        null,
        null,
        BufferImpl.buffer("{\"parentJobExecutionId\":\"52dae5b1-616f-40d1-802a-aa449c6ad678\",\"jobExecutions\":[{\"id\":\"52dae5b1-616f-40d1-802a-aa449c6ad678\",\"hrId\":26,\"parentJobId\":\"52dae5b1-616f-40d1-802a-aa449c6ad678\",\"subordinationType\":\"PARENT_SINGLE\",\"sourcePath\":\"No file name\",\"fileName\":\"No file name\",\"runBy\":{\"firstName\":\"Consortium_admin\",\"lastName\":\"ADMINISTRATOR\"},\"progress\":{\"current\":0,\"total\":100},\"startedDate\":\"2023-09-19T08:25:01.912+00:00\",\"status\":\"NEW\",\"uiStatus\":\"INITIALIZATION\",\"userId\":\"9c811638-95db-428b-87e4-902c36c9770b\"}]}"),
        new ArrayList<String>());

    HttpResponse<Buffer> targetManagerClientResponseBuffer =
      new HttpResponseImpl(
        null,
        200,
        "Ok",
        null,
        null,
        null,
        null,
        new ArrayList<String>());

    HttpResponse<Buffer> targetManagerClientPostContentResponseBuffer =
      new HttpResponseImpl(
        null,
        204,
        "Ok",
        null,
        null,
        null,
        null,
        new ArrayList<String>());

    // when
    consortiumInstanceSharingHandler = spy(new ConsortiumInstanceSharingHandler(vertx, storage, kafkaConfig));

    when(consortiumInstanceSharingHandler.getSourceStorageRecordsClient(anyMap()))
      .thenReturn(mockedSourceStorageRecordsClient);

    //getting MARC record from SRS
    when(mockedSourceStorageRecordsClient.getSourceStorageRecordsFormattedById(anyString(), anyString()))
      .thenReturn(Future.succeededFuture(sourceStorageRecordsClientResponseBuffer));

    //starting data import
    when(consortiumInstanceSharingHandler.getChangeManagerClient(anyMap()))
      .thenReturn(mockedTargetManagerClient);

    //getJobExecutionId
    doAnswer(invocationOnMock -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> successHandler = invocationOnMock.getArgument(1);
      successHandler.handle(Future.succeededFuture(targetManagerClientJobExecutionResponseBuffer));
      return null;
    }).when(mockedTargetManagerClient).postChangeManagerJobExecutions(any(InitJobExecutionsRqDto.class), any());

    //link jobProfile to jobExecution
    doAnswer(invocationOnMock -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.handle(Future.succeededFuture(targetManagerClientResponseBuffer));
      return null;
    }).when(mockedTargetManagerClient).putChangeManagerJobExecutionsJobProfileById(anyString(), eq(JOB_PROFILE_INFO), any());

    //sending 1st chunk of records
    doAnswer(invocationOnMock -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> successHandler = invocationOnMock.getArgument(3);
      successHandler.handle(Future.succeededFuture(targetManagerClientPostContentResponseBuffer));
      return null;
    }).when(mockedTargetManagerClient).postChangeManagerJobExecutionsRecordsById(anyString(), eq(true), any(RawRecordsDto.class), any());

    //sending last chunk of records
    doAnswer(invocationOnMock -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> successHandler = invocationOnMock.getArgument(3);
      successHandler.handle(Future.succeededFuture(targetManagerClientPostContentResponseBuffer));
      return null;
    }).when(mockedTargetManagerClient).postChangeManagerJobExecutionsRecordsById(anyString(), eq(false), any(RawRecordsDto.class), any());

    //check data import status
    doAnswer(invocationOnMock -> {
      return Future.succeededFuture(COMMITTED);
    }).when(consortiumInstanceSharingHandler).checkDataImportStatus(anyString(), any(SharingInstance.class), anyLong(), anyInt(), any());

    //delete source records
    doAnswer(invocationOnMock -> {
      return Future.succeededFuture(targetManagerClientResponseBuffer);
    }).when(mockedSourceStorageRecordsClient).deleteSourceStorageRecordsById(anyString());

    //update source of sharing instance to CONSORTIUM-MARC
    doAnswer(invocationOnMock -> {
      Consumer<Success<Instance>> successHandler = invocationOnMock.getArgument(1);
      successHandler.accept(new Success<>(existingInstance));
      return null;
    }).when(mockedSourceInstanceCollection).update(any(Instance.class), any(), any());

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
          + " to the target tenant=consortium. Because source is SOURCE"));
      async.complete();
    });
  }

  @AfterClass
  public static void tearDownClass(TestContext context) {
    Async async = context.async();
    vertx.close(ar -> async.complete());
  }

  private String recordJson = "{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"snapshotId\":\"7376bb73-845e-44ce-ade4-53394f7526a6\",\"matchedId\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\",\"generation\":1,\"recordType\":\"MARC_BIB\",\"rawRecord\":{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"content\":\"01746cas a2200469 a 4500001001400000003000600014005001700020006001900037007001500056008004100071010001700112040005700129019001500186022002800201035003900229042001400268050001600282050001000298082001400308049000900322130005300331210003900384222005400423245001700477246001100494260006100505310001500566336002600581337002600607338003600633362004600669500014700715650004400862650004300906650004100949655002500990710005201015711003501067776010001102841001701202856005701219\\u001ein00000000001\\u001eOCoLC\\u001e20200717090254.0\\u001em     o  d        \\u001ecr cnu||||||||\\u001e060504c20069999txufr pso     0   a0eng c\\u001e  \\u001fa  2006214613\\u001e  \\u001faNSD\\u001fbeng\\u001fcNSD\\u001fdWAU\\u001fdDLC\\u001fdHUL\\u001fdOCLCQ\\u001fdOCLCF\\u001fdOCL\\u001fdAU@\\u001e  \\u001fa1058285745\\u001e0 \\u001fa1931-7603\\u001fl1931-7603\\u001f21\\u001e  \\u001fa(OCoLC)68188263\\u001fz(OCoLC)1058285745\\u001e  \\u001fapcc\\u001fansdp\\u001e10\\u001faISSN RECORD\\u001e14\\u001faQL640\\u001e10\\u001fa598.1\\u001f214\\u001e  \\u001faILGA\\u001e0 \\u001faHerpetological conservation and biology (Online)\\u001e0 \\u001faHerpetol. conserv. biol.\\u001fb(Online)\\u001e 0\\u001faHerpetological conservation and biology\\u001fb(Online)\\u001e10\\u001faBiology!!!!!\\u001e13\\u001faHCBBBB\\u001e  \\u001fa[Texarkana, Tex.] :\\u001fb[publisher not identified],\\u001fc[2006]\\u001e  \\u001faSemiannual\\u001e  \\u001fatext\\u001fbtxt\\u001f2rdacontent\\u001e  \\u001facomputer\\u001fbc\\u001f2rdamedia\\u001e  \\u001faonline resource\\u001fbcr\\u001f2rdacarrier\\u001e1 \\u001faBegan with: Vol. 1, issue 1 (Sept. 2006).\\u001e  \\u001faPublished in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\\u001e 0\\u001faHerpetology\\u001fxConservation\\u001fvPeriodicals.\\u001e 0\\u001faAmphibians\\u001fxConservation\\u001fvPeriodicals.\\u001e 0\\u001faReptiles\\u001fxConservation\\u001fvPeriodicals.\\u001e 0\\u001faElectronic journals.\\u001e2 \\u001faPartners in Amphibian and Reptile Conservation.\\u001e2 \\u001faWorld Congress of Herpetology.\\u001e1 \\u001ftHerpetological conservation and biology (Print)\\u001fx2151-0733\\u001fw(DLC)  2009202029\\u001fw(OCoLC)427887140\\u001e  \\u001fav.1- (1992-)\\u001e40\\u001fuhttp://www.herpconbio.org\\u001fzAvailable to Lehigh users\\u001e\\u001d\"},\"parsedRecord\":{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"content\":{\"fields\":[{\"001\":\"in00000000001\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"  2006214613\"}]}},{\"019\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"1058285745\"}]}},{\"022\":{\"ind1\":\"0\",\"ind2\":\" \"," +
    "\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}]}},{\"035\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}]}},{\"040\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"ILGA\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"ISSN RECORD\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"4\",\"subfields\":[{\"a\":\"QL640\"}]}},{\"082\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}]}},{\"130\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}]}},{\"210\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}]}},{\"222\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}]}},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Biology!!!!!\"}]}},{\"246\":{\"ind1\":\"1\",\"ind2\":\"3\",\"subfields\":[{\"a\":\"HCBBBB\"}]}},{\"260\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}]}},{\"310\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Semiannual\"}]}},{\"336\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}]}},{\"337\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}]}},{\"338\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}]}},{\"362\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"655\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Electronic journals.\"}]}},{\"710\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}]}},{\"711\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}]}},{\"776\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}]}},{\"841\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"v.1- (1992-)\"}]}},{\"856\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\"}]}}],\"leader\":\"01819cas a2200469 a 4500\"},\"formattedContent\":\"LEADER 01819cas a2200469 a 4500\\n001 in00000000001\\n006 m     o  d        \\n007 cr cnu||||||||\\n008 060504c20069999txufr pso     0   a0eng c\\n005 20230915131710.4\\n010   $a  2006214613\\n019   $a1058285745\\n022 0 $a1931-7603$l1931-7603$21\\n035   $a(OCoLC)68188263$z(OCoLC)1058285745\\n040   $aNSD$beng$cNSD$dWAU$dDLC$dHUL$dOCLCQ$dOCLCF$dOCL$dAU@\\n042   $apcc$ansdp\\n049   $aILGA\\n050 10$aISSN RECORD\\n050 14$aQL640\\n082 10$a598.1$214\\n130 0 $aHerpetological conservation and biology (Online)\\n210 0 $aHerpetol. conserv. biol.$b(Online)\\n222  0$aHerpetological conservation and biology$b(Online)\\n245 10$aBiology!!!!!\\n246 13$aHCBBBB\\n260   $a[Texarkana, Tex.] :$b[publisher not identified],$c[2006]\\n310   $aSemiannual\\n336   $atext$btxt$2rdacontent\\n337   $acomputer$bc$2rdamedia\\n338   $aonline resource$bcr$2rdacarrier\\n362 1 $aBegan with: Vol. 1, issue 1 (Sept. 2006).\\n500   $aPublished in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\\n650  0$aHerpetology$xConservation$vPeriodicals.\\n650  0$aAmphibians$xConservation$vPeriodicals.\\n650  0$aReptiles$xConservation$vPeriodicals.\\n655  0$aElectronic journals.\\n710 2 $aPartners in Amphibian and Reptile Conservation.\\n711 2 $aWorld Congress of Herpetology.\\n776 1 $tHerpetological conservation and biology (Print)$x2151-0733$w(DLC)  2009202029$w(OCoLC)427887140\\n841   $av.1- (1992-)\\n856 40$uhttp://www.herpconbio.org$zAvailable to Lehigh users\\n999 ff$s0ecd6e9f-f02f-47b7-8326-2743bfa3fc43$ifea6477b-d8f5-4d22-9e86-6218407c780b\\n\\n\"}," +
    "\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\",\"instanceHrid\":\"in00000000001\"}," +
    "\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\"," +
    "\"metadata\":{\"createdDate\":\"2023-09-15T13:17:10.571+00:00\",\"updatedDate\":\"2023-09-15T13:17:10.571+00:00\"}}";

}
