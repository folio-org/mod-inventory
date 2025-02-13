package org.folio.inventory.consortium.handlers;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import org.folio.Authority;
import org.folio.HttpStatus;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.Record;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.api.request.PagingParameters;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.consortium.entities.SharingInstance;
import org.folio.inventory.services.EntitiesLinksServiceImpl;
import org.folio.inventory.consortium.util.InstanceOperationsHelper;
import org.folio.inventory.consortium.util.RestDataImportHelper;
import org.folio.inventory.domain.AuthorityRecordCollection;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.inventory.storage.Storage;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.MockitoAnnotations;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;

import static org.folio.HttpStatus.HTTP_INTERNAL_SERVER_ERROR;
import static org.folio.HttpStatus.HTTP_NO_CONTENT;
import static org.folio.HttpStatus.HTTP_OK;
import static org.folio.inventory.TestUtil.buildHttpResponseWithBuffer;
import static org.folio.inventory.consortium.handlers.MarcInstanceSharingHandlerImpl.SRS_RECORD_ID_TYPE;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.INSTANCE_ID_TYPE;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.ArgumentMatchers.argThat;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
@RunWith(VertxUnitRunner.class)
public class MarcInstanceSharingHandlerImplTest {

  private static final String AUTHORITY_ID_1 = "58600684-c647-408d-bf3e-756e9055a988";
  private static final String AUTHORITY_ID_2 = "3f2923d3-6f8e-41a6-94e1-09eaf32872e0";
  private static final String INSTANCE_AUTHORITY_LINKS = "[{\"id\":1,\"authorityId\":\"58600684-c647-408d-bf3e-756e9055a988\",\"authorityNaturalId\":\"test123\",\"instanceId\":\"eb89b292-d2b7-4c36-9bfc-f816d6f96418\",\"linkingRuleId\":1,\"status\":\"ACTUAL\"},{\"id\":2,\"authorityId\":\"3f2923d3-6f8e-41a6-94e1-09eaf32872e0\",\"authorityNaturalId\":\"test123\",\"instanceId\":\"eb89b292-d2b7-4c36-9bfc-f816d6f96418\",\"linkingRuleId\":2,\"status\":\"ACTUAL\"}]";
  private static final String LINKING_RULES = "[{\"id\":1,\"bibField\":\"100\",\"authorityField\":\"100\",\"authoritySubfields\":[\"a\",\"b\",\"c\",\"d\",\"j\",\"q\"],\"validation\":{\"existence\":[{\"t\":false}]},\"autoLinkingEnabled\":true}]";
  private static final String CONSORTIUM_TENANT = "consortium";
  private static final String MEMBER_TENANT = "diku";
  private static final String INSTANCE_PATH = "src/test/resources/handlers/instance.json";

  private static Vertx vertx;
  private static HttpClient httpClient;

  private MarcInstanceSharingHandlerImpl marcHandler;
  @Mock
  private InstanceOperationsHelper instanceOperationsHelper;
  @Mock
  private SourceStorageRecordsClient sourceStorageClient;
  @Mock
  private RestDataImportHelper restDataImportHelper;
  @Mock
  private EntitiesLinksServiceImpl entitiesLinksService;
  @Mock
  private Storage storage;
  @Mock
  private AuthorityRecordCollection authorityRecordCollection;
  @Mock
  private Source source;
  @Mock
  private Target target;

  private Instance instance;
  private SharingInstance sharingInstanceMetadata;
  private Map<String, String> kafkaHeaders;

  @BeforeClass
  public static void setUpClass() {
    vertx = Vertx.vertx();
    httpClient = vertx.createHttpClient();
  }

  @Before
  public void setUp() throws IOException {
    MockitoAnnotations.openMocks(this);

    kafkaHeaders = new HashMap<>();
    instance = mock(Instance.class);
    JsonObject jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonInstance.put("source", "MARC");
    jsonInstance.put("subjects", new JsonArray().add(new JsonObject().put("authorityId", "null").put("value", "\\\"Test subject\\\"")));
    when(instance.getJsonForStorage()).thenReturn(Instance.fromJson(jsonInstance).getJsonForStorage());

    when(source.getTenantId()).thenReturn(MEMBER_TENANT);
    when(target.getTenantId()).thenReturn(CONSORTIUM_TENANT);

    sharingInstanceMetadata = mock(SharingInstance.class);
    when(sharingInstanceMetadata.getInstanceIdentifier()).thenReturn(UUID.randomUUID());
    when(sharingInstanceMetadata.getTargetTenantId()).thenReturn(CONSORTIUM_TENANT);

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(new ArrayList<>()));

    when(entitiesLinksService.getLinkingRules(any()))
      .thenReturn(Future.succeededFuture(List.of(Json.decodeValue(LINKING_RULES, LinkingRuleDto[].class))));

    when(storage.getAuthorityRecordCollection(any()))
      .thenReturn(authorityRecordCollection);
  }

  private final HttpResponse<Buffer> sourceStorageRecordsResponseBuffer =
    buildHttpResponseWithBuffer(BufferImpl.buffer(recordJson), HttpStatus.HTTP_OK);

  @Test
  public void publishInstanceTest(TestContext testContext) {
    Async async = testContext.async();

    String instanceId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String targetInstanceHrid = "consin0000000000101";
    Record record = sourceStorageRecordsResponseBuffer.bodyAsJson(Record.class);

    //given
    marcHandler = spy(new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, storage, vertx, httpClient));
    setField(marcHandler, "restDataImportHelper", restDataImportHelper);
    setField(marcHandler, "entitiesLinksService", entitiesLinksService);

    doReturn(sourceStorageClient).when(marcHandler).getSourceStorageRecordsClient(anyString(), eq(kafkaHeaders));
    doReturn(Future.succeededFuture(record)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());

    when(restDataImportHelper.importMarcRecord(any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    doReturn(Future.succeededFuture(instanceId)).when(marcHandler).deleteSourceRecordByRecordId(any(), any(), any(), any());
    when(instance.getHrid()).thenReturn(targetInstanceHrid);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture());
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));

    doReturn(Future.succeededFuture(instanceId)).when(instanceOperationsHelper).updateInstance(any(), any());

    // when
    Future<String> future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(instanceId));

      ArgumentCaptor<Instance> updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));
      verify(marcHandler, times(0)).updateSourceRecordSuppressFromDiscoveryByInstanceId(any(), anyBoolean(), any());
      Instance updatedInstance = updatedInstanceCaptor.getValue();
      testContext.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      testContext.assertEquals(targetInstanceHrid, updatedInstance.getHrid());
      testContext.assertFalse(updatedInstance.getSubjects().isEmpty());
      async.complete();
    });

  }

  @Test
  public void publishInstanceAndRelinkAuthoritiesTest(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String instanceId = "eb89b292-d2b7-4c36-9bfc-f816d6f96418";
    String targetInstanceHrid = "consin0000000000101";

    Record record = buildHttpResponseWithBuffer(BufferImpl.buffer(recordJsonWithLinkedAuthorities), HttpStatus.HTTP_OK)
      .bodyAsJson(Record.class);

    Authority authority1 = new Authority().withId(AUTHORITY_ID_1).withSource(Authority.Source.MARC);
    Authority authority2 = new Authority().withId(AUTHORITY_ID_2).withSource(Authority.Source.CONSORTIUM_MARC);

    //given
    marcHandler = spy(new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, storage, vertx, httpClient));
    setField(marcHandler, "restDataImportHelper", restDataImportHelper);
    setField(marcHandler, "entitiesLinksService", entitiesLinksService);

    doReturn(sourceStorageClient).when(marcHandler).getSourceStorageRecordsClient(anyString(), eq(kafkaHeaders));
    doReturn(Future.succeededFuture(record)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());

    when(sharingInstanceMetadata.getInstanceIdentifier()).thenReturn(UUID.fromString(instanceId));

    when(restDataImportHelper.importMarcRecord(any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    List<Link> links =  List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(links));

    doReturn(Future.succeededFuture(instanceId)).when(marcHandler).deleteSourceRecordByRecordId(any(), any(), any(), any());
    when(instance.getHrid()).thenReturn(targetInstanceHrid);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture());
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));

    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(List.of(authority1, authority2), 2);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(authorityRecordCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", AUTHORITY_ID_1, AUTHORITY_ID_2))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    when(entitiesLinksService.putInstanceAuthorityLinks(any(), any(), any())).thenReturn(Future.succeededFuture());

    doReturn(Future.succeededFuture(instanceId)).when(instanceOperationsHelper).updateInstance(any(), any());

    // when
    Future<String> future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verify(entitiesLinksService, times(1)).putInstanceAuthorityLinks(argThat(context -> context.getTenantId().equals(CONSORTIUM_TENANT)),
      Mockito.argThat(id -> id.equals(instanceId)),
      Mockito.argThat(sharedAuthorityLinks -> sharedAuthorityLinks.size() == 1
        && sharedAuthorityLinks.get(0).getAuthorityId().equals(AUTHORITY_ID_2)));

    verify(restDataImportHelper, times(1))
      .importMarcRecord(Mockito.argThat(marcRecord ->
          JsonObject.mapFrom(marcRecord.getParsedRecord().getContent()).getJsonArray("fields").encode().equals(parsedRecordFieldsAfterUnlink)),
        any(), any());

    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(instanceId));

      ArgumentCaptor<Instance> updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));

      Instance updatedInstance = updatedInstanceCaptor.getValue();
      testContext.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      testContext.assertEquals(targetInstanceHrid, updatedInstance.getHrid());
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureIfErrorDuringRetrievingAuthoritiesDuringUnlinkingAuthorities(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String instanceId = "eb89b292-d2b7-4c36-9bfc-f816d6f96418";

    Record record = buildHttpResponseWithBuffer(BufferImpl.buffer(recordJsonWithLinkedAuthorities), HttpStatus.HTTP_OK).bodyAsJson(Record.class);

    //given
    marcHandler = spy(new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, storage, vertx, httpClient));
    setField(marcHandler, "restDataImportHelper", restDataImportHelper);
    setField(marcHandler, "entitiesLinksService", entitiesLinksService);

    doReturn(sourceStorageClient).when(marcHandler).getSourceStorageRecordsClient(anyString(), eq(kafkaHeaders));
    doReturn(Future.succeededFuture(record)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());

    when(sharingInstanceMetadata.getInstanceIdentifier()).thenReturn(UUID.fromString(instanceId));

    List<Link> links =  List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(links));

    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(3);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(authorityRecordCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", AUTHORITY_ID_1, AUTHORITY_ID_2))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    // when
    Future<String> future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verify(entitiesLinksService, times(0)).putInstanceAuthorityLinks(any(), any(), any());
    verify(restDataImportHelper, times(0)).importMarcRecord(any(), any(), any());

    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      async.complete();
    });
  }

  @Test
  public void shouldNotPutLinkInstanceAuthoritiesIfInstanceNotLinkedToSharedAuthorities(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String instanceId = "eb89b292-d2b7-4c36-9bfc-f816d6f96418";
    String targetInstanceHrid = "consin0000000000101";

    Record record = buildHttpResponseWithBuffer(BufferImpl.buffer(recordJsonWithLinkedAuthorities), HttpStatus.HTTP_OK).bodyAsJson(Record.class);

    Authority authority1 = new Authority().withId(AUTHORITY_ID_1).withSource(Authority.Source.MARC);
    Authority authority2 = new Authority().withId(AUTHORITY_ID_2).withSource(Authority.Source.MARC);

    //given
    marcHandler = spy(new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, storage, vertx, httpClient));
    setField(marcHandler, "restDataImportHelper", restDataImportHelper);
    setField(marcHandler, "entitiesLinksService", entitiesLinksService);

    doReturn(sourceStorageClient).when(marcHandler).getSourceStorageRecordsClient(anyString(), eq(kafkaHeaders));
    doReturn(Future.succeededFuture(record)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());

    when(sharingInstanceMetadata.getInstanceIdentifier()).thenReturn(UUID.fromString(instanceId));

    when(restDataImportHelper.importMarcRecord(any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    List<Link> links =  List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(links));

    doReturn(Future.succeededFuture(instanceId)).when(marcHandler).deleteSourceRecordByRecordId(any(), any(), any(), any());
    when(instance.getHrid()).thenReturn(targetInstanceHrid);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture());
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));

    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(List.of(authority1, authority2), 2);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(authorityRecordCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", AUTHORITY_ID_1, AUTHORITY_ID_2))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    when(entitiesLinksService.putInstanceAuthorityLinks(any(), any(), any())).thenReturn(Future.succeededFuture());

    doReturn(Future.succeededFuture(instanceId)).when(instanceOperationsHelper).updateInstance(any(), any());

    // when
    Future<String> future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verify(entitiesLinksService, times(0)).putInstanceAuthorityLinks(any(),
      Mockito.argThat(id -> id.equals(instanceId)),
      Mockito.argThat(sharedAuthorityLinks -> sharedAuthorityLinks.size() == 1
        && sharedAuthorityLinks.get(0).getAuthorityId().equals(AUTHORITY_ID_2)));

    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(instanceId));

      ArgumentCaptor<Instance> updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));
      Instance updatedInstance = updatedInstanceCaptor.getValue();
      testContext.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      testContext.assertEquals(targetInstanceHrid, updatedInstance.getHrid());
      async.complete();
    });
  }

  @Test
  public void publishInstanceAndNotModifyMarcRecordIfLocalAuthoritiesNotLinkedToMarcRecord(TestContext testContext) throws UnsupportedEncodingException {
    Async async = testContext.async();

    String instanceId = "eb89b292-d2b7-4c36-9bfc-f816d6f96418";
    String targetInstanceHrid = "consin0000000000101";

    Record record = buildHttpResponseWithBuffer(BufferImpl.buffer(recordJsonWithLinkedAuthorities), HttpStatus.HTTP_OK).bodyAsJson(Record.class);

    Authority authority1 = new Authority().withId(AUTHORITY_ID_1).withSource(Authority.Source.CONSORTIUM_MARC);
    Authority authority2 = new Authority().withId(AUTHORITY_ID_2).withSource(Authority.Source.CONSORTIUM_MARC);

    //given
    marcHandler = spy(new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, storage, vertx, httpClient));
    setField(marcHandler, "restDataImportHelper", restDataImportHelper);
    setField(marcHandler, "entitiesLinksService", entitiesLinksService);

    doReturn(sourceStorageClient).when(marcHandler).getSourceStorageRecordsClient(anyString(), eq(kafkaHeaders));
    doReturn(Future.succeededFuture(record)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());

    when(sharingInstanceMetadata.getInstanceIdentifier()).thenReturn(UUID.fromString(instanceId));

    when(restDataImportHelper.importMarcRecord(any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    List<Link> links =  List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(links));

    doReturn(Future.succeededFuture(instanceId)).when(marcHandler).deleteSourceRecordByRecordId(any(), any(), any(), any());
    when(instance.getHrid()).thenReturn(targetInstanceHrid);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture());
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));

    doAnswer(invocationOnMock -> {
      MultipleRecords result = new MultipleRecords<>(List.of(authority1, authority2), 2);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(authorityRecordCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", AUTHORITY_ID_1, AUTHORITY_ID_2))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    when(entitiesLinksService.putInstanceAuthorityLinks(any(), any(), any())).thenReturn(Future.succeededFuture());

    doReturn(Future.succeededFuture(instanceId)).when(instanceOperationsHelper).updateInstance(any(), any());

    // when
    Future<String> future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verify(restDataImportHelper, times(1))
      .importMarcRecord(Mockito.argThat(marcRecord ->
          JsonObject.mapFrom(marcRecord.getParsedRecord().getContent()).getString("leader").equals(new JsonObject(recordJsonWithLinkedAuthorities).getJsonObject("parsedRecord").getJsonObject("content").getString("leader"))),
        any(), any());

    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(instanceId));

      ArgumentCaptor<Instance> updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));
      Instance updatedInstance = updatedInstanceCaptor.getValue();
      testContext.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      testContext.assertEquals(targetInstanceHrid, updatedInstance.getHrid());
      async.complete();
    });
  }

  @Test
  public void getSourceMARCByInstanceIdSuccessTest() {

    String instanceId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String sourceTenant = "consortium";

    Record mockRecord = new Record();
    mockRecord.setId(instanceId);

    HttpResponse<Buffer> recordHttpResponse = mock(HttpResponse.class);

    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.succeededFuture(recordHttpResponse));

    when(recordHttpResponse.statusCode()).thenReturn(HttpStatus.HTTP_OK.toInt());
    when(recordHttpResponse.bodyAsString()).thenReturn("{\"id\":\"" + instanceId + "\"}");
    when(recordHttpResponse.bodyAsJson(Record.class)).thenReturn(mockRecord);

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.getSourceMARCByInstanceId(instanceId, sourceTenant, sourceStorageClient).onComplete(result -> {
      Record record = result.result();
      assertEquals(instanceId, record.getId());
    });
  }

  @Test
  public void getSourceMARCByInstanceIdFailTest() {

    String instanceId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String sourceTenant = "sourceTenant";

    Record mockRecord = new Record();
    mockRecord.setId(instanceId);

    HttpResponse<Buffer> recordHttpResponse = mock(HttpResponse.class);

    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    when(recordHttpResponse.statusCode()).thenReturn(HttpStatus.HTTP_OK.toInt());
    when(recordHttpResponse.bodyAsString()).thenReturn("{\"id\":\"" + instanceId + "\"}");
    when(recordHttpResponse.bodyAsJson(Record.class)).thenReturn(mockRecord);

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.getSourceMARCByInstanceId(instanceId, sourceTenant, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));
  }

  @Test
  public void deleteSourceRecordByInstanceIdSuccessTest() {

    String recordId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    String instanceId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String tenant = "sourceTenant";

    HttpResponse<Buffer> mockedResponse = mock(HttpResponse.class);

    when(mockedResponse.statusCode()).thenReturn(HTTP_NO_CONTENT.toInt());
    when(sourceStorageClient.deleteSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.succeededFuture(mockedResponse));

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.deleteSourceRecordByRecordId(recordId, instanceId, tenant, sourceStorageClient)
      .onComplete(result -> assertEquals(instanceId, result.result()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE);
  }

  @Test
  public void deleteSourceRecordByInstanceIdFailedTest() {

    String instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    String recordId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String tenant = "sourceTenant";

    when(sourceStorageClient.deleteSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.deleteSourceRecordByRecordId(recordId, instanceId, tenant, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE);
  }

  @Test
  public void deleteSourceRecordByInstanceIdFailedTestWhenResponseStatusIsNotNoContent() {
    String instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    String recordId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    String tenant = "sourceTenant";

    HttpResponse<Buffer> mockedResponse = mock(HttpResponse.class);
    when(mockedResponse.statusCode()).thenReturn(HTTP_INTERNAL_SERVER_ERROR.toInt());
    when(sourceStorageClient.deleteSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.succeededFuture(mockedResponse));

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.deleteSourceRecordByRecordId(recordId, instanceId, tenant, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE);
  }

  @Test
  public void updateSourceRecordSuppressFromDiscoveryByInstanceIdSuccessTest() {

    String instanceId = "fea6477b-d8f5-4d22-9e86-6218407c780b";

    HttpResponse<Buffer> mockedResponse = mock(HttpResponse.class);

    when(mockedResponse.statusCode()).thenReturn(HTTP_OK.toInt());
    when(sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(any(), any(), anyBoolean()))
      .thenReturn(Future.succeededFuture(mockedResponse));

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.updateSourceRecordSuppressFromDiscoveryByInstanceId(instanceId, true, sourceStorageClient)
      .onComplete(result -> assertEquals(instanceId, result.result()));

    verify(sourceStorageClient, times(1)).putSourceStorageRecordsSuppressFromDiscoveryById(instanceId, INSTANCE_ID_TYPE, true);
  }

  @Test
  public void updateSourceRecordSuppressFromDiscoveryByInstanceIdFailedTest() {

    String instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";

    when(sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(any(), any(), anyBoolean()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.updateSourceRecordSuppressFromDiscoveryByInstanceId(instanceId, true, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).putSourceStorageRecordsSuppressFromDiscoveryById(instanceId, INSTANCE_ID_TYPE, true);
  }

  @Test
  public void updateSourceRecordSuppressFromDiscoveryByInstanceIdFailedTestWhenResponseStatusIsNotOk() {

    String instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";

    HttpResponse<Buffer> mockedResponse = mock(HttpResponse.class);

    when(mockedResponse.statusCode()).thenReturn(HTTP_INTERNAL_SERVER_ERROR.toInt());
    when(sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(any(), any(), anyBoolean()))
      .thenReturn(Future.succeededFuture(mockedResponse));

    MarcInstanceSharingHandlerImpl handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.updateSourceRecordSuppressFromDiscoveryByInstanceId(instanceId, true, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).putSourceStorageRecordsSuppressFromDiscoveryById(instanceId, INSTANCE_ID_TYPE, true);
  }

  @Test
  public void shouldPopulateTargetInstanceWithNonMarcControlledFields(TestContext testContext) {
    //given
    Async async = testContext.async();
    String instanceId = UUID.randomUUID().toString();
    String targetInstanceHrid = "consin001";
    Record record = sourceStorageRecordsResponseBuffer.bodyAsJson(Record.class);

    Instance localSourceInstance = new Instance(instanceId, "1", "001", "MARC", "testTitle", UUID.randomUUID().toString())
      .setDiscoverySuppress(Boolean.TRUE)
      .setStaffSuppress(Boolean.TRUE)
      .setDeleted(Boolean.TRUE)
      .setCatalogedDate("1970-01-01")
      .setStatusId(UUID.randomUUID().toString())
      .setStatisticalCodeIds(List.of(UUID.randomUUID().toString()))
      .setAdministrativeNotes(List.of("test-note"))
      .setNatureOfContentTermIds(List.of(UUID.randomUUID().toString()));

    Instance importedTargetInstance = new Instance(instanceId, "1", targetInstanceHrid, "MARC", "testTitle", UUID.randomUUID().toString());

    SharingInstance sharingInstanceMetadata = new SharingInstance()
      .withInstanceIdentifier(UUID.fromString(instanceId))
      .withSourceTenantId("diku")
      .withTargetTenantId(CONSORTIUM_TENANT);

    marcHandler = spy(new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, storage, vertx, httpClient));
    setField(marcHandler, "restDataImportHelper", restDataImportHelper);
    setField(marcHandler, "entitiesLinksService", entitiesLinksService);

    doReturn(sourceStorageClient).when(marcHandler).getSourceStorageRecordsClient(anyString(), eq(kafkaHeaders));
    doReturn(Future.succeededFuture(record)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());
    doReturn(Future.succeededFuture(instanceId)).when(marcHandler).deleteSourceRecordByRecordId(any(), any(), any(), any());
    doReturn(Future.succeededFuture(instanceId)).when(marcHandler).updateSourceRecordSuppressFromDiscoveryByInstanceId(any(), anyBoolean(), any());

    when(restDataImportHelper.importMarcRecord(any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(importedTargetInstance));
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture(instanceId));

    // when
    Future<String> future = marcHandler.publishInstance(localSourceInstance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(instanceId));

      ArgumentCaptor<Instance> updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> CONSORTIUM_TENANT.equals(p.getTenantId())));
      verify(marcHandler, times(1)).updateSourceRecordSuppressFromDiscoveryByInstanceId(instanceId, true, sourceStorageClient);
      Instance targetInstanceWithNonMarcData = updatedInstanceCaptor.getValue();
      testContext.assertEquals("MARC", targetInstanceWithNonMarcData.getSource());
      testContext.assertEquals(targetInstanceHrid, targetInstanceWithNonMarcData.getHrid());
      testContext.assertEquals(localSourceInstance.getDiscoverySuppress(), targetInstanceWithNonMarcData.getDiscoverySuppress());
      testContext.assertEquals(localSourceInstance.getStaffSuppress(), targetInstanceWithNonMarcData.getStaffSuppress());
      testContext.assertEquals(localSourceInstance.getDeleted(), targetInstanceWithNonMarcData.getDeleted());
      testContext.assertEquals(localSourceInstance.getCatalogedDate(), targetInstanceWithNonMarcData.getCatalogedDate());
      testContext.assertEquals(localSourceInstance.getStatusId(), targetInstanceWithNonMarcData.getStatusId());
      testContext.assertEquals(localSourceInstance.getStatisticalCodeIds(), targetInstanceWithNonMarcData.getStatisticalCodeIds());
      testContext.assertEquals(localSourceInstance.getAdministrativeNotes(), targetInstanceWithNonMarcData.getAdministrativeNotes());
      testContext.assertEquals(localSourceInstance.getNatureOfContentTermIds(), targetInstanceWithNonMarcData.getNatureOfContentTermIds());
      async.complete();
    });
  }

  private static void setField(Object instance, String fieldName, Object fieldValue) {
    try {
      Field field = instance.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(instance, fieldValue);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to set field value using reflection", e);
    }
  }

  private final static String recordJson = "{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"snapshotId\":\"7376bb73-845e-44ce-ade4-53394f7526a6\",\"matchedId\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\",\"generation\":1,\"recordType\":\"MARC_BIB\"," +
    "\"parsedRecord\":{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"content\":{\"fields\":[{\"001\":\"in00000000001\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"  2006214613\"}]}},{\"019\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"1058285745\"}]}},{\"022\":{\"ind1\":\"0\",\"ind2\":\" \"," +
    "\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}]}},{\"035\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}]}},{\"040\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"ILGA\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"ISSN RECORD\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"4\",\"subfields\":[{\"a\":\"QL640\"}]}},{\"082\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}]}},{\"130\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}]}},{\"210\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}]}},{\"222\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}]}},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Biology!!!!!\"}]}},{\"246\":{\"ind1\":\"1\",\"ind2\":\"3\",\"subfields\":[{\"a\":\"HCBBBB\"}]}},{\"260\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}]}},{\"310\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Semiannual\"}]}},{\"336\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}]}},{\"337\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}]}},{\"338\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}]}},{\"362\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"655\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Electronic journals.\"}]}},{\"710\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}]}},{\"711\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}]}},{\"776\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}]}},{\"841\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"v.1- (1992-)\"}]}},{\"856\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\"}]}}],\"leader\":\"01819cas a2200469 a 4500\"},\"formattedContent\":\"LEADER 01819cas a2200469 a 4500\\n001 in00000000001\\n006 m     o  d        \\n007 cr cnu||||||||\\n008 060504c20069999txufr pso     0   a0eng c\\n005 20230915131710.4\\n010   $a  2006214613\\n019   $a1058285745\\n022 0 $a1931-7603$l1931-7603$21\\n035   $a(OCoLC)68188263$z(OCoLC)1058285745\\n040   $aNSD$beng$cNSD$dWAU$dDLC$dHUL$dOCLCQ$dOCLCF$dOCL$dAU@\\n042   $apcc$ansdp\\n049   $aILGA\\n050 10$aISSN RECORD\\n050 14$aQL640\\n082 10$a598.1$214\\n130 0 $aHerpetological conservation and biology (Online)\\n210 0 $aHerpetol. conserv. biol.$b(Online)\\n222  0$aHerpetological conservation and biology$b(Online)\\n245 10$aBiology!!!!!\\n246 13$aHCBBBB\\n260   $a[Texarkana, Tex.] :$b[publisher not identified],$c[2006]\\n310   $aSemiannual\\n336   $atext$btxt$2rdacontent\\n337   $acomputer$bc$2rdamedia\\n338   $aonline resource$bcr$2rdacarrier\\n362 1 $aBegan with: Vol. 1, issue 1 (Sept. 2006).\\n500   $aPublished in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\\n650  0$aHerpetology$xConservation$vPeriodicals.\\n650  0$aAmphibians$xConservation$vPeriodicals.\\n650  0$aReptiles$xConservation$vPeriodicals.\\n655  0$aElectronic journals.\\n710 2 $aPartners in Amphibian and Reptile Conservation.\\n711 2 $aWorld Congress of Herpetology.\\n776 1 $tHerpetological conservation and biology (Print)$x2151-0733$w(DLC)  2009202029$w(OCoLC)427887140\\n841   $av.1- (1992-)\\n856 40$uhttp://www.herpconbio.org$zAvailable to Lehigh users\\n999 ff$s0ecd6e9f-f02f-47b7-8326-2743bfa3fc43$ifea6477b-d8f5-4d22-9e86-6218407c780b\\n\\n\"}," +
    "\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\",\"instanceHrid\":\"in00000000001\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\",\"metadata\":{\"createdDate\":\"2023-09-15T13:17:10.571+00:00\",\"updatedDate\":\"2023-09-15T13:17:10.571+00:00\"}}";

  private final static String recordJsonWithLinkedAuthorities = "{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"snapshotId\":\"7376bb73-845e-44ce-ade4-53394f7526a6\",\"matchedId\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\",\"generation\":1,\"recordType\":\"MARC_BIB\"," +
    "\"parsedRecord\":{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"content\":{\"fields\":[{\"001\":\"in00000000001\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"  2006214613\"}]}},{\"019\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"1058285745\"}]}},{\"022\":{\"ind1\":\"0\",\"ind2\":\" \"," +
    "\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}]}},{\"035\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}]}},{\"040\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"ILGA\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"ISSN RECORD\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"4\",\"subfields\":[{\"a\":\"QL640\"}]}},{\"082\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}]}},{\"130\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}]}},{\"210\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}]}},{\"222\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}]}},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Biology!!!!!\"}]}},{\"246\":{\"ind1\":\"1\",\"ind2\":\"3\",\"subfields\":[{\"a\":\"HCBBBB\"}]}},{\"260\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}]}},{\"310\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Semiannual\"}]}},{\"336\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}]}},{\"337\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}]}},{\"338\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}]}},{\"362\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"655\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Electronic journals.\"}]}},{\"100\":[{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"},{\"9\":\"58600684-c647-408d-bf3e-756e9055a988\"}]}]},{\"100\":[{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"},{\"9\":\"3f2923d3-6f8e-41a6-94e1-09eaf32872e0\"}]}]},{\"710\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}]}},{\"711\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}]}},{\"776\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}]}},{\"841\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"v.1- (1992-)\"}]}},{\"856\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\"}]}}],\"leader\":\"01819cas a2200469 a 4500\"},\"formattedContent\":\"LEADER 01819cas a2200469 a 4500\\n001 in00000000001\\n006 m     o  d        \\n007 cr cnu||||||||\\n008 060504c20069999txufr pso     0   a0eng c\\n005 20230915131710.4\\n010   $a  2006214613\\n019   $a1058285745\\n022 0 $a1931-7603$l1931-7603$21\\n035   $a(OCoLC)68188263$z(OCoLC)1058285745\\n040   $aNSD$beng$cNSD$dWAU$dDLC$dHUL$dOCLCQ$dOCLCF$dOCL$dAU@\\n042   $apcc$ansdp\\n049   $aILGA\\n050 10$aISSN RECORD\\n050 14$aQL640\\n082 10$a598.1$214\\n130 0 $aHerpetological conservation and biology (Online)\\n210 0 $aHerpetol. conserv. biol.$b(Online)\\n222  0$aHerpetological conservation and biology$b(Online)\\n245 10$aBiology!!!!!\\n246 13$aHCBBBB\\n260   $a[Texarkana, Tex.] :$b[publisher not identified],$c[2006]\\n310   $aSemiannual\\n336   $atext$btxt$2rdacontent\\n337   $acomputer$bc$2rdamedia\\n338   $aonline resource$bcr$2rdacarrier\\n362 1 $aBegan with: Vol. 1, issue 1 (Sept. 2006).\\n500   $aPublished in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\\n650  0$aHerpetology$xConservation$vPeriodicals.\\n650  0$aAmphibians$xConservation$vPeriodicals.\\n650  0$aReptiles$xConservation$vPeriodicals.\\n655  0$aElectronic journals.\\n100   $aPartners in Amphibian and Reptile Conservation.$958600684-c647-408d-bf3e-756e9055a988\\n100   $aPartners in Amphibian and Reptile Conservation.$93f2923d3-6f8e-41a6-94e1-09eaf32872e0\\n710 2 $aPartners in Amphibian and Reptile Conservation.\\n711 2 $aWorld Congress of Herpetology.\\n776 1 $tHerpetological conservation and biology (Print)$x2151-0733$w(DLC)  2009202029$w(OCoLC)427887140\\n841   $av.1- (1992-)\\n856 40$uhttp://www.herpconbio.org$zAvailable to Lehigh users\\n999 ff$s0ecd6e9f-f02f-47b7-8326-2743bfa3fc43$ifea6477b-d8f5-4d22-9e86-6218407c780b\\n\\n\"}," +
    "\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\",\"instanceHrid\":\"in00000000001\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\",\"metadata\":{\"createdDate\":\"2023-09-15T13:17:10.571+00:00\",\"updatedDate\":\"2023-09-15T13:17:10.571+00:00\"}}";

  private final static String parsedRecordFieldsAfterUnlink = "[{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"subfields\":[{\"a\":\"  2006214613\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"019\":{\"subfields\":[{\"a\":\"1058285745\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"022\":{\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"040\":{\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"042\":{\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"049\":{\"subfields\":[{\"a\":\"ILGA\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"050\":{\"subfields\":[{\"a\":\"ISSN RECORD\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"050\":{\"subfields\":[{\"a\":\"QL640\"}],\"ind1\":\"1\",\"ind2\":\"4\"}},{\"082\":{\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"130\":{\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"210\":{\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"222\":{\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"245\":{\"subfields\":[{\"a\":\"Biology!!!!!\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"246\":{\"subfields\":[{\"a\":\"HCBBBB\"}],\"ind1\":\"1\",\"ind2\":\"3\"}},{\"260\":{\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"310\":{\"subfields\":[{\"a\":\"Semiannual\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"336\":{\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"337\":{\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"338\":{\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"362\":{\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"650\":{\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"655\":{\"subfields\":[{\"a\":\"Electronic journals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"100\":{\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"100\":{\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"},{\"9\":\"3f2923d3-6f8e-41a6-94e1-09eaf32872e0\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"710\":{\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"711\":{\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"776\":{\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"841\":{\"subfields\":[{\"a\":\"v.1- (1992-)\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}],\"ind1\":\"4\",\"ind2\":\"0\"}},{\"999\":{\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]";
}
