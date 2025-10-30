package org.folio.inventory.consortium.handlers;

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

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.http.HttpClient;
import io.vertx.core.json.Json;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.function.Consumer;
import org.folio.Authority;
import org.folio.HttpStatus;
import org.folio.Link;
import org.folio.LinkingRuleDto;
import org.folio.Record;
import org.folio.inventory.TestUtil;
import org.folio.inventory.common.Context;
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

@RunWith(VertxUnitRunner.class)
public class MarcInstanceSharingHandlerImplTest {

  private static final String AUTHORITY_ID_1 = "58600684-c647-408d-bf3e-756e9055a988";
  private static final String AUTHORITY_ID_2 = "3f2923d3-6f8e-41a6-94e1-09eaf32872e0";
  private static final String INSTANCE_ID_1 = "eb89b292-d2b7-4c36-9bfc-f816d6f96418";
  private static final String INSTANCE_ID_2 = "fea6477b-d8f5-4d22-9e86-6218407c780b";
  private static final String TARGET_INSTANCE_HRID = "consin0000000000101";
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
  private Record bibRecord;
  private Authority localAuthority;
  private Authority sharedAuthority;

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
    var jsonInstance = new JsonObject(TestUtil.readFileFromPath(INSTANCE_PATH));
    jsonInstance.put("source", "MARC");
    jsonInstance.put("subjects", new JsonArray().add(new JsonObject().put("authorityId", "null").put("value", "\\\"Test subject\\\"")));
    when(instance.getJsonForStorage()).thenReturn(Instance.fromJson(jsonInstance).getJsonForStorage());

    when(source.getTenantId()).thenReturn(MEMBER_TENANT);
    when(target.getTenantId()).thenReturn(CONSORTIUM_TENANT);

    sharingInstanceMetadata = mock(SharingInstance.class);
    when(sharingInstanceMetadata.getInstanceIdentifier()).thenReturn(UUID.fromString(INSTANCE_ID_1));
    when(sharingInstanceMetadata.getTargetTenantId()).thenReturn(CONSORTIUM_TENANT);
    when(sharingInstanceMetadata.getSourceTenantId()).thenReturn(MEMBER_TENANT);

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(new ArrayList<>()));
    when(entitiesLinksService.getLinkingRules(any()))
      .thenReturn(Future.succeededFuture(List.of(Json.decodeValue(LINKING_RULES, LinkingRuleDto[].class))));

    when(storage.getAuthorityRecordCollection(any())).thenReturn(authorityRecordCollection);
    localAuthority = new Authority().withId(AUTHORITY_ID_1).withSource(Authority.Source.MARC);
    sharedAuthority = new Authority().withId(AUTHORITY_ID_2).withSource(Authority.Source.CONSORTIUM_MARC);
    setupMarcHandler();
  }

  private void setupMarcHandler() {
    marcHandler = spy(new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, storage, vertx, httpClient));
    setField(marcHandler, "restDataImportHelper", restDataImportHelper);
    setField(marcHandler, "entitiesLinksService", entitiesLinksService);
    doReturn(sourceStorageClient).when(marcHandler).getSourceStorageRecordsClient(anyString(), eq(kafkaHeaders));

    var recordWithLinkedAuthorities = buildHttpResponseWithBuffer(BufferImpl.buffer(RECORD_JSON_WITH_LINKED_AUTHORITIES), HttpStatus.HTTP_OK)
      .bodyAsJson(Record.class);
    bibRecord = sourceStorageRecordsResponseBuffer.bodyAsJson(Record.class);
    doReturn(Future.succeededFuture(recordWithLinkedAuthorities)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());
    doReturn(Future.succeededFuture(INSTANCE_ID_1)).when(marcHandler).deleteSourceRecordByRecordId(any(), any(), any(), any());
  }

  private final HttpResponse<Buffer> sourceStorageRecordsResponseBuffer =
    buildHttpResponseWithBuffer(BufferImpl.buffer(RECORD_JSON), HttpStatus.HTTP_OK);

  @Test
  public void publishInstanceTest(TestContext testContext) {
    var async = testContext.async();
    //given
    doReturn(Future.succeededFuture(bibRecord)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());

    when(restDataImportHelper.importMarcRecord(any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    doReturn(Future.succeededFuture(INSTANCE_ID_2)).when(marcHandler).deleteSourceRecordByRecordId(any(), any(), any(), any());
    when(instance.getHrid()).thenReturn(TARGET_INSTANCE_HRID);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture());
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));
    doReturn(Future.succeededFuture(INSTANCE_ID_2)).when(instanceOperationsHelper).updateInstance(any(), any());

    // when
    var future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(INSTANCE_ID_2));

      var updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));
      verify(marcHandler, times(0)).updateSourceRecordSuppressFromDiscoveryByInstanceId(any(), anyBoolean(), any());
      var updatedInstance = updatedInstanceCaptor.getValue();
      testContext.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      testContext.assertEquals(TARGET_INSTANCE_HRID, updatedInstance.getHrid());
      testContext.assertFalse(updatedInstance.getSubjects().isEmpty());
      async.complete();
    });

  }

  @Test
  public void publishInstanceAndRelinkAuthoritiesTest(TestContext testContext) throws UnsupportedEncodingException {
    var async = testContext.async();
    //given
    when(restDataImportHelper.importMarcRecord(any(), any(), any())).thenReturn(Future.succeededFuture("COMMITTED"));
    var links = List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any())).thenReturn(Future.succeededFuture(links));
    when(instance.getHrid()).thenReturn(TARGET_INSTANCE_HRID);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture(INSTANCE_ID_1));
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));
    when(entitiesLinksService.putInstanceAuthorityLinks(any(), any(), any())).thenReturn(Future.succeededFuture());
    mockSuccessRetrievingAuthorities();

    // when
    var future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verifyCleanUoAllAuthorityLinksForMemberTenant();
    verifySharedLinksCreation(1);

    verify(restDataImportHelper, times(1))
      .importMarcRecord(Mockito.argThat(marcRecord ->
          JsonObject.mapFrom(marcRecord.getParsedRecord().getContent()).getJsonArray("fields").encode().equals(PARSED_RECORD_FIELDS_AFTER_UNLINK)),
        any(), any());

    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(INSTANCE_ID_1));

      var updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));

      var updatedInstance = updatedInstanceCaptor.getValue();
      testContext.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      testContext.assertEquals(TARGET_INSTANCE_HRID, updatedInstance.getHrid());
      async.complete();
    });
  }

  @Test
  public void shouldSharedLocalInstanceAndUnlinkLocalAuthorityLinks(TestContext testContext) throws UnsupportedEncodingException {
    var async = testContext.async();
    //given
    when(restDataImportHelper.importMarcRecord(any(), any(), any())).thenReturn(Future.succeededFuture("COMMITTED"));
    var links = List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));
    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(List.of(links.getFirst())));
    when(instance.getHrid()).thenReturn(TARGET_INSTANCE_HRID);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture(INSTANCE_ID_1));
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));
    when(entitiesLinksService.putInstanceAuthorityLinks(any(), any(), any())).thenReturn(Future.succeededFuture());

    var recordWithLinkedAuthorities = buildHttpResponseWithBuffer(BufferImpl.buffer(RECORD_JSON_WITH_LOCAL_LINKED_AUTHORITIES), HttpStatus.HTTP_OK)
      .bodyAsJson(Record.class);
    doReturn(Future.succeededFuture(recordWithLinkedAuthorities)).when(marcHandler).getSourceMARCByInstanceId(any(), any(), any());

    doAnswer(invocationOnMock -> {
      var result = new MultipleRecords<>(List.of(localAuthority), 1);
      Consumer<Success<MultipleRecords<Authority>>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(authorityRecordCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s)", AUTHORITY_ID_1))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    // when
    var future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verifyCleanUoAllAuthorityLinksForMemberTenant();
    // verify no shared links were created for the central tenant as there were no shared authorities links
    verifySharedLinksCreation(0);

    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(INSTANCE_ID_1));

      verify(restDataImportHelper, times(1))
        .importMarcRecord(Mockito.argThat(marcRecord ->
            JsonObject.mapFrom(marcRecord.getParsedRecord().getContent())
              .getJsonArray("fields").encode().equals(PARSED_RECORD_FIELDS_AFTER_UNLINK_LOCAL_LINKS)),
          any(), any());

      var updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));

      var updatedInstance = updatedInstanceCaptor.getValue();
      testContext.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      testContext.assertEquals(TARGET_INSTANCE_HRID, updatedInstance.getHrid());
      async.complete();
    });
  }

  @Test
  public void shouldRollbackAuthorityLinksWhenUpdateOfSharedLinksFails(TestContext testContext) throws UnsupportedEncodingException {
    var async = testContext.async();

    //given
    mockSuccessRetrievingAuthorities();
    var links = List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));
    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any())).thenReturn(Future.succeededFuture(links));
    when(instance.getHrid()).thenReturn(TARGET_INSTANCE_HRID);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture(INSTANCE_ID_1));
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));
    when(restDataImportHelper.importMarcRecord(any(), any(), any())).thenReturn(Future.succeededFuture("COMMITTED"));
    when(entitiesLinksService.putInstanceAuthorityLinks(any(Context.class), eq(INSTANCE_ID_1), eq(List.of()))).thenReturn(Future.succeededFuture());

    when(entitiesLinksService.putInstanceAuthorityLinks(any(Context.class), eq(INSTANCE_ID_1), eq(List.of(links.get(1))))).thenReturn(Future.failedFuture(
      new RuntimeException("Failed to put shared Authority links for central tenant")));

    // when
    var future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verifyCleanUoAllAuthorityLinksForMemberTenant();
    verifySharedLinksCreation(1);
    verifyRollbackAuthorityLinksForMemberTenant(links);

    future.onFailure(ar -> {
      var updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper, times(0)).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));
      async.complete();
    });
  }

  @Test
  public void shouldRollbackAuthorityLinksWhenAuthorityLinksCleanupFails(TestContext testContext) throws UnsupportedEncodingException {
    var async = testContext.async();

    //given
    mockSuccessRetrievingAuthorities();
    var links = List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));
    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any())).thenReturn(Future.succeededFuture(links));
    when(instance.getHrid()).thenReturn(TARGET_INSTANCE_HRID);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture(INSTANCE_ID_1));
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));
    when(restDataImportHelper.importMarcRecord(any(), any(), any())).thenReturn(Future.succeededFuture("COMMITTED"));
    when(entitiesLinksService.putInstanceAuthorityLinks(any(Context.class), eq(INSTANCE_ID_1), eq(List.of()))).thenReturn(Future.failedFuture(
      new RuntimeException("Failed to clean-up Authority links for member tenant")));

    // when
    var future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verifyCleanUoAllAuthorityLinksForMemberTenant();
    // verify no shared links were created for the central tenant as cleanup failed
    verifySharedLinksCreation(0);

    future.onFailure(ar -> {
      var updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper, times(0)).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));
      async.complete();
    });
  }

  @Test
  public void shouldReturnFailedFutureIfErrorDuringRetrievingAuthoritiesDuringUnlinkingAuthorities(TestContext testContext) throws UnsupportedEncodingException {
    var async = testContext.async();
    //given
    var links = List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(links));
    when(entitiesLinksService.putInstanceAuthorityLinks(any(), any(), any())).thenReturn(Future.succeededFuture());
    mockErrorDuringRetrievingAuthorities();

    // when
    var future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    future.onComplete(ar -> {
      testContext.assertTrue(ar.failed());
      verify(restDataImportHelper, times(0)).importMarcRecord(any(), any(), any());
      verifyCleanUoAllAuthorityLinksForMemberTenant();
      verifyRollbackAuthorityLinksForMemberTenant(links);
      // no shared links should be created for the central tenant
      verifySharedLinksCreation(0);
      async.complete();
    });
  }

  @Test
  public void shouldNotPutLinkInstanceAuthoritiesIfInstanceNotLinkedToSharedAuthorities(TestContext testContext) throws UnsupportedEncodingException {
    var async = testContext.async();
    var authority1 = new Authority().withId(AUTHORITY_ID_1).withSource(Authority.Source.MARC);
    var authority2 = new Authority().withId(AUTHORITY_ID_2).withSource(Authority.Source.MARC);

    //given
    when(restDataImportHelper.importMarcRecord(any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    var links = List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(links));
    when(instance.getHrid()).thenReturn(TARGET_INSTANCE_HRID);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture());
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));

    doAnswer(invocationOnMock -> {
      var result = new MultipleRecords<>(List.of(authority1, authority2), 2);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(authorityRecordCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", AUTHORITY_ID_1, AUTHORITY_ID_2))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    when(entitiesLinksService.putInstanceAuthorityLinks(any(), any(), any())).thenReturn(Future.succeededFuture());

    doReturn(Future.succeededFuture(INSTANCE_ID_1)).when(instanceOperationsHelper).updateInstance(any(), any());

    // when
    var future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verify(entitiesLinksService, times(0)).putInstanceAuthorityLinks(any(),
      Mockito.argThat(id -> id.equals(INSTANCE_ID_1)),
      Mockito.argThat(sharedAuthorityLinks -> sharedAuthorityLinks.size() == 1
        && sharedAuthorityLinks.getFirst().getAuthorityId().equals(AUTHORITY_ID_2)));

    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(INSTANCE_ID_1));

      var updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));
      var updatedInstance = updatedInstanceCaptor.getValue();
      testContext.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      testContext.assertEquals(TARGET_INSTANCE_HRID, updatedInstance.getHrid());
      async.complete();
    });
  }

  @Test
  public void publishInstanceAndNotModifyMarcRecordIfLocalAuthoritiesNotLinkedToMarcRecord(TestContext testContext) throws UnsupportedEncodingException {
    var async = testContext.async();
    var authority1 = new Authority().withId(AUTHORITY_ID_1).withSource(Authority.Source.CONSORTIUM_MARC);
    var authority2 = new Authority().withId(AUTHORITY_ID_2).withSource(Authority.Source.CONSORTIUM_MARC);

    //given
    when(restDataImportHelper.importMarcRecord(any(), any(), any()))
      .thenReturn(Future.succeededFuture("COMMITTED"));

    var links = List.of(Json.decodeValue(INSTANCE_AUTHORITY_LINKS, Link[].class));

    when(entitiesLinksService.getInstanceAuthorityLinks(any(), any()))
      .thenReturn(Future.succeededFuture(links));


    when(instance.getHrid()).thenReturn(TARGET_INSTANCE_HRID);
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture());
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(instance));

    doAnswer(invocationOnMock -> {
      var result = new MultipleRecords<>(List.of(authority1, authority2), 2);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(authorityRecordCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", AUTHORITY_ID_1, AUTHORITY_ID_2))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));

    when(entitiesLinksService.putInstanceAuthorityLinks(any(), any(), any())).thenReturn(Future.succeededFuture());

    doReturn(Future.succeededFuture(INSTANCE_ID_1)).when(instanceOperationsHelper).updateInstance(any(), any());

    // when
    var future = marcHandler.publishInstance(instance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    verify(restDataImportHelper, times(1))
      .importMarcRecord(Mockito.argThat(marcRecord ->
          JsonObject.mapFrom(marcRecord.getParsedRecord().getContent()).getString("leader").equals(new JsonObject(RECORD_JSON_WITH_LINKED_AUTHORITIES).getJsonObject("parsedRecord").getJsonObject("content").getString("leader"))),
        any(), any());

    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(INSTANCE_ID_1));

      var updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> MEMBER_TENANT.equals(p.getTenantId())));
      var updatedInstance = updatedInstanceCaptor.getValue();
      testContext.assertEquals("CONSORTIUM-MARC", updatedInstance.getSource());
      testContext.assertEquals(TARGET_INSTANCE_HRID, updatedInstance.getHrid());
      async.complete();
    });
  }

  @Test
  public void getSourceMARCByInstanceIdSuccessTest() {
    var sourceTenant = "consortium";

    var mockRecord = new Record();
    mockRecord.setId(INSTANCE_ID_2);

    HttpResponse<Buffer> recordHttpResponse = mock(HttpResponse.class);

    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.succeededFuture(recordHttpResponse));

    when(recordHttpResponse.statusCode()).thenReturn(HttpStatus.HTTP_OK.toInt());
    when(recordHttpResponse.bodyAsString()).thenReturn("{\"id\":\"" + INSTANCE_ID_2 + "\"}");
    when(recordHttpResponse.bodyAsJson(Record.class)).thenReturn(mockRecord);

    var handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.getSourceMARCByInstanceId(INSTANCE_ID_2, sourceTenant, sourceStorageClient).onComplete(result -> {
      var resultRecord = result.result();
      assertEquals(INSTANCE_ID_2, resultRecord.getId());
    });
  }

  @Test
  public void getSourceMARCByInstanceIdFailTest() {
    var sourceTenant = "sourceTenant";

    var mockRecord = new Record();
    mockRecord.setId(INSTANCE_ID_2);

    HttpResponse<Buffer> recordHttpResponse = mock(HttpResponse.class);

    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    when(recordHttpResponse.statusCode()).thenReturn(HttpStatus.HTTP_OK.toInt());
    when(recordHttpResponse.bodyAsString()).thenReturn("{\"id\":\"" + INSTANCE_ID_2 + "\"}");
    when(recordHttpResponse.bodyAsJson(Record.class)).thenReturn(mockRecord);

    var handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.getSourceMARCByInstanceId(INSTANCE_ID_2, sourceTenant, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));
  }

  @Test
  public void deleteSourceRecordByInstanceIdSuccessTest() {

    var recordId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    var tenant = "sourceTenant";

    HttpResponse<Buffer> mockedResponse = mock(HttpResponse.class);

    when(mockedResponse.statusCode()).thenReturn(HTTP_NO_CONTENT.toInt());
    when(sourceStorageClient.deleteSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.succeededFuture(mockedResponse));

    var handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.deleteSourceRecordByRecordId(recordId, INSTANCE_ID_2, tenant, sourceStorageClient)
      .onComplete(result -> assertEquals(INSTANCE_ID_2, result.result()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE);
  }

  @Test
  public void deleteSourceRecordByInstanceIdFailedTest() {

    var instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    var recordId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    var tenant = "sourceTenant";

    when(sourceStorageClient.deleteSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    var handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.deleteSourceRecordByRecordId(recordId, instanceId, tenant, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE);
  }

  @Test
  public void deleteSourceRecordByInstanceIdFailedTestWhenResponseStatusIsNotNoContent() {
    var instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    var recordId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    var tenant = "sourceTenant";

    HttpResponse<Buffer> mockedResponse = mock(HttpResponse.class);
    when(mockedResponse.statusCode()).thenReturn(HTTP_INTERNAL_SERVER_ERROR.toInt());
    when(sourceStorageClient.deleteSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.succeededFuture(mockedResponse));

    var handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.deleteSourceRecordByRecordId(recordId, instanceId, tenant, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE);
  }

  @Test
  public void updateSourceRecordSuppressFromDiscoveryByInstanceIdSuccessTest() {
    HttpResponse<Buffer> mockedResponse = mock(HttpResponse.class);

    when(mockedResponse.statusCode()).thenReturn(HTTP_OK.toInt());
    when(sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(any(), any(), anyBoolean()))
      .thenReturn(Future.succeededFuture(mockedResponse));

    var handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.updateSourceRecordSuppressFromDiscoveryByInstanceId(INSTANCE_ID_2, true, sourceStorageClient)
      .onComplete(result -> assertEquals(INSTANCE_ID_2, result.result()));

    verify(sourceStorageClient, times(1)).putSourceStorageRecordsSuppressFromDiscoveryById(INSTANCE_ID_2, INSTANCE_ID_TYPE, true);
  }

  @Test
  public void updateSourceRecordSuppressFromDiscoveryByInstanceIdFailedTest() {

    var instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";

    when(sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(any(), any(), anyBoolean()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    var handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.updateSourceRecordSuppressFromDiscoveryByInstanceId(instanceId, true, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).putSourceStorageRecordsSuppressFromDiscoveryById(instanceId, INSTANCE_ID_TYPE, true);
  }

  @Test
  public void updateSourceRecordSuppressFromDiscoveryByInstanceIdFailedTestWhenResponseStatusIsNotOk() {

    var instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";

    HttpResponse<Buffer> mockedResponse = mock(HttpResponse.class);

    when(mockedResponse.statusCode()).thenReturn(HTTP_INTERNAL_SERVER_ERROR.toInt());
    when(sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(any(), any(), anyBoolean()))
      .thenReturn(Future.succeededFuture(mockedResponse));

    var handler = new MarcInstanceSharingHandlerImpl(instanceOperationsHelper, null, vertx, httpClient);
    handler.updateSourceRecordSuppressFromDiscoveryByInstanceId(instanceId, true, sourceStorageClient)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).putSourceStorageRecordsSuppressFromDiscoveryById(instanceId, INSTANCE_ID_TYPE, true);
  }

  @Test
  public void shouldPopulateTargetInstanceWithNonMarcControlledFields(TestContext testContext) {
    //given
    var async = testContext.async();
    var instanceId = UUID.randomUUID().toString();
    var targetInstanceHrid = "consin001";

    var localSourceInstance = new Instance(instanceId, 1, "001", "MARC", "testTitle", UUID.randomUUID().toString())
      .setDiscoverySuppress(Boolean.TRUE)
      .setStaffSuppress(Boolean.TRUE)
      .setDeleted(Boolean.TRUE)
      .setCatalogedDate("1970-01-01")
      .setStatusId(UUID.randomUUID().toString())
      .setStatisticalCodeIds(List.of(UUID.randomUUID().toString()))
      .setAdministrativeNotes(List.of("test-note"))
      .setNatureOfContentTermIds(List.of(UUID.randomUUID().toString()));

    var importedTargetInstance = new Instance(instanceId, 1, targetInstanceHrid, "MARC", "testTitle", UUID.randomUUID().toString());

    doReturn(Future.succeededFuture(instanceId)).when(marcHandler).updateSourceRecordSuppressFromDiscoveryByInstanceId(any(), anyBoolean(), any());
    when(restDataImportHelper.importMarcRecord(any(), any(), any())).thenReturn(Future.succeededFuture("COMMITTED"));
    when(instanceOperationsHelper.getInstanceById(any(), any())).thenReturn(Future.succeededFuture(importedTargetInstance));
    when(instanceOperationsHelper.updateInstance(any(), any())).thenReturn(Future.succeededFuture(instanceId));

    // when
    var future = marcHandler.publishInstance(localSourceInstance, sharingInstanceMetadata, source, target, kafkaHeaders);

    //then
    future.onComplete(ar -> {
      testContext.assertTrue(ar.succeeded());
      testContext.assertTrue(ar.result().equals(instanceId));

      var updatedInstanceCaptor = ArgumentCaptor.forClass(Instance.class);
      verify(instanceOperationsHelper).updateInstance(updatedInstanceCaptor.capture(), argThat(p -> CONSORTIUM_TENANT.equals(p.getTenantId())));
      verify(marcHandler, times(1)).updateSourceRecordSuppressFromDiscoveryByInstanceId(instanceId, true, sourceStorageClient);
      var targetInstanceWithNonMarcData = updatedInstanceCaptor.getValue();
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

  private void mockSuccessRetrievingAuthorities() throws UnsupportedEncodingException {
    doAnswer(invocationOnMock -> {
      var result = new MultipleRecords<>(List.of(localAuthority, sharedAuthority), 2);
      Consumer<Success<MultipleRecords>> successHandler = invocationOnMock.getArgument(2);
      successHandler.accept(new Success<>(result));
      return null;
    }).when(authorityRecordCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", AUTHORITY_ID_1, AUTHORITY_ID_2))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));
  }

  private void mockErrorDuringRetrievingAuthorities() throws UnsupportedEncodingException {
    doAnswer(invocationOnMock -> {
      Consumer<Failure> failureHandler = invocationOnMock.getArgument(3);
      failureHandler.accept(new Failure("Internal Server Error", 500));
      return null;
    }).when(authorityRecordCollection).findByCql(Mockito.argThat(cql -> cql.equals(String.format("id==(%s OR %s)", AUTHORITY_ID_1, AUTHORITY_ID_2))),
      any(PagingParameters.class), any(Consumer.class), any(Consumer.class));
  }

  private void verifyCleanUoAllAuthorityLinksForMemberTenant() {
    verify(entitiesLinksService, times(1)).putInstanceAuthorityLinks(argThat(context -> context.getTenantId().equals(MEMBER_TENANT)),
      Mockito.argThat(id -> id.equals(INSTANCE_ID_1)),
      Mockito.argThat(List::isEmpty));
  }

  private void verifyRollbackAuthorityLinksForMemberTenant(List<Link> links) {
    verify(entitiesLinksService, times(1)).putInstanceAuthorityLinks(argThat(context -> context.getTenantId().equals(MEMBER_TENANT)),
      Mockito.argThat(id -> id.equals(INSTANCE_ID_1)),
      Mockito.argThat(sharedAuthorityLinks -> sharedAuthorityLinks.equals(links)));
  }

  private void verifySharedLinksCreation(int wantedNumberOfInvocations) {
    verify(entitiesLinksService, times(wantedNumberOfInvocations)).putInstanceAuthorityLinks(argThat(context -> context.getTenantId().equals(CONSORTIUM_TENANT)),
      Mockito.argThat(id -> id.equals(INSTANCE_ID_1)),
      Mockito.argThat(sharedAuthorityLinks -> sharedAuthorityLinks.size() == 1
        && sharedAuthorityLinks.getFirst().getAuthorityId().equals(AUTHORITY_ID_2)));
  }

  private static void setField(Object instance, String fieldName, Object fieldValue) {
    try {
      var field = instance.getClass().getDeclaredField(fieldName);
      field.setAccessible(true);
      field.set(instance, fieldValue);
    } catch (NoSuchFieldException | IllegalAccessException e) {
      throw new RuntimeException("Failed to set field value using reflection", e);
    }
  }

  private static final String RECORD_JSON = "{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"snapshotId\":\"7376bb73-845e-44ce-ade4-53394f7526a6\",\"matchedId\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\",\"generation\":1,\"recordType\":\"MARC_BIB\"," +
    "\"parsedRecord\":{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"content\":{\"fields\":[{\"001\":\"in00000000001\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"  2006214613\"}]}},{\"019\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"1058285745\"}]}},{\"022\":{\"ind1\":\"0\",\"ind2\":\" \"," +
    "\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}]}},{\"035\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}]}},{\"040\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"ILGA\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"ISSN RECORD\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"4\",\"subfields\":[{\"a\":\"QL640\"}]}},{\"082\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}]}},{\"130\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}]}},{\"210\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}]}},{\"222\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}]}},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Biology!!!!!\"}]}},{\"246\":{\"ind1\":\"1\",\"ind2\":\"3\",\"subfields\":[{\"a\":\"HCBBBB\"}]}},{\"260\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}]}},{\"310\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Semiannual\"}]}},{\"336\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}]}},{\"337\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}]}},{\"338\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}]}},{\"362\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"655\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Electronic journals.\"}]}},{\"710\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}]}},{\"711\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}]}},{\"776\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}]}},{\"841\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"v.1- (1992-)\"}]}},{\"856\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\"}]}}],\"leader\":\"01819cas a2200469 a 4500\"},\"formattedContent\":\"LEADER 01819cas a2200469 a 4500\\n001 in00000000001\\n006 m     o  d        \\n007 cr cnu||||||||\\n008 060504c20069999txufr pso     0   a0eng c\\n005 20230915131710.4\\n010   $a  2006214613\\n019   $a1058285745\\n022 0 $a1931-7603$l1931-7603$21\\n035   $a(OCoLC)68188263$z(OCoLC)1058285745\\n040   $aNSD$beng$cNSD$dWAU$dDLC$dHUL$dOCLCQ$dOCLCF$dOCL$dAU@\\n042   $apcc$ansdp\\n049   $aILGA\\n050 10$aISSN RECORD\\n050 14$aQL640\\n082 10$a598.1$214\\n130 0 $aHerpetological conservation and biology (Online)\\n210 0 $aHerpetol. conserv. biol.$b(Online)\\n222  0$aHerpetological conservation and biology$b(Online)\\n245 10$aBiology!!!!!\\n246 13$aHCBBBB\\n260   $a[Texarkana, Tex.] :$b[publisher not identified],$c[2006]\\n310   $aSemiannual\\n336   $atext$btxt$2rdacontent\\n337   $acomputer$bc$2rdamedia\\n338   $aonline resource$bcr$2rdacarrier\\n362 1 $aBegan with: Vol. 1, issue 1 (Sept. 2006).\\n500   $aPublished in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\\n650  0$aHerpetology$xConservation$vPeriodicals.\\n650  0$aAmphibians$xConservation$vPeriodicals.\\n650  0$aReptiles$xConservation$vPeriodicals.\\n655  0$aElectronic journals.\\n710 2 $aPartners in Amphibian and Reptile Conservation.\\n711 2 $aWorld Congress of Herpetology.\\n776 1 $tHerpetological conservation and biology (Print)$x2151-0733$w(DLC)  2009202029$w(OCoLC)427887140\\n841   $av.1- (1992-)\\n856 40$uhttp://www.herpconbio.org$zAvailable to Lehigh users\\n999 ff$s0ecd6e9f-f02f-47b7-8326-2743bfa3fc43$ifea6477b-d8f5-4d22-9e86-6218407c780b\\n\\n\"}," +
    "\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\",\"instanceHrid\":\"in00000000001\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\",\"metadata\":{\"createdDate\":\"2023-09-15T13:17:10.571+00:00\",\"updatedDate\":\"2023-09-15T13:17:10.571+00:00\"}}";

  private static final String RECORD_JSON_WITH_LINKED_AUTHORITIES = "{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"snapshotId\":\"7376bb73-845e-44ce-ade4-53394f7526a6\",\"matchedId\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\",\"generation\":1,\"recordType\":\"MARC_BIB\"," +
    "\"parsedRecord\":{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"content\":{\"fields\":[{\"001\":\"in00000000001\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"  2006214613\"}]}},{\"019\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"1058285745\"}]}},{\"022\":{\"ind1\":\"0\",\"ind2\":\" \"," +
    "\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}]}},{\"035\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}]}},{\"040\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"ILGA\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"ISSN RECORD\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"4\",\"subfields\":[{\"a\":\"QL640\"}]}},{\"082\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}]}},{\"130\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}]}},{\"210\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}]}},{\"222\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}]}},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Biology!!!!!\"}]}},{\"246\":{\"ind1\":\"1\",\"ind2\":\"3\",\"subfields\":[{\"a\":\"HCBBBB\"}]}},{\"260\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}]}},{\"310\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Semiannual\"}]}},{\"336\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}]}},{\"337\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}]}},{\"338\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}]}},{\"362\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"655\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Electronic journals.\"}]}},{\"100\":[{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"},{\"9\":\"58600684-c647-408d-bf3e-756e9055a988\"}]}]},{\"100\":[{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"},{\"9\":\"3f2923d3-6f8e-41a6-94e1-09eaf32872e0\"}]}]},{\"710\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}]}},{\"711\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}]}},{\"776\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}]}},{\"841\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"v.1- (1992-)\"}]}},{\"856\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\"}]}}],\"leader\":\"01819cas a2200469 a 4500\"},\"formattedContent\":\"LEADER 01819cas a2200469 a 4500\\n001 in00000000001\\n006 m     o  d        \\n007 cr cnu||||||||\\n008 060504c20069999txufr pso     0   a0eng c\\n005 20230915131710.4\\n010   $a  2006214613\\n019   $a1058285745\\n022 0 $a1931-7603$l1931-7603$21\\n035   $a(OCoLC)68188263$z(OCoLC)1058285745\\n040   $aNSD$beng$cNSD$dWAU$dDLC$dHUL$dOCLCQ$dOCLCF$dOCL$dAU@\\n042   $apcc$ansdp\\n049   $aILGA\\n050 10$aISSN RECORD\\n050 14$aQL640\\n082 10$a598.1$214\\n130 0 $aHerpetological conservation and biology (Online)\\n210 0 $aHerpetol. conserv. biol.$b(Online)\\n222  0$aHerpetological conservation and biology$b(Online)\\n245 10$aBiology!!!!!\\n246 13$aHCBBBB\\n260   $a[Texarkana, Tex.] :$b[publisher not identified],$c[2006]\\n310   $aSemiannual\\n336   $atext$btxt$2rdacontent\\n337   $acomputer$bc$2rdamedia\\n338   $aonline resource$bcr$2rdacarrier\\n362 1 $aBegan with: Vol. 1, issue 1 (Sept. 2006).\\n500   $aPublished in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\\n650  0$aHerpetology$xConservation$vPeriodicals.\\n650  0$aAmphibians$xConservation$vPeriodicals.\\n650  0$aReptiles$xConservation$vPeriodicals.\\n655  0$aElectronic journals.\\n100   $aPartners in Amphibian and Reptile Conservation.$958600684-c647-408d-bf3e-756e9055a988\\n100   $aPartners in Amphibian and Reptile Conservation.$93f2923d3-6f8e-41a6-94e1-09eaf32872e0\\n710 2 $aPartners in Amphibian and Reptile Conservation.\\n711 2 $aWorld Congress of Herpetology.\\n776 1 $tHerpetological conservation and biology (Print)$x2151-0733$w(DLC)  2009202029$w(OCoLC)427887140\\n841   $av.1- (1992-)\\n856 40$uhttp://www.herpconbio.org$zAvailable to Lehigh users\\n999 ff$s0ecd6e9f-f02f-47b7-8326-2743bfa3fc43$ifea6477b-d8f5-4d22-9e86-6218407c780b\\n\\n\"}," +
    "\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\",\"instanceHrid\":\"in00000000001\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\",\"metadata\":{\"createdDate\":\"2023-09-15T13:17:10.571+00:00\",\"updatedDate\":\"2023-09-15T13:17:10.571+00:00\"}}";

  private static final String RECORD_JSON_WITH_LOCAL_LINKED_AUTHORITIES = "{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"snapshotId\":\"7376bb73-845e-44ce-ade4-53394f7526a6\",\"matchedId\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\",\"generation\":1,\"recordType\":\"MARC_BIB\"," +
    "\"parsedRecord\":{\"id\":\"5e525f1e-d373-4a07-9aff-b80856bacfef\",\"content\":{\"fields\":[{\"001\":\"in00000000001\"},{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"  2006214613\"}]}},{\"019\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"1058285745\"}]}},{\"022\":{\"ind1\":\"0\",\"ind2\":\" \"," +
    "\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}]}},{\"035\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}]}},{\"040\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}]}},{\"042\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}]}},{\"049\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"ILGA\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"ISSN RECORD\"}]}},{\"050\":{\"ind1\":\"1\",\"ind2\":\"4\",\"subfields\":[{\"a\":\"QL640\"}]}},{\"082\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}]}},{\"130\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}]}},{\"210\":{\"ind1\":\"0\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}]}},{\"222\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}]}},{\"245\":{\"ind1\":\"1\",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Biology!!!!!\"}]}},{\"246\":{\"ind1\":\"1\",\"ind2\":\"3\",\"subfields\":[{\"a\":\"HCBBBB\"}]}},{\"260\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}]}},{\"310\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Semiannual\"}]}},{\"336\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}]}},{\"337\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}]}},{\"338\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}]}},{\"362\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}]}},{\"500\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"650\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}]}},{\"655\":{\"ind1\":\" \",\"ind2\":\"0\",\"subfields\":[{\"a\":\"Electronic journals.\"}]}},{\"100\":[{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"},{\"9\":\"58600684-c647-408d-bf3e-756e9055a988\"}]}]},{\"710\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}]}},{\"711\":{\"ind1\":\"2\",\"ind2\":\" \",\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}]}},{\"776\":{\"ind1\":\"1\",\"ind2\":\" \",\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}]}},{\"841\":{\"ind1\":\" \",\"ind2\":\" \",\"subfields\":[{\"a\":\"v.1- (1992-)\"}]}},{\"856\":{\"ind1\":\"4\",\"ind2\":\"0\",\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}]}},{\"999\":{\"ind1\":\"f\",\"ind2\":\"f\",\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"eb89b292-d2b7-4c36-9bfc-f816d6f96418\"}]}}],\"leader\":\"01819cas a2200469 a 4500\"},\"formattedContent\":\"LEADER 01819cas a2200469 a 4500\\n001 in00000000001\\n006 m     o  d        \\n007 cr cnu||||||||\\n008 060504c20069999txufr pso     0   a0eng c\\n005 20230915131710.4\\n010   $a  2006214613\\n019   $a1058285745\\n022 0 $a1931-7603$l1931-7603$21\\n035   $a(OCoLC)68188263$z(OCoLC)1058285745\\n040   $aNSD$beng$cNSD$dWAU$dDLC$dHUL$dOCLCQ$dOCLCF$dOCL$dAU@\\n042   $apcc$ansdp\\n049   $aILGA\\n050 10$aISSN RECORD\\n050 14$aQL640\\n082 10$a598.1$214\\n130 0 $aHerpetological conservation and biology (Online)\\n210 0 $aHerpetol. conserv. biol.$b(Online)\\n222  0$aHerpetological conservation and biology$b(Online)\\n245 10$aBiology!!!!!\\n246 13$aHCBBBB\\n260   $a[Texarkana, Tex.] :$b[publisher not identified],$c[2006]\\n310   $aSemiannual\\n336   $atext$btxt$2rdacontent\\n337   $acomputer$bc$2rdamedia\\n338   $aonline resource$bcr$2rdacarrier\\n362 1 $aBegan with: Vol. 1, issue 1 (Sept. 2006).\\n500   $aPublished in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\\n650  0$aHerpetology$xConservation$vPeriodicals.\\n650  0$aAmphibians$xConservation$vPeriodicals.\\n650  0$aReptiles$xConservation$vPeriodicals.\\n655  0$aElectronic journals.\\n100   $aPartners in Amphibian and Reptile Conservation.$958600684-c647-408d-bf3e-756e9055a988\\n100   $aPartners in Amphibian and Reptile Conservation.$93f2923d3-6f8e-41a6-94e1-09eaf32872e0\\n710 2 $aPartners in Amphibian and Reptile Conservation.\\n711 2 $aWorld Congress of Herpetology.\\n776 1 $tHerpetological conservation and biology (Print)$x2151-0733$w(DLC)  2009202029$w(OCoLC)427887140\\n841   $av.1- (1992-)\\n856 40$uhttp://www.herpconbio.org$zAvailable to Lehigh users\\n999 ff$s0ecd6e9f-f02f-47b7-8326-2743bfa3fc43$ieb89b292-d2b7-4c36-9bfc-f816d6f96418\\n\\n\"}," +
    "\"deleted\":false,\"order\":0,\"externalIdsHolder\":{\"instanceId\":\"eb89b292-d2b7-4c36-9bfc-f816d6f96418\",\"instanceHrid\":\"in00000000001\"},\"additionalInfo\":{\"suppressDiscovery\":false},\"state\":\"ACTUAL\",\"leaderRecordStatus\":\"c\",\"metadata\":{\"createdDate\":\"2023-09-15T13:17:10.571+00:00\",\"updatedDate\":\"2023-09-15T13:17:10.571+00:00\"}}";

  private static final String PARSED_RECORD_FIELDS_AFTER_UNLINK = "[{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"subfields\":[{\"a\":\"  2006214613\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"019\":{\"subfields\":[{\"a\":\"1058285745\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"022\":{\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"040\":{\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"042\":{\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"049\":{\"subfields\":[{\"a\":\"ILGA\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"050\":{\"subfields\":[{\"a\":\"ISSN RECORD\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"050\":{\"subfields\":[{\"a\":\"QL640\"}],\"ind1\":\"1\",\"ind2\":\"4\"}},{\"082\":{\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"130\":{\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"210\":{\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"222\":{\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"245\":{\"subfields\":[{\"a\":\"Biology!!!!!\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"246\":{\"subfields\":[{\"a\":\"HCBBBB\"}],\"ind1\":\"1\",\"ind2\":\"3\"}},{\"260\":{\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"310\":{\"subfields\":[{\"a\":\"Semiannual\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"336\":{\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"337\":{\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"338\":{\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"362\":{\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"650\":{\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"655\":{\"subfields\":[{\"a\":\"Electronic journals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"100\":{\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"100\":{\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"},{\"9\":\"3f2923d3-6f8e-41a6-94e1-09eaf32872e0\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"710\":{\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"711\":{\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"776\":{\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"841\":{\"subfields\":[{\"a\":\"v.1- (1992-)\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}],\"ind1\":\"4\",\"ind2\":\"0\"}},{\"999\":{\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"fea6477b-d8f5-4d22-9e86-6218407c780b\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]";
  private static final String PARSED_RECORD_FIELDS_AFTER_UNLINK_LOCAL_LINKS = "[{\"006\":\"m     o  d        \"},{\"007\":\"cr cnu||||||||\"},{\"008\":\"060504c20069999txufr pso     0   a0eng c\"},{\"005\":\"20230915131710.4\"},{\"010\":{\"subfields\":[{\"a\":\"  2006214613\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"019\":{\"subfields\":[{\"a\":\"1058285745\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"022\":{\"subfields\":[{\"a\":\"1931-7603\"},{\"l\":\"1931-7603\"},{\"2\":\"1\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"035\":{\"subfields\":[{\"a\":\"(OCoLC)68188263\"},{\"z\":\"(OCoLC)1058285745\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"040\":{\"subfields\":[{\"a\":\"NSD\"},{\"b\":\"eng\"},{\"c\":\"NSD\"},{\"d\":\"WAU\"},{\"d\":\"DLC\"},{\"d\":\"HUL\"},{\"d\":\"OCLCQ\"},{\"d\":\"OCLCF\"},{\"d\":\"OCL\"},{\"d\":\"AU@\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"042\":{\"subfields\":[{\"a\":\"pcc\"},{\"a\":\"nsdp\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"049\":{\"subfields\":[{\"a\":\"ILGA\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"050\":{\"subfields\":[{\"a\":\"ISSN RECORD\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"050\":{\"subfields\":[{\"a\":\"QL640\"}],\"ind1\":\"1\",\"ind2\":\"4\"}},{\"082\":{\"subfields\":[{\"a\":\"598.1\"},{\"2\":\"14\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"130\":{\"subfields\":[{\"a\":\"Herpetological conservation and biology (Online)\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"210\":{\"subfields\":[{\"a\":\"Herpetol. conserv. biol.\"},{\"b\":\"(Online)\"}],\"ind1\":\"0\",\"ind2\":\" \"}},{\"222\":{\"subfields\":[{\"a\":\"Herpetological conservation and biology\"},{\"b\":\"(Online)\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"245\":{\"subfields\":[{\"a\":\"Biology!!!!!\"}],\"ind1\":\"1\",\"ind2\":\"0\"}},{\"246\":{\"subfields\":[{\"a\":\"HCBBBB\"}],\"ind1\":\"1\",\"ind2\":\"3\"}},{\"260\":{\"subfields\":[{\"a\":\"[Texarkana, Tex.] :\"},{\"b\":\"[publisher not identified],\"},{\"c\":\"[2006]\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"310\":{\"subfields\":[{\"a\":\"Semiannual\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"336\":{\"subfields\":[{\"a\":\"text\"},{\"b\":\"txt\"},{\"2\":\"rdacontent\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"337\":{\"subfields\":[{\"a\":\"computer\"},{\"b\":\"c\"},{\"2\":\"rdamedia\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"338\":{\"subfields\":[{\"a\":\"online resource\"},{\"b\":\"cr\"},{\"2\":\"rdacarrier\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"362\":{\"subfields\":[{\"a\":\"Began with: Vol. 1, issue 1 (Sept. 2006).\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"500\":{\"subfields\":[{\"a\":\"Published in partnership with: Partners in Amphibian & Reptile Conservation (PARC), World Congress of Herpetology, Las Vegas Springs Preserve.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"650\":{\"subfields\":[{\"a\":\"Herpetology\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Amphibians\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"650\":{\"subfields\":[{\"a\":\"Reptiles\"},{\"x\":\"Conservation\"},{\"v\":\"Periodicals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"655\":{\"subfields\":[{\"a\":\"Electronic journals.\"}],\"ind1\":\" \",\"ind2\":\"0\"}},{\"100\":{\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"710\":{\"subfields\":[{\"a\":\"Partners in Amphibian and Reptile Conservation.\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"711\":{\"subfields\":[{\"a\":\"World Congress of Herpetology.\"}],\"ind1\":\"2\",\"ind2\":\" \"}},{\"776\":{\"subfields\":[{\"t\":\"Herpetological conservation and biology (Print)\"},{\"x\":\"2151-0733\"},{\"w\":\"(DLC)  2009202029\"},{\"w\":\"(OCoLC)427887140\"}],\"ind1\":\"1\",\"ind2\":\" \"}},{\"841\":{\"subfields\":[{\"a\":\"v.1- (1992-)\"}],\"ind1\":\" \",\"ind2\":\" \"}},{\"856\":{\"subfields\":[{\"u\":\"http://www.herpconbio.org\"},{\"z\":\"Available to Lehigh users\"}],\"ind1\":\"4\",\"ind2\":\"0\"}},{\"999\":{\"subfields\":[{\"s\":\"0ecd6e9f-f02f-47b7-8326-2743bfa3fc43\"},{\"i\":\"eb89b292-d2b7-4c36-9bfc-f816d6f96418\"}],\"ind1\":\"f\",\"ind2\":\"f\"}}]";
}
