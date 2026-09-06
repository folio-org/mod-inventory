package org.folio.inventory.consortium.util;

import static org.folio.HttpStatus.HTTP_INTERNAL_SERVER_ERROR;
import static org.folio.HttpStatus.HTTP_NO_CONTENT;
import static org.folio.HttpStatus.HTTP_OK;
import static org.folio.inventory.consortium.util.SourceStorageHelper.SRS_RECORD_ID_TYPE;
import static org.folio.inventory.dataimport.handlers.actions.ReplaceInstanceEventHandler.INSTANCE_ID_TYPE;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.ext.web.client.HttpResponse;
import java.util.HashMap;
import java.util.Map;
import org.folio.HttpStatus;
import org.folio.Record;
import org.folio.inventory.exceptions.NotFoundException;
import org.folio.rest.client.SourceStorageRecordsClient;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.mockito.junit.jupiter.MockitoSettings;
import org.mockito.quality.Strictness;

@ExtendWith(MockitoExtension.class)
@MockitoSettings(strictness = Strictness.LENIENT)
class SourceStorageHelperTest {

  private static final String INSTANCE_ID_2 = "fea6477b-d8f5-4d22-9e86-6218407c780b";
  private static Vertx vertx;
  private static HttpClient httpClient;

  @Mock
  private SourceStorageRecordsClient sourceStorageClient;
  @Mock
  private HttpResponse<Buffer> httpResponse;

  private SourceStorageHelper client;
  private Map<String, String> kafkaHeaders;

  @BeforeAll
  static void setUpClass() {
    vertx = Vertx.vertx();
    httpClient = vertx.createHttpClient();
  }

  @AfterAll
  static void tearDownClass() {
    httpClient.close();
  }

  @BeforeEach
  void setUp() {
    kafkaHeaders = new HashMap<>();
    client = spy(new SourceStorageHelper(httpClient));
    doReturn(sourceStorageClient).when(client).prepareClient(anyString(), eq(kafkaHeaders));
  }

  @Test
  void getSourceMarcByInstanceIdSuccessTest() {
    var sourceTenant = "consortium";

    var mockRecord = new Record();
    mockRecord.setId(INSTANCE_ID_2);

    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.succeededFuture(httpResponse));

    when(httpResponse.statusCode()).thenReturn(HttpStatus.HTTP_OK.toInt());
    when(httpResponse.bodyAsString()).thenReturn("{\"id\":\"" + INSTANCE_ID_2 + "\"}");
    when(httpResponse.bodyAsJson(Record.class)).thenReturn(mockRecord);

    client.getSourceRecordByInstanceId(INSTANCE_ID_2, sourceTenant, kafkaHeaders).onComplete(result -> {
      var resultRecord = result.result();
      assertEquals(INSTANCE_ID_2, resultRecord.getId());
    });
  }

  @Test
  void getSourceMarcByInstanceIdFailTest() {
    var sourceTenant = "sourceTenant";

    when(sourceStorageClient.getSourceStorageRecordsFormattedById(any(), any()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    client.getSourceRecordByInstanceId(INSTANCE_ID_2, sourceTenant, kafkaHeaders)
      .onComplete(result -> assertTrue(result.failed()));
  }

  @Test
  void deleteSourceRecordByInstanceIdSuccessTest() {
    var recordId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    var tenant = "sourceTenant";

    when(httpResponse.statusCode()).thenReturn(HTTP_NO_CONTENT.toInt());
    when(sourceStorageClient.deleteSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.succeededFuture(httpResponse));

    client.deleteSourceRecordByRecordId(recordId, INSTANCE_ID_2, tenant, kafkaHeaders)
      .onComplete(result -> assertEquals(INSTANCE_ID_2, result.result()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE);
  }

  @Test
  void deleteSourceRecordByInstanceIdFailedTest() {
    var instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    var recordId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    var tenant = "sourceTenant";

    when(sourceStorageClient.deleteSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    client.deleteSourceRecordByRecordId(recordId, instanceId, tenant, kafkaHeaders)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE);
  }

  @Test
  void deleteSourceRecordByInstanceIdFailedTestWhenResponseStatusIsNotNoContent() {
    var instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";
    var recordId = "fea6477b-d8f5-4d22-9e86-6218407c780b";
    var tenant = "sourceTenant";

    when(httpResponse.statusCode()).thenReturn(HTTP_INTERNAL_SERVER_ERROR.toInt());
    when(sourceStorageClient.deleteSourceStorageRecordsById(any(), any()))
      .thenReturn(Future.succeededFuture(httpResponse));

    client.deleteSourceRecordByRecordId(recordId, instanceId, tenant, kafkaHeaders)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).deleteSourceStorageRecordsById(recordId, SRS_RECORD_ID_TYPE);
  }

  @Test
  void updateSuppressFromDiscoveryByInstanceIdSuccessTest() {
    when(httpResponse.statusCode()).thenReturn(HTTP_OK.toInt());
    when(sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(any(), any(), anyBoolean()))
      .thenReturn(Future.succeededFuture(httpResponse));

    client.updateSourceRecordSuppressFromDiscovery(INSTANCE_ID_2, true, "sourceTenant", kafkaHeaders)
      .onComplete(result -> assertEquals(INSTANCE_ID_2, result.result()));

    verify(sourceStorageClient, times(1)).putSourceStorageRecordsSuppressFromDiscoveryById(INSTANCE_ID_2,
      INSTANCE_ID_TYPE, true);
  }

  @Test
  void updateSuppressFromDiscoveryByInstanceIdFailedTest() {
    var instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";

    when(sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(any(), any(), anyBoolean()))
      .thenReturn(Future.failedFuture(new NotFoundException("Not found")));

    client.updateSourceRecordSuppressFromDiscovery(instanceId, true, "sourceTenant", kafkaHeaders)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).putSourceStorageRecordsSuppressFromDiscoveryById(instanceId, INSTANCE_ID_TYPE,
      true);
  }

  @Test
  void updateSuppressFromDiscoveryByInstanceIdFailedTestWhenResponseStatusIsNotOk() {
    var instanceId = "991f37c8-cd22-4db7-9543-a4ec68735e95";

    when(httpResponse.statusCode()).thenReturn(HTTP_INTERNAL_SERVER_ERROR.toInt());
    when(sourceStorageClient.putSourceStorageRecordsSuppressFromDiscoveryById(any(), any(), anyBoolean()))
      .thenReturn(Future.succeededFuture(httpResponse));

    client.updateSourceRecordSuppressFromDiscovery(instanceId, true, "sourceTenant", kafkaHeaders)
      .onComplete(result -> assertTrue(result.failed()));

    verify(sourceStorageClient, times(1)).putSourceStorageRecordsSuppressFromDiscoveryById(instanceId, INSTANCE_ID_TYPE,
      true);
  }
}
