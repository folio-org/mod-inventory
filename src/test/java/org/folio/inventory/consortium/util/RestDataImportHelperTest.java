package org.folio.inventory.consortium.util;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import org.folio.HttpStatus;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.RecordsMetadata;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

import static io.vertx.core.buffer.Buffer.buffer;
import static org.folio.inventory.TestUtil.buildHttpResponseWithBuffer;
import static org.folio.inventory.consortium.util.RestDataImportHelper.FIELD_JOB_EXECUTIONS;
import static org.folio.inventory.consortium.util.RestDataImportHelper.STATUS_COMMITTED;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@RunWith(VertxUnitRunner.class)
public class RestDataImportHelperTest {

  private ChangeManagerClient changeManagerClient;
  private RestDataImportHelper restDataImportHelper;

  @Before
  public void init() {
    changeManagerClient = mock(ChangeManagerClient.class);
    restDataImportHelper = new RestDataImportHelper(mock(Vertx.class)) {
      @Override
      public ChangeManagerClient getChangeManagerClient(Map<String, String> kafkaHeaders) {
        return changeManagerClient;
      }
    };
  }

  @Test
  public void initJobExecutionTest() {

    // given
    Map<String, String> kafkaHeaders = new HashMap<>();
    String expectedJobExecutionId = UUID.randomUUID().toString();

    JsonObject responseBody = new JsonObject()
      .put(FIELD_JOB_EXECUTIONS, new JsonArray().add(new JsonObject().put("id", expectedJobExecutionId)));

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(buffer(responseBody.encode()), HttpStatus.HTTP_CREATED);
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());

    // when
    restDataImportHelper.initJobExecution(UUID.randomUUID().toString(), changeManagerClient, kafkaHeaders)
      .onComplete(asyncResult -> {
        // then
        assertTrue(asyncResult.succeeded());
        assertEquals(expectedJobExecutionId, asyncResult.result());
      });
  }

  @Test
  public void initJobExecutionFailedInternalServerErrorTest() {

    // given
    String expectedJobExecutionId = UUID.randomUUID().toString();
    Map<String, String> kafkaHeaders = new HashMap<>();
    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(HttpStatus.HTTP_INTERNAL_SERVER_ERROR, "Ok");
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());

    // when
    restDataImportHelper.initJobExecution(expectedJobExecutionId, changeManagerClient, kafkaHeaders)
      .onComplete(asyncResult -> {
        // then
        assertTrue(asyncResult.failed());
        assertEquals("Error receiving new JobExecution for sharing instance with InstanceId=" +
          expectedJobExecutionId + ". Status message: Ok. Status code: 500", asyncResult.cause().getMessage());
      });
  }

  @Test
  public void initJobExecutionFailedWithoutJobExecutionsArrayTest() {

    // given
    Map<String, String> kafkaHeaders = new HashMap<>();
    JsonObject responseBody = new JsonObject().put("jobExecutions", new JsonArray().add(""));

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(Buffer.buffer(responseBody.encode()), HttpStatus.HTTP_CREATED);
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());

    // when
    restDataImportHelper.initJobExecution(UUID.randomUUID().toString(), changeManagerClient, kafkaHeaders)
      .onComplete(asyncResult -> {
        // then
        assertTrue(asyncResult.failed());
        assertEquals("class java.lang.String cannot be cast to class io.vertx.core.json.JsonObject (java.lang.String is in module java.base of loader 'bootstrap'; io.vertx.core.json.JsonObject is in unnamed module of loader 'app')", asyncResult.cause().getMessage());
      });
  }

  @Test
  public void initJobExecutionFailedWithJobExecutionsEmptyArrayTest() {

    // given
    String expectedJobExecutionId = UUID.randomUUID().toString();
    Map<String, String> kafkaHeaders = new HashMap<>();
    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(Buffer.buffer("{\"jobExecutions\":[]}"), HttpStatus.HTTP_CREATED);
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());

    // when
    restDataImportHelper.initJobExecution(expectedJobExecutionId, changeManagerClient, kafkaHeaders)
      .onComplete(asyncResult -> {
        // then
        assertTrue(asyncResult.failed());
        assertEquals("Response body doesn't contains JobExecution object for sharing instance with InstanceId=" +
          expectedJobExecutionId + ".", asyncResult.cause().getMessage());
      });
  }

  @Test
  public void setDefaultJobProfileToJobExecutionTest() {

    // given
    String expectedJobExecutionId = UUID.randomUUID().toString();

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(HttpStatus.HTTP_OK);
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(2);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).putChangeManagerJobExecutionsJobProfileById(any(), any(JobProfileInfo.class), any());

    // when
    restDataImportHelper.setDefaultJobProfileToJobExecution(expectedJobExecutionId, changeManagerClient)
      .onComplete(asyncResult -> {
        // then
        assertTrue(asyncResult.succeeded());
        assertEquals(expectedJobExecutionId, asyncResult.result());
      });
  }

  @Test
  public void setDefaultJobProfileToJobExecutionFailedInternalServerErrorTest() {

    // given
    String expectedJobExecutionId = UUID.randomUUID().toString();

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(HttpStatus.HTTP_INTERNAL_SERVER_ERROR, "Ok");
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(2);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).putChangeManagerJobExecutionsJobProfileById(any(), any(JobProfileInfo.class), any());

    // when
    restDataImportHelper.setDefaultJobProfileToJobExecution(expectedJobExecutionId, changeManagerClient)
      .onComplete(asyncResult -> {
        // then
        assertFalse(asyncResult.succeeded());
        assertEquals("Failed to set JobProfile for JobExecution with jobExecutionId=" +
          expectedJobExecutionId + ". Status message: Ok. Status code: 500", asyncResult.cause().getMessage());
      });
  }

  @Test
  public void postChunkTest() {

    // given
    String expectedJobExecutionId = UUID.randomUUID().toString();

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(HttpStatus.HTTP_NO_CONTENT);
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    RawRecordsDto rawRecordsDto = new RawRecordsDto()
      .withId(UUID.randomUUID().toString())
      .withRecordsMetadata(new RecordsMetadata()
        .withLast(true)
        .withCounter(1)
        .withTotal(1)
        .withContentType(RecordsMetadata.ContentType.MARC_JSON))
      .withInitialRecords(null);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(3);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutionsRecordsById(eq(expectedJobExecutionId),
      eq(true), eq(rawRecordsDto), any());

    // when
    restDataImportHelper.postChunk(expectedJobExecutionId, true, rawRecordsDto, changeManagerClient)
      .onComplete(asyncResult -> {
        // then
        assertTrue(asyncResult.succeeded());
        assertEquals(expectedJobExecutionId, asyncResult.result());
      });
  }

  @Test
  public void getJobExecutionStatusByJobExecutionId() {

    // given
    String expectedJobExecutionId = UUID.randomUUID().toString();

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(Buffer.buffer("{\"status\":\"" + STATUS_COMMITTED + "\"}"), HttpStatus.HTTP_OK);
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).getChangeManagerJobExecutionsById(eq(expectedJobExecutionId), any());

    // when
    restDataImportHelper.getJobExecutionStatusByJobExecutionId(expectedJobExecutionId, changeManagerClient)
      .onComplete(asyncResult -> {
        // then
        assertTrue(asyncResult.succeeded());
        assertEquals(STATUS_COMMITTED, asyncResult.result());
      });
  }

  @Test
  public void getJobExecutionStatusByJobExecutionIdFailedWithEmptyResponseBodyTest() {

    // given
    String expectedJobExecutionId = UUID.randomUUID().toString();

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(HttpStatus.HTTP_OK);
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).getChangeManagerJobExecutionsById(eq(expectedJobExecutionId), any());

    // when
    restDataImportHelper.getJobExecutionStatusByJobExecutionId(expectedJobExecutionId, changeManagerClient)
      .onComplete(asyncResult -> {
        // then
        assertFalse(asyncResult.succeeded());
        assertEquals("Response body doesn't contains data for jobExecutionId=" + expectedJobExecutionId
          + ".", asyncResult.cause().getMessage());
      });
  }

  @Test
  public void getJobExecutionStatusByJobExecutionIdFailedInternalServerErrorTest() {

    // given
    String expectedJobExecutionId = UUID.randomUUID().toString();

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(HttpStatus.HTTP_INTERNAL_SERVER_ERROR, "Ok");
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).getChangeManagerJobExecutionsById(eq(expectedJobExecutionId), any());

    // when
    restDataImportHelper.getJobExecutionStatusByJobExecutionId(expectedJobExecutionId, changeManagerClient)
      .onComplete(asyncResult -> {
        // then
        assertFalse(asyncResult.succeeded());
        assertEquals("Error getting jobExecution by jobExecutionId=" + expectedJobExecutionId
          + ". Status message: Ok. Status code: 500", asyncResult.cause().getMessage());
      });
  }

}
