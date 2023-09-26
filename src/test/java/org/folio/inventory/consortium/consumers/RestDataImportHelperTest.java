package org.folio.inventory.consortium.consumers;

import io.vertx.core.AsyncResult;
import io.vertx.core.Future;
import io.vertx.core.Handler;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.buffer.impl.BufferImpl;
import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.impl.HttpResponseImpl;
import org.folio.HttpStatus;
import org.folio.rest.client.ChangeManagerClient;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

@RunWith(VertxUnitRunner.class)
public class RestDataImportHelperTest {

  private ChangeManagerClient changeManagerClient;
  private RestDataImportHelper restDataImportHelper;

  @Before
  public void init() {
    changeManagerClient = mock(ChangeManagerClient.class);
    restDataImportHelper = new RestDataImportHelper(mock(Vertx.class));
  }

  @Test
  public void initJobExecutionTest() {

    // given
    Map<String, String> kafkaHeaders = new HashMap<>();
    String tenantId = "consortium";
    String expectedJobExecutionId = "jobExecutionId";

    JsonObject responseBody = new JsonObject()
      .put("jobExecutions", new JsonArray().add(new JsonObject().put("id", expectedJobExecutionId)));

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(HttpStatus.HTTP_CREATED, BufferImpl.buffer(responseBody.encode()));
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());

    // when
    restDataImportHelper.initJobExecution(tenantId, changeManagerClient, kafkaHeaders).onComplete(asyncResult -> {
      // then
      assertTrue(asyncResult.succeeded());
      assertEquals(expectedJobExecutionId, asyncResult.result());
    });
  }

  @Test
  public void initJobExecutionInternalServerErrorTest() {

    // given
    Map<String, String> kafkaHeaders = new HashMap<>();
    String tenantId = "consortium";
    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(HttpStatus.HTTP_INTERNAL_SERVER_ERROR, null);
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());

    // when
    restDataImportHelper.initJobExecution(tenantId, changeManagerClient, kafkaHeaders).onComplete(asyncResult -> {
      // then
      assertTrue(asyncResult.failed());
      assertEquals("Error receiving new JobExecution for sharing instance with InstanceId=consortium. Status message: Ok. Status code: 500",
        asyncResult.cause().getMessage());
    });
  }

  @Test
  public void initJobExecutionFailedWithoutJobExecutionsArrayTest() {

    // given
    Map<String, String> kafkaHeaders = new HashMap<>();
    String tenantId = "consortium";
    String expectedJobExecutionId = "jobExecutionId";

    JsonObject responseBody = new JsonObject().put("jobExecutions", new JsonArray().add(""));

    HttpResponseImpl<Buffer> jobExecutionResponse =
      buildHttpResponseWithBuffer(HttpStatus.HTTP_CREATED, BufferImpl.buffer(responseBody.encode()));
    Future<HttpResponse<Buffer>> futureResponse = Future.succeededFuture(jobExecutionResponse);

    doAnswer(invocation -> {
      Handler<AsyncResult<HttpResponse<Buffer>>> handler = invocation.getArgument(1);
      handler.handle(futureResponse);
      return null;
    }).when(changeManagerClient).postChangeManagerJobExecutions(any(), any());

    // when
    restDataImportHelper.initJobExecution(tenantId, changeManagerClient, kafkaHeaders).onComplete(asyncResult -> {
      // then
      assertTrue(asyncResult.failed());
      assertEquals("class java.lang.String cannot be cast to class io.vertx.core.json.JsonObject (java.lang.String is in module java.base of loader 'bootstrap'; io.vertx.core.json.JsonObject is in unnamed module of loader 'app')", asyncResult.cause().getMessage());
    });
  }

  @Test
  public void checkDataImportStatus() {
  }

  @Test
  public void getJobExecutionStatusByJobExecutionId() {
  }

  private static HttpResponseImpl<Buffer> buildHttpResponseWithBuffer(HttpStatus httpStatus, Buffer buffer) {
    return new HttpResponseImpl(
      null,
      httpStatus.toInt(),
      "Ok",
      null,
      null,
      null,
      buffer,
      new ArrayList<String>());
  }
}
