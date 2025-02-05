package org.folio.inventory.client;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.common.Slf4jNotifier;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import com.github.tomakehurst.wiremock.junit.WireMockRule;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Future;
import io.vertx.core.Vertx;
import io.vertx.core.buffer.Buffer;
import io.vertx.ext.unit.Async;
import io.vertx.ext.unit.TestContext;
import io.vertx.ext.unit.junit.VertxUnitRunner;
import io.vertx.ext.web.client.HttpResponse;
import org.folio.inventory.client.wrappers.ChangeManagerClientWrapper;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import java.util.UUID;

import static api.ApiTestSuite.TENANT_ID;
import static com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus.SC_CREATED;
import static com.github.dockerjava.zerodep.shaded.org.apache.hc.core5.http.HttpStatus.SC_OK;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TENANT;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_TOKEN;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_URL;
import static org.folio.inventory.dataimport.handlers.matching.util.EventHandlingUtil.OKAPI_USER_ID;

@RunWith(VertxUnitRunner.class)
public class ChangeManagerClientWrapperTest {
  private final Vertx vertx = Vertx.vertx();
  private ChangeManagerClientWrapper changeManagerClientWrapper;
  private RawRecordsDto stubRawRecordsDto;
  private InitJobExecutionsRqDto stubInitJobExecutionsRqDto;
  private JobExecution stubJobExecution;
  private JobProfileInfo stubJobProfileInfo;
  private StatusDto stubStatusDto;
  private ParsedRecordDto stubParsedRecordDto;
  private static final String TOKEN = "token";
  private static final String USER_ID = "userId";

  @Rule
  public WireMockRule mockServer = new WireMockRule(
    WireMockConfiguration.wireMockConfig()
      .dynamicPort()
      .notifier(new Slf4jNotifier(true)));

  @Before
  public void setUp() {
    changeManagerClientWrapper = new ChangeManagerClientWrapper(mockServer.baseUrl(), TENANT_ID, TOKEN, USER_ID,
      vertx.createHttpClient());

    stubRawRecordsDto = new RawRecordsDto().withId(UUID.randomUUID().toString());
    stubInitJobExecutionsRqDto = new InitJobExecutionsRqDto().withParentJobId(UUID.randomUUID().toString());
    stubJobExecution = new JobExecution().withId(UUID.randomUUID().toString());
    stubJobProfileInfo = new JobProfileInfo().withId(UUID.randomUUID().toString());
    stubStatusDto = new StatusDto();
    stubParsedRecordDto = new ParsedRecordDto().withId(UUID.randomUUID().toString());

    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions"), true))
      .withHeader(OKAPI_URL, equalTo(mockServer.baseUrl()))
      .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
      .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
      .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
      .willReturn(WireMock.created()));

    WireMock.stubFor(post(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/" + stubRawRecordsDto.getId() + "/records"), true))
      .withQueryParam("acceptInstanceId", equalTo("true"))
      .withHeader(OKAPI_URL, equalTo(mockServer.baseUrl()))
      .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
      .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
      .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
      .willReturn(WireMock.created()));

    WireMock.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/" + stubJobExecution.getId()), true))
      .withHeader(OKAPI_URL, equalTo(mockServer.baseUrl()))
      .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
      .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
      .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
      .willReturn(WireMock.ok()));

    WireMock.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/" + stubJobProfileInfo.getId() + "/jobProfile"), true))
      .withHeader(OKAPI_URL, equalTo(mockServer.baseUrl()))
      .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
      .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
      .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
      .willReturn(WireMock.ok()));

    WireMock.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/jobExecutions/" + stubJobExecution.getId() + "/status"), true))
      .withHeader(OKAPI_URL, equalTo(mockServer.baseUrl()))
      .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
      .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
      .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
      .willReturn(WireMock.ok()));

    WireMock.stubFor(put(new UrlPathPattern(new RegexPattern("/change-manager/parsedRecords/" + stubParsedRecordDto.getId()), true))
      .withHeader(OKAPI_URL, equalTo(mockServer.baseUrl()))
      .withHeader(OKAPI_TOKEN, equalTo(TOKEN))
      .withHeader(OKAPI_TENANT, equalTo(TENANT_ID))
      .withHeader(OKAPI_USER_ID, equalTo(USER_ID))
      .willReturn(WireMock.ok()));
  }

  @Test
  public void shouldPostChangeManagerJobExecutions(TestContext context) {
    Async async = context.async();

    Future<HttpResponse<Buffer>> optionalFuture = changeManagerClientWrapper.postChangeManagerJobExecutions(stubInitJobExecutionsRqDto);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(ar.result().statusCode(), SC_CREATED);
      async.complete();
    });
  }

  @Test
  public void shouldPostChangeManagerJobExecutionsRecordsById(TestContext context) {
    Async async = context.async();

    Future<HttpResponse<Buffer>> optionalFuture = changeManagerClientWrapper
      .postChangeManagerJobExecutionsRecordsById(stubRawRecordsDto.getId(), true, stubRawRecordsDto);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(ar.result().statusCode(), SC_CREATED);
      async.complete();
    });
  }

  @Test
  public void shouldPutChangeManagerJobExecutionsById(TestContext context) {
    Async async = context.async();

    Future<HttpResponse<Buffer>> optionalFuture = changeManagerClientWrapper
      .putChangeManagerJobExecutionsById(stubJobExecution.getId(), stubJobExecution);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(ar.result().statusCode(), SC_OK);
      async.complete();
    });
  }

  @Test
  public void shouldPutChangeManagerJobExecutionsJobProfileById(TestContext context) {
    Async async = context.async();

    Future<HttpResponse<Buffer>> optionalFuture = changeManagerClientWrapper
      .putChangeManagerJobExecutionsJobProfileById(stubJobProfileInfo.getId(), stubJobProfileInfo);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(ar.result().statusCode(), SC_OK);
      async.complete();
    });
  }

  @Test
  public void shouldPutChangeManagerJobExecutionsStatusById(TestContext context) {
    Async async = context.async();

    Future<HttpResponse<Buffer>> optionalFuture = changeManagerClientWrapper
      .putChangeManagerJobExecutionsStatusById(stubJobExecution.getId(), stubStatusDto);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(ar.result().statusCode(), SC_OK);
      async.complete();
    });
  }

  @Test
  public void shouldPutChangeManagerParsedRecordsById(TestContext context) {
    Async async = context.async();

    Future<HttpResponse<Buffer>> optionalFuture = changeManagerClientWrapper
      .putChangeManagerParsedRecordsById(stubParsedRecordDto.getId(), stubParsedRecordDto);

    optionalFuture.onComplete(ar -> {
      context.assertTrue(ar.succeeded());
      context.assertEquals(ar.result().statusCode(), SC_OK);
      async.complete();
    });
  }
}
