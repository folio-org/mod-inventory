package org.folio.inventory.client;

import static api.ApiTestSuite.TENANT_ID;
import static com.github.tomakehurst.wiremock.client.WireMock.equalTo;
import static com.github.tomakehurst.wiremock.client.WireMock.post;
import static com.github.tomakehurst.wiremock.client.WireMock.put;
import static org.folio.HttpStatus.SC_CREATED;
import static org.folio.HttpStatus.SC_OK;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.matching.RegexPattern;
import com.github.tomakehurst.wiremock.matching.UrlPathPattern;
import io.vertx.core.Vertx;
import io.vertx.junit5.VertxExtension;
import io.vertx.junit5.VertxTestContext;
import java.util.UUID;
import org.folio.dataimport.testsupport.rest.BaseWireMockTest;
import org.folio.dataimport.util.FolioHeaders;
import org.folio.inventory.client.wrappers.ChangeManagerClientWrapper;
import org.folio.okapi.common.XOkapiHeaders;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;

@ExtendWith(VertxExtension.class)
class ChangeManagerClientWrapperTest extends BaseWireMockTest {

  private static final String TOKEN = "stub-token";
  private static final String USER_ID = "12345";
  private static final String REQUEST_ID = "req-12456";

  private static final String JOB_EXECUTIONS_PATH = "/change-manager/jobExecutions";

  private ChangeManagerClientWrapper changeManagerClientWrapper;
  private RawRecordsDto stubRawRecordsDto;
  private InitJobExecutionsRqDto stubInitJobExecutionsRqDto;
  private JobExecution stubJobExecution;
  private JobProfileInfo stubJobProfileInfo;
  private StatusDto stubStatusDto;

  @BeforeEach
  void setUp(Vertx vertx) {
    var headers = FolioHeaders.builder()
      .connectionUrl(WIRE_MOCK.baseUrl())
      .token(TOKEN)
      .tenant(TENANT_ID)
      .userId(USER_ID)
      .requestId(REQUEST_ID);
    changeManagerClientWrapper = new ChangeManagerClientWrapper(headers, vertx.createHttpClient());

    stubRawRecordsDto = new RawRecordsDto().withId(UUID.randomUUID().toString());
    stubInitJobExecutionsRqDto = new InitJobExecutionsRqDto().withParentJobId(UUID.randomUUID().toString());
    stubJobExecution = new JobExecution().withId(UUID.randomUUID().toString());
    stubJobProfileInfo = new JobProfileInfo().withId(UUID.randomUUID().toString());
    stubStatusDto = new StatusDto();

    WIRE_MOCK.stubFor(post(new UrlPathPattern(new RegexPattern(JOB_EXECUTIONS_PATH), true))
      .withHeader(XOkapiHeaders.URL.toLowerCase(), equalTo(WIRE_MOCK.baseUrl()))
      .withHeader(XOkapiHeaders.TOKEN.toLowerCase(), equalTo(TOKEN))
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
      .withHeader(XOkapiHeaders.USER_ID.toLowerCase(), equalTo(USER_ID))
      .willReturn(WireMock.created()));

    WIRE_MOCK.stubFor(post(
      new UrlPathPattern(new RegexPattern(JOB_EXECUTIONS_PATH + "/" + stubRawRecordsDto.getId() + "/records"),
        true))
      .withQueryParam("acceptInstanceId", equalTo("true"))
      .withHeader(XOkapiHeaders.URL.toLowerCase(), equalTo(WIRE_MOCK.baseUrl()))
      .withHeader(XOkapiHeaders.TOKEN.toLowerCase(), equalTo(TOKEN))
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
      .withHeader(XOkapiHeaders.USER_ID.toLowerCase(), equalTo(USER_ID))
      .willReturn(WireMock.created()));

    WIRE_MOCK.stubFor(
      put(new UrlPathPattern(new RegexPattern(JOB_EXECUTIONS_PATH + "/" + stubJobExecution.getId()), true))
        .withHeader(XOkapiHeaders.URL.toLowerCase(), equalTo(WIRE_MOCK.baseUrl()))
        .withHeader(XOkapiHeaders.TOKEN.toLowerCase(), equalTo(TOKEN))
        .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
        .withHeader(XOkapiHeaders.USER_ID.toLowerCase(), equalTo(USER_ID))
        .willReturn(WireMock.ok()));

    WIRE_MOCK.stubFor(put(new UrlPathPattern(
      new RegexPattern(JOB_EXECUTIONS_PATH + "/" + stubJobProfileInfo.getId() + "/jobProfile"), true))
      .withHeader(XOkapiHeaders.URL.toLowerCase(), equalTo(WIRE_MOCK.baseUrl()))
      .withHeader(XOkapiHeaders.TOKEN.toLowerCase(), equalTo(TOKEN))
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
      .withHeader(XOkapiHeaders.USER_ID.toLowerCase(), equalTo(USER_ID))
      .willReturn(WireMock.ok()));

    WIRE_MOCK.stubFor(put(
      new UrlPathPattern(new RegexPattern(JOB_EXECUTIONS_PATH + "/" + stubJobExecution.getId() + "/status"), true))
      .withHeader(XOkapiHeaders.URL.toLowerCase(), equalTo(WIRE_MOCK.baseUrl()))
      .withHeader(XOkapiHeaders.TOKEN.toLowerCase(), equalTo(TOKEN))
      .withHeader(XOkapiHeaders.TENANT.toLowerCase(), equalTo(TENANT_ID))
      .withHeader(XOkapiHeaders.USER_ID.toLowerCase(), equalTo(USER_ID))
      .willReturn(WireMock.ok()));
  }

  @Test
  void shouldPostChangeManagerJobExecutions(VertxTestContext testContext) {
    var optionalFuture = changeManagerClientWrapper.postChangeManagerJobExecutions(stubInitJobExecutionsRqDto);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_CREATED, result.statusCode());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldPostChangeManagerJobExecutionsRecordsById(VertxTestContext testContext) {
    var optionalFuture = changeManagerClientWrapper
      .postChangeManagerJobExecutionsRecordsById(stubRawRecordsDto.getId(), true, stubRawRecordsDto);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_CREATED, result.statusCode());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldPutChangeManagerJobExecutionsById(VertxTestContext testContext) {
    var optionalFuture = changeManagerClientWrapper
      .putChangeManagerJobExecutionsById(stubJobExecution.getId(), stubJobExecution);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_OK, result.statusCode());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldPutChangeManagerJobExecutionsJobProfileById(VertxTestContext testContext) {
    var optionalFuture = changeManagerClientWrapper
      .putChangeManagerJobExecutionsJobProfileById(stubJobProfileInfo.getId(), stubJobProfileInfo);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_OK, result.statusCode());
      testContext.completeNow();
    })));
  }

  @Test
  void shouldPutChangeManagerJobExecutionsStatusById(VertxTestContext testContext) {
    var optionalFuture = changeManagerClientWrapper
      .putChangeManagerJobExecutionsStatusById(stubJobExecution.getId(), stubStatusDto);

    optionalFuture.onComplete(testContext.succeeding(result -> testContext.verify(() -> {
      assertEquals(SC_OK, result.statusCode());
      testContext.completeNow();
    })));
  }
}
