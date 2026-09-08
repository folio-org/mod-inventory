package org.folio.inventory.client.util;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import io.netty.handler.codec.http.HttpHeaderNames;
import io.netty.handler.codec.http.HttpHeaderValues;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.WebClient;
import java.util.Map;
import org.folio.dataimport.util.FolioHeaders;
import org.folio.okapi.common.XOkapiHeaders;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

@ExtendWith(MockitoExtension.class)
class ClientWrapperUtilTest {

  private static final String BASE_URL = "http://localhost:9000";
  private static final String REQUEST_PATH = "/api/resource";
  private static final String TENANT = "test-tenant";
  private static final String TOKEN = "test-token";
  private static final String USER_ID = "user-id-123";
  private static final String REQUEST_ID = "request-id-456";

  @Mock
  private WebClient webClient;

  @Mock
  private HttpRequest<Buffer> httpRequest;

  @Test
  @DisplayName("should return empty buffer when object is null")
  void shouldReturnEmptyBuffer_whenObjectIsNull() {
    // act
    var buffer = ClientWrapperUtil.getBuffer(null);

    // assert
    assertThat(buffer.length()).isZero();
  }

  @Test
  @DisplayName("should return json-encoded buffer when object is not null")
  void shouldReturnJsonEncodedBuffer_whenObjectIsNotNull() {
    // arrange
    var data = Map.of("field", "value");

    // act
    var buffer = ClientWrapperUtil.getBuffer(data);

    // assert
    assertThat(buffer.toString()).contains("field").contains("value");
  }

  @Test
  @DisplayName("should invoke requestAbs with combined connection url and path when connection url is present")
  void shouldInvokeRequestAbsWithCombinedUrl_whenConnectionUrlIsPresent() {
    // arrange
    var folioHeaders = FolioHeaders.builder().connectionUrl(BASE_URL).tenant(TENANT).token(TOKEN);
    when(webClient.requestAbs(any(), anyString())).thenReturn(httpRequest);

    // act
    ClientWrapperUtil.createRequest(HttpMethod.GET, REQUEST_PATH, folioHeaders, webClient);

    // assert
    verify(webClient).requestAbs(HttpMethod.GET, BASE_URL + REQUEST_PATH);
  }

  @Test
  @DisplayName("should use empty string as base url when connection url is absent")
  void shouldUseEmptyStringAsBaseUrl_whenConnectionUrlIsAbsent() {
    // arrange
    var folioHeaders = FolioHeaders.builder().tenant(TENANT).token(TOKEN);
    when(webClient.requestAbs(any(), anyString())).thenReturn(httpRequest);

    // act
    ClientWrapperUtil.createRequest(HttpMethod.POST, REQUEST_PATH, folioHeaders, webClient);

    // assert
    verify(webClient).requestAbs(HttpMethod.POST, REQUEST_PATH);
  }

  @Test
  @DisplayName("should set content-type and accept headers on the request")
  void shouldSetContentTypeAndAcceptHeaders() {
    // arrange
    var folioHeaders = FolioHeaders.builder().connectionUrl(BASE_URL).tenant(TENANT);
    when(webClient.requestAbs(any(), anyString())).thenReturn(httpRequest);

    // act
    ClientWrapperUtil.createRequest(HttpMethod.GET, REQUEST_PATH, folioHeaders, webClient);

    // assert
    verify(httpRequest).putHeader(HttpHeaderNames.CONTENT_TYPE, HttpHeaderValues.APPLICATION_JSON);
    verify(httpRequest).putHeader(HttpHeaderNames.ACCEPT,
      HttpHeaderValues.APPLICATION_JSON.concat(",").concat(HttpHeaderValues.TEXT_PLAIN));
  }

  @Test
  @DisplayName("should set folio headers from FolioHeaders on the request")
  void shouldSetFolioHeadersOnRequest() {
    // arrange
    var folioHeaders = FolioHeaders.builder()
      .connectionUrl(BASE_URL)
      .tenant(TENANT)
      .token(TOKEN)
      .userId(USER_ID)
      .requestId(REQUEST_ID);
    when(webClient.requestAbs(any(), anyString())).thenReturn(httpRequest);

    // act
    ClientWrapperUtil.createRequest(HttpMethod.POST, REQUEST_PATH, folioHeaders, webClient);

    // assert
    verify(httpRequest).putHeader(XOkapiHeaders.TENANT, TENANT);
    verify(httpRequest).putHeader(XOkapiHeaders.TOKEN, TOKEN);
    verify(httpRequest).putHeader(XOkapiHeaders.USER_ID, USER_ID);
    verify(httpRequest).putHeader(XOkapiHeaders.REQUEST_ID, REQUEST_ID);
    verify(httpRequest).putHeader(XOkapiHeaders.URL, BASE_URL);
  }

  @Test
  @DisplayName("should return the request created by the web client")
  void shouldReturnRequestCreatedByWebClient() {
    // arrange
    var folioHeaders = FolioHeaders.builder().connectionUrl(BASE_URL).tenant(TENANT);
    when(webClient.requestAbs(any(), anyString())).thenReturn(httpRequest);

    // act
    var result = ClientWrapperUtil.createRequest(HttpMethod.GET, REQUEST_PATH, folioHeaders, webClient);

    // assert
    assertThat(result).isSameAs(httpRequest);
  }
}
