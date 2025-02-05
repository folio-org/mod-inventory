package org.folio.inventory.client.wrappers;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpRequest;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;

import static org.folio.inventory.client.util.ClientWrapperUtil.ACCEPT;
import static org.folio.inventory.client.util.ClientWrapperUtil.APPLICATION_JSON;
import static org.folio.inventory.client.util.ClientWrapperUtil.APPLICATION_JSON_TEXT_PLAIN;
import static org.folio.inventory.client.util.ClientWrapperUtil.CONTENT_TYPE;
import static org.folio.inventory.client.util.ClientWrapperUtil.getBuffer;
import static org.folio.inventory.client.util.ClientWrapperUtil.populateOkapiHeaders;

public class ChangeManagerClientWrapper extends ChangeManagerClient {
  private final String tenantId;
  private final String token;
  private final String okapiUrl;
  private final String userId;
  private final WebClient webClient;
  public static final String CHANGE_MANAGER_JOB_EXECUTIONS = "/change-manager/jobExecutions/";
  public static final String CHANGE_MANAGER_PARSED_RECORDS = "/change-manager/parsedRecords/";

  public ChangeManagerClientWrapper(String okapiUrl, String tenantId, String token, String userId, HttpClient httpClient) {
    super(okapiUrl, tenantId, token, httpClient);
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.token = token;
    this.userId = userId;
    this.webClient = WebClient.wrap(httpClient);
  }

  @Override
  public Future<HttpResponse<Buffer>> postChangeManagerJobExecutions(InitJobExecutionsRqDto initJobExecutionsRqDto) {
    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, okapiUrl + "/change-manager/jobExecutions");
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(initJobExecutionsRqDto));
  }

  @Override
  public Future<HttpResponse<Buffer>> postChangeManagerJobExecutionsRecordsById(String id, boolean acceptInstanceId, RawRecordsDto rawRecordsDto) {
    StringBuilder queryParams = new StringBuilder("?");
    queryParams.append("acceptInstanceId=");
    queryParams.append(acceptInstanceId);

    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.POST, okapiUrl + CHANGE_MANAGER_JOB_EXECUTIONS + id + "/records" + queryParams);
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(rawRecordsDto));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerJobExecutionsById(String id, JobExecution jobExecution) {
    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.PUT, okapiUrl + CHANGE_MANAGER_JOB_EXECUTIONS + id);
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(jobExecution));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerJobExecutionsJobProfileById(String id, JobProfileInfo jobProfileInfo) {
    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.PUT, okapiUrl + CHANGE_MANAGER_JOB_EXECUTIONS + id + "/jobProfile");
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(jobProfileInfo));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerJobExecutionsStatusById(String id, StatusDto statusDto) {
    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.PUT, okapiUrl + CHANGE_MANAGER_JOB_EXECUTIONS + id + "/status");
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(statusDto));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerParsedRecordsById(String id, ParsedRecordDto parsedRecordDto) {
    HttpRequest<Buffer> request = webClient.requestAbs(HttpMethod.PUT, okapiUrl + CHANGE_MANAGER_PARSED_RECORDS + id);
    request.putHeader(CONTENT_TYPE, APPLICATION_JSON);
    request.putHeader(ACCEPT, APPLICATION_JSON_TEXT_PLAIN);
    populateOkapiHeaders(request, okapiUrl, tenantId, token, userId);

    return request.sendBuffer(getBuffer(parsedRecordDto));
  }
}
