package org.folio.inventory.client.wrappers;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.ParsedRecordDto;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;

import static org.folio.inventory.client.util.ClientWrapperUtil.createRequest;
import static org.folio.inventory.client.util.ClientWrapperUtil.getBuffer;

/**
 * Wrapper class for ChangeManagerClient to handle POST and PUT HTTP requests with x-okapi-user-id header.
 */
public class ChangeManagerClientWrapper extends ChangeManagerClient {
  private final String tenantId;
  private final String token;
  private final String okapiUrl;
  private final String userId;
  private final String requestId;
  private final WebClient webClient;
  public static final String CHANGE_MANAGER_JOB_EXECUTIONS = "/change-manager/jobExecutions/";
  public static final String CHANGE_MANAGER_PARSED_RECORDS = "/change-manager/parsedRecords/";

  public ChangeManagerClientWrapper(String okapiUrl, String tenantId, String token, String userId, String requestId, HttpClient httpClient) {
    super(okapiUrl, tenantId, token, httpClient);
    this.okapiUrl = okapiUrl;
    this.tenantId = tenantId;
    this.token = token;
    this.userId = userId;
    this.requestId = requestId;
    this.webClient = WebClient.wrap(httpClient);
  }

  @Override
  public Future<HttpResponse<Buffer>> postChangeManagerJobExecutions(InitJobExecutionsRqDto initJobExecutionsRqDto) {
    return createRequest(HttpMethod.POST, okapiUrl + "/change-manager/jobExecutions", okapiUrl, tenantId, token, userId, requestId, webClient)
      .sendBuffer(getBuffer(initJobExecutionsRqDto));
  }

  @Override
  public Future<HttpResponse<Buffer>> postChangeManagerJobExecutionsRecordsById(String id, boolean acceptInstanceId, RawRecordsDto rawRecordsDto) {
    StringBuilder queryParams = new StringBuilder("?");
    queryParams.append("acceptInstanceId=");
    queryParams.append(acceptInstanceId);

    return createRequest(HttpMethod.POST, okapiUrl + CHANGE_MANAGER_JOB_EXECUTIONS + id + "/records" + queryParams,
      okapiUrl, tenantId, token, userId, requestId, webClient)
      .sendBuffer(getBuffer(rawRecordsDto));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerJobExecutionsById(String id, JobExecution jobExecution) {
    return createRequest(HttpMethod.PUT, okapiUrl + CHANGE_MANAGER_JOB_EXECUTIONS + id,
      okapiUrl, tenantId, token, userId, requestId, webClient)
      .sendBuffer(getBuffer(jobExecution));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerJobExecutionsJobProfileById(String id, JobProfileInfo jobProfileInfo) {
    return createRequest(HttpMethod.PUT, okapiUrl + CHANGE_MANAGER_JOB_EXECUTIONS + id + "/jobProfile",
      okapiUrl, tenantId, token, userId, requestId, webClient)
      .sendBuffer(getBuffer(jobProfileInfo));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerJobExecutionsStatusById(String id, StatusDto statusDto) {
    return createRequest(HttpMethod.PUT, okapiUrl + CHANGE_MANAGER_JOB_EXECUTIONS + id + "/status",
      okapiUrl, tenantId, token, userId, requestId, webClient)
      .sendBuffer(getBuffer(statusDto));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerParsedRecordsById(String id, ParsedRecordDto parsedRecordDto) {
    return createRequest(HttpMethod.PUT, okapiUrl + CHANGE_MANAGER_PARSED_RECORDS + id,
      okapiUrl, tenantId, token, userId, requestId, webClient)
      .sendBuffer(getBuffer(parsedRecordDto));
  }
}
