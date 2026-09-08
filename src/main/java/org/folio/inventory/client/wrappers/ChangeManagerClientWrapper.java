package org.folio.inventory.client.wrappers;

import static org.folio.inventory.client.util.ClientWrapperUtil.createRequest;
import static org.folio.inventory.client.util.ClientWrapperUtil.getBuffer;

import io.vertx.core.Future;
import io.vertx.core.buffer.Buffer;
import io.vertx.core.http.HttpClient;
import io.vertx.core.http.HttpMethod;
import io.vertx.ext.web.client.HttpResponse;
import io.vertx.ext.web.client.WebClient;
import org.folio.dataimport.util.FolioHeaders;
import org.folio.rest.client.ChangeManagerClient;
import org.folio.rest.jaxrs.model.InitJobExecutionsRqDto;
import org.folio.rest.jaxrs.model.JobExecution;
import org.folio.rest.jaxrs.model.JobProfileInfo;
import org.folio.rest.jaxrs.model.RawRecordsDto;
import org.folio.rest.jaxrs.model.StatusDto;

/**
 * Wrapper class for ChangeManagerClient to handle POST and PUT HTTP requests with x-okapi-user-id header.
 */
public class ChangeManagerClientWrapper extends ChangeManagerClient {

  private static final String JOBS_PATH = "/change-manager/jobExecutions";
  private static final String JOB_BY_ID_PATH = JOBS_PATH + "/%s";
  private static final String JOB_PROFILE_PATH = JOB_BY_ID_PATH + "/jobProfile";
  private static final String JOB_STATUS_PATH = JOB_BY_ID_PATH + "/status";
  private static final String JOB_RECORDS_PATH = JOB_BY_ID_PATH + "/records";

  private final WebClient webClient;
  private final FolioHeaders folioHeaders;

  public ChangeManagerClientWrapper(FolioHeaders folioHeaders, HttpClient httpClient) {
    super(folioHeaders.getConnectionUrl().orElse(null),
      folioHeaders.getTenantId().orElse(null),
      folioHeaders.getToken().orElse(null),
      httpClient);
    this.folioHeaders = folioHeaders;
    this.webClient = WebClient.wrap(httpClient);
  }

  @Override
  public Future<HttpResponse<Buffer>> postChangeManagerJobExecutions(InitJobExecutionsRqDto initJobExecutionsRqDto) {
    return createRequest(HttpMethod.POST, JOBS_PATH, folioHeaders, webClient)
      .sendBuffer(getBuffer(initJobExecutionsRqDto));
  }

  @Override
  public Future<HttpResponse<Buffer>> postChangeManagerJobExecutionsRecordsById(String id, boolean acceptInstanceId,
                                                                                RawRecordsDto rawRecordsDto) {
    String queryParams = "?" + "acceptInstanceId=" + acceptInstanceId;

    return createRequest(HttpMethod.POST, JOB_RECORDS_PATH.formatted(id) + queryParams, folioHeaders, webClient)
      .sendBuffer(getBuffer(rawRecordsDto));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerJobExecutionsById(String id, JobExecution jobExecution) {
    return createRequest(HttpMethod.PUT, JOB_BY_ID_PATH.formatted(id), folioHeaders, webClient)
      .sendBuffer(getBuffer(jobExecution));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerJobExecutionsJobProfileById(String id,
                                                                                  JobProfileInfo jobProfileInfo) {
    return createRequest(HttpMethod.PUT, JOB_PROFILE_PATH.formatted(id), folioHeaders, webClient)
      .sendBuffer(getBuffer(jobProfileInfo));
  }

  @Override
  public Future<HttpResponse<Buffer>> putChangeManagerJobExecutionsStatusById(String id, StatusDto statusDto) {
    return createRequest(HttpMethod.PUT, JOB_STATUS_PATH.formatted(id), folioHeaders, webClient)
      .sendBuffer(getBuffer(statusDto));
  }
}
