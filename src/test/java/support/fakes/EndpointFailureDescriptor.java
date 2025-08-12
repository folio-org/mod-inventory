package support.fakes;

import java.util.Date;

public class EndpointFailureDescriptor {
  private Date failureExpireDate;
  private int statusCode;
  private String contentType;
  private String body;
  private String method;
  private String bodyContains;

  public Date getFailureExpireDate() {
    return failureExpireDate;
  }

  public EndpointFailureDescriptor setFailureExpireDate(Date failureExpireDate) {
    this.failureExpireDate = failureExpireDate;
    return this;
  }

  public int getStatusCode() {
    return statusCode;
  }

  public EndpointFailureDescriptor setStatusCode(int statusCode) {
    this.statusCode = statusCode;
    return this;
  }

  public String getContentType() {
    return contentType;
  }

  public EndpointFailureDescriptor setContentType(String contentType) {
    this.contentType = contentType;
    return this;
  }

  public String getBody() {
    return body;
  }

  public EndpointFailureDescriptor setBody(String body) {
    this.body = body;
    return this;
  }

  public String getMethod() {
    return method;
  }

  public EndpointFailureDescriptor setMethod(String method) {
    this.method = method;
    return this;
  }

  public String getBodyContains() {
    return bodyContains;
  }

  public EndpointFailureDescriptor setBodyContains(String bodyContains) {
    this.bodyContains = bodyContains;
    return this;
  }
}
