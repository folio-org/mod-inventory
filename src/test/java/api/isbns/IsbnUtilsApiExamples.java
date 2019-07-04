package api.isbns;

import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.inventory.resources.IsbnUtilsApi.CONVERTER_MISSING_REQUIRED_PARAM_MSG;
import static org.folio.inventory.resources.IsbnUtilsApi.INVALID_HYPHENS_VALUE_MSG;
import static org.folio.inventory.resources.IsbnUtilsApi.INVALID_ISBN_MESSAGE;
import static org.folio.inventory.resources.IsbnUtilsApi.ISBN_PARAM;
import static org.folio.inventory.resources.IsbnUtilsApi.VALIDATOR_MISSING_REQUIRED_PARAMS_MSG;
import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

import java.net.MalformedURLException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.folio.inventory.resources.IsbnUtilsApi;
import org.folio.inventory.support.http.client.Response;
import org.folio.inventory.support.http.client.ResponseHandler;
import org.junit.Test;

import api.support.ApiRoot;
import api.support.ApiTests;
import io.vertx.core.json.JsonObject;

public class IsbnUtilsApiExamples extends ApiTests {

  public IsbnUtilsApiExamples() throws MalformedURLException {
    super();
  }

  @Test
  public void testIsbn13ValidatorWithValidIsbn() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    verifyValidator("isbn13=9781930110991", true);
  }

  @Test
  public void testIsbn13ValidatorWithInvalidIsbn() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    verifyValidator("isbn13=9781930a10991", false);
  }

  @Test
  public void testIsbn10ValidatorWithValidIsbn() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    verifyValidator("isbn10=1-930110-99-5", true);
  }

  @Test
  public void testIsbn10ValidatorWithInvalidIsbn() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    verifyValidator("isbn10=0318Y40648", false);
  }

  @Test
  public void testAnyIsbnValidatorWithValidIsbn13() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    verifyValidator("isbn=9781930110991", true);
  }

  @Test
  public void testAnyIsbnValidatorWithValidIsbn10() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    verifyValidator("isbn=1-930110-99-5", true);
  }

  @Test
  public void testAnyIsbnValidatorWithInvalidIsbn() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    verifyValidator("isbn=1-930110-99--5", false);
  }

  @Test
  public void testIsbnConvertTo10FromIsbn13WithValidIsbnWithoutHyphens() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9781930110991"), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkThatResultIsExpected(conversionResponse, "1930110995");
  }

  @Test
  public void testIsbnConvertTo10FromIsbn13WithValidIsbnWithHyphensTrue() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9781930110991&hyphens=true"), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkThatResultIsExpected(conversionResponse, "1-930110-99-5");
  }

  @Test
  public void testIsbnConvertTo10FromIsbn13WithValidIsbnWithHyphensFalse() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9781930110991&hyphens=false"), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkThatResultIsExpected(conversionResponse, "1930110995");
  }

  @Test
  public void testIsbnConvertTo10FromIsbn10WithValidIsbnWithHyphensTrue() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9999999999&hyphens=true"), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkThatResultIsExpected(conversionResponse, "999999999-9");
  }

  @Test
  public void testIsbnConvertTo13FromIsbn10WithValidIsbnWithoutHyphens() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo13("isbn=1930110995"), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkThatResultIsExpected(conversionResponse, "9781930110991");
  }

  @Test
  public void testIsbnConvertTo13FromIsbn10WithValidIsbnWithHyphensTrue() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo13("isbn=1930110995&hyphens=true"), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkThatResultIsExpected(conversionResponse, "978-1-930110-99-1");
  }

  @Test
  public void testIsbnConvertTo13FromIsbn13WithValidIsbnWithHyphensTrue() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo13("isbn=9781930110991&hyphens=true"), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkThatResultIsExpected(conversionResponse, "978-1-930110-99-1");
  }

  private void checkThatResultIsExpected(Response conversionResponse, String expectedCode) {
    assertThat(conversionResponse.getStatusCode(), is(200));
    JsonObject result = conversionResponse.getJson();
    assertThat(result.getString(ISBN_PARAM), is(expectedCode));
  }

  @Test
  public void testIsbnConvertTo13InvalidHyphens() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    String query = "isbn=9781930110991&hyphens=123";
    okapiClient.get(ApiRoot.isbnConvertTo13(query), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkErrorResponse(conversionResponse, INVALID_HYPHENS_VALUE_MSG, query);
  }

  @Test
  public void testIsbnConvertTo13FromIsbn10WithInvalidIsbn() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo13("isbn=1930211099&hyphens=true"), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkErrorResponse(conversionResponse, INVALID_ISBN_MESSAGE, "1930211099");
  }

  @Test
  public void testIsbnConvertTo10FromIsbn13WithInvalidIsbn() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo10("isbn=97819301109911&hyphens=true"), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);


    checkErrorResponse(conversionResponse, INVALID_ISBN_MESSAGE, "97819301109911");
  }

  private void checkErrorResponse(Response conversionResponse, String message, String errorParameterValue) {
    assertThat(conversionResponse.getStatusCode(), is(400));
    JsonObject error = conversionResponse.getJson();
    assertThat(error.getString("message"), is(message));
    assertThat(error.getJsonArray("parameters").getJsonObject(0).getString("value"), is(errorParameterValue));
  }

  @Test
  public void testIsbnConvertTo10FromIsbn13IsbnQueryParamIsMissing() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnConvertTo10(EMPTY), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkErrorResponse(conversionResponse, CONVERTER_MISSING_REQUIRED_PARAM_MSG, EMPTY);
  }

  @Test
  public void testIsbnValidatorQueryParamIsMissing() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnValidate(EMPTY), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkErrorResponse(conversionResponse, VALIDATOR_MISSING_REQUIRED_PARAMS_MSG, EMPTY);
  }

  @Test
  public void testIsbnValidatorQueryMoreThanOneParam() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> conversionCompleted = new CompletableFuture<>();
    String query = "isbn10=109310410&isbn13=07417041";
    okapiClient.get(ApiRoot.isbnValidate(query), ResponseHandler.json(conversionCompleted));

    Response conversionResponse = conversionCompleted.get(5, TimeUnit.SECONDS);

    checkErrorResponse(conversionResponse, VALIDATOR_MISSING_REQUIRED_PARAMS_MSG, query);
  }

  private void verifyValidator(String isbnParam, boolean isValid) throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    CompletableFuture<Response> validateGetCompleted = new CompletableFuture<>();
    okapiClient.get(ApiRoot.isbnValidate(isbnParam), ResponseHandler.json(validateGetCompleted));

    Response validateGetResponse = validateGetCompleted.get(5, TimeUnit.SECONDS);

    assertThat(validateGetResponse.getStatusCode(), is(200));
    JsonObject result = validateGetResponse.getJson();
    assertThat(result.getBoolean(IsbnUtilsApi.IS_VALID), is(isValid));
  }
}
