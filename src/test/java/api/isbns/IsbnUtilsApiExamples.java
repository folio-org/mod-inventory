package api.isbns;

import api.support.ApiRoot;
import api.support.ApiTests;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.resources.IsbnUtilsApi;
import org.folio.inventory.support.http.client.Response;
import org.junit.Test;

import java.net.MalformedURLException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.inventory.resources.IsbnUtilsApi.CONVERTER_MISSING_REQUIRED_PARAM_MSG;
import static org.folio.inventory.resources.IsbnUtilsApi.INVALID_HYPHENS_VALUE_MSG;
import static org.folio.inventory.resources.IsbnUtilsApi.INVALID_ISBN_MESSAGE;
import static org.folio.inventory.resources.IsbnUtilsApi.ISBN_PARAM;
import static org.folio.inventory.resources.IsbnUtilsApi.VALIDATOR_MISSING_REQUIRED_PARAMS_MSG;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

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
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9781930110991"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "1930110995");
  }

  @Test
  public void testIsbnConvertTo10FromIsbn13WithValidIsbnWithHyphensTrue() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9781930110991&hyphens=true"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "1-930110-99-5");
  }

  @Test
  public void testIsbnConvertTo10FromIsbn13WithValidIsbnWithHyphensFalse() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9781930110991&hyphens=false"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "1930110995");
  }

  @Test
  public void testIsbnConvertTo10FromIsbn10WithValidIsbnWithHyphensTrue() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9999999999&hyphens=true"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "999999999-9");
  }

  @Test
  public void testIsbnConvertTo13FromIsbn10WithValidIsbnWithoutHyphens() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo13("isbn=1930110995"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "9781930110991");
  }

  @Test
  public void testIsbnConvertTo13FromIsbn10WithValidIsbnWithHyphensTrue() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo13("isbn=1930110995&hyphens=true"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "978-1-930110-99-1");
  }

  @Test
  public void testIsbnConvertTo13FromIsbn13WithValidIsbnWithHyphensTrue() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo13("isbn=9781930110991&hyphens=true"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "978-1-930110-99-1");
  }

  private void checkThatResultIsExpected(Response conversionResponse, String expectedCode) {
    assertThat(conversionResponse.getStatusCode(), is(200));
    JsonObject result = conversionResponse.getJson();
    assertThat(result.getString(ISBN_PARAM), is(expectedCode));
  }

  @Test
  public void testIsbnConvertTo13InvalidHyphens() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo13("isbn=9781930110991&hyphens=123"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkErrorResponse(conversionResponse, INVALID_HYPHENS_VALUE_MSG);
  }

  @Test
  public void testIsbnConvertTo13FromIsbn10WithInvalidIsbn() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo13("isbn=1930211099&hyphens=true"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkErrorResponse(conversionResponse, String.format(INVALID_ISBN_MESSAGE, "1930211099"));
  }

  @Test
  public void testIsbnConvertTo10FromIsbn13WithInvalidIsbn() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnConvertTo10("isbn=97819301109911&hyphens=true"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkErrorResponse(conversionResponse, String.format(INVALID_ISBN_MESSAGE, "97819301109911"));
  }

  private void checkErrorResponse(Response conversionResponse, String message) {
    assertThat(conversionResponse.getStatusCode(), is(400));
    assertThat(conversionResponse.getBody(), is(message));
  }

  @Test
  public void testIsbnConvertTo10FromIsbn13IsbnQueryParamIsMissing() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted = okapiClient.get(ApiRoot.isbnConvertTo10(EMPTY));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkErrorResponse(conversionResponse, CONVERTER_MISSING_REQUIRED_PARAM_MSG);
  }

  @Test
  public void testIsbnValidatorQueryParamIsMissing() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted = okapiClient.get(ApiRoot.isbnValidate(EMPTY));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkErrorResponse(conversionResponse, VALIDATOR_MISSING_REQUIRED_PARAMS_MSG);
  }

  @Test
  public void testIsbnValidatorQueryMoreThanOneParam() throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var conversionCompleted
      = okapiClient.get(ApiRoot.isbnValidate("isbn10=109310410&isbn13=07417041"));

    Response conversionResponse = conversionCompleted.toCompletableFuture().get(5, SECONDS);

    checkErrorResponse(conversionResponse, VALIDATOR_MISSING_REQUIRED_PARAMS_MSG);
  }

  private void verifyValidator(String isbnParam, boolean isValid) throws MalformedURLException, InterruptedException, ExecutionException, TimeoutException {
    final var validateGetCompleted = okapiClient.get(ApiRoot.isbnValidate(isbnParam));

    Response validateGetResponse = validateGetCompleted.toCompletableFuture().get(5, SECONDS);

    assertThat(validateGetResponse.getStatusCode(), is(200));
    JsonObject result = validateGetResponse.getJson();
    assertThat(result.getBoolean(IsbnUtilsApi.IS_VALID), is(isValid));
  }
}
