package api.isbns;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.inventory.resources.IsbnUtilsApi.CONVERTER_MISSING_REQUIRED_PARAM_MSG;
import static org.folio.inventory.resources.IsbnUtilsApi.INVALID_HYPHENS_VALUE_MSG;
import static org.folio.inventory.resources.IsbnUtilsApi.INVALID_ISBN_MESSAGE;
import static org.folio.inventory.resources.IsbnUtilsApi.ISBN_PARAM;
import static org.folio.inventory.resources.IsbnUtilsApi.VALIDATOR_MISSING_REQUIRED_PARAMS_MSG;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import io.vertx.core.json.JsonObject;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeoutException;
import lombok.SneakyThrows;
import org.folio.inventory.resources.IsbnUtilsApi;
import org.folio.inventory.support.http.client.Response;
import org.junit.jupiter.api.Test;
import support.ApiRoot;
import support.ApiTests;
import support.FutureAssistance;

@SuppressWarnings("java:S5786")
public class IsbnUtilsApiTest extends ApiTests {

  @Test
  void testIsbn13ValidatorWithValidIsbn() {
    verifyValidator("isbn13=9781930110991", true);
  }

  @Test
  void testIsbn13ValidatorWithInvalidIsbn() {
    verifyValidator("isbn13=9781930a10991", false);
  }

  @Test
  void testIsbn10ValidatorWithValidIsbn() {
    verifyValidator("isbn10=1-930110-99-5", true);
  }

  @Test
  void testIsbn10ValidatorWithInvalidIsbn() {
    verifyValidator("isbn10=0318Y40648", false);
  }

  @Test
  void testAnyIsbnValidatorWithValidIsbn13() {
    verifyValidator("isbn=9781930110991", true);
  }

  @Test
  void testAnyIsbnValidatorWithValidIsbn10() {
    verifyValidator("isbn=1-930110-99-5", true);
  }

  @Test
  void testAnyIsbnValidatorWithInvalidIsbn() {
    verifyValidator("isbn=1-930110-99--5", false);
  }

  @Test
  void testIsbnConvertTo10FromIsbn13WithValidIsbnWithoutHyphens()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9781930110991")), 5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "1930110995");
  }

  @Test
  void testIsbnConvertTo10FromIsbn13WithValidIsbnWithHyphensTrue()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9781930110991&hyphens=true")), 5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "1-930110-99-5");
  }

  @Test
  void testIsbnConvertTo10FromIsbn13WithValidIsbnWithHyphensFalse()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9781930110991&hyphens=false")), 5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "1930110995");
  }

  @Test
  void testIsbnConvertTo10FromIsbn10WithValidIsbnWithHyphensTrue()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo10("isbn=9999999999&hyphens=true")), 5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "999999999-9");
  }

  @Test
  void testIsbnConvertTo13FromIsbn10WithValidIsbnWithoutHyphens()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo13("isbn=1930110995")), 5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "9781930110991");
  }

  @Test
  void testIsbnConvertTo13FromIsbn10WithValidIsbnWithHyphensTrue()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo13("isbn=1930110995&hyphens=true")), 5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "978-1-930110-99-1");
  }

  @Test
  void testIsbnConvertTo13FromIsbn13WithValidIsbnWithHyphensTrue()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo13("isbn=9781930110991&hyphens=true")), 5, SECONDS);

    checkThatResultIsExpected(conversionResponse, "978-1-930110-99-1");
  }

  @Test
  void testIsbnConvertTo13InvalidHyphens() throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo13("isbn=9781930110991&hyphens=123")), 5, SECONDS);

    checkErrorResponse(conversionResponse, INVALID_HYPHENS_VALUE_MSG);
  }

  @Test
  void testIsbnConvertTo13FromIsbn10WithInvalidIsbn()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo13("isbn=1930211099&hyphens=true")), 5, SECONDS);

    checkErrorResponse(conversionResponse, String.format(INVALID_ISBN_MESSAGE, "1930211099"));
  }

  @Test
  void testIsbnConvertTo10FromIsbn13WithInvalidIsbn()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo10("isbn=97819301109911&hyphens=true")), 5, SECONDS);

    checkErrorResponse(conversionResponse, String.format(INVALID_ISBN_MESSAGE, "97819301109911"));
  }

  @Test
  void testIsbnConvertTo10FromIsbn13IsbnQueryParamIsMissing()
    throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnConvertTo10(EMPTY)), 5, SECONDS);

    checkErrorResponse(conversionResponse, CONVERTER_MISSING_REQUIRED_PARAM_MSG);
  }

  @Test
  void testIsbnValidatorQueryParamIsMissing() throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnValidate(EMPTY)), 5, SECONDS);

    checkErrorResponse(conversionResponse, VALIDATOR_MISSING_REQUIRED_PARAMS_MSG);
  }

  @Test
  void testIsbnValidatorQueryMoreThanOneParam() throws InterruptedException, ExecutionException, TimeoutException {
    Response conversionResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnValidate("isbn10=109310410&isbn13=07417041")), 5, SECONDS);

    checkErrorResponse(conversionResponse, VALIDATOR_MISSING_REQUIRED_PARAMS_MSG);
  }

  private void checkThatResultIsExpected(Response conversionResponse, String expectedCode) {
    assertThat(conversionResponse.statusCode(), is(200));
    JsonObject result = conversionResponse.getJson();
    assertThat(result.getString(ISBN_PARAM), is(expectedCode));
  }

  private void checkErrorResponse(Response conversionResponse, String message) {
    assertThat(conversionResponse.statusCode(), is(400));
    assertThat(conversionResponse.body(), is(message));
  }

  @SneakyThrows
  private void verifyValidator(String isbnParam, boolean isValid) {
    Response validateGetResponse = FutureAssistance.getOnCompletion(
      okapiClient.get(ApiRoot.isbnValidate(isbnParam)), 5, SECONDS);

    assertThat(validateGetResponse.statusCode(), is(200));
    JsonObject result = validateGetResponse.getJson();
    assertThat(result.getBoolean(IsbnUtilsApi.IS_VALID), is(isValid));
  }
}
