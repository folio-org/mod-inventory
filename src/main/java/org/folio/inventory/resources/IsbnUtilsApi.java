package org.folio.inventory.resources;

import static org.apache.commons.collections.CollectionUtils.isEmpty;
import static org.apache.commons.collections.CollectionUtils.isNotEmpty;
import static org.apache.commons.lang3.StringUtils.EMPTY;
import static org.folio.inventory.support.http.server.JsonResponse.badRequest;

import java.lang.invoke.MethodHandles;
import java.util.Map;
import java.util.Optional;
import java.util.function.UnaryOperator;

import org.folio.inventory.support.http.server.JsonResponse;
import org.folio.inventory.support.http.server.ValidationError;
import org.folio.isbn.IsbnUtil;

import com.github.ladutsko.isbn.ISBNException;

import io.vertx.core.MultiMap;
import io.vertx.core.json.JsonObject;
import io.vertx.core.logging.Logger;
import io.vertx.core.logging.LoggerFactory;
import io.vertx.ext.web.Router;
import io.vertx.ext.web.RoutingContext;

public class IsbnUtilsApi {

  private static final Logger log = LoggerFactory.getLogger(MethodHandles.lookup().lookupClass());

  private static final String ISBN_VALIDATOR_PATH = "/isbn/validator";
  private static final String ISBN_CONVERT_TO_ISBN10_PATH = "/isbn/convertTo10";
  private static final String ISBN_CONVERT_TO_ISBN13_PATH = "/isbn/convertTo13";
  public static final String HYPHENS_PARAM = "hyphens";
  public static final String ISBN_PARAM = "isbn";
  public static final String IS_VALID = "isValid";
  public static final String CONVERTER_MISSING_REQUIRED_PARAM_MSG = "Isbn must be specified";
  public static final String INVALID_HYPHENS_VALUE_MSG = "Hyphens must be true or false";
  public static final String INVALID_ISBN_MESSAGE = "ISBN value is invalid";
  public static final String VALIDATOR_MISSING_REQUIRED_PARAMS_MSG = "Only one of following query params must be specified: isbn, isbn10, isbn13";
  public static final String QUERY = "query";


  public void register(Router router) {
    router.get(ISBN_VALIDATOR_PATH).handler(this::validate);
    router.get(ISBN_CONVERT_TO_ISBN10_PATH).handler(this::convertToIsbn10);
    router.get(ISBN_CONVERT_TO_ISBN13_PATH).handler(this::convertToIsbn13);
  }

  private void convertToIsbn13(RoutingContext routingContext) {
    UnaryOperator<String> toIsbn13Converter = isbnCode ->
      IsbnUtil.isValid13DigitNumber(isbnCode)
        ? isbnCode
        : IsbnUtil.convertTo13DigitNumber(isbnCode);

    convert(routingContext, toIsbn13Converter);
  }

  private void convertToIsbn10(RoutingContext routingContext) {
    UnaryOperator<String> toIsbn10Converter = isbnCode ->
      IsbnUtil.isValid10DigitNumber(isbnCode)
        ? isbnCode
        : IsbnUtil.convertTo10DigitNumber(isbnCode);

    convert(routingContext, toIsbn10Converter);
  }

  private void convert(RoutingContext routingContext, UnaryOperator<String> converter) {

    String isbnCode = EMPTY;

    try {

      isbnCode = getIsbnParamValue(routingContext);
      boolean isHyphens = getHyphensParamValue(routingContext);

      isbnCode = Optional.ofNullable(converter.apply(isbnCode))
        .orElseThrow(() -> new ISBNException(INVALID_ISBN_MESSAGE));

      isbnCode = insertHyphens(isbnCode, isHyphens);

      JsonObject result = new JsonObject();
      result.put(ISBN_PARAM, isbnCode);
      JsonResponse.success(routingContext.response(), result);

    } catch (ISBNException e) {
      log.error(e);
      ValidationError error = new ValidationError(e.getMessage(), ISBN_PARAM, isbnCode);
      badRequest(routingContext.response(), error);
    } catch (IllegalArgumentException e) {
      log.error(e);
      ValidationError error = new ValidationError(e.getMessage(), QUERY, routingContext.request().query());
      badRequest(routingContext.response(), error);
    }
  }

  private String getIsbnParamValue(RoutingContext routingContext) {
    String isbnCode;
    if (isEmpty(routingContext.queryParam(ISBN_PARAM))) {
      throw new IllegalArgumentException(CONVERTER_MISSING_REQUIRED_PARAM_MSG);
    }

    isbnCode = routingContext.queryParam(ISBN_PARAM).get(0);
    return isbnCode;
  }

  private boolean getHyphensParamValue(RoutingContext routingContext) {
    if (isNotEmpty(routingContext.queryParam(HYPHENS_PARAM))) {
      String hyphensValue = routingContext.queryParam(HYPHENS_PARAM).get(0);

      if ("true".equalsIgnoreCase(hyphensValue) || "false".equalsIgnoreCase(hyphensValue)) {
        return Boolean.valueOf(hyphensValue);
      }
      throw new IllegalArgumentException(INVALID_HYPHENS_VALUE_MSG);
    }
    return false;
  }

  private String insertHyphens(String isbnCode, boolean isHyphens) {
    if (isHyphens) {
      isbnCode = IsbnUtil.insertHyphens(isbnCode);
    }
    return isbnCode;
  }

  private void validate(RoutingContext routingContext) {
    routingContext.request().query();
    JsonObject result = new JsonObject();
    boolean isValid;
    String paramName = EMPTY;
    String isbnCode = EMPTY;

    MultiMap params = routingContext.queryParams();

    if (params.entries().size() == 1) {
      Map.Entry<String, String> param = params.entries().get(0);
      paramName = param.getKey();
      isbnCode = param.getValue();
    }

    switch (paramName) {
      case "isbn10":
        isValid = IsbnUtil.isValid10DigitNumber(isbnCode);
        break;
      case "isbn13":
        isValid = IsbnUtil.isValid13DigitNumber(isbnCode);
        break;
      case ISBN_PARAM:
        isValid = IsbnUtil.isValid10DigitNumber(isbnCode) || IsbnUtil.isValid13DigitNumber(isbnCode);
        break;
      default: badRequest(routingContext.response(),
        new ValidationError(VALIDATOR_MISSING_REQUIRED_PARAMS_MSG, QUERY, routingContext.request().query()));
        return;
    }

    result.put(IS_VALID, isValid);
    JsonResponse.success(routingContext.response(), result);
  }

}
