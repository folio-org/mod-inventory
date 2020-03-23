package org.folio.inventory.dataimport.handlers.matching.util;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.StringUtils;
import org.folio.inventory.common.Context;
import org.folio.inventory.support.JsonHelper;

import io.vertx.core.json.JsonObject;

public final class EventHandlingUtil {

  private EventHandlingUtil() {}

  public static Context constructContext(String tenantId, String token, String okapiUrl) {
    return new Context() {
      @Override
      public String getTenantId() {
        return tenantId;
      }

      @Override
      public String getToken() {
        return token;
      }

      @Override
      public String getOkapiLocation() {
        return okapiUrl;
      }

      @Override
      public String getUserId() {
        return "";
      }
    };
  }

  public static List<String> validateJsonByRequiredFields(final JsonObject jsonObject, final List<String> requiredFields) {
    ArrayList<String> errorMessages = new ArrayList<>();
    for (String fieldPath : requiredFields) {
      String field = StringUtils.substringBefore(fieldPath, ".");
      String nestedField = StringUtils.substringAfter(fieldPath, ".");
      if (!isExistsRequiredProperty(jsonObject, field, nestedField)) {
        errorMessages.add(String.format("Field '%s' is a required field and can not be null", fieldPath));
      }
    }
    return errorMessages;
  }

  private static boolean isExistsRequiredProperty(JsonObject representation, String propertyName, String nestedPropertyName) {
    String propertyValue = StringUtils.isEmpty(nestedPropertyName)
      ? JsonHelper.getString(representation, propertyName)
      : JsonHelper.getNestedProperty(representation, propertyName, nestedPropertyName);
    return StringUtils.isNotEmpty(propertyValue);
  }
}
