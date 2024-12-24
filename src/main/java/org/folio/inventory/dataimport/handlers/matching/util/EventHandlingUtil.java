package org.folio.inventory.dataimport.handlers.matching.util;

import io.vertx.core.json.Json;
import io.vertx.core.json.JsonObject;
import org.apache.commons.lang3.StringUtils;
import org.folio.DataImportEventPayload;
import org.folio.MatchProfile;
import org.folio.inventory.common.Context;
import org.folio.inventory.support.JsonHelper;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;

public final class EventHandlingUtil {
  public static final String PAYLOAD_USER_ID = "userId";
  public static final String OKAPI_USER_ID = "x-okapi-user-id";
  public static final String OKAPI_REQUEST_ID = "x-okapi-request-id";
  private static final String CENTRAL_TENANT_ID = "CENTRAL_TENANT_ID";

  private EventHandlingUtil() {}

  public static Context constructContext(String tenantId, String token, String okapiUrl) {
    return constructContext(tenantId, token, okapiUrl, null, null);
  }

  public static Context constructContext(String tenantId, String token, String okapiUrl, String userId) {
    return constructContext(tenantId, token, okapiUrl, userId, null);
  }

  public static Context constructContext(String tenantId, String token, String okapiUrl, String userId, String requestId) {
    return new Context() {
      @Override
      public String getTenantId() {
        return tenantId;
      }

      @Override
      public String getToken() {
        return isSystemUserEnabled() ? "" : token;
      }

      @Override
      public String getOkapiLocation() {
        return okapiUrl;
      }

      @Override
      public String getUserId() {
        return Optional.ofNullable(userId).orElse("");
      }

      @Override
      public String getRequestId() {
        return Optional.ofNullable(requestId).orElse("");
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

  public static String getTenant(DataImportEventPayload payload) {
    String centralTenantId = payload.getContext().get(CENTRAL_TENANT_ID);
    if (centralTenantId != null) {
      return centralTenantId;
    }
    return payload.getTenant();
  }

  /**
   * Extracts match profile from event payload
   * Additional json encoding is needed to return a copy of object not to modify eventPayload
   * @return MatchProfile object deep copy
   * */
  public static MatchProfile extractMatchProfile(DataImportEventPayload dataImportEventPayload) {
    if (dataImportEventPayload.getCurrentNode().getContent() instanceof Map) {
      return (new JsonObject((Map)dataImportEventPayload.getCurrentNode().getContent()))
        .mapTo(MatchProfile.class);
    }

    return new JsonObject(Json.encode(dataImportEventPayload.getCurrentNode().getContent()))
      .mapTo(MatchProfile.class);
  }

  private static boolean isExistsRequiredProperty(JsonObject representation, String propertyName, String nestedPropertyName) {
    String propertyValue = StringUtils.isEmpty(nestedPropertyName)
      ? JsonHelper.getString(representation, propertyName)
      : JsonHelper.getNestedProperty(representation, propertyName, nestedPropertyName);
    return StringUtils.isNotEmpty(propertyValue);
  }

  /**
   * Checks if the system user is enabled based on a system property.
   * <p>
   * This method reads the `SYSTEM_USER_ENABLED` system property and parses
   * its value as a boolean. If the property is not found or cannot be parsed,
   * it defaults to `true`. The method then negates the parsed value and returns it.
   * <p>
   * Note: This functionality is specific to the Eureka environment.
   *
   * @return {@code true} if the system user is set for Eureka env; otherwise {@code false}.
   */
  public static boolean isSystemUserEnabled() {
    return !Boolean.parseBoolean(System.getProperty("SYSTEM_USER_ENABLED", "true"));
  }

}
