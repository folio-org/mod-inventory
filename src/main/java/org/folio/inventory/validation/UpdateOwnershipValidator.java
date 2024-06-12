package org.folio.inventory.validation;

import io.vertx.core.json.JsonArray;
import io.vertx.core.json.JsonObject;
import org.folio.inventory.support.http.server.ValidationError;

import java.lang.reflect.Field;
import java.util.Arrays;
import java.util.List;
import java.util.Optional;

import static org.folio.inventory.support.MoveApiUtil.TARGET_TENANT_ID;

public final class UpdateOwnershipValidator {
  private UpdateOwnershipValidator() { }

  public static Optional<ValidationError> updateOwnershipHasRequiredFields(String sourceTenant, JsonObject updateOwnershipRequest, Class<?> updateOwnershipClass) {
    List<String> requiredFields = Arrays.stream(updateOwnershipClass.getDeclaredFields()).map(Field::getName).toList();
    for (String field: requiredFields) {
      var value = updateOwnershipRequest.getValue(field);
      if (value == null || (value instanceof JsonArray && ((JsonArray) value).isEmpty())) {
        return Optional.of(new ValidationError(field + " is a required field", field, null));
      }
    }
    String targetTenantId = updateOwnershipRequest.getString(TARGET_TENANT_ID);
    if (sourceTenant.equals(targetTenantId)) {
      return Optional.of(new ValidationError("targetTenantId field cannot be equal to source tenant id", TARGET_TENANT_ID, targetTenantId));
    }
    return Optional.empty();
  }
}
