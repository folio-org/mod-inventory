package org.folio.inventory.dataimport.util;

import org.folio.inventory.domain.instances.Instance;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Util for detailed validation different entities.
 */
public class ValidationUtil {

  private ValidationUtil() {
  }

  public static List<String> validateUUIDs(Instance instance) {
    ArrayList<String> errorMessages = new ArrayList<>();

    //TODO: There will be extended for different fields and entities.That's why it is so different methods for 1 field. Branch for it: MODINV-1012-extended
    validateField(errorMessages, instance.getNatureOfContentTermIds(), "natureOfContentTermIds");

    return errorMessages;
  }

  private static void validateField(List<String> errorMessages, List<String> values, String fieldName) {
    values.stream()
      .filter(value -> !isUUID(value))
      .forEach(value -> errorMessages.add(String.format("Value '%s' is not a UUID for %s field", value, fieldName)));
  }

  private static boolean isUUID(String value) {
    try {
      UUID.fromString(value);
      return true;
    } catch (Exception ex) {
      return false;
    }
  }
}
