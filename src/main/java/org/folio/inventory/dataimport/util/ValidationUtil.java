package org.folio.inventory.dataimport.util;

import org.folio.inventory.domain.instances.Instance;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

/**
 * Util for detailed validation different entities.
 */
public class ValidationUtil {

  public static final String INVALID_STATISTICAL_CODE_MSG = "Provided Statistical code is not a valid value.";

  private ValidationUtil() {
  }

  /**
   * Validate fields inside the Instance entity. Validation based on checking if specific fields were mapped as UUIDs.
   * If not - then the list with errors will be returned.
   * Example: "Value 'invalid not UUID value' is not a UUID for someFieldName field"
   * @param instance target Instance for validation
   * @return ArrayList with errors when the needed fields are NOT as UUID.
   */
  public static List<String> validateUUIDs(Instance instance) {
    ArrayList<String> errorMessages = new ArrayList<>();

    //TODO: This will be extended for different fields and entities.That's why there are so many methods just for 1 field.
    // Branch for it extending validation: MODINV-1012-extended
    validateField(errorMessages, instance.getNatureOfContentTermIds(), "natureOfContentTermIds");
    validateStatisticalCodeIds(errorMessages, instance.getStatisticalCodeIds());

    return errorMessages;
  }

  /**
   * Validates that all provided statistical code IDs are valid UUIDs.
   * Returns an error message for each invalid entry.
   * @param ids list of statistical code IDs to validate
   * @return list of error messages; empty if all IDs are valid UUIDs
   */
  public static List<String> validateStatisticalCodeIds(List<String> ids) {
    ArrayList<String> errorMessages = new ArrayList<>();
    validateStatisticalCodeIds(errorMessages, ids);
    return errorMessages;
  }

  private static void validateStatisticalCodeIds(List<String> errorMessages, List<String> ids) {
    ids.stream()
      .filter(id -> !isUUID(id))
      .forEach(id -> errorMessages.add(INVALID_STATISTICAL_CODE_MSG));
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
