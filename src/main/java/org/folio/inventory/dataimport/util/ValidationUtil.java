package org.folio.inventory.dataimport.util;

import org.folio.HoldingsRecord;
import org.folio.inventory.domain.instances.Instance;
import org.folio.inventory.domain.items.Item;

import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

public class ValidationUtil {
  public static List<String> validateUUIDs(Instance instance) {
    ArrayList<String> errorMessages = new ArrayList<>();

    validateField(errorMessages, instance.getStatisticalCodeIds(), "statisticalCodeIds");
    instance.getAlternativeTitles().forEach(altTitle -> validateField(errorMessages, altTitle.getAlternativeTitleTypeId(), "alternativeTitles.alternativeTitleTypeId"));
    instance.getIdentifiers().forEach(identifier -> validateField(errorMessages, identifier.getIdentifierTypeId(), "identifiers.identifierTypeId"));
    instance.getContributors().forEach(contributor -> validateField(errorMessages, contributor.getContributorTypeId(), "contributors.contributorTypeId"));
    instance.getContributors().forEach(contributor -> validateField(errorMessages, contributor.getContributorNameTypeId(), "contributors.contributorNameTypeId"));
    validateField(errorMessages, instance.getNatureOfContentTermIds(), "natureOfContentTermIds");
    validateField(errorMessages, instance.getInstanceFormatIds(), "instanceFormatIds");
    instance.getNotes().forEach(note -> validateField(errorMessages, note.getInstanceNoteTypeId(), "notes.instanceNoteTypeId"));
    instance.getClassifications().forEach(note -> validateField(errorMessages, note.getClassificationTypeId(), "classifications.classificationTypeId"));
    return errorMessages;
  }

  public static List<String> validateUUIDs(HoldingsRecord holdingsRecord) {
    ArrayList<String> errorMessages = new ArrayList<>();

    //TODO: The same for Holdings
    return errorMessages;
  }

  public static List<String> validateUUIDs(Item holdingsRecord) {
    ArrayList<String> errorMessages = new ArrayList<>();

    //TODO: The same for Item
    return errorMessages;
  }

  private static void validateField(List<String> errorMessages, String value, String fieldName) {
    if (!isUUID(value)) {
      errorMessages.add(String.format("Value '%s' is not a UUID for %s field", value, fieldName));
    }
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
