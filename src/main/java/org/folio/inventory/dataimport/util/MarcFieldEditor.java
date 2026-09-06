package org.folio.inventory.dataimport.util;

import static org.apache.commons.lang3.StringUtils.isNotEmpty;

import java.util.List;
import org.apache.commons.collections4.CollectionUtils;
import org.marc4j.marc.ControlField;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.Record;
import org.marc4j.marc.Subfield;
import org.marc4j.marc.VariableField;

/**
 * Pure marc4j field-manipulation logic, with no dependency on any FOLIO {@code Record} type, no cache and no
 * I/O (parsing/serialization). Every method here takes an already-parsed {@link Record} and
 * either mutates it in place or reads a value off it - callers own parsing the record in, and writing/caching
 * it back out.
 *
 * <p>This is the extraction candidate for a future shared library (see the mod-inventory refactor plan): keeping
 * it free of {@code org.folio.*} types, Caffeine/cache types, and marc4j reader/writer (I/O) types is what makes
 * it portable.
 */
public final class MarcFieldEditor {

  /**
   * Both indicators used to mark data fields (e.g. tag 999) added by inventory itself, as opposed to fields
   * that arrived on the incoming record - matches {@code AdditionalFieldsUtil.INDICATOR_F}. Duplicated (not
   * referenced) so this class carries no dependency on any FOLIO-facing class.
   */
  private static final char INDICATOR_F = 'f';

  private MarcFieldEditor() {
  }

  /**
   * Adds a new controlled (control) field to the marc record, or replaces the existing field with the same tag.
   *
   * @param marcRecord marc4j record to mutate
   * @param tag        tag of the controlled field
   * @param value      value of the field
   * @param replace    if true, replaces an existing field with the same tag; if false, appends a new field
   */
  public static void addOrReplaceControlField(Record marcRecord, String tag, String value, boolean replace) {
    if (replace) {
      var currentField = (ControlField) marcRecord.getVariableField(tag);
      var newControlField = MarcFactory.newInstance().newControlField(tag, value);
      if (currentField != null) {
        marcRecord.getControlFields().set(marcRecord.getControlFields().indexOf(currentField), newControlField);
      } else {
        marcRecord.addVariableField(newControlField);
      }
    } else {
      ControlField controlField = MarcFactory.newInstance().newControlField(tag, value);
      marcRecord.addVariableField(controlField);
    }
  }

  /**
   * Adds a subfield with the given value to the first data field found with the given tag and indicators
   * {@code ff} (see {@link #getSingleFieldByIndicators}), replacing any existing subfield with the same code on
   * that field; if no such field exists, a new data field with indicators {@code ff} is created.
   *
   * @param marcRecord marc4j record to mutate
   * @param field      tag of the data field
   * @param subfield   subfield code to add
   * @param value      value of the subfield to add
   */
  public static void addSubfieldToField(Record marcRecord, String field, char subfield, String value) {
    MarcFactory factory = MarcFactory.newInstance();
    VariableField variableField = getSingleFieldByIndicators(marcRecord.getVariableFields(field));
    DataField dataField;
    if (variableField != null
        && ((DataField) variableField).getIndicator1() == INDICATOR_F
        && ((DataField) variableField).getIndicator2() == INDICATOR_F
    ) {
      dataField = (DataField) variableField;
      marcRecord.removeVariableField(variableField);
      dataField.removeSubfield(dataField.getSubfield(subfield));
    } else {
      dataField = factory.newDataField(field, INDICATOR_F, INDICATOR_F);
    }
    dataField.addSubfield(factory.newSubfield(subfield, value));
    marcRecord.addVariableField(dataField);
  }

  /**
   * Inserts a data field into the record's data fields in ascending tag order, relying on
   * {@link Record#getDataFields()} returning the record's live (mutable) internal list rather
   * than a copy - an implementation detail of marc4j's {@code RecordImpl}, not a contract promised by the
   * {@code Record} interface, but pinned by round-trip tests.
   *
   * @param marcRecord marc4j record to mutate
   * @param field      data field to insert
   */
  public static void addDataFieldInOrder(Record marcRecord, DataField field) {
    String tag = field.getTag();
    List<DataField> dataFields = marcRecord.getDataFields();
    for (int i = 0; i < dataFields.size(); i++) {
      if (dataFields.get(i).getTag().compareTo(tag) > 0) {
        marcRecord.getDataFields().add(i, field);
        return;
      }
    }
    marcRecord.addVariableField(field);
  }

  /**
   * Removes the first variable field found with the given tag.
   *
   * @param marcRecord marc4j record to mutate
   * @param fieldName  tag of the field to remove
   * @return true if a field was found and removed, false otherwise
   */
  public static boolean removeFirstField(Record marcRecord, String fieldName) {
    VariableField variableField = marcRecord.getVariableField(fieldName);
    if (variableField != null) {
      marcRecord.removeVariableField(variableField);
      return true;
    }
    return false;
  }

  /**
   * Removes the first variable field found with the given tag whose selected subfield contains the given value.
   *
   * @param marcRecord marc4j record to mutate
   * @param fieldName  tag of the field to search
   * @param subfield   subfield of the field to check
   * @param value      value that the subfield should contain
   * @return true if a matching field was found and removed, false otherwise
   */
  public static boolean removeFieldWithSubfieldValue(Record marcRecord, String fieldName, char subfield, String value) {
    List<VariableField> variableFields = marcRecord.getVariableFields(fieldName);
    for (VariableField variableField : variableFields) {
      if (fieldContainsSubfieldValue(variableField, subfield, value)) {
        marcRecord.removeVariableField(variableField);
        return true;
      }
    }
    return false;
  }

  /**
   * Removes all variable fields with the given tag from the marc record.
   *
   * @param marcRecord marc4j record to mutate
   * @param tag        tag of the field(s) to remove
   * @return true if at least one field was found and removed, false otherwise
   */
  public static boolean removeAllFieldsWithTag(Record marcRecord, String tag) {
    List<VariableField> fieldsToRemove = List.copyOf(marcRecord.getVariableFields(tag));
    fieldsToRemove.forEach(marcRecord::removeVariableField);
    return !fieldsToRemove.isEmpty();
  }

  /**
   * Removes subfields with the given code and any of the given values, from every field with one of the given
   * tags.
   *
   * @param marcRecord   marc4j record to mutate
   * @param tags         tags that could contain the subfield
   * @param subfieldCode subfield code to remove
   * @param values       values of the subfield to remove
   */
  public static void removeSubfieldValues(Record marcRecord, List<String> tags, char subfieldCode,
                                          List<String> values) {
    for (VariableField variableField : marcRecord.getVariableFields(tags.toArray(new String[0]))) {
      if (!(variableField instanceof DataField dataField)) {
        continue;
      }
      List<Subfield> subfields = dataField.getSubfields(subfieldCode);
      for (Subfield subfield : subfields) {
        if (subfield != null && values.contains(subfield.getData())) {
          dataField.removeSubfield(subfield);
        }
      }
    }
  }

  /**
   * Checks whether a data field or control field with the given tag exists with a subfield (or, for control
   * fields, the field's data) matching the given value.
   *
   * @param marcRecord marc4j record to search
   * @param tag        tag of the field
   * @param subfield   subfield to check on data fields
   * @param value      value to match, already known non-null by the caller
   * @return true if a matching field exists
   */
  public static boolean fieldExists(Record marcRecord, String tag, char subfield, String value) {
    for (VariableField field : marcRecord.getVariableFields(tag)) {
      if (field instanceof DataField dataField) {
        for (Subfield sub : dataField.getSubfields(subfield)) {
          if (isNotEmpty(sub.getData()) && sub.getData().equals(value.trim())) {
            return true;
          }
        }
      } else if (field instanceof ControlField controlField
                 && isNotEmpty(controlField.getData())
                 && controlField.getData().equals(value.trim())) {
        return true;
      }
    }
    return false;
  }

  /**
   * Checks if any data field contains a subfield with the given code.
   *
   * @param marcRecord   marc4j record to search
   * @param subfieldCode subfield code to look for
   * @return true if a data field with a subfield of this code exists, false otherwise
   */
  public static boolean subfieldExists(Record marcRecord, char subfieldCode) {
    for (DataField dataField : marcRecord.getDataFields()) {
      if (dataField.getSubfield(subfieldCode) != null) {
        return true;
      }
    }
    return false;
  }

  /**
   * Reads the value of the first controlled field with the given tag.
   *
   * @param marcRecord marc4j record to search
   * @param tag        tag to read
   * @return value from field, or null if no controlled field with the given tag exists
   */
  public static String getControlFieldValue(Record marcRecord, String tag) {
    return marcRecord.getControlFields()
      .stream()
      .filter(field -> field.getTag().equals(tag))
      .findFirst()
      .map(ControlField::getData)
      .orElse(null);
  }

  /**
   * Reads the value of a subfield from the first matching data field in a MARC record, identified by
   * {@code tag} and indicator values.
   *
   * @param marcRecord marc4j record to search
   * @param tag        tag of the data field
   * @param ind1       first indicator
   * @param ind2       second indicator
   * @param subfield   subfield code whose data value should be returned
   * @return the data of the first matching subfield, or null if no matching field or subfield is found
   */
  public static String getDataFieldSubfieldValue(Record marcRecord, String tag, char ind1,
                                                 char ind2, char subfield) {
    return marcRecord.getDataFields().stream()
      .filter(df -> df.getTag().equals(tag) && df.getIndicator1() == ind1 && df.getIndicator2() == ind2)
      .findFirst()
      .flatMap(df -> df.getSubfields(subfield).stream().findFirst().map(Subfield::getData))
      .orElse(null);
  }

  /**
   * Reads the value of a subfield from the first matching data field in a MARC record, identified by
   * {@code tag} only, disregarding indicator values.
   *
   * @param marcRecord marc4j record to search
   * @param tag        tag of the data field
   * @param subfield   subfield code whose data value should be returned
   * @return the data of the first matching subfield in any data field with the given tag, or null if no
   *   matching field or subfield is found
   */
  public static String getDataFieldSubfieldValue(Record marcRecord, String tag, char subfield) {
    return marcRecord.getDataFields().stream()
      .filter(df -> df.getTag().equals(tag))
      .flatMap(df -> df.getSubfields(subfield).stream())
      .findFirst()
      .map(Subfield::getData)
      .orElse(null);
  }

  private static VariableField getSingleFieldByIndicators(List<VariableField> list) {
    if (CollectionUtils.isEmpty(list)) {
      return null;
    }
    return list.stream()
      .filter(DataField.class::isInstance)
      .map(DataField.class::cast)
      .filter(f -> f.getIndicator1() == INDICATOR_F && f.getIndicator2() == INDICATOR_F)
      .findFirst()
      .orElse(null);
  }

  /**
   * Checks if the field contains a certain value in the selected subfield.
   *
   * @param field    from MARC BIB record
   * @param subfield subfield of the field
   * @param value    value of the field
   * @return true if contains, false otherwise
   */
  private static boolean fieldContainsSubfieldValue(VariableField field, char subfield, String value) {
    if (field instanceof DataField dataField) {
      for (Subfield sub : dataField.getSubfields(subfield)) {
        if (isNotEmpty(sub.getData()) && sub.getData().contains(value.trim())) {
          return true;
        }
      }
    }
    return false;
  }
}
