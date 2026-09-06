package org.folio.inventory.dataimport.util;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.marc4j.marc.DataField;
import org.marc4j.marc.MarcFactory;
import org.marc4j.marc.Record;

class MarcFieldEditorTest {

  private static final MarcFactory FACTORY = MarcFactory.newInstance();

  private static Record newRecord() {
    return FACTORY.newRecord();
  }

  @DisplayName("should append a new control field when replace is false and no field with that tag exists")
  @Test
  void shouldAppendControlField_whenReplaceIsFalseAndFieldAbsent() {
    // arrange
    Record record = newRecord();

    // act
    MarcFieldEditor.addOrReplaceControlField(record, "008", "value1", false);

    // assert
    assertThat(record.getControlFields()).hasSize(1);
    assertThat(record.getControlFields().getFirst().getData()).isEqualTo("value1");
  }

  @DisplayName("should append a second control field when replace is false and a field with that tag already exists")
  @Test
  void shouldAppendSecondControlField_whenReplaceIsFalseAndFieldAlreadyExists() {
    // arrange
    Record record = newRecord();
    record.addVariableField(FACTORY.newControlField("008", "original"));

    // act
    MarcFieldEditor.addOrReplaceControlField(record, "008", "extra", false);

    // assert
    assertThat(record.getControlFields()).hasSize(2);
    assertThat(record.getControlFields().stream().map(cf -> cf.getData())).containsExactly("original", "extra");
  }

  @DisplayName("should add a new control field when replace is true and no field with that tag exists")
  @Test
  void shouldAddControlField_whenReplaceIsTrueAndFieldAbsent() {
    // arrange
    Record record = newRecord();

    // act
    MarcFieldEditor.addOrReplaceControlField(record, "005", "20240315103045.1", true);

    // assert
    assertThat(record.getControlFields()).hasSize(1);
    assertThat(record.getControlFields().getFirst().getTag()).isEqualTo("005");
    assertThat(record.getControlFields().getFirst().getData()).isEqualTo("20240315103045.1");
  }

  @DisplayName("should replace the existing control field in place when replace is true and a field with that tag exists")
  @Test
  void shouldReplaceControlFieldInPlace_whenReplaceIsTrueAndFieldExists() {
    // arrange
    Record record = newRecord();
    record.addVariableField(FACTORY.newControlField("001", "in001"));
    record.addVariableField(FACTORY.newControlField("005", "old-value"));

    // act
    MarcFieldEditor.addOrReplaceControlField(record, "005", "new-value", true);

    // assert: exactly one 005 remains, and field order is preserved (001 first)
    assertThat(record.getControlFields()).hasSize(2);
    assertThat(record.getControlFields().stream().map(cf -> cf.getTag())).containsExactly("001", "005");
    assertThat(MarcFieldEditor.getControlFieldValue(record, "005")).isEqualTo("new-value");
  }

  @DisplayName("should create a new ff-indicator data field and add the subfield when no matching field exists")
  @Test
  void shouldCreateNewDataField_whenNoExistingFieldMatchesIndicators() {
    // arrange
    Record record = newRecord();

    // act
    MarcFieldEditor.addSubfieldToField(record, "999", 'i', "instance-id");

    // assert
    assertThat(record.getDataFields()).hasSize(1);
    DataField field = record.getDataFields().getFirst();
    assertThat(field.getIndicator1()).isEqualTo('f');
    assertThat(field.getIndicator2()).isEqualTo('f');
    assertThat(field.getSubfield('i').getData()).isEqualTo("instance-id");
  }

  @DisplayName("should add a second subfield to the existing ff-indicator field with the same tag")
  @Test
  void shouldAddSubfieldToExistingFfField_whenFieldWithSameTagAndIndicatorsExists() {
    // arrange
    Record record = newRecord();
    MarcFieldEditor.addSubfieldToField(record, "999", 's', "source-id");

    // act
    MarcFieldEditor.addSubfieldToField(record, "999", 'i', "instance-id");

    // assert: both subfields live on the single 999 ff field, not on two separate fields
    assertThat(record.getDataFields()).hasSize(1);
    DataField field = record.getDataFields().getFirst();
    assertThat(field.getSubfield('s').getData()).isEqualTo("source-id");
    assertThat(field.getSubfield('i').getData()).isEqualTo("instance-id");
  }

  @DisplayName("should replace the existing subfield value when the subfield code already exists on the ff field")
  @Test
  void shouldReplaceExistingSubfieldValue_whenSubfieldCodeAlreadyPresentOnFfField() {
    // arrange
    Record record = newRecord();
    MarcFieldEditor.addSubfieldToField(record, "999", 'i', "old-instance-id");

    // act
    MarcFieldEditor.addSubfieldToField(record, "999", 'i', "new-instance-id");

    // assert
    assertThat(record.getDataFields()).hasSize(1);
    assertThat(record.getDataFields().getFirst().getSubfields('i')).hasSize(1);
    assertThat(record.getDataFields().getFirst().getSubfield('i').getData()).isEqualTo("new-instance-id");
  }

  @DisplayName("should create a new ff field when an existing field with the tag has non-ff indicators")
  @Test
  void shouldCreateNewFfField_whenExistingFieldWithSameTagHasDifferentIndicators() {
    // arrange
    Record record = newRecord();
    DataField nonFfField = FACTORY.newDataField("999", ' ', ' ');
    nonFfField.addSubfield(FACTORY.newSubfield('a', "unrelated"));
    record.addVariableField(nonFfField);

    // act
    MarcFieldEditor.addSubfieldToField(record, "999", 'i', "instance-id");

    // assert: the original non-ff field survives untouched, alongside a new ff field
    assertThat(record.getDataFields()).hasSize(2);
    assertThat(record.getVariableFields("999").stream()
      .map(DataField.class::cast)
      .anyMatch(df -> df.getIndicator1() == ' ' && df.getSubfield('a') != null)).isTrue();
    assertThat(record.getVariableFields("999").stream()
      .map(DataField.class::cast)
      .anyMatch(df -> df.getIndicator1() == 'f' && df.getIndicator2() == 'f'
                      && "instance-id".equals(df.getSubfield('i').getData()))).isTrue();
  }

  @DisplayName("should not throw when the field tag identifies a control field instead of a data field")
  @Test
  void shouldNotThrow_whenAddSubfieldToFieldTargetsControlFieldTag() {
    // arrange: "001" is a control field, so getVariableFields("001") returns a ControlField, not a DataField -
    // the instanceof/cast guard inside addSubfieldToField must filter it out rather than blow up
    Record record = newRecord();
    record.addVariableField(FACTORY.newControlField("001", "in001"));

    // act
    MarcFieldEditor.addSubfieldToField(record, "001", 'z', "value");

    // assert: a brand-new ff data field with tag "001" was added instead
    assertThat(record.getDataFields()).hasSize(1);
    assertThat(record.getDataFields().getFirst().getSubfield('z').getData()).isEqualTo("value");
  }

  @DisplayName("should insert a data field before the first field whose tag sorts after it")
  @Test
  void shouldInsertDataFieldInAscendingTagOrder_whenAMiddleTagIsInserted() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("100", 'a', "first"));
    record.addVariableField(newDataField("500", 'a', "last"));
    DataField middleField = newDataField("245", 'a', "middle");

    // act
    MarcFieldEditor.addDataFieldInOrder(record, middleField);

    // assert
    assertThat(record.getDataFields().stream().map(DataField::getTag)).containsExactly("100", "245", "500");
  }

  @DisplayName("should append a data field at the end when its tag sorts after every existing tag")
  @Test
  void shouldAppendDataFieldAtEnd_whenTagSortsAfterAllExistingTags() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("100", 'a', "first"));
    record.addVariableField(newDataField("245", 'a', "second"));
    DataField lastField = newDataField("999", 'a', "third");

    // act
    MarcFieldEditor.addDataFieldInOrder(record, lastField);

    // assert
    assertThat(record.getDataFields().stream().map(DataField::getTag)).containsExactly("100", "245", "999");
  }

  @DisplayName("should remove the first field found with the given tag and report success")
  @Test
  void shouldRemoveFirstField_whenFieldWithTagExists() {
    // arrange
    Record record = newRecord();
    record.addVariableField(FACTORY.newControlField("001", "in001"));

    // act
    boolean removed = MarcFieldEditor.removeFirstField(record, "001");

    // assert
    assertThat(removed).isTrue();
    assertThat(record.getVariableFields("001")).isEmpty();
  }

  @DisplayName("should report failure when removing the first field of a tag that does not exist")
  @Test
  void shouldReportFalse_whenRemoveFirstFieldTagDoesNotExist() {
    // arrange
    Record record = newRecord();

    // act
    boolean removed = MarcFieldEditor.removeFirstField(record, "001");

    // assert
    assertThat(removed).isFalse();
  }

  @DisplayName("should remove the field whose subfield contains the given value and stop at the first match")
  @Test
  void shouldRemoveFieldWithMatchingSubfieldValue_whenOneOfSeveralFieldsMatches() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("035", 'a', "(ybp)first"));
    record.addVariableField(newDataField("035", 'a', "(ybp)second"));

    // act
    boolean removed = MarcFieldEditor.removeFieldWithSubfieldValue(record, "035", 'a', "(ybp)first");

    // assert: only the matching field is gone, the other survives
    assertThat(removed).isTrue();
    assertThat(record.getDataFields()).hasSize(1);
    assertThat(record.getDataFields().getFirst().getSubfield('a').getData()).isEqualTo("(ybp)second");
  }

  @DisplayName("should report failure when no field's subfield contains the given value")
  @Test
  void shouldReportFalse_whenNoFieldSubfieldContainsValue() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("035", 'a', "(ybp)first"));

    // act
    boolean removed = MarcFieldEditor.removeFieldWithSubfieldValue(record, "035", 'a', "no-match");

    // assert
    assertThat(removed).isFalse();
    assertThat(record.getDataFields()).hasSize(1);
  }

  @DisplayName("should not throw and report false when checking subfield value against a control field with the same tag")
  @Test
  void shouldReportFalse_whenRemoveFieldWithSubfieldValueTargetsControlFieldTag() {
    // arrange: "001" is a control field - the instanceof guard means it can never "contain" a subfield value
    Record record = newRecord();
    record.addVariableField(FACTORY.newControlField("001", "in001"));

    // act
    boolean removed = MarcFieldEditor.removeFieldWithSubfieldValue(record, "001", 'a', "in001");

    // assert
    assertThat(removed).isFalse();
    assertThat(record.getVariableFields("001")).hasSize(1);
  }

  @DisplayName("should remove all fields sharing the given tag, not just the first")
  @Test
  void shouldRemoveAllFieldsWithTag_whenMultipleFieldsShareTheTag() {
    // arrange: pins the fix for the old "removes only every other match" bug (defect 3 in the refactor plan)
    Record record = newRecord();
    record.addVariableField(newDataField("700", 'a', "Author One"));
    record.addVariableField(newDataField("700", 'a', "Author Two"));
    record.addVariableField(newDataField("245", 'a', "Title"));

    // act
    boolean removed = MarcFieldEditor.removeAllFieldsWithTag(record, "700");

    // assert
    assertThat(removed).isTrue();
    assertThat(record.getVariableFields("700")).isEmpty();
    assertThat(record.getDataFields()).hasSize(1);
    assertThat(record.getDataFields().getFirst().getTag()).isEqualTo("245");
  }

  @DisplayName("should report false and leave the record unchanged when no field with the tag exists")
  @Test
  void shouldReportFalse_whenRemoveAllFieldsWithTagFindsNoMatch() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("245", 'a', "Title"));

    // act
    boolean removed = MarcFieldEditor.removeAllFieldsWithTag(record, "700");

    // assert
    assertThat(removed).isFalse();
    assertThat(record.getDataFields()).hasSize(1);
  }

  @DisplayName("should remove only subfields whose value is in the given list, across all given tags")
  @Test
  void shouldRemoveSubfieldValues_whenSubfieldValueIsInTheGivenList() {
    // arrange
    Record record = newRecord();
    DataField field245 = FACTORY.newDataField("245", ' ', ' ');
    field245.addSubfield(FACTORY.newSubfield('9', "remove-me"));
    field245.addSubfield(FACTORY.newSubfield('a', "title"));
    record.addVariableField(field245);
    DataField field700 = FACTORY.newDataField("700", ' ', ' ');
    field700.addSubfield(FACTORY.newSubfield('9', "keep-me"));
    record.addVariableField(field700);

    // act
    MarcFieldEditor.removeSubfieldValues(record, List.of("245", "700"), '9', List.of("remove-me"));

    // assert
    assertThat(field245.getSubfield('9')).isNull();
    assertThat(field245.getSubfield('a').getData()).isEqualTo("title");
    assertThat(field700.getSubfield('9').getData()).isEqualTo("keep-me");
  }

  @DisplayName("should not throw when one of the given tags identifies a control field")
  @Test
  void shouldNotThrow_whenRemoveSubfieldValuesTargetsControlFieldTag() {
    // arrange: "001" is a control field - the instanceof guard must skip it rather than throw a ClassCastException
    Record record = newRecord();
    record.addVariableField(FACTORY.newControlField("001", "in001"));

    // act / assert
    org.junit.jupiter.api.Assertions.assertDoesNotThrow(() ->
      MarcFieldEditor.removeSubfieldValues(record, List.of("001"), '9', List.of("value")));
    assertThat(record.getVariableFields("001")).hasSize(1);
  }

  @DisplayName("should find a data field whose subfield exactly equals the given value")
  @Test
  void shouldFindField_whenDataFieldSubfieldEqualsValue() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("035", 'a', "target-value"));

    // act
    boolean exists = MarcFieldEditor.fieldExists(record, "035", 'a', "target-value");

    // assert
    assertThat(exists).isTrue();
  }

  @DisplayName("should find a control field whose data exactly equals the given value")
  @Test
  void shouldFindField_whenControlFieldDataEqualsValue() {
    // arrange
    Record record = newRecord();
    record.addVariableField(FACTORY.newControlField("001", "ybp7406411"));

    // act
    boolean exists = MarcFieldEditor.fieldExists(record, "001", ' ', "ybp7406411");

    // assert
    assertThat(exists).isTrue();
  }

  @DisplayName("should not find a field when no field with the tag matches the value")
  @Test
  void shouldNotFindField_whenNoFieldWithTagMatchesValue() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("035", 'a', "other-value"));

    // act
    boolean exists = MarcFieldEditor.fieldExists(record, "035", 'a', "target-value");

    // assert
    assertThat(exists).isFalse();
  }

  @DisplayName("should match the value with surrounding whitespace trimmed")
  @Test
  void shouldFindField_whenValueMatchesAfterTrimming() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("035", 'a', "target-value"));

    // act
    boolean exists = MarcFieldEditor.fieldExists(record, "035", 'a', "  target-value  ");

    // assert
    assertThat(exists).isTrue();
  }

  @DisplayName("should report true when a data field contains a subfield with the given code")
  @Test
  void shouldReportTrue_whenSubfieldExists() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("100", '9', "test"));

    // act
    boolean exists = MarcFieldEditor.subfieldExists(record, '9');

    // assert
    assertThat(exists).isTrue();
  }

  @DisplayName("should report false when no data field contains a subfield with the given code")
  @Test
  void shouldReportFalse_whenSubfieldDoesNotExist() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("100", 'a', "John Doe"));

    // act
    boolean exists = MarcFieldEditor.subfieldExists(record, '9');

    // assert
    assertThat(exists).isFalse();
  }

  @DisplayName("should return the control field value for the given tag")
  @Test
  void shouldReturnControlFieldValue_whenControlFieldWithTagExists() {
    // arrange
    Record record = newRecord();
    record.addVariableField(FACTORY.newControlField("001", "ybp7406411"));

    // act
    String value = MarcFieldEditor.getControlFieldValue(record, "001");

    // assert
    assertThat(value).isEqualTo("ybp7406411");
  }

  @DisplayName("should return null when no control field with the given tag exists")
  @Test
  void shouldReturnNull_whenControlFieldWithTagDoesNotExist() {
    // arrange
    Record record = newRecord();

    // act
    String value = MarcFieldEditor.getControlFieldValue(record, "001");

    // assert
    assertThat(value).isNull();
  }

  @DisplayName("should return the subfield value from the data field matching tag and indicators")
  @Test
  void shouldReturnDataFieldSubfieldValue_whenTagAndIndicatorsMatch() {
    // arrange
    Record record = newRecord();
    DataField field = FACTORY.newDataField("999", 'f', 'f');
    field.addSubfield(FACTORY.newSubfield('i', "instance-id"));
    record.addVariableField(field);

    // act
    String value = MarcFieldEditor.getDataFieldSubfieldValue(record, "999", 'f', 'f', 'i');

    // assert
    assertThat(value).isEqualTo("instance-id");
  }

  @DisplayName("should return null when the field's indicators do not match")
  @Test
  void shouldReturnNull_whenDataFieldIndicatorsDoNotMatch() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("999", 'i', "test"));

    // act
    String value = MarcFieldEditor.getDataFieldSubfieldValue(record, "999", 'f', 'f', 'i');

    // assert
    assertThat(value).isNull();
  }

  @DisplayName("should return the subfield value from the data field matching the tag, disregarding indicators")
  @Test
  void shouldReturnDataFieldSubfieldValue_whenOnlyTagMatters() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("245", 'a', "title"));

    // act
    String value = MarcFieldEditor.getDataFieldSubfieldValue(record, "245", 'a');

    // assert
    assertThat(value).isEqualTo("title");
  }

  @DisplayName("should return null when no data field with the given tag has the requested subfield")
  @Test
  void shouldReturnNull_whenTagOnlyLookupFindsNoMatchingSubfield() {
    // arrange
    Record record = newRecord();
    record.addVariableField(newDataField("245", 'a', "title"));

    // act
    String value = MarcFieldEditor.getDataFieldSubfieldValue(record, "245", 'z');

    // assert
    assertThat(value).isNull();
  }

  private static DataField newDataField(String tag, char subfieldCode, String subfieldValue) {
    DataField field = FACTORY.newDataField(tag, ' ', ' ');
    field.addSubfield(FACTORY.newSubfield(subfieldCode, subfieldValue));
    return field;
  }
}
