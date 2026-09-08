package org.folio.inventory.dataimport.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;
import org.folio.inventory.domain.instances.Instance;
import org.junit.jupiter.api.Test;

class ValidationUtilTest {

  private static final String INVALID_STATISTICAL_CODE_MSG = "Provided Statistical code(s) are not a valid values: ";

  @Test
  void shouldHaveNoErrorIfNatureAreAsUuid() {
    Instance instance =
      new Instance(UUID.randomUUID().toString(), 1, "001", "MARC", "Title", UUID.randomUUID().toString());
    instance.setNatureOfContentTermIds(Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    List<String> errors = ValidationUtil.validateUuids(instance);
    assertEquals(0, errors.size());
  }

  @Test
  void shouldHaveSeveralErrorsIfSomeNatureAreNotAsUuid() {
    Instance instance =
      new Instance(UUID.randomUUID().toString(), 1, "001", "MARC", "Title", UUID.randomUUID().toString());
    instance.setNatureOfContentTermIds(
      Arrays.asList(UUID.randomUUID().toString(), "not uuid value", UUID.randomUUID().toString(),
        UUID.randomUUID().toString(), "second not UUID value"));
    List<String> errors = ValidationUtil.validateUuids(instance);
    assertEquals(2, errors.size());
    assertEquals("Value 'not uuid value' is not a UUID for natureOfContentTermIds field", errors.get(0));
    assertEquals("Value 'second not UUID value' is not a UUID for natureOfContentTermIds field", errors.get(1));
  }

  @Test
  void shouldHaveNoErrorIfStatisticalCodeIdsAreAllUuids() {
    Instance instance =
      new Instance(UUID.randomUUID().toString(), 1, "in001", "MARC", "Title", UUID.randomUUID().toString());
    instance.setStatisticalCodeIds(List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    List<String> errors = ValidationUtil.validateUuids(instance);
    assertEquals(0, errors.size());
  }

  @Test
  void shouldHaveErrorIfAllStatisticalCodeIdsInInstanceAreNotUuids() {
    Instance instance =
      new Instance(UUID.randomUUID().toString(), 1, "in001", "MARC", "Title", UUID.randomUUID().toString());
    instance.setStatisticalCodeIds(Arrays.asList("ebookss", UUID.randomUUID().toString(), "another-invalid"));
    List<String> errors = ValidationUtil.validateUuids(instance);
    assertEquals(1, errors.size());
    assertTrue(errors.getFirst().startsWith(INVALID_STATISTICAL_CODE_MSG));
    assertTrue(errors.getFirst().contains("'ebookss'"));
    assertTrue(errors.getFirst().contains("'another-invalid'"));
  }

  @Test
  void shouldHaveNoErrorIfStatisticalCodeIdsAreEmpty() {
    List<String> errors = ValidationUtil.validateStatisticalCodeIds(Collections.emptyList());
    assertTrue(errors.isEmpty());
  }

  @Test
  void shouldReturnOneErrorForAllInvalidStatisticalCodeIds() {
    List<String> errors = ValidationUtil.validateStatisticalCodeIds(
      Arrays.asList("invalid-code1", UUID.randomUUID().toString(), "invalid-code2"));
    assertEquals(1, errors.size());
    assertTrue(errors.getFirst().startsWith(INVALID_STATISTICAL_CODE_MSG));
    assertTrue(errors.getFirst().contains("'invalid-code1'"));
    assertTrue(errors.getFirst().contains("'invalid-code2'"));
  }

  @Test
  void shouldReturnNoErrorWhenAllStatisticalCodeIdsAreValidUuid() {
    List<String> errors =
      ValidationUtil.validateStatisticalCodeIds(List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    assertTrue(errors.isEmpty());
  }
}
