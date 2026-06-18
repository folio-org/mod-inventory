package org.folio.inventory.dataimport.util;

import org.folio.inventory.domain.instances.Instance;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.UUID;

@RunWith(BlockJUnit4ClassRunner.class)
public class ValidationUtilTest {

  private static final String INVALID_STATISTICAL_CODE_MSG = "Provided Statistical code(s) are not a valid values: ";

  @Test
  public void shouldHaveNoErrorIfNatureAreAsUUID() {
    Instance instance = new Instance(UUID.randomUUID().toString(), 1, "001", "MARC", "Title", UUID.randomUUID().toString());
    instance.setNatureOfContentTermIds(Arrays.asList(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    List<String> errors = ValidationUtil.validateUUIDs(instance);
    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void shouldHaveSeveralErrorsIfSomeNatureAreNotAsUUID() {
    Instance instance = new Instance(UUID.randomUUID().toString(), 1, "001", "MARC", "Title", UUID.randomUUID().toString());
    instance.setNatureOfContentTermIds(Arrays.asList(UUID.randomUUID().toString(), "not uuid value", UUID.randomUUID().toString(), UUID.randomUUID().toString(), "second not UUID value"));
    List<String> errors = ValidationUtil.validateUUIDs(instance);
    Assert.assertEquals(2, errors.size());
    Assert.assertEquals("Value 'not uuid value' is not a UUID for natureOfContentTermIds field", errors.get(0));
    Assert.assertEquals("Value 'second not UUID value' is not a UUID for natureOfContentTermIds field", errors.get(1));

  }

  @Test
  public void shouldHaveNoErrorIfStatisticalCodeIdsAreAllUUIDs() {
    Instance instance = new Instance(UUID.randomUUID().toString(), 1, "in001", "MARC", "Title", UUID.randomUUID().toString());
    instance.setStatisticalCodeIds(List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    List<String> errors = ValidationUtil.validateUUIDs(instance);
    Assert.assertEquals(0, errors.size());
  }

  @Test
  public void shouldHaveErrorIfAllStatisticalCodeIdsInInstanceAreNotUUIDs() {
    Instance instance = new Instance(UUID.randomUUID().toString(), 1, "in001", "MARC", "Title", UUID.randomUUID().toString());
    instance.setStatisticalCodeIds(Arrays.asList("ebookss", UUID.randomUUID().toString(), "another-invalid"));
    List<String> errors = ValidationUtil.validateUUIDs(instance);
    Assert.assertEquals(1, errors.size());
    Assert.assertTrue(errors.getFirst().startsWith(INVALID_STATISTICAL_CODE_MSG));
    Assert.assertTrue(errors.getFirst().contains("'ebookss'"));
    Assert.assertTrue(errors.getFirst().contains("'another-invalid'"));
  }

  @Test
  public void shouldHaveNoErrorIfStatisticalCodeIdsAreEmpty() {
    List<String> errors = ValidationUtil.validateStatisticalCodeIds(Collections.emptyList());
    Assert.assertTrue(errors.isEmpty());
  }

  @Test
  public void shouldReturnOneErrorForAllInvalidStatisticalCodeIds() {
    List<String> errors = ValidationUtil.validateStatisticalCodeIds(
      Arrays.asList("invalid-code1", UUID.randomUUID().toString(), "invalid-code2"));
    Assert.assertEquals(1, errors.size());
    Assert.assertTrue(errors.getFirst().startsWith(INVALID_STATISTICAL_CODE_MSG));
    Assert.assertTrue(errors.getFirst().contains("'invalid-code1'"));
    Assert.assertTrue(errors.getFirst().contains("'invalid-code2'"));
  }

  @Test
  public void shouldReturnNoErrorWhenAllStatisticalCodeIdsAreValidUUIDs() {
    List<String> errors =
      ValidationUtil.validateStatisticalCodeIds(List.of(UUID.randomUUID().toString(), UUID.randomUUID().toString()));
    Assert.assertTrue(errors.isEmpty());
  }
}
