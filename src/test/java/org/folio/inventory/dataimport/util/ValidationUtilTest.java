package org.folio.inventory.dataimport.util;

import org.folio.inventory.domain.instances.Instance;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.BlockJUnit4ClassRunner;

import java.util.Arrays;
import java.util.List;
import java.util.UUID;

@RunWith(BlockJUnit4ClassRunner.class)
public class ValidationUtilTest {

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
}
