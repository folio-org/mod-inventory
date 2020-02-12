package org.folio.inventory.domain.converters;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertNotNull;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;

import junitparams.JUnitParamsRunner;
import junitparams.Parameters;

@RunWith(JUnitParamsRunner.class)
public class EntityConvertersTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  @Parameters({
    "org.folio.inventory.domain.items.Status"
  })
  public void canReturnConverterForSupportedClass(String className) throws Exception {
    Class<?> entityType = Class.forName(className);

    assertNotNull(EntityConverters.converterForClass(entityType));
  }

  @Test
  public void cannotReturnConverterForUnsupportedType() {
    expectedException.expect(instanceOf(IllegalArgumentException.class));
    expectedException.expectMessage("No entity converter found for java.lang.String");

    EntityConverters.converterForClass(String.class);
  }

}
