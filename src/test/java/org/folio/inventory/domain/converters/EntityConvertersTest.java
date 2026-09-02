package org.folio.inventory.domain.converters;

import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class EntityConvertersTest {

  @ParameterizedTest
  @ValueSource(strings = {
    "org.folio.inventory.domain.items.Status"
  })
  void canReturnConverterForSupportedClass(String className) throws Exception {
    Class<?> entityType = Class.forName(className);

    assertNotNull(EntityConverters.converterForClass(entityType));
  }

  @Test
  void cannotReturnConverterForUnsupportedType() {
    IllegalArgumentException exception = assertThrows(IllegalArgumentException.class,
      () -> EntityConverters.converterForClass(String.class));

    assertNotNull(exception.getMessage());
    org.assertj.core.api.Assertions.assertThat(exception.getMessage())
      .isEqualTo("No entity converter found for java.lang.String");
  }
}
