package org.folio.inventory.domain.converters;

import java.util.HashMap;
import java.util.Map;
import org.folio.inventory.domain.converters.impl.StatusConverter;
import org.folio.inventory.domain.items.Status;

public final class EntityConverters {
  private static final Map<Class<?>, EntityConverter<?>> CONVERTERS = new HashMap<>();

  static {
    CONVERTERS.put(Status.class, new StatusConverter());
  }

  private EntityConverters() { }

  /**
   * Returns EntityConverter for given class, if any, otherwise throws
   * {@link IllegalArgumentException}.
   *
   * @param classType - Class to return converter for.
   * @param <T>    - Class type.
   * @return EntityConverter for the class.
   * @throws IllegalArgumentException if there is no converter for the class.
   */
  @SuppressWarnings("unchecked")
  public static <T> EntityConverter<T> converterForClass(Class<T> classType) {
    EntityConverter<?> entityConverter = CONVERTERS.get(classType);

    if (entityConverter == null) {
      throw new IllegalArgumentException("No entity converter found for " + classType.getName());
    }

    return (EntityConverter<T>) entityConverter;
  }
}
