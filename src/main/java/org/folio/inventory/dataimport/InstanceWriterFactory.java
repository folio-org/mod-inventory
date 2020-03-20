package org.folio.inventory.dataimport;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;

public class InstanceWriterFactory implements WriterFactory {

  @Override
  public Writer createWriter() {
    return new JsonBasedWriter(EntityType.INSTANCE);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return EntityType.INSTANCE == entityType;

  }
}
