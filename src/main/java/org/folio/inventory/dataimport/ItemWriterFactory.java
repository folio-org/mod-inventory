package org.folio.inventory.dataimport;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;

public class ItemWriterFactory implements WriterFactory {

  @Override
  public Writer createWriter() {
    return new JsonBasedWriter(EntityType.ITEM);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return EntityType.ITEM == entityType;
  }
}
