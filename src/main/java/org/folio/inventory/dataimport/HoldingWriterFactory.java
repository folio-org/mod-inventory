package org.folio.inventory.dataimport;

import org.folio.processing.mapping.mapper.writer.Writer;
import org.folio.processing.mapping.mapper.writer.WriterFactory;
import org.folio.processing.mapping.mapper.writer.common.JsonBasedWriter;
import org.folio.rest.jaxrs.model.EntityType;

public class HoldingWriterFactory implements WriterFactory {

  @Override
  public Writer createWriter() {
    return new JsonBasedWriter(EntityType.HOLDINGS);
  }

  @Override
  public boolean isEligibleForEntityType(EntityType entityType) {
    return EntityType.HOLDINGS == entityType;
  }
}
