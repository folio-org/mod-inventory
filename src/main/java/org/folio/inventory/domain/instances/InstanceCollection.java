package org.folio.inventory.domain.instances;

import java.io.UnsupportedEncodingException;
import java.util.function.Consumer;
import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.common.domain.MultipleRecords;
import org.folio.inventory.common.domain.PagingParameters;
import org.folio.inventory.common.domain.Success;
import org.folio.inventory.domain.AsynchronousCollection;
import org.folio.inventory.domain.SearchableCollection;
import org.folio.inventory.domain.SynchronousCollection;

public interface InstanceCollection extends AsynchronousCollection<Instance>,
  SearchableCollection<Instance>, SynchronousCollection<Instance> {

  /**
   * Finds instances by CQL, optionally excluding consortium shadow copies (instances with source
   * CONSORTIUM-MARC or CONSORTIUM-FOLIO) from the result via the storage includeShadowCopies flag.
   * Implementations that have no notion of shadow copies can rely on the default, which ignores the
   * flag and behaves exactly like the plain CQL search.
   */
  default void findByCql(String cqlQuery, boolean includeShadowCopies, PagingParameters pagingParameters,
                         Consumer<Success<MultipleRecords<Instance>>> resultCallback,
                         Consumer<Failure> failureCallback) throws UnsupportedEncodingException {
    findByCql(cqlQuery, pagingParameters, resultCallback, failureCallback);
  }
}
