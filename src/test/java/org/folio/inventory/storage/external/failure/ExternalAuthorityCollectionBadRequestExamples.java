package org.folio.inventory.storage.external.failure;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.storage.external.ExternalStorageCollections;

public class ExternalAuthorityCollectionBadRequestExamples extends ExternalAuthorityCollectionFailureExamples {

  public ExternalAuthorityCollectionBadRequestExamples() {
    super(ExternalStorageFailureSuite.createUsing(
        it -> new ExternalStorageCollections(it,
            ExternalStorageFailureSuite.getBadRequestStorageAddress(), it.createHttpClient())));
  }

  @Override
  protected void check(Failure failure) {
    assertThat(failure.getReason(), is("Bad Request"));
    assertThat(failure.getStatusCode(), is(400));
  }
}
