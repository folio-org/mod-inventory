package org.folio.inventory.storage.external.failure;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.storage.external.ExternalStorageCollections;

public class ExternalAuthorityCollectionServerErrorExamples extends ExternalAuthorityCollectionFailureExamples{

  public ExternalAuthorityCollectionServerErrorExamples() {
    super(ExternalStorageFailureSuite.createUsing(
        it -> new ExternalStorageCollections(
                ExternalStorageFailureSuite.getServerErrorStorageAddress(), it.createHttpClient())));
  }

  @Override
  protected void check(Failure failure) {
    assertThat(failure.getReason(), is("Server Error"));
    assertThat(failure.getStatusCode(), is(500));
  }
}
