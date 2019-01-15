package org.folio.inventory.storage.external.failure;

import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.storage.external.ExternalStorageCollections;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExternalItemCollectionServerErrorExamples
  extends ExternalItemCollectionFailureExamples {

  public ExternalItemCollectionServerErrorExamples() {
    super(ExternalStorageFailureSuite.createUsing(
      it -> new ExternalStorageCollections(it,
        ExternalStorageFailureSuite.getServerErrorStorageAddress(), it.createHttpClient())));
  }

  @Override
  protected void check(Failure failure) {
    assertThat(failure.getReason(), is("Server Error"));
    assertThat(failure.getStatusCode(), is(500));
  }
}
