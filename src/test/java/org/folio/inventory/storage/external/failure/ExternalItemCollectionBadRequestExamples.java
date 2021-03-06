package org.folio.inventory.storage.external.failure;

import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.storage.external.ExternalStorageCollections;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.is;

public class ExternalItemCollectionBadRequestExamples
  extends ExternalItemCollectionFailureExamples {

  public ExternalItemCollectionBadRequestExamples() {
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
