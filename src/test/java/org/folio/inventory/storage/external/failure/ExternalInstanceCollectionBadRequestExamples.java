package org.folio.inventory.storage.external.failure;

import org.folio.inventory.common.domain.Failure;
import org.folio.inventory.storage.external.ExternalStorageCollections;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertThat;

public class ExternalInstanceCollectionBadRequestExamples
  extends ExternalInstanceCollectionFailureExamples {

  public ExternalInstanceCollectionBadRequestExamples() {
    super(ExternalStorageFailureSuite.createUsing(
      it -> new ExternalStorageCollections(it,
        ExternalStorageFailureSuite.getBadRequestStorageAddress())));
  }

  @Override
  protected void check(Failure failure) {
    assertThat(failure.getReason(), is("Bad Request"));
    assertThat(failure.getStatusCode(), is(400));
  }
}
