package org.folio.inventory.storage.external.failure

import org.folio.inventory.common.domain.Failure
import org.folio.inventory.storage.external.ExternalStorageCollections

import static org.hamcrest.CoreMatchers.is
import static org.junit.Assert.assertThat

class ExternalInstanceCollectionServerErrorExamples
  extends ExternalInstanceCollectionFailureExamples {

  ExternalInstanceCollectionServerErrorExamples() {
    super(ExternalStorageFailureSuite.useVertx(
      { new ExternalStorageCollections(it,
        ExternalStorageFailureSuite.serverErrorStorageAddress)}))
  }

  @Override
  protected check(Failure failure) {
    assertThat(failure.reason, is("Server Error"))
    assertThat(failure.statusCode, is(500))
  }
}
