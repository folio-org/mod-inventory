package org.folio.inventory.domain.items;

import static org.hamcrest.Matchers.instanceOf;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

public class StatusTest {

  @Rule
  public ExpectedException expectedException = ExpectedException.none();

  @Test
  public void cannotCreateStatusIfStatusNameIsNull() {
    expectedException.expect(instanceOf(NullPointerException.class));
    expectedException.expectMessage("Status name is required");

    new Status(null, "date");
  }
}
