package org.folio.inventory.storage.external;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.Test;

public class CqlQueryTest {

  @Test
  public void exactMatch() {
    assertThat(CqlQuery.exactMatch("foo", "bar*baz").toString(), is("foo==\"bar\\*baz\""));
  }

  @Test
  public void match() {
    assertThat(CqlQuery.match("foo", "bar\\baz").toString(), is("foo=\"bar\\\\baz\""));
  }

  @Test
  public void notEqual() {
    assertThat(CqlQuery.notEqual("foo", "bar\"baz").toString(), is("foo<>\"bar\\\"baz\""));
  }

}
