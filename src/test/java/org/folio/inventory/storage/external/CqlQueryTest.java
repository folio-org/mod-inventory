package org.folio.inventory.storage.external;

import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.MatcherAssert.assertThat;

import org.junit.jupiter.api.Test;

class CqlQueryTest {

  @Test
  void exactMatch() {
    assertThat(CqlQuery.exactMatch("foo", "bar*baz").toString(), is("foo==\"bar\\*baz\""));
  }

  @Test
  void match() {
    assertThat(CqlQuery.match("foo", "bar\\baz").toString(), is("foo=\"bar\\\\baz\""));
  }

  @Test
  void notEqual() {
    assertThat(CqlQuery.notEqual("foo", "bar\"baz").toString(), is("foo<>\"bar\\\"baz\""));
  }
}
