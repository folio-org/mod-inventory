package org.folio.inventory.domain.authority;

import org.folio.Authority;
import org.folio.inventory.domain.AsynchronousCollection;
import org.folio.inventory.domain.SearchableCollection;

public interface AuthorityCollection extends AsynchronousCollection<Authority>, SearchableCollection<Authority> {
}
