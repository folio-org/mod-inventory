# Finding subscription-journal articles with FOLIO: a modest proposal

## The problem

Researchers need to find _and access_ journal articles relevant to their work, but many such articles are behind paywalls. A researcher's instution holds subscriptions to packages of paywalled journals, by means of which these articles can be obtained. However, two quite separate sets of information are required in order to solve this problem:

1. Article-level metadata, such that a search for e.g. `mesozoic ecosystems` can find Aberhan et al.'s 2006 paper [_Testing the role of biological interactions in the evolution of mid-Mesozoic marine benthic ecosystems_](http://www.bioone.org/doi/abs/10.1666/05028.1?journalCode=pbio).
2. Package metadata, so that once this article from _Paleobiology_ **32(2)**:259-277 has been found, we can determine what package, if any, provides access to it.

## EBSCO's solution

EBSCO provides an integrated solution to its customers: EDS (the EBSCO discovery service) has article-level metadata; and access to articles is determined by reference to "The EBSCO KB".

> Side-questions:
> 1. Does the EBSCO KB have a more specific name?
> 2. Do all customer institutions but both EDS and the KB, or can they buy one and not the other?

## An alternative FOLIO-based solution

EBSCO's EDS/KB-based solution is fine for EBSCO customers. But FOLIO is intended to be vendor neutral, so we do not want to be in a position where EBSCO users are forced to use EBSCO's solution.

FOLIO will apparently not have access to EBSCO's article-level metadata, so it can't use this for the initial discovery phase. But it _will_ have access to the EBSCO KB that contains the subscription-package information.

But article-level metadata is also available from other sources -- for example [Google Scholar](https://scholar.google.com/) has very good coverage and is free to use. The problem is how to connect Google Scholar results to FOLIO (and thereby to the EBSCO KB, or to any other KB that may be provided as a FOLIO module).

But [the OpenURL standard](https://en.wikipedia.org/wiki/OpenURL) was created _precisely_ to solve this problem: bridging from a discovered article to a link resolver that knows how to get to licenced copy of that article. And Google Scholar [explicitly supports OpenURL](https://scholar.google.com/intl/en/scholar/libraries.html) for this purpose.

The only part of the equation that's missing is an OpenURL resolver that knows how to query FOLIO about packages. But we can easily build this: Index Data has [open-access OpenURL resolver code](http://search.cpan.org/~mirk/Keystone-Resolver/). Its front-end us good; and we can repurpose it with a back-end that emits WSAPI calls to Okapi, which will proxy them as appropriate to a KB module.

## Appendix: other knowledge-bases

Since the EBSCO KB will be accessed via a FOLIO module with a well-defined WSAPI interface, other KBs can be slipped into a FOLIO installation in place of EBSCO's product: for example, a module that accesses the SFX KB, if a licence ca be negotiated; or one that uses GoKB.

Putting this together, FOLIO customers will have a choice of what article-level discovery to use (e.g. Google Scholar), _and_ what KB to use; the FOLIO OpenURL resolver provides the necessary glue.

