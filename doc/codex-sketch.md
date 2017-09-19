# A sketch of a Codex WSAPI

## Introduction

The Codex is a normalization and virtualization layer that allows FOLIO to integrate metadata about various resources regardless of source, format or encoding. This is something that libraries have long wanted -- for example, so that print resources and electronic resources can be treated uniformly in those respects where that makes sense, while also respecting the very real differences.

Modules such as Inventory and the Knowledge Base will feed into the Codex; any module might search within it.

## Writing to the Codex

When a module such as Inventory accepts a new record, it can pass on a suitably massaged form to the Codex for indexing and aggregation using a call along the lines of:
```
POST /codex
{
  "author": "J. R. R. Tolkein",
  "title": "The Fellowship of the Ring",
  ...
}

201 Created
Location: /codex/12345
```
And when it accepts a modified version of an existing record, it can pass on a suitably massaged form to the Codex using a call along the lines of:
```
PUT /codex/12345
{
  "author": "John Ronald Reuel Tolkein",
  "title": "The Fellowship of the Ring",
  ...
}

200 OK
```

In order to do this, each module that updates the codex needs to understand the Codex's schema, and know how to translate its own native format into the Codex format. This might be done, for example, by transforming MARC records into MARCXML and feeding the result through an XSLT stylesheet that emits a Codex-format record.

## Reading from the Codex

Searching in the Codex may be done using CQL, as for other FOLIO modules. The returned records may be brief:
```
GET /codex?query=author=(tolkien or lewis)

200 OK
{
  "totalRecords": "7",
  "records": [
    {
      "id": "12345",
      "author": "John Ronald Reuel Tolkein",
      "title": "The Fellowship of the Ring"
    },
    {
      "id": "67890",
      "author": "C. S. Lewis",
      "title": "The Screwtape Letters"
    },
    ...
  ]
}
```
And full records may be obtained by ID:
```
GET /codex/12345

200 OK
{
  "id": "12345",
  "author": "John Ronald Reuel Tolkein",
  "title": "The Fellowship of the Ring",
  "date": "1954",
  "publisher": "George Allen & Unwin",
  ...
}
```

Result records may include information necessary to obtain the source record from the module that contributed it.

## Metadata model

There is existing documentation for [the Codex metadata model](https://wiki.folio.org/pages/viewpage.action?pageId=1415393).

