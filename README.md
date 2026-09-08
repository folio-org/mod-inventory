# mod-inventory

Copyright (C) 2016–2026 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

# Goal

FOLIO compatible inventory module.

Provides inventory management for instances, holdings, and items, including data import via Kafka, authority record linking, MARC bibliographic record processing, and batch operations.

Written in Java and uses Maven as its build system.

# Prerequisites

## Required

- Java 21 JDK
- Maven 3.8.0 or later

## Optional

- Node.js (for API linting and documentation generation)
- NPM (for API linting and documentation generation)

# Git Submodules

There are some common RAML definitions that are shared between FOLIO projects via Git submodules.

To initialise these please run `git submodule init && git submodule update` in the root directory.

If these are not initialised, document generation and API linting operations may fail.

More information is available on the [developer site](https://dev.folio.org/guides/developer-setup#update-git-submodules).

# Building

Run `mvn compile` from the root directory.

In order to build an executable Jar, run `mvn package`.

# Running

## Port

When running the jar file the module looks for the `http.port` and `port`
system property variables in this order, and uses the default 9403 as fallback. Example:

`java -Dhttp.port=8008 -jar target/mod-inventory.jar`

The Docker container exposes port 9403.

## Dependencies

In order to activate the inventory module for a tenant, the dependencies described in the [Module Descriptor](ModuleDescriptor.json) need to be fulfilled.

The simplest way to fulfil these is to use the [inventory storage module](https://github.com/folio-org/mod-inventory-storage).

## Interaction with Kafka

There are several properties that should be set for modules that interact with Kafka: **KAFKA_HOST, KAFKA_PORT, OKAPI_URL, ENV** (unique env ID).
After setup, it is good to check logs in all related modules for errors.

**Environment variables** that can be adjusted for this module and default values:
 * These variables are relevant from the **Iris** release. Module version from 16.3.0:
    * "_inventory.kafka.DataImportConsumerVerticle.instancesNumber_": 5
    * "_inventory.kafka.MarcBibInstanceHridSetConsumerVerticle.instancesNumber_": 5
    * "_inventory.kafka.DataImportConsumer.loadLimit_": 5
    * "_inventory.kafka.DataImportConsumerVerticle.maxDistributionNumber_": 100
    * "_inventory.kafka.MarcBibInstanceHridSetConsumer.loadLimit_": 5
 * These variables are relevant from the **Iris** release (module version from 16.3.0) to **Kiwi** release (module version from 18.0.0):
    * "_kafkacache.topic.number.partitions_": 1
    * "_kafkacache.topic.replication.factor_": 1
    * "_kafkacache.log.retention.ms_": 18000000
    * "_kafkacache.topic_": events_cache
 * MarcBib update consumer variables:
    * "_inventory.kafka.MarcBibUpdateConsumerVerticle.instancesNumber_": 3
    * "_inventory.kafka.MarcBibUpdateConsumer.loadLimit_": 5
    * "_inventory.kafka.MarcBibUpdateConsumer.maxDistributionNumber_": 100
* Variables for setting number of partitions of topics:
  * DI_INVENTORY_INSTANCE_CREATED_PARTITIONS
  * DI_INVENTORY_HOLDING_CREATED_PARTITIONS
  * DI_INVENTORY_ITEM_CREATED_PARTITIONS
  * DI_INVENTORY_INSTANCE_MATCHED_PARTITIONS
  * DI_INVENTORY_HOLDING_MATCHED_PARTITIONS
  * DI_INVENTORY_ITEM_MATCHED_PARTITIONS
  * DI_SRS_MARC_BIB_RECORD_MATCHED_PARTITIONS
  * DI_INVENTORY_INSTANCE_UPDATED_PARTITIONS
  * DI_INVENTORY_HOLDING_UPDATED_PARTITIONS
  * DI_INVENTORY_ITEM_UPDATED_PARTITIONS
  * DI_INVENTORY_INSTANCE_NOT_MATCHED_PARTITIONS
  * DI_INVENTORY_HOLDING_NOT_MATCHED_PARTITIONS
  * DI_INVENTORY_ITEM_NOT_MATCHED_PARTITIONS
  * DI_SRS_MARC_BIB_RECORD_NOT_MATCHED_PARTITIONS
  * DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING_PARTITIONS
  * DI_SRS_MARC_BIB_RECORD_MODIFIED_PARTITIONS
  * INVENTORY_INSTANCE_INGRESS_PARTITIONS

Default value for all partitions is 1

## Properties

`AUTHORITY_EXTENDED` environment variable enables extended mapping for Authority to support advanced references classification in 5xx fields:
* broader terms (`$wg` tag)
* narrower terms (`$wh` tag)
* earlier headings (`$wa` tag)
* later headings (`$wb` tag)

Default value for `AUTHORITY_EXTENDED` is `false`.
The mapping itself is implemented in [data-import-processing-core](https://github.com/folio-org/data-import-processing-core).

If `AUTHORITY_EXTENDED`=true then data import process start to use extended version of authority record mapping which can produce additional elements `saftBroaderTerm`, `saftNarrowerTerm`, `saftEarlierHeading`, `saftLaterHeading`, `saftPersonalNameTrunc`, `saftPersonalNameTitleTrunc`, `saftGenreTermTrunc`, `saftGeographicNameTrunc`, `saftCorporateNameTrunc`, `saftCorporateNameTitleTrunc`, `saftMeetingNameTrunc`, `saftMeetingNameTitleTrunc`, `saftUniformTitleTrunc`, `saftTopicalTermTrunc` in json for mapped records and kafka messages, module mod-entitied-links starts to convert extended `AuthorityDto` to db entity and adds `"relationshipType"` and truncated versions of saft headings to the entry in `"authority"` and `"authority_archive"` tables in `"saft_headings"` column.

# Making Requests

This module provides HTTP based APIs rather than any UI itself.

As FOLIO is a multi-tenant system, many of the requests made to this module are tenant aware, which means most requests need to be made via a system which understands tenant headers (e.g. another module or UI built using [Stripes](https://github.com/folio-org/stripes-core)).

It is suggested that requests to the API are made via tools such as curl or [postman](https://www.getpostman.com/), or via a browser plugin for adding headers, such as [Requestly](https://chrome.google.com/webstore/detail/requestly/mdnleldcmiljblolnjhpnblkcekpdkpa).

# Additional Information

Other FOLIO Developer documentation is at [dev.folio.org](https://dev.folio.org/)

## Issue tracker

See project [MODINV](https://folio-org.atlassian.net/jira/software/c/projects/MODINV/summary)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker/).

## ModuleDescriptor

See the built `target/ModuleDescriptor.json` for the interfaces that this module
requires and provides, the permissions, and the additional module metadata.

## API documentation

This module's [API documentation](https://dev.folio.org/reference/api/#mod-inventory).

## Code analysis

[SonarQube analysis](https://sonarcloud.io/dashboard?id=org.folio%3Amod-inventory).

## Download and configuration

The built artifacts for this module are available.
See [configuration](https://dev.folio.org/download/artifacts) for repository access,
and the [Docker image](https://hub.docker.com/r/folioorg/mod-inventory/).

