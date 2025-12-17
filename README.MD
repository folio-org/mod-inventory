# mod-inventory

Copyright (C) 2016-2025 The Open Library Foundation

This software is distributed under the terms of the Apache License,
Version 2.0. See the file "[LICENSE](LICENSE)" for more information.

# Goal

FOLIO compatible inventory module.

Provides basic physical item inventory management, currently limited to basic representations of local instances and items, which can only be created via a MODS import.

Written in Java and uses Maven as its build system.

# Further Documentation

This readme is intended to get these modules up and running. For further information on what they do, see the [guide](doc/guide.md).

# Prerequisites

## Required

- Java 21 JDK
- Maven 3.8.0 or later

## Optional

- Node 6.4.0 (for API linting and documentation generation)
- NPM 3.10.3 (for API linting and documentation generation)
- Python 3.6.0 (for deployment scripts and tests via okapi setup)

# Preparation

## Git Submodules

There are some common RAML definitions that are shared between FOLIO projects via Git submodules.

To initialise these please run `git submodule init && git submodule update` in the root directory.

If these are not initialised, document generation and API linting operations may fail.

More information is available on the [developer site](https://dev.folio.org/guides/developer-setup#update-git-submodules).

## Groovy and Gradle

If sdkman is installed, run `source ./setup-environment.sh` to setup the shell with the appropriate versions of Gradle and Groovy.

# Building

run `mvn compile` from the root directory.

In order to build an executable Jar (e.g. for Okapi to deploy), run `mvn package`.

# Running

## Port

When running the jar file the module looks for the `http.port` and `port`
system property variables in this order, and uses the default 9403 as fallback. Example:

`java -Dhttp.port=8008 -jar target/mod-inventory.jar`

The Docker container exposes port 9403.

## Preparation

### Running Okapi

Make sure that [Okapi](https://github.com/folio-org/okapi) is running on its default port of 9130 (see the [guide](https://github.com/folio-org/okapi/blob/master/doc/guide.md) for instructions).

A script for building and running Okapi is provided. Run this module's `start-okapi.sh` from the root of the Okapi source.

As this runs Okapi using Postgres storage, some database preparation is required. This can be achieved by running `./create-okapi-database.sh` from the root of this repository.

### Dependencies

In order to activate the inventory module for a tenant, the dependencies described in the [Module Descriptor](ModuleDescriptor.json]) need to be fulfilled.

The simplest way to fulfil these is to use the [inventory storage module](http://https://github.com/folio-org/mod-inventory-storage).

## Registration

To register the module with deployment instructions and activate it for a demo tenant, run `./start-managed-demo.sh` from the root directory.

To deactivate and unregister the module, run `./stop-managed-demo.sh` from the root directory.

## Interaction with Kafka


There are several properties that should be set for modules that interact with Kafka: **KAFKA_HOST, KAFKA_PORT, OKAPI_URL, ENV**(unique env ID).
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
* These variables are relevant from the **XXXXX** release. Module version from X.X.X:
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
  * DI_INVENTORY_AUTHORITY_UPDATED_PARTITIONS
  * DI_INVENTORY_HOLDINGS_CREATED_READY_FOR_POST_PROCESSING_PARTITIONS
  * DI_INVENTORY_AUTHORITY_CREATED_READY_FOR_POST_PROCESSING_PARTITIONS
  * DI_INVENTORY_AUTHORITY_UPDATED_READY_FOR_POST_PROCESSING_PARTITIONS
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

These modules provide HTTP based APIs rather than any UI themselves.

As FOLIO is a multi-tenant system, many of the requests made to these modules are tenant aware (via the X-Okapi-Tenant header), which means most requests need to be made via a system which understands these headers (e.g. another module or UI built using [Stripes](https://github.com/folio-org/stripes-core)).

Therefore, it is suggested that requests to the API are made via tools such as curl or [postman](https://www.getpostman.com/), or via a browser plugin for adding headers, such as [Requestly](https://chrome.google.com/webstore/detail/requestly/mdnleldcmiljblolnjhpnblkcekpdkpa).

## Okapi Root Address

It is recommended that the modules are located via Okapi. Access via Okapi requires passing the X-Okapi-Tenant header (see the Okapi guide above for details).

http://localhost:9130/inventory

# Operating System Support

Most of the development for these modules, thus far, has been performed on OS X, with some on Ubuntu. Feedback for these, and particularly other operating systems is very welcome.

The GitHub Actions file [.github/workflows/macos.yml](.github/workflows/macos.yml) for macOS demonstrates how to use Homebrew to setup the infrastructure, run the module, enable it for the `diku` tenant and run a request.

## Permissions

The inventory.all permission set currently represents all of the permissions needed to use the inventory related parts of the system (e.g. the scan application and its configuration). This means that it contains additional permissions than those directly needed by the inventory module itself.

# Additional Information

The guide and other [documentation](doc) for this module.

Other [modules](https://dev.folio.org/source-code/#server-side).

Other FOLIO Developer documentation is at [dev.folio.org](https://dev.folio.org/)

### Issue tracker

See project [MODINV](https://issues.folio.org/browse/MODINV)
at the [FOLIO issue tracker](https://dev.folio.org/guidelines/issue-tracker/).

### ModuleDescriptor

See the built `target/ModuleDescriptor.json` for the interfaces that this module
requires and provides, the permissions, and the additional module metadata.

### API documentation

This module's [API documentation](https://dev.folio.org/reference/api/#mod-inventory).

### Code analysis

[SonarQube analysis](https://sonarcloud.io/dashboard?id=org.folio%3Amod-inventory).

### Download and configuration

The built artifacts for this module are available.
See [configuration](https://dev.folio.org/download/artifacts) for repository access,
and the [Docker image](https://hub.docker.com/r/folioorg/mod-inventory/).

# Appendix 1 - Docker Information

## When Using the Modules as Docker Containers

For the modules to communicate via Okapi Proxy, when running in Docker containers, the address for Okapi Proxy needs to be routable from inside the container.

This can be achieved by passing a parameter to the script used to start Okapi, as follows `../mod-inventory/start-okapi.sh http://192.168.X.X:9130`

Where 192.168.X.X is a routable IP address for the host from container instances and both repository clones are at the same directory level on your machine.

### Finding a Routable Address

Finding the appropriate IP address can be OS and Docker implementation dependent, so this is a very early guide rather than thorough treatment of the topic.

If these methods don't work for you, please do get in touch, so this section can be improved.

On Linux, `ifconfig docker0 | grep 'inet addr:'` should give output similar to `inet addr:192.168.X.X  Bcast:0.0.0.0  Mask:255.255.0.0`, , the first IP address is usually routable from within containers.

On Mac OS X (using Docker Native), `ifconfig en0 | grep 'inet '` should give output similar to `inet 192.168.X.X netmask 0xffffff00 broadcast 192.168.X.X`, the first IP address is usually routable from within containers.
