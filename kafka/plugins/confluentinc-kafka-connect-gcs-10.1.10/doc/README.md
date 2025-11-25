# Introduction

This project provides a connectors for Kafka Connect to write data to Google Cloud Storage.

# Documentation

Documentation on the connector is hosted on Confluent's
[docs site](https://docs.confluent.io/current/connect/kafka-connect-gcs/).

Source code is located in Confluent's
[docs repo](https://github.com/confluentinc/docs/tree/master/connect/kafka-connect-gcs). If changes
are made to configuration options for the connector, be sure to generate the RST docs (as described
below) and open a PR against the docs repo to publish those changes!

# Configs

Documentation on the configurations for each connector can be automatically generated via Maven.

To generate documentation for the sink connector:
```bash
mvn -Pdocs exec:java@sink-config-docs
```

# Integration Tests
Integration test are run by default when the connector is built.

These tests require GCS credentials. To run these tests locally, set the environment variable
`$LOCAL_GCS_CREDS` to refer to the path to your GCS credentials. For example:
```bash
LOCAL_GCS_CREDS=/Users/me/gcs-creds.json mvn clean integration-test
```

To disable them, set the property `skipIntegrationTests` to `true`. For example:
```bash
mvn clean install -DskipIntegrationTests=true
```