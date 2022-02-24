/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.integrations.destination.snowflake;

import com.fasterxml.jackson.databind.JsonNode;
import com.google.common.collect.ImmutableMap;
import io.airbyte.integrations.base.Destination;
import io.airbyte.integrations.base.IntegrationRunner;
import io.airbyte.integrations.destination.jdbc.copy.SwitchingDestination;
import java.util.Map;

public class SnowflakeDestination extends SwitchingDestination<SnowflakeDestination.DestinationType> {

  enum DestinationType {
    COPY_S3,
    COPY_GCS,
    COPY_AZURE_BLOB,
    INTERNAL_STAGING
  }

  public SnowflakeDestination() {
    super(DestinationType.class, SnowflakeDestination::getTypeFromConfig, getTypeToDestination());
  }

  private static DestinationType getTypeFromConfig(final JsonNode config) {
    if (isS3Copy(config)) {
      return DestinationType.COPY_S3;
    } else if (isGcsCopy(config)) {
      return DestinationType.COPY_GCS;
    } else if (isAzureBlobCopy(config)) {
      return DestinationType.COPY_AZURE_BLOB;
    } else {
      return DestinationType.INTERNAL_STAGING;
    }
  }

  public static boolean isS3Copy(final JsonNode config) {
    return config.has("loading_method") && config.get("loading_method").isObject() && config.get("loading_method").has("s3_bucket_name");
  }

  public static boolean isGcsCopy(final JsonNode config) {
    return config.has("loading_method") && config.get("loading_method").isObject() && config.get("loading_method").has("project_id");
  }

  public static boolean isAzureBlobCopy(final JsonNode config) {
    return config.has("loading_method") && config.get("loading_method").isObject()
        && config.get("loading_method").has("azure_blob_storage_account_name");
  }

  private static Map<DestinationType, Destination> getTypeToDestination() {
    final SnowflakeCopyS3Destination copyS3Destination = new SnowflakeCopyS3Destination();
    final SnowflakeCopyGcsDestination copyGcsDestination = new SnowflakeCopyGcsDestination();
    final SnowflakeCopyAzureBlobStorageDestination azureBlobStorageDestination = new SnowflakeCopyAzureBlobStorageDestination();
    final SnowflakeInternalStagingDestination internalStagingDestination = new SnowflakeInternalStagingDestination();

    return ImmutableMap.of(
        DestinationType.COPY_S3, copyS3Destination,
        DestinationType.COPY_GCS, copyGcsDestination,
        DestinationType.COPY_AZURE_BLOB, azureBlobStorageDestination,
        DestinationType.INTERNAL_STAGING, internalStagingDestination);
  }

  public static void main(final String[] args) throws Exception {
    final Destination destination = new SnowflakeDestination();
    new IntegrationRunner(destination).run(args);
  }

}
