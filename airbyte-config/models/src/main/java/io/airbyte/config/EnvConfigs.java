/*
 * Copyright (c) 2021 Airbyte, Inc., all rights reserved.
 */

package io.airbyte.config;

import com.google.common.base.Preconditions;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import io.airbyte.commons.version.AirbyteVersion;
import io.airbyte.config.helpers.LogClientSingleton;
import io.airbyte.config.helpers.LogConfigs;
import io.airbyte.config.storage.CloudStorageConfigs;
import io.airbyte.config.storage.CloudStorageConfigs.GcsConfig;
import io.airbyte.config.storage.CloudStorageConfigs.MinioConfig;
import io.airbyte.config.storage.CloudStorageConfigs.S3Config;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EnvConfigs implements Configs {

  private static final Logger LOGGER = LoggerFactory.getLogger(EnvConfigs.class);

  // env variable names
  public static final String AIRBYTE_ROLE = "AIRBYTE_ROLE";
  public static final String AIRBYTE_VERSION = "AIRBYTE_VERSION";
  public static final String INTERNAL_API_HOST = "INTERNAL_API_HOST";
  public static final String WORKER_ENVIRONMENT = "WORKER_ENVIRONMENT";
  public static final String SPEC_CACHE_BUCKET = "SPEC_CACHE_BUCKET";
  public static final String WORKSPACE_ROOT = "WORKSPACE_ROOT";
  public static final String WORKSPACE_DOCKER_MOUNT = "WORKSPACE_DOCKER_MOUNT";
  public static final String LOCAL_ROOT = "LOCAL_ROOT";
  public static final String LOCAL_DOCKER_MOUNT = "LOCAL_DOCKER_MOUNT";
  public static final String CONFIG_ROOT = "CONFIG_ROOT";
  public static final String DOCKER_NETWORK = "DOCKER_NETWORK";
  public static final String TRACKING_STRATEGY = "TRACKING_STRATEGY";
  public static final String DEPLOYMENT_MODE = "DEPLOYMENT_MODE";
  public static final String DATABASE_USER = "DATABASE_USER";
  public static final String DATABASE_PASSWORD = "DATABASE_PASSWORD";
  public static final String DATABASE_URL = "DATABASE_URL";
  public static final String CONFIG_DATABASE_USER = "CONFIG_DATABASE_USER";
  public static final String CONFIG_DATABASE_PASSWORD = "CONFIG_DATABASE_PASSWORD";
  public static final String CONFIG_DATABASE_URL = "CONFIG_DATABASE_URL";
  public static final String RUN_DATABASE_MIGRATION_ON_STARTUP = "RUN_DATABASE_MIGRATION_ON_STARTUP";
  public static final String WEBAPP_URL = "WEBAPP_URL";
  public static final String JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY = "JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY";
  public static final String JOB_KUBE_TOLERATIONS = "JOB_KUBE_TOLERATIONS";
  public static final String JOB_KUBE_NODE_SELECTORS = "JOB_KUBE_NODE_SELECTORS";
  public static final String JOB_KUBE_SOCAT_IMAGE = "JOB_KUBE_SOCAT_IMAGE";
  public static final String JOB_KUBE_BUSYBOX_IMAGE = "JOB_KUBE_BUSYBOX_IMAGE";
  public static final String JOB_KUBE_CURL_IMAGE = "JOB_KUBE_CURL_IMAGE";
  public static final String SYNC_JOB_MAX_ATTEMPTS = "SYNC_JOB_MAX_ATTEMPTS";
  public static final String SYNC_JOB_MAX_TIMEOUT_DAYS = "SYNC_JOB_MAX_TIMEOUT_DAYS";
  private static final String CONNECTOR_SPECIFIC_RESOURCE_DEFAULTS_ENABLED = "CONNECTOR_SPECIFIC_RESOURCE_DEFAULTS_ENABLED";
  private static final String MINIMUM_WORKSPACE_RETENTION_DAYS = "MINIMUM_WORKSPACE_RETENTION_DAYS";
  private static final String MAXIMUM_WORKSPACE_RETENTION_DAYS = "MAXIMUM_WORKSPACE_RETENTION_DAYS";
  private static final String MAXIMUM_WORKSPACE_SIZE_MB = "MAXIMUM_WORKSPACE_SIZE_MB";
  public static final String MAX_SPEC_WORKERS = "MAX_SPEC_WORKERS";
  public static final String MAX_CHECK_WORKERS = "MAX_CHECK_WORKERS";
  public static final String MAX_DISCOVER_WORKERS = "MAX_DISCOVER_WORKERS";
  public static final String MAX_SYNC_WORKERS = "MAX_SYNC_WORKERS";
  private static final String TEMPORAL_HOST = "TEMPORAL_HOST";
  private static final String TEMPORAL_WORKER_PORTS = "TEMPORAL_WORKER_PORTS";
  private static final String TEMPORAL_HISTORY_RETENTION_IN_DAYS = "TEMPORAL_HISTORY_RETENTION_IN_DAYS";
  public static final String JOB_KUBE_NAMESPACE = "JOB_KUBE_NAMESPACE";
  private static final String SUBMITTER_NUM_THREADS = "SUBMITTER_NUM_THREADS";
  public static final String JOB_MAIN_CONTAINER_CPU_REQUEST = "JOB_MAIN_CONTAINER_CPU_REQUEST";
  public static final String JOB_MAIN_CONTAINER_CPU_LIMIT = "JOB_MAIN_CONTAINER_CPU_LIMIT";
  public static final String JOB_MAIN_CONTAINER_MEMORY_REQUEST = "JOB_MAIN_CONTAINER_MEMORY_REQUEST";
  public static final String JOB_MAIN_CONTAINER_MEMORY_LIMIT = "JOB_MAIN_CONTAINER_MEMORY_LIMIT";
  public static final String JOB_DEFAULT_ENV_MAP = "JOB_DEFAULT_ENV_MAP";
  public static final String JOB_DEFAULT_ENV_PREFIX = "JOB_DEFAULT_ENV_";
  private static final String SECRET_PERSISTENCE = "SECRET_PERSISTENCE";
  public static final String JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET = "JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET";
  public static final String PUBLISH_METRICS = "PUBLISH_METRICS";
  private static final String CONFIGS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION = "CONFIGS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION";
  private static final String CONFIGS_DATABASE_INITIALIZATION_TIMEOUT_MS = "CONFIGS_DATABASE_INITIALIZATION_TIMEOUT_MS";
  private static final String JOBS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION = "JOBS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION";
  private static final String JOBS_DATABASE_INITIALIZATION_TIMEOUT_MS = "JOBS_DATABASE_INITIALIZATION_TIMEOUT_MS";
  private static final String CONTAINER_ORCHESTRATOR_ENABLED = "CONTAINER_ORCHESTRATOR_ENABLED";
  private static final String CONTAINER_ORCHESTRATOR_SECRET_NAME = "CONTAINER_ORCHESTRATOR_SECRET_NAME";
  private static final String CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH = "CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH";
  private static final String CONTAINER_ORCHESTRATOR_IMAGE = "CONTAINER_ORCHESTRATOR_IMAGE";
  private static final String DD_AGENT_HOST = "DD_AGENT_HOST";
  private static final String DD_DOGSTATSD_PORT = "DD_DOGSTATSD_PORT";

  public static final String STATE_STORAGE_S3_BUCKET_NAME = "STATE_STORAGE_S3_BUCKET_NAME";
  public static final String STATE_STORAGE_S3_REGION = "STATE_STORAGE_S3_REGION";
  public static final String STATE_STORAGE_S3_ACCESS_KEY = "STATE_STORAGE_S3_ACCESS_KEY";
  public static final String STATE_STORAGE_S3_SECRET_ACCESS_KEY = "STATE_STORAGE_S3_SECRET_ACCESS_KEY";
  public static final String STATE_STORAGE_MINIO_BUCKET_NAME = "STATE_STORAGE_MINIO_BUCKET_NAME";
  public static final String STATE_STORAGE_MINIO_ENDPOINT = "STATE_STORAGE_MINIO_ENDPOINT";
  public static final String STATE_STORAGE_MINIO_ACCESS_KEY = "STATE_STORAGE_MINIO_ACCESS_KEY";
  public static final String STATE_STORAGE_MINIO_SECRET_ACCESS_KEY = "STATE_STORAGE_MINIO_SECRET_ACCESS_KEY";
  public static final String STATE_STORAGE_GCS_BUCKET_NAME = "STATE_STORAGE_GCS_BUCKET_NAME";
  public static final String STATE_STORAGE_GCS_APPLICATION_CREDENTIALS = "STATE_STORAGE_GCS_APPLICATION_CREDENTIALS";

  public static final String ACTIVITY_MAX_TIMEOUT_SECOND = "ACTIVITY_MAX_TIMEOUT_SECOND";
  public static final String ACTIVITY_MAX_ATTEMPT = "ACTIVITY_MAX_ATTEMPT";
  public static final String ACTIVITY_DELAY_IN_SECOND_BETWEEN_ATTEMPTS = "ACTIVITY_DELAY_IN_SECOND_BETWEEN_ATTEMPTS";

  private static final String SHOULD_RUN_GET_SPEC_WORKFLOWS = "SHOULD_RUN_GET_SPEC_WORKFLOWS";
  private static final String SHOULD_RUN_CHECK_CONNECTION_WORKFLOWS = "SHOULD_RUN_CHECK_CONNECTION_WORKFLOWS";
  private static final String SHOULD_RUN_DISCOVER_WORKFLOWS = "SHOULD_RUN_DISCOVER_WORKFLOWS";
  private static final String SHOULD_RUN_SYNC_WORKFLOWS = "SHOULD_RUN_SYNC_WORKFLOWS";
  private static final String SHOULD_RUN_CONNECTION_MANAGER_WORKFLOWS = "SHOULD_RUN_CONNECTION_MANAGER_WORKFLOWS";

  // job-type-specific overrides
  public static final String SPEC_JOB_KUBE_NODE_SELECTORS = "SPEC_JOB_KUBE_NODE_SELECTORS";
  public static final String CHECK_JOB_KUBE_NODE_SELECTORS = "CHECK_JOB_KUBE_NODE_SELECTORS";
  public static final String DISCOVER_JOB_KUBE_NODE_SELECTORS = "DISCOVER_JOB_KUBE_NODE_SELECTORS";

  private static final String REPLICATION_ORCHESTRATOR_CPU_REQUEST = "REPLICATION_ORCHESTRATOR_CPU_REQUEST";
  private static final String REPLICATION_ORCHESTRATOR_CPU_LIMIT = "REPLICATION_ORCHESTRATOR_CPU_LIMIT";
  private static final String REPLICATION_ORCHESTRATOR_MEMORY_REQUEST = "REPLICATION_ORCHESTRATOR_MEMORY_REQUEST";
  private static final String REPLICATION_ORCHESTRATOR_MEMORY_LIMIT = "REPLICATION_ORCHESTRATOR_MEMORY_LIMIT";

  private static final String DEFAULT_WORKER_STATUS_CHECK_INTERVAL = "DEFAULT_WORKER_STATUS_CHECK_INTERVAL";
  private static final String SPEC_WORKER_STATUS_CHECK_INTERVAL = "SPEC_WORKER_STATUS_CHECK_INTERVAL";
  private static final String CHECK_WORKER_STATUS_CHECK_INTERVAL = "CHECK_WORKER_STATUS_CHECK_INTERVAL";
  private static final String DISCOVER_WORKER_STATUS_CHECK_INTERVAL = "DISCOVER_WORKER_STATUS_CHECK_INTERVAL";
  private static final String REPLICATION_WORKER_STATUS_CHECK_INTERVAL = "REPLICATION_WORKER_STATUS_CHECK_INTERVAL";

  static final String CHECK_JOB_MAIN_CONTAINER_CPU_REQUEST = "CHECK_JOB_MAIN_CONTAINER_CPU_REQUEST";
  static final String CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT = "CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT";
  static final String CHECK_JOB_MAIN_CONTAINER_MEMORY_REQUEST = "CHECK_JOB_MAIN_CONTAINER_MEMORY_REQUEST";
  static final String CHECK_JOB_MAIN_CONTAINER_MEMORY_LIMIT = "CHECK_JOB_MAIN_CONTAINER_MEMORY_LIMIT";

  // defaults
  private static final String DEFAULT_SPEC_CACHE_BUCKET = "io-airbyte-cloud-spec-cache";
  public static final String DEFAULT_JOB_KUBE_NAMESPACE = "default";
  private static final String DEFAULT_JOB_CPU_REQUIREMENT = null;
  private static final String DEFAULT_JOB_MEMORY_REQUIREMENT = null;
  private static final String DEFAULT_JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY = "IfNotPresent";
  private static final String SECRET_STORE_GCP_PROJECT_ID = "SECRET_STORE_GCP_PROJECT_ID";
  private static final String SECRET_STORE_GCP_CREDENTIALS = "SECRET_STORE_GCP_CREDENTIALS";
  private static final String DEFAULT_JOB_KUBE_SOCAT_IMAGE = "alpine/socat:1.7.4.1-r1";
  private static final String DEFAULT_JOB_KUBE_BUSYBOX_IMAGE = "busybox:1.28";
  private static final String DEFAULT_JOB_KUBE_CURL_IMAGE = "curlimages/curl:7.77.0";
  private static final long DEFAULT_MINIMUM_WORKSPACE_RETENTION_DAYS = 1;
  private static final long DEFAULT_MAXIMUM_WORKSPACE_RETENTION_DAYS = 60;
  private static final long DEFAULT_MAXIMUM_WORKSPACE_SIZE_MB = 5000;
  private static final int DEFAULT_DATABASE_INITIALIZATION_TIMEOUT_MS = 60 * 1000;

  private static final Duration DEFAULT_DEFAULT_WORKER_STATUS_CHECK_INTERVAL = Duration.ofSeconds(30);
  private static final Duration DEFAULT_SPEC_WORKER_STATUS_CHECK_INTERVAL = Duration.ofSeconds(1);
  private static final Duration DEFAULT_CHECK_WORKER_STATUS_CHECK_INTERVAL = Duration.ofSeconds(1);
  private static final Duration DEFAULT_DISCOVER_WORKER_STATUS_CHECK_INTERVAL = Duration.ofSeconds(1);
  private static final Duration DEFAULT_REPLICATION_WORKER_STATUS_CHECK_INTERVAL = Duration.ofSeconds(30);

  public static final long DEFAULT_MAX_SPEC_WORKERS = 5;
  public static final long DEFAULT_MAX_CHECK_WORKERS = 5;
  public static final long DEFAULT_MAX_DISCOVER_WORKERS = 5;
  public static final long DEFAULT_MAX_SYNC_WORKERS = 5;

  public static final String DEFAULT_NETWORK = "host";

  public static final int DEFAULT_TEMPORAL_HISTORY_RETENTION_IN_DAYS = 30;

  private final Function<String, String> getEnv;
  private final Supplier<Set<String>> getAllEnvKeys;
  private final LogConfigs logConfigs;
  private final CloudStorageConfigs stateStorageCloudConfigs;

  /**
   * Constructs {@link EnvConfigs} from actual environment variables.
   */
  public EnvConfigs() {
    this(System.getenv());
  }

  /**
   * Constructs {@link EnvConfigs} from a provided map. This can be used for testing or getting
   * variables from a non-envvar source.
   */
  public EnvConfigs(final Map<String, String> envMap) {
    this.getEnv = envMap::get;
    this.getAllEnvKeys = envMap::keySet;
    this.logConfigs = new LogConfigs(getLogConfiguration().orElse(null));
    this.stateStorageCloudConfigs = getStateStorageConfiguration().orElse(null);
  }

  private Optional<CloudStorageConfigs> getLogConfiguration() {
    if (getEnv(LogClientSingleton.GCS_LOG_BUCKET) != null && !getEnv(LogClientSingleton.GCS_LOG_BUCKET).isBlank()) {
      return Optional.of(CloudStorageConfigs.gcs(new GcsConfig(
          getEnvOrDefault(LogClientSingleton.GCS_LOG_BUCKET, ""),
          getEnvOrDefault(LogClientSingleton.GOOGLE_APPLICATION_CREDENTIALS, ""))));
    } else if (getEnv(LogClientSingleton.S3_MINIO_ENDPOINT) != null && !getEnv(LogClientSingleton.S3_MINIO_ENDPOINT).isBlank()) {
      return Optional.of(CloudStorageConfigs.minio(new MinioConfig(
          getEnvOrDefault(LogClientSingleton.S3_LOG_BUCKET, ""),
          getEnvOrDefault(LogClientSingleton.AWS_ACCESS_KEY_ID, ""),
          getEnvOrDefault(LogClientSingleton.AWS_SECRET_ACCESS_KEY, ""),
          getEnvOrDefault(LogClientSingleton.S3_MINIO_ENDPOINT, ""))));
    } else if (getEnv(LogClientSingleton.S3_LOG_BUCKET_REGION) != null && !getEnv(LogClientSingleton.S3_LOG_BUCKET_REGION).isBlank()) {
      return Optional.of(CloudStorageConfigs.s3(new S3Config(
          getEnvOrDefault(LogClientSingleton.S3_LOG_BUCKET, ""),
          getEnvOrDefault(LogClientSingleton.AWS_ACCESS_KEY_ID, ""),
          getEnvOrDefault(LogClientSingleton.AWS_SECRET_ACCESS_KEY, ""),
          getEnvOrDefault(LogClientSingleton.S3_LOG_BUCKET_REGION, ""))));
    } else {
      return Optional.empty();
    }
  }

  private Optional<CloudStorageConfigs> getStateStorageConfiguration() {
    if (getEnv(STATE_STORAGE_GCS_BUCKET_NAME) != null) {
      return Optional.of(CloudStorageConfigs.gcs(new GcsConfig(
          getEnvOrDefault(STATE_STORAGE_GCS_BUCKET_NAME, ""),
          getEnvOrDefault(STATE_STORAGE_GCS_APPLICATION_CREDENTIALS, ""))));
    } else if (getEnv(STATE_STORAGE_MINIO_ENDPOINT) != null) {
      return Optional.of(CloudStorageConfigs.minio(new MinioConfig(
          getEnvOrDefault(STATE_STORAGE_MINIO_BUCKET_NAME, ""),
          getEnvOrDefault(STATE_STORAGE_MINIO_ACCESS_KEY, ""),
          getEnvOrDefault(STATE_STORAGE_MINIO_SECRET_ACCESS_KEY, ""),
          getEnvOrDefault(STATE_STORAGE_MINIO_ENDPOINT, ""))));
    } else if (getEnv(STATE_STORAGE_S3_REGION) != null) {
      return Optional.of(CloudStorageConfigs.s3(new S3Config(
          getEnvOrDefault(STATE_STORAGE_S3_BUCKET_NAME, ""),
          getEnvOrDefault(STATE_STORAGE_S3_ACCESS_KEY, ""),
          getEnvOrDefault(STATE_STORAGE_S3_SECRET_ACCESS_KEY, ""),
          getEnvOrDefault(STATE_STORAGE_S3_REGION, ""))));
    } else {
      return Optional.empty();
    }
  }

  // CORE
  // General
  @Override
  public String getAirbyteRole() {
    return getEnv(AIRBYTE_ROLE);
  }

  @Override
  public AirbyteVersion getAirbyteVersion() {
    return new AirbyteVersion(getEnsureEnv(AIRBYTE_VERSION));
  }

  @Override
  public String getAirbyteVersionOrWarning() {
    return Optional.ofNullable(getEnv(AIRBYTE_VERSION)).orElse("version not set");
  }

  @Override
  public String getSpecCacheBucket() {
    return getEnvOrDefault(SPEC_CACHE_BUCKET, DEFAULT_SPEC_CACHE_BUCKET);
  }

  @Override
  public DeploymentMode getDeploymentMode() {
    return getEnvOrDefault(DEPLOYMENT_MODE, DeploymentMode.OSS, s -> {
      try {
        return DeploymentMode.valueOf(s);
      } catch (final IllegalArgumentException e) {
        LOGGER.info(s + " not recognized, defaulting to " + DeploymentMode.OSS);
        return DeploymentMode.OSS;
      }
    });
  }

  @Override
  public WorkerEnvironment getWorkerEnvironment() {
    return getEnvOrDefault(WORKER_ENVIRONMENT, WorkerEnvironment.DOCKER, s -> WorkerEnvironment.valueOf(s.toUpperCase()));
  }

  @Override
  public Path getConfigRoot() {
    return getPath(CONFIG_ROOT);
  }

  @Override
  public Path getWorkspaceRoot() {
    return getPath(WORKSPACE_ROOT);
  }

  // Docker Only
  @Override
  public String getWorkspaceDockerMount() {
    return getEnvOrDefault(WORKSPACE_DOCKER_MOUNT, getWorkspaceRoot().toString());
  }

  @Override
  public String getLocalDockerMount() {
    return getEnvOrDefault(LOCAL_DOCKER_MOUNT, getLocalRoot().toString());
  }

  @Override
  public String getDockerNetwork() {
    return getEnvOrDefault(DOCKER_NETWORK, DEFAULT_NETWORK);
  }

  @Override
  public Path getLocalRoot() {
    return getPath(LOCAL_ROOT);
  }

  // Secrets
  @Override
  public String getSecretStoreGcpCredentials() {
    return getEnv(SECRET_STORE_GCP_CREDENTIALS);
  }

  @Override
  public String getSecretStoreGcpProjectId() {
    return getEnv(SECRET_STORE_GCP_PROJECT_ID);
  }

  @Override
  public SecretPersistenceType getSecretPersistenceType() {
    final var secretPersistenceStr = getEnvOrDefault(SECRET_PERSISTENCE, SecretPersistenceType.NONE.name());
    return SecretPersistenceType.valueOf(secretPersistenceStr);
  }

  // Database
  @Override
  public String getDatabaseUser() {
    return getEnsureEnv(DATABASE_USER);
  }

  @Override
  public String getDatabasePassword() {
    return getEnsureEnv(DATABASE_PASSWORD);
  }

  @Override
  public String getDatabaseUrl() {
    return getEnsureEnv(DATABASE_URL);
  }

  @Override
  public String getJobsDatabaseMinimumFlywayMigrationVersion() {
    return getEnsureEnv(JOBS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION);
  }

  @Override
  public long getJobsDatabaseInitializationTimeoutMs() {
    return getEnvOrDefault(JOBS_DATABASE_INITIALIZATION_TIMEOUT_MS, DEFAULT_DATABASE_INITIALIZATION_TIMEOUT_MS);
  }

  @Override
  public String getConfigDatabaseUser() {
    // Default to reuse the job database
    return getEnvOrDefault(CONFIG_DATABASE_USER, getDatabaseUser());
  }

  @Override
  public String getConfigDatabasePassword() {
    // Default to reuse the job database
    return getEnvOrDefault(CONFIG_DATABASE_PASSWORD, getDatabasePassword(), true);
  }

  @Override
  public String getConfigDatabaseUrl() {
    // Default to reuse the job database
    return getEnvOrDefault(CONFIG_DATABASE_URL, getDatabaseUrl());
  }

  @Override
  public String getConfigsDatabaseMinimumFlywayMigrationVersion() {
    return getEnsureEnv(CONFIGS_DATABASE_MINIMUM_FLYWAY_MIGRATION_VERSION);
  }

  @Override
  public long getConfigsDatabaseInitializationTimeoutMs() {
    return getEnvOrDefault(CONFIGS_DATABASE_INITIALIZATION_TIMEOUT_MS, DEFAULT_DATABASE_INITIALIZATION_TIMEOUT_MS);
  }

  @Override
  public boolean runDatabaseMigrationOnStartup() {
    return getEnvOrDefault(RUN_DATABASE_MIGRATION_ON_STARTUP, true);
  }

  // Airbyte Services
  @Override
  public String getTemporalHost() {
    return getEnvOrDefault(TEMPORAL_HOST, "airbyte-temporal:7233");
  }

  @Override
  public int getTemporalRetentionInDays() {
    return getEnvOrDefault(TEMPORAL_HISTORY_RETENTION_IN_DAYS, DEFAULT_TEMPORAL_HISTORY_RETENTION_IN_DAYS);
  }

  @Override
  public String getAirbyteApiHost() {
    return getEnsureEnv(INTERNAL_API_HOST).split(":")[0];
  }

  @Override
  public int getAirbyteApiPort() {
    return Integer.parseInt(getEnsureEnv(INTERNAL_API_HOST).split(":")[1]);
  }

  @Override
  public String getWebappUrl() {
    return getEnsureEnv(WEBAPP_URL);
  }

  // Jobs
  @Override
  public int getSyncJobMaxAttempts() {
    return Integer.parseInt(getEnvOrDefault(SYNC_JOB_MAX_ATTEMPTS, "3"));
  }

  @Override
  public int getSyncJobMaxTimeoutDays() {
    return Integer.parseInt(getEnvOrDefault(SYNC_JOB_MAX_TIMEOUT_DAYS, "3"));
  }

  @Override
  public boolean connectorSpecificResourceDefaultsEnabled() {
    return getEnvOrDefault(CONNECTOR_SPECIFIC_RESOURCE_DEFAULTS_ENABLED, false);
  }

  /**
   * Returns worker pod tolerations parsed from its own environment variable. The value of the env is
   * a string that represents one or more tolerations.
   * <ul>
   * <li>Tolerations are separated by a `;`
   * <li>Each toleration contains k=v pairs mentioning some/all of key, effect, operator and value and
   * separated by `,`
   * </ul>
   * <p>
   * For example:- The following represents two tolerations, one checking existence and another
   * matching a value
   * <p>
   * key=airbyte-server,operator=Exists,effect=NoSchedule;key=airbyte-server,operator=Equals,value=true,effect=NoSchedule
   *
   * @return list of WorkerKubeToleration parsed from env
   */
  @Override
  public List<TolerationPOJO> getJobKubeTolerations() {
    final String tolerationsStr = getEnvOrDefault(JOB_KUBE_TOLERATIONS, "");

    final Stream<String> tolerations = Strings.isNullOrEmpty(tolerationsStr) ? Stream.of()
        : Splitter.on(";")
            .splitToStream(tolerationsStr)
            .filter(tolerationStr -> !Strings.isNullOrEmpty(tolerationStr));

    return tolerations
        .map(this::parseToleration)
        .filter(Objects::nonNull)
        .collect(Collectors.toList());
  }

  private TolerationPOJO parseToleration(final String tolerationStr) {
    final Map<String, String> tolerationMap = Splitter.on(",")
        .splitToStream(tolerationStr)
        .map(s -> s.split("="))
        .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    if (tolerationMap.containsKey("key") && tolerationMap.containsKey("effect") && tolerationMap.containsKey("operator")) {
      return new TolerationPOJO(
          tolerationMap.get("key"),
          tolerationMap.get("effect"),
          tolerationMap.get("value"),
          tolerationMap.get("operator"));
    } else {
      LOGGER.warn(
          "Ignoring toleration {}, missing one of key,effect or operator",
          tolerationStr);
      return null;
    }
  }

  /**
   * Returns a map of node selectors for any job type. Used as a default if a particular job type does
   * not define its own node selector environment variable.
   *
   * @return map containing kv pairs of node selectors, or empty optional if none present.
   */
  @Override
  public Optional<Map<String, String>> getJobKubeNodeSelectors() {
    return getNodeSelectorsFromEnvString(getEnvOrDefault(JOB_KUBE_NODE_SELECTORS, ""));
  }

  /**
   * Returns a map of node selectors for Spec job pods specifically.
   *
   * @return map containing kv pairs of node selectors, or empty optional if none present.
   */
  @Override
  public Optional<Map<String, String>> getSpecJobKubeNodeSelectors() {
    return getNodeSelectorsFromEnvString(getEnvOrDefault(SPEC_JOB_KUBE_NODE_SELECTORS, ""));
  }

  /**
   * Returns a map of node selectors for Check job pods specifically.
   *
   * @return map containing kv pairs of node selectors, or empty optional if none present.
   */
  @Override
  public Optional<Map<String, String>> getCheckJobKubeNodeSelectors() {
    return getNodeSelectorsFromEnvString(getEnvOrDefault(CHECK_JOB_KUBE_NODE_SELECTORS, ""));
  }

  /**
   * Returns a map of node selectors for Discover job pods specifically.
   *
   * @return map containing kv pairs of node selectors, or empty optional if none present.
   */
  @Override
  public Optional<Map<String, String>> getDiscoverJobKubeNodeSelectors() {
    return getNodeSelectorsFromEnvString(getEnvOrDefault(DISCOVER_JOB_KUBE_NODE_SELECTORS, ""));
  }

  /**
   * Parse string containing node selectors into a map. Each kv-pair is separated by a `,`
   * <p>
   * For example:- The following represents two node selectors
   * <p>
   * airbyte=server,type=preemptive
   *
   * @param envString string that represents one or more node selector labels.
   * @return map containing kv pairs of node selectors, or empty optional if none present.
   */
  private Optional<Map<String, String>> getNodeSelectorsFromEnvString(final String envString) {
    final Map<String, String> selectors = Splitter.on(",")
        .splitToStream(envString)
        .filter(s -> !Strings.isNullOrEmpty(s) && s.contains("="))
        .map(s -> s.split("="))
        .collect(Collectors.toMap(s -> s[0], s -> s[1]));

    return selectors.isEmpty() ? Optional.empty() : Optional.of(selectors);
  }

  @Override
  public String getJobKubeMainContainerImagePullPolicy() {
    return getEnvOrDefault(JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY, DEFAULT_JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_POLICY);
  }

  /**
   * Returns the name of the secret to be used when pulling down docker images for jobs. Automatically
   * injected in the KubePodProcess class and used in the job pod templates. The empty string is a
   * no-op value.
   */
  @Override
  public String getJobKubeMainContainerImagePullSecret() {
    return getEnvOrDefault(JOB_KUBE_MAIN_CONTAINER_IMAGE_PULL_SECRET, "");
  }

  @Override
  public String getJobKubeSocatImage() {
    return getEnvOrDefault(JOB_KUBE_SOCAT_IMAGE, DEFAULT_JOB_KUBE_SOCAT_IMAGE);
  }

  @Override
  public String getJobKubeBusyboxImage() {
    return getEnvOrDefault(JOB_KUBE_BUSYBOX_IMAGE, DEFAULT_JOB_KUBE_BUSYBOX_IMAGE);
  }

  @Override
  public String getJobKubeCurlImage() {
    return getEnvOrDefault(JOB_KUBE_CURL_IMAGE, DEFAULT_JOB_KUBE_CURL_IMAGE);
  }

  @Override
  public String getJobKubeNamespace() {
    return getEnvOrDefault(JOB_KUBE_NAMESPACE, DEFAULT_JOB_KUBE_NAMESPACE);
  }

  @Override
  public Duration getDefaultWorkerStatusCheckInterval() {
    return getEnvOrDefault(
        DEFAULT_WORKER_STATUS_CHECK_INTERVAL,
        DEFAULT_DEFAULT_WORKER_STATUS_CHECK_INTERVAL,
        value -> Duration.ofSeconds(Integer.parseInt(value)));
  }

  @Override
  public Duration getSpecWorkerStatusCheckInterval() {
    return getEnvOrDefault(
        SPEC_WORKER_STATUS_CHECK_INTERVAL,
        DEFAULT_SPEC_WORKER_STATUS_CHECK_INTERVAL,
        value -> Duration.ofSeconds(Integer.parseInt(value)));
  }

  @Override
  public Duration getCheckWorkerStatusCheckInterval() {
    return getEnvOrDefault(
        CHECK_WORKER_STATUS_CHECK_INTERVAL,
        DEFAULT_CHECK_WORKER_STATUS_CHECK_INTERVAL,
        value -> Duration.ofSeconds(Integer.parseInt(value)));
  }

  @Override
  public Duration getDiscoverWorkerStatusCheckInterval() {
    return getEnvOrDefault(
        DISCOVER_WORKER_STATUS_CHECK_INTERVAL,
        DEFAULT_DISCOVER_WORKER_STATUS_CHECK_INTERVAL,
        value -> Duration.ofSeconds(Integer.parseInt(value)));
  }

  @Override
  public Duration getReplicationWorkerStatusCheckInterval() {
    return getEnvOrDefault(
        REPLICATION_WORKER_STATUS_CHECK_INTERVAL,
        DEFAULT_REPLICATION_WORKER_STATUS_CHECK_INTERVAL,
        value -> Duration.ofSeconds(Integer.parseInt(value)));
  }

  @Override
  public String getJobMainContainerCpuRequest() {
    return getEnvOrDefault(JOB_MAIN_CONTAINER_CPU_REQUEST, DEFAULT_JOB_CPU_REQUIREMENT);
  }

  @Override
  public String getJobMainContainerCpuLimit() {
    return getEnvOrDefault(JOB_MAIN_CONTAINER_CPU_LIMIT, DEFAULT_JOB_CPU_REQUIREMENT);
  }

  @Override
  public String getJobMainContainerMemoryRequest() {
    return getEnvOrDefault(JOB_MAIN_CONTAINER_MEMORY_REQUEST, DEFAULT_JOB_MEMORY_REQUIREMENT);
  }

  @Override
  public String getJobMainContainerMemoryLimit() {
    return getEnvOrDefault(JOB_MAIN_CONTAINER_MEMORY_LIMIT, DEFAULT_JOB_MEMORY_REQUIREMENT);
  }

  @Override
  public Map<String, String> getJobDefaultEnvMap() {
    return getAllEnvKeys.get().stream()
        .filter(key -> key.startsWith(JOB_DEFAULT_ENV_PREFIX))
        .collect(Collectors.toMap(key -> key.replace(JOB_DEFAULT_ENV_PREFIX, ""), getEnv));
  }

  @Override
  public String getCheckJobMainContainerCpuRequest() {
    return getEnvOrDefault(CHECK_JOB_MAIN_CONTAINER_CPU_REQUEST, getJobMainContainerCpuRequest());
  }

  @Override
  public String getCheckJobMainContainerCpuLimit() {
    return getEnvOrDefault(CHECK_JOB_MAIN_CONTAINER_CPU_LIMIT, getJobMainContainerCpuLimit());
  }

  @Override
  public String getCheckJobMainContainerMemoryRequest() {
    return getEnvOrDefault(CHECK_JOB_MAIN_CONTAINER_MEMORY_REQUEST, getJobMainContainerMemoryRequest());
  }

  @Override
  public String getCheckJobMainContainerMemoryLimit() {
    return getEnvOrDefault(CHECK_JOB_MAIN_CONTAINER_MEMORY_LIMIT, getJobMainContainerMemoryLimit());
  }

  @Override
  public LogConfigs getLogConfigs() {
    return logConfigs;
  }

  @Override
  public String getGoogleApplicationCredentials() {
    return getEnvOrDefault(LogClientSingleton.GOOGLE_APPLICATION_CREDENTIALS, null);
  }

  @Override
  public CloudStorageConfigs getStateStorageCloudConfigs() {
    return stateStorageCloudConfigs;
  }

  @Override
  public boolean getPublishMetrics() {
    return getEnvOrDefault(PUBLISH_METRICS, false);
  }

  @Override
  public String getDDAgentHost() {
    return getEnvOrDefault(DD_AGENT_HOST, "");
  }

  @Override
  public String getDDDogStatsDPort() {
    return getEnvOrDefault(DD_DOGSTATSD_PORT, "");
  }

  @Override
  public TrackingStrategy getTrackingStrategy() {
    return getEnvOrDefault(TRACKING_STRATEGY, TrackingStrategy.LOGGING, s -> {
      try {
        return TrackingStrategy.valueOf(s.toUpperCase());
      } catch (final IllegalArgumentException e) {
        LOGGER.info(s + " not recognized, defaulting to " + TrackingStrategy.LOGGING);
        return TrackingStrategy.LOGGING;
      }
    });
  }

  // APPLICATIONS
  // Worker
  @Override
  public MaxWorkersConfig getMaxWorkers() {
    return new MaxWorkersConfig(
        Math.toIntExact(getEnvOrDefault(MAX_SPEC_WORKERS, DEFAULT_MAX_SPEC_WORKERS)),
        Math.toIntExact(getEnvOrDefault(MAX_CHECK_WORKERS, DEFAULT_MAX_CHECK_WORKERS)),
        Math.toIntExact(getEnvOrDefault(MAX_DISCOVER_WORKERS, DEFAULT_MAX_DISCOVER_WORKERS)),
        Math.toIntExact(getEnvOrDefault(MAX_SYNC_WORKERS, DEFAULT_MAX_SYNC_WORKERS)));
  }

  @Override
  public boolean shouldRunGetSpecWorkflows() {
    return getEnvOrDefault(SHOULD_RUN_GET_SPEC_WORKFLOWS, true);
  }

  @Override
  public boolean shouldRunCheckConnectionWorkflows() {
    return getEnvOrDefault(SHOULD_RUN_CHECK_CONNECTION_WORKFLOWS, true);
  }

  @Override
  public boolean shouldRunDiscoverWorkflows() {
    return getEnvOrDefault(SHOULD_RUN_DISCOVER_WORKFLOWS, true);
  }

  @Override
  public boolean shouldRunSyncWorkflows() {
    return getEnvOrDefault(SHOULD_RUN_SYNC_WORKFLOWS, true);
  }

  @Override
  public boolean shouldRunConnectionManagerWorkflows() {
    return getEnvOrDefault(SHOULD_RUN_CONNECTION_MANAGER_WORKFLOWS, true);
  }

  @Override
  public Set<Integer> getTemporalWorkerPorts() {
    final var ports = getEnvOrDefault(TEMPORAL_WORKER_PORTS, "");
    if (ports.isEmpty()) {
      return new HashSet<>();
    }
    return Arrays.stream(ports.split(",")).map(Integer::valueOf).collect(Collectors.toSet());
  }

  // Scheduler
  @Override
  public WorkspaceRetentionConfig getWorkspaceRetentionConfig() {
    final long minDays = getEnvOrDefault(MINIMUM_WORKSPACE_RETENTION_DAYS, DEFAULT_MINIMUM_WORKSPACE_RETENTION_DAYS);
    final long maxDays = getEnvOrDefault(MAXIMUM_WORKSPACE_RETENTION_DAYS, DEFAULT_MAXIMUM_WORKSPACE_RETENTION_DAYS);
    final long maxSizeMb = getEnvOrDefault(MAXIMUM_WORKSPACE_SIZE_MB, DEFAULT_MAXIMUM_WORKSPACE_SIZE_MB);

    return new WorkspaceRetentionConfig(minDays, maxDays, maxSizeMb);
  }

  @Override
  public String getSubmitterNumThreads() {
    return getEnvOrDefault(SUBMITTER_NUM_THREADS, "5");
  }

  @Override
  public boolean getContainerOrchestratorEnabled() {
    return getEnvOrDefault(CONTAINER_ORCHESTRATOR_ENABLED, false, Boolean::valueOf);
  }

  @Override
  public String getContainerOrchestratorSecretName() {
    return getEnvOrDefault(CONTAINER_ORCHESTRATOR_SECRET_NAME, null);
  }

  @Override
  public String getContainerOrchestratorSecretMountPath() {
    return getEnvOrDefault(CONTAINER_ORCHESTRATOR_SECRET_MOUNT_PATH, null);
  }

  @Override
  public String getContainerOrchestratorImage() {
    return getEnvOrDefault(CONTAINER_ORCHESTRATOR_IMAGE, "airbyte/container-orchestrator:" + getAirbyteVersion().serialize());
  }

  @Override
  public String getReplicationOrchestratorCpuRequest() {
    return getEnvOrDefault(REPLICATION_ORCHESTRATOR_CPU_REQUEST, null);
  }

  @Override
  public String getReplicationOrchestratorCpuLimit() {
    return getEnvOrDefault(REPLICATION_ORCHESTRATOR_CPU_LIMIT, null);
  }

  @Override
  public String getReplicationOrchestratorMemoryRequest() {
    return getEnvOrDefault(REPLICATION_ORCHESTRATOR_MEMORY_REQUEST, null);
  }

  @Override
  public String getReplicationOrchestratorMemoryLimit() {
    return getEnvOrDefault(REPLICATION_ORCHESTRATOR_MEMORY_LIMIT, null);
  }

  @Override
  public int getMaxActivityTimeoutSecond() {
    return Integer.parseInt(getEnvOrDefault(ACTIVITY_MAX_TIMEOUT_SECOND, "120"));
  }

  @Override
  public int getDelayBetweenActivityAttempts() {
    return Integer.parseInt(getEnvOrDefault(ACTIVITY_MAX_TIMEOUT_SECOND, "30"));
  }

  @Override
  public int getActivityNumberOfAttempt() {
    return Integer.parseInt(getEnvOrDefault(ACTIVITY_MAX_ATTEMPT, "10"));
  }

  // Helpers
  public String getEnvOrDefault(final String key, final String defaultValue) {
    return getEnvOrDefault(key, defaultValue, Function.identity(), false);
  }

  public String getEnvOrDefault(final String key, final String defaultValue, final boolean isSecret) {
    return getEnvOrDefault(key, defaultValue, Function.identity(), isSecret);
  }

  public long getEnvOrDefault(final String key, final long defaultValue) {
    return getEnvOrDefault(key, defaultValue, Long::parseLong, false);
  }

  public int getEnvOrDefault(final String key, final int defaultValue) {
    return getEnvOrDefault(key, defaultValue, Integer::parseInt, false);
  }

  public boolean getEnvOrDefault(final String key, final boolean defaultValue) {
    return getEnvOrDefault(key, defaultValue, Boolean::parseBoolean);
  }

  public <T> T getEnvOrDefault(final String key, final T defaultValue, final Function<String, T> parser) {
    return getEnvOrDefault(key, defaultValue, parser, false);
  }

  public <T> T getEnvOrDefault(final String key, final T defaultValue, final Function<String, T> parser, final boolean isSecret) {
    final String value = getEnv.apply(key);
    if (value != null && !value.isEmpty()) {
      return parser.apply(value);
    } else {
      LOGGER.info("Using default value for environment variable {}: '{}'", key, isSecret ? "*****" : defaultValue);
      return defaultValue;
    }
  }

  public String getEnv(final String name) {
    return getEnv.apply(name);
  }

  public String getEnsureEnv(final String name) {
    final String value = getEnv(name);
    Preconditions.checkArgument(value != null, "'%s' environment variable cannot be null", name);

    return value;
  }

  private Path getPath(final String name) {
    final String value = getEnv.apply(name);
    if (value == null) {
      throw new IllegalArgumentException("Env variable not defined: " + name);
    }
    return Path.of(value);
  }

}
