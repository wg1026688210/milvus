-- create database
CREATE DATABASE if not exists milvus_meta CHARACTER SET utf8mb4;

/*
 create tables script

 Notices:
    1. id, tenant_id, is_deleted, created_at, updated_at are 5 common columns for all collections.
    2. Query index in community version CANNOT includes tenant_id, since tenant_id is not existed and will miss query index.
 */

-- collections
CREATE TABLE if not exists milvus_meta.collections (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    collection_id BIGINT NOT NULL,
    collection_name VARCHAR(256),
    description VARCHAR(2048) DEFAULT NULL,
    auto_id BOOL DEFAULT FALSE,
    shards_num INT,
    start_position TEXT,
    consistency_level INT,
    status INT NOT NULL,
    properties VARCHAR(512),
    ts BIGINT UNSIGNED DEFAULT 0,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_id_collection_id_ts (tenant_id, collection_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- collection aliases
CREATE TABLE if not exists milvus_meta.collection_aliases (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    collection_id BIGINT NOT NULL,
    collection_alias VARCHAR(128),
    ts BIGINT UNSIGNED DEFAULT 0,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_id_collection_alias_ts (tenant_id, collection_alias, ts),
    INDEX idx_tenant_id_collection_id_ts (tenant_id, collection_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- channels
CREATE TABLE if not exists milvus_meta.collection_channels (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    collection_id BIGINT NOT NULL,
    virtual_channel_name VARCHAR(256) NOT NULL,
    physical_channel_name VARCHAR(256) NOT NULL,
    removed BOOL DEFAULT FALSE,
    ts BIGINT UNSIGNED DEFAULT 0,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_id_collection_id_virtual_channel_name_ts (tenant_id, collection_id, virtual_channel_name, ts),
    INDEX idx_tenant_id_collection_id_ts (tenant_id, collection_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- fields
CREATE TABLE if not exists milvus_meta.field_schemas (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    field_id BIGINT NOT NULL,
    field_name VARCHAR(256) NOT NULL,
    is_primary_key BOOL NOT NULL,
    description VARCHAR(2048) DEFAULT NULL,
    data_type INT UNSIGNED NOT NULL,
    type_params VARCHAR(2048),
    index_params VARCHAR(2048),
    auto_id BOOL NOT NULL,
    collection_id     BIGINT NOT NULL,
    ts BIGINT UNSIGNED DEFAULT 0,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_id_collection_id_field_name_ts (tenant_id, collection_id, field_name, ts),
    INDEX idx_tenant_id_collection_id_field_id_ts (tenant_id, collection_id, field_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- partitions
CREATE TABLE if not exists milvus_meta.`partitions` (
    id BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    partition_id     BIGINT NOT NULL,
    partition_name     VARCHAR(256),
    partition_created_timestamp bigint unsigned,
    collection_id     BIGINT NOT NULL,
    status INT NOT NULL,
    ts BIGINT UNSIGNED DEFAULT 0,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_id_collection_id_partition_name_ts (tenant_id, collection_id, partition_name, ts),
    INDEX idx_tenant_id_collection_id_partition_id_ts (tenant_id, collection_id, partition_id, ts)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- indexes
CREATE TABLE if not exists milvus_meta.`indexes` (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    field_id BIGINT NOT NULL,
    collection_id BIGINT NOT NULL,
    index_id BIGINT NOT NULL,
    index_name VARCHAR(256),
    index_params VARCHAR(2048),
    user_index_params VARCHAR(2048),
    is_auto_index BOOL DEFAULT FALSE,
    create_time bigint unsigned,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    INDEX idx_tenant_id_collection_id_index_id (tenant_id, collection_id, index_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- index file paths
CREATE TABLE if not exists milvus_meta.index_file_paths (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    index_build_id BIGINT NOT NULL,
    index_file_path VARCHAR(256),
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    INDEX idx_tenant_id_index_build_id (tenant_id, index_build_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- segments
CREATE TABLE if not exists milvus_meta.segments (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    segment_id BIGINT NOT NULL,
    collection_id BIGINT NOT NULL,
    partition_id BIGINT NOT NULL,
    num_rows BIGINT NOT NULL,
    max_row_num INT COMMENT 'estimate max rows',
    dm_channel VARCHAR(128) NOT NULL,
    dml_position TEXT COMMENT 'checkpoint',
    start_position TEXT,
    compaction_from VARCHAR(4096) COMMENT 'old segment IDs',
    created_by_compaction BOOL,
    segment_state TINYINT UNSIGNED NOT NULL,
    last_expire_time bigint unsigned COMMENT 'segment assignment expiration time',
    dropped_at bigint unsigned,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    INDEX idx_tenant_id_collection_id_segment_id (tenant_id, collection_id, segment_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- segment indexes
CREATE TABLE if not exists milvus_meta.segment_indexes (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    collection_id BIGINT NOT NULL,
    partition_id BIGINT NOT NULL,
    segment_id BIGINT NOT NULL,
    field_id BIGINT NOT NULL,
    index_id BIGINT NOT NULL,
    index_build_id BIGINT,
    enable_index BOOL NOT NULL,
    create_time bigint unsigned,
    index_file_paths VARCHAR(4096),
    index_size BIGINT UNSIGNED,
    `version` INT UNSIGNED,
    is_deleted BOOL DEFAULT FALSE COMMENT 'as mark_deleted',
    recycled BOOL DEFAULT FALSE COMMENT 'binlog files truly deleted',
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    UNIQUE KEY uk_tenant_id_segment_id_index_id (tenant_id, segment_id, index_id),
    INDEX idx_tenant_id_collection_id_segment_id_index_id (tenant_id, collection_id, segment_id, index_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- binlog files info
CREATE TABLE if not exists milvus_meta.binlogs (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    field_id BIGINT NOT NULL,
    segment_id BIGINT NOT NULL,
    collection_id BIGINT NOT NULL,
    log_type SMALLINT UNSIGNED NOT NULL COMMENT 'binlog、stats binlog、delta binlog',
    num_entries BIGINT,
    timestamp_from BIGINT UNSIGNED,
    timestamp_to BIGINT UNSIGNED,
    log_path VARCHAR(256) NOT NULL,
    log_size BIGINT NOT NULL,
    is_deleted BOOL DEFAULT FALSE,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    INDEX idx_tenant_id_segment_id_log_type (tenant_id, segment_id, log_type)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- users
CREATE TABLE if not exists milvus_meta.credential_users (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    username VARCHAR(128) NOT NULL,
    encrypted_password VARCHAR(256) NOT NULL,
    is_super BOOL NOT NULL DEFAULT false,
    is_deleted BOOL NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    PRIMARY KEY (id),
    INDEX idx_tenant_id_username (tenant_id, username)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- role
CREATE TABLE if not exists milvus_meta.role (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    name VARCHAR(128) NOT NULL,
    is_deleted BOOL NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    INDEX idx_role_tenant_name (tenant_id, name, is_deleted),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- user-role
CREATE TABLE if not exists milvus_meta.user_role (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    user_id     BIGINT NOT NULL,
    role_id     BIGINT NOT NULL,
    is_deleted BOOL NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    INDEX idx_role_mapping_tenant_user_role (tenant_id, user_id, role_id, is_deleted),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- grant
CREATE TABLE if not exists milvus_meta.grant (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    role_id     BIGINT NOT NULL,
    object VARCHAR(128) NOT NULL,
    object_name VARCHAR(128) NOT NULL,
    is_deleted BOOL NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    INDEX idx_grant_principal_resource_tenant (tenant_id, role_id, object, object_name, is_deleted),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;

-- grant-id
CREATE TABLE if not exists milvus_meta.grant_id (
    id     BIGINT NOT NULL AUTO_INCREMENT,
    tenant_id VARCHAR(128) DEFAULT NULL,
    grant_id     BIGINT NOT NULL,
    grantor_id     BIGINT NOT NULL,
    privilege VARCHAR(128) NOT NULL,
    is_deleted BOOL NOT NULL DEFAULT false,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP on update current_timestamp,
    INDEX idx_grant_id_tenant_grantor (tenant_id, grant_id, grantor_id, is_deleted),
    PRIMARY KEY (id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4;