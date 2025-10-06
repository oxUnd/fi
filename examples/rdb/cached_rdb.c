#include "cached_rdb.h"
#include <errno.h>

/* Forward declarations */
static int cached_rdb_cache_table_metadata(cached_rdb_t *cached_rdb, const char *table_name, rdb_table_t *table);
static int cached_rdb_cache_row_data(cached_rdb_t *cached_rdb, const char *table_name, uint64_t row_id, rdb_row_t *row);
static int cached_rdb_cache_query_result(cached_rdb_t *cached_rdb, const char *sql, fi_array *result);
static fi_array* cached_rdb_get_cached_query_result(cached_rdb_t *cached_rdb, const char *sql);
static void cached_rdb_update_stats(cached_rdb_t *cached_rdb, bool is_hit, uint64_t query_time_us);

/* Hash function for cached RDB keys */
uint32_t cached_rdb_hash_key(const cached_rdb_key_t *key) {
    if (!key) return 0;
    
    uint32_t hash = 0x811c9dc5;  /* FNV offset basis */
    
    /* Hash key type */
    hash ^= key->type;
    hash *= 0x01000193;
    
    /* Hash table name */
    const char *table_name = key->table_name;
    while (*table_name) {
        hash ^= *table_name++;
        hash *= 0x01000193;
    }
    
    /* Hash row ID */
    hash ^= key->row_id;
    hash *= 0x01000193;
    
    /* Hash index name */
    const char *index_name = key->index_name;
    while (*index_name) {
        hash ^= *index_name++;
        hash *= 0x01000193;
    }
    
    /* Hash query hash */
    hash ^= key->query_hash;
    hash *= 0x01000193;
    
    return hash;
}

/* Compare cached RDB keys */
int cached_rdb_compare_keys(const cached_rdb_key_t *key1, const cached_rdb_key_t *key2) {
    if (!key1 || !key2) return (key1 == key2) ? 0 : -1;
    
    /* Compare key type */
    if (key1->type != key2->type) return key1->type - key2->type;
    
    /* Compare table name */
    int table_cmp = strcmp(key1->table_name, key2->table_name);
    if (table_cmp != 0) return table_cmp;
    
    /* Compare row ID */
    if (key1->row_id != key2->row_id) return (key1->row_id < key2->row_id) ? -1 : 1;
    
    /* Compare index name */
    int index_cmp = strcmp(key1->index_name, key2->index_name);
    if (index_cmp != 0) return index_cmp;
    
    /* Compare query hash */
    if (key1->query_hash != key2->query_hash) return (key1->query_hash < key2->query_hash) ? -1 : 1;
    
    return 0;
}

/* Create cached RDB key */
cached_rdb_key_t* cached_rdb_create_key(cached_rdb_key_type_t type, const char *table_name,
                                        uint64_t row_id, const char *index_name, uint32_t query_hash) {
    cached_rdb_key_t *key = malloc(sizeof(cached_rdb_key_t));
    if (!key) return NULL;
    
    key->type = type;
    key->row_id = row_id;
    key->query_hash = query_hash;
    
    /* Copy table name */
    if (table_name) {
        strncpy(key->table_name, table_name, sizeof(key->table_name) - 1);
        key->table_name[sizeof(key->table_name) - 1] = '\0';
    } else {
        key->table_name[0] = '\0';
    }
    
    /* Copy index name */
    if (index_name) {
        strncpy(key->index_name, index_name, sizeof(key->index_name) - 1);
        key->index_name[sizeof(key->index_name) - 1] = '\0';
    } else {
        key->index_name[0] = '\0';
    }
    
    /* Calculate checksum */
    key->checksum = cached_rdb_hash_key(key);
    
    return key;
}

/* Free cached RDB key */
void cached_rdb_free_key(cached_rdb_key_t *key) {
    if (key) free(key);
}

/* Get default configuration */
cached_rdb_config_t* cached_rdb_get_default_config(void) {
    cached_rdb_config_t *config = malloc(sizeof(cached_rdb_config_t));
    if (!config) return NULL;
    
    /* Set default cache levels */
    config->cache_levels = CACHED_RDB_DEFAULT_CACHE_LEVELS;
    
    /* Create default cache configurations */
    config->cache_configs = malloc(sizeof(rdb_cache_level_config_t) * config->cache_levels);
    if (!config->cache_configs) {
        free(config);
        return NULL;
    }
    
    /* Level 0: Fast memory cache with W-TinyLFU */
    config->cache_configs[0].level = 0;
    config->cache_configs[0].max_size = CACHED_RDB_DEFAULT_CACHE_SIZE / 2;
    config->cache_configs[0].max_entries = 10000;
    config->cache_configs[0].algorithm = RDB_CACHE_W_TINY_LFU;
    config->cache_configs[0].is_memory = true;
    config->cache_configs[0].hit_ratio_threshold = 0.8;
    config->cache_configs[0].write_buffer_size = 0;
    
    /* Level 1: Slower memory cache with AURA */
    config->cache_configs[1].level = 1;
    config->cache_configs[1].max_size = CACHED_RDB_DEFAULT_CACHE_SIZE / 2;
    config->cache_configs[1].max_entries = 5000;
    config->cache_configs[1].algorithm = RDB_CACHE_AURA;
    config->cache_configs[1].is_memory = true;
    config->cache_configs[1].hit_ratio_threshold = 0.7;
    config->cache_configs[1].write_buffer_size = 0;
    
    /* Set persistence configuration */
    config->persistence_mode = CACHED_RDB_DEFAULT_PERSISTENCE_MODE;
    config->persistence_dir = malloc(strlen(RDB_PERSISTENCE_DEFAULT_DIR) + 1);
    if (!config->persistence_dir) {
        free(config->cache_configs);
        free(config);
        return NULL;
    }
    strcpy(config->persistence_dir, RDB_PERSISTENCE_DEFAULT_DIR);
    
    /* Set query cache configuration */
    config->enable_query_cache = true;
    config->query_cache_size = 1024 * 1024;  /* 1MB query cache */
    
    /* Set auto-tuning configuration */
    config->enable_auto_tuning = true;
    config->target_hit_ratio = 0.85;  /* 85% target hit ratio */
    config->tune_interval = 300;       /* 5 minutes */
    
    return config;
}

/* Free configuration */
void cached_rdb_free_config(cached_rdb_config_t *config) {
    if (!config) return;
    
    if (config->cache_configs) free(config->cache_configs);
    if (config->persistence_dir) free(config->persistence_dir);
    free(config);
}

/* Create cached RDB instance */
cached_rdb_t* cached_rdb_create(const char *name, const cached_rdb_config_t *config) {
    if (!name) return NULL;
    
    cached_rdb_t *cached_rdb = malloc(sizeof(cached_rdb_t));
    if (!cached_rdb) return NULL;
    
    /* Create underlying RDB database */
    cached_rdb->db = rdb_create_database(name);
    if (!cached_rdb->db) {
        free(cached_rdb);
        return NULL;
    }
    
    /* Copy configuration */
    if (config) {
        memcpy(&cached_rdb->config, config, sizeof(cached_rdb_config_t));
        
        /* Deep copy persistence directory */
        if (config->persistence_dir) {
            cached_rdb->config.persistence_dir = malloc(strlen(config->persistence_dir) + 1);
            if (!cached_rdb->config.persistence_dir) {
                rdb_destroy_database(cached_rdb->db);
                free(cached_rdb);
                return NULL;
            }
            strcpy(cached_rdb->config.persistence_dir, config->persistence_dir);
        }
        
        /* Deep copy cache configurations */
        cached_rdb->config.cache_configs = malloc(sizeof(rdb_cache_level_config_t) * config->cache_levels);
        if (!cached_rdb->config.cache_configs) {
            if (cached_rdb->config.persistence_dir) free(cached_rdb->config.persistence_dir);
            rdb_destroy_database(cached_rdb->db);
            free(cached_rdb);
            return NULL;
        }
        memcpy(cached_rdb->config.cache_configs, config->cache_configs, 
               sizeof(rdb_cache_level_config_t) * config->cache_levels);
    } else {
        /* Use default configuration */
        cached_rdb_config_t *default_config = cached_rdb_get_default_config();
        if (!default_config) {
            rdb_destroy_database(cached_rdb->db);
            free(cached_rdb);
            return NULL;
        }
        memcpy(&cached_rdb->config, default_config, sizeof(cached_rdb_config_t));
        free(default_config);
    }
    
    /* Initialize cache system */
    cached_rdb->cache_system = rdb_cache_system_create(name, cached_rdb->config.cache_levels,
                                                      cached_rdb->config.cache_configs);
    if (!cached_rdb->cache_system) {
        if (cached_rdb->config.persistence_dir) free(cached_rdb->config.persistence_dir);
        if (cached_rdb->config.cache_configs) free(cached_rdb->config.cache_configs);
        rdb_destroy_database(cached_rdb->db);
        free(cached_rdb);
        return NULL;
    }
    
    /* Initialize persistence manager */
    cached_rdb->persistence_manager = rdb_persistence_create(cached_rdb->config.persistence_dir,
                                                           cached_rdb->config.persistence_mode);
    if (!cached_rdb->persistence_manager) {
        rdb_cache_system_destroy(cached_rdb->cache_system);
        if (cached_rdb->config.persistence_dir) free(cached_rdb->config.persistence_dir);
        if (cached_rdb->config.cache_configs) free(cached_rdb->config.cache_configs);
        rdb_destroy_database(cached_rdb->db);
        free(cached_rdb);
        return NULL;
    }
    
    /* Initialize query cache */
    if (cached_rdb->config.enable_query_cache) {
        cached_rdb->query_cache = fi_map_create_string_ptr(100);
        if (!cached_rdb->query_cache) {
            rdb_persistence_destroy(cached_rdb->persistence_manager);
            rdb_cache_system_destroy(cached_rdb->cache_system);
            if (cached_rdb->config.persistence_dir) free(cached_rdb->config.persistence_dir);
            if (cached_rdb->config.cache_configs) free(cached_rdb->config.cache_configs);
            rdb_destroy_database(cached_rdb->db);
            free(cached_rdb);
            return NULL;
        }
    } else {
        cached_rdb->query_cache = NULL;
    }
    
    /* Initialize statistics */
    memset(&cached_rdb->stats, 0, sizeof(cached_rdb_stats_t));
    cached_rdb->stats.last_reset = time(NULL);
    
    /* Initialize auto-tuning */
    cached_rdb->auto_tuning_enabled = cached_rdb->config.enable_auto_tuning;
    cached_rdb->last_tune_time = time(NULL);
    cached_rdb->tuning_thread_running = false;
    
    /* Initialize thread safety */
    if (pthread_rwlock_init(&cached_rdb->rwlock, NULL) != 0) {
        if (cached_rdb->query_cache) fi_map_destroy(cached_rdb->query_cache);
        rdb_persistence_destroy(cached_rdb->persistence_manager);
        rdb_cache_system_destroy(cached_rdb->cache_system);
        if (cached_rdb->config.persistence_dir) free(cached_rdb->config.persistence_dir);
        if (cached_rdb->config.cache_configs) free(cached_rdb->config.cache_configs);
        rdb_destroy_database(cached_rdb->db);
        free(cached_rdb);
        return NULL;
    }
    
    if (pthread_mutex_init(&cached_rdb->stats_mutex, NULL) != 0) {
        pthread_rwlock_destroy(&cached_rdb->rwlock);
        if (cached_rdb->query_cache) fi_map_destroy(cached_rdb->query_cache);
        rdb_persistence_destroy(cached_rdb->persistence_manager);
        rdb_cache_system_destroy(cached_rdb->cache_system);
        if (cached_rdb->config.persistence_dir) free(cached_rdb->config.persistence_dir);
        if (cached_rdb->config.cache_configs) free(cached_rdb->config.cache_configs);
        rdb_destroy_database(cached_rdb->db);
        free(cached_rdb);
        return NULL;
    }
    
    if (pthread_mutex_init(&cached_rdb->config_mutex, NULL) != 0) {
        pthread_mutex_destroy(&cached_rdb->stats_mutex);
        pthread_rwlock_destroy(&cached_rdb->rwlock);
        if (cached_rdb->query_cache) fi_map_destroy(cached_rdb->query_cache);
        rdb_persistence_destroy(cached_rdb->persistence_manager);
        rdb_cache_system_destroy(cached_rdb->cache_system);
        if (cached_rdb->config.persistence_dir) free(cached_rdb->config.persistence_dir);
        if (cached_rdb->config.cache_configs) free(cached_rdb->config.cache_configs);
        rdb_destroy_database(cached_rdb->db);
        free(cached_rdb);
        return NULL;
    }
    
    return cached_rdb;
}

/* Destroy cached RDB instance */
void cached_rdb_destroy(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return;
    
    /* Stop auto-tuning thread if running */
    if (cached_rdb->tuning_thread_running) {
        cached_rdb_stop_auto_tuning(cached_rdb);
    }
    
    /* Clean up query cache */
    if (cached_rdb->query_cache) {
        fi_map_destroy(cached_rdb->query_cache);
    }
    
    /* Clean up persistence manager */
    if (cached_rdb->persistence_manager) {
        rdb_persistence_destroy(cached_rdb->persistence_manager);
    }
    
    /* Clean up cache system */
    if (cached_rdb->cache_system) {
        rdb_cache_system_destroy(cached_rdb->cache_system);
    }
    
    /* Clean up underlying database */
    if (cached_rdb->db) {
        rdb_destroy_database(cached_rdb->db);
    }
    
    /* Clean up configuration */
    if (cached_rdb->config.persistence_dir) {
        free(cached_rdb->config.persistence_dir);
    }
    if (cached_rdb->config.cache_configs) {
        free(cached_rdb->config.cache_configs);
    }
    
    /* Clean up thread safety */
    pthread_rwlock_destroy(&cached_rdb->rwlock);
    pthread_mutex_destroy(&cached_rdb->stats_mutex);
    pthread_mutex_destroy(&cached_rdb->config_mutex);
    
    free(cached_rdb);
}

/* Initialize cached RDB */
int cached_rdb_init(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return -1;
    
    /* Initialize persistence manager */
    if (rdb_persistence_init(cached_rdb->persistence_manager) != 0) {
        return -1;
    }
    
    /* Start auto-tuning if enabled */
    if (cached_rdb->auto_tuning_enabled) {
        if (cached_rdb_start_auto_tuning(cached_rdb) != 0) {
            /* Auto-tuning failure is not critical */
        }
    }
    
    return 0;
}

/* Shutdown cached RDB */
int cached_rdb_shutdown(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return -1;
    
    /* Stop auto-tuning */
    if (cached_rdb->tuning_thread_running) {
        cached_rdb_stop_auto_tuning(cached_rdb);
    }
    
    /* Shutdown persistence manager */
    if (cached_rdb->persistence_manager) {
        rdb_persistence_shutdown(cached_rdb->persistence_manager);
    }
    
    return 0;
}

/* Open cached RDB */
int cached_rdb_open(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return -1;
    
    pthread_rwlock_wrlock(&cached_rdb->rwlock);
    
    /* Open underlying database */
    int result = rdb_open_database(cached_rdb->db);
    if (result != 0) {
        pthread_rwlock_unlock(&cached_rdb->rwlock);
        return result;
    }
    
    /* Load database from persistence */
    if (rdb_persistence_open_database(cached_rdb->persistence_manager, cached_rdb->db) != 0) {
        rdb_close_database(cached_rdb->db);
        pthread_rwlock_unlock(&cached_rdb->rwlock);
        return -1;
    }
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return 0;
}

/* Close cached RDB */
int cached_rdb_close(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return -1;
    
    pthread_rwlock_wrlock(&cached_rdb->rwlock);
    
    /* Save database to persistence */
    if (rdb_persistence_close_database(cached_rdb->persistence_manager, cached_rdb->db) != 0) {
        /* Log error but continue */
    }
    
    /* Close underlying database */
    int result = rdb_close_database(cached_rdb->db);
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return result;
}

/* Check if cached RDB is open */
bool cached_rdb_is_open(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return false;
    
    pthread_rwlock_rdlock(&cached_rdb->rwlock);
    bool is_open = cached_rdb->db && cached_rdb->db->is_open;
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    
    return is_open;
}

/* Create table with caching */
int cached_rdb_create_table(cached_rdb_t *cached_rdb, const char *table_name, fi_array *columns) {
    if (!cached_rdb || !table_name || !columns) return -1;
    
    pthread_rwlock_wrlock(&cached_rdb->rwlock);
    
    /* Create table in underlying database */
    int result = rdb_create_table(cached_rdb->db, table_name, columns);
    if (result != 0) {
        pthread_rwlock_unlock(&cached_rdb->rwlock);
        return result;
    }
    
    /* Cache table metadata */
    rdb_table_t *table = rdb_get_table(cached_rdb->db, table_name);
    if (table) {
        cached_rdb_cache_table_metadata(cached_rdb, table_name, table);
    }
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return 0;
}

/* Drop table with cache invalidation */
int cached_rdb_drop_table(cached_rdb_t *cached_rdb, const char *table_name) {
    if (!cached_rdb || !table_name) return -1;
    
    pthread_rwlock_wrlock(&cached_rdb->rwlock);
    
    /* Remove table from cache */
    cached_rdb_cache_remove(cached_rdb, CACHED_RDB_KEY_TABLE, table_name, 0, NULL);
    
    /* Drop table from underlying database */
    int result = rdb_drop_table(cached_rdb->db, table_name);
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return result;
}

/* Get table with caching */
rdb_table_t* cached_rdb_get_table(cached_rdb_t *cached_rdb, const char *table_name) {
    if (!cached_rdb || !table_name) return NULL;
    
    pthread_rwlock_rdlock(&cached_rdb->rwlock);
    
    /* Try to get from cache first */
    void *cached_data = NULL;
    size_t cached_size = 0;
    
    if (cached_rdb_cache_get(cached_rdb, CACHED_RDB_KEY_TABLE, table_name, 0, NULL,
                             &cached_data, &cached_size) == 0) {
        /* Table found in cache */
        pthread_rwlock_unlock(&cached_rdb->rwlock);
        return (rdb_table_t*)cached_data;
    }
    
    /* Get from underlying database */
    rdb_table_t *table = rdb_get_table(cached_rdb->db, table_name);
    if (table) {
        /* Cache the table */
        cached_rdb_cache_table_metadata(cached_rdb, table_name, table);
    }
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return table;
}

/* Check if table exists */
bool cached_rdb_table_exists(cached_rdb_t *cached_rdb, const char *table_name) {
    if (!cached_rdb || !table_name) return false;
    
    pthread_rwlock_rdlock(&cached_rdb->rwlock);
    bool exists = rdb_table_exists(cached_rdb->db, table_name);
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    
    return exists;
}

/* Insert row with caching */
int cached_rdb_insert_row(cached_rdb_t *cached_rdb, const char *table_name, fi_array *values) {
    if (!cached_rdb || !table_name || !values) return -1;
    
    pthread_rwlock_wrlock(&cached_rdb->rwlock);
    
    /* Insert row in underlying database */
    int result = rdb_insert_row(cached_rdb->db, table_name, values);
    if (result != 0) {
        pthread_rwlock_unlock(&cached_rdb->rwlock);
        return result;
    }
    
    /* Get the inserted row */
    rdb_table_t *table = rdb_get_table(cached_rdb->db, table_name);
    if (table && fi_array_count(table->rows) > 0) {
        rdb_row_t **last_row_ptr = (rdb_row_t**)fi_array_get(table->rows, fi_array_count(table->rows) - 1);
        if (last_row_ptr && *last_row_ptr) {
            /* Cache the row data */
            cached_rdb_cache_row_data(cached_rdb, table_name, (*last_row_ptr)->row_id, *last_row_ptr);
        }
    }
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return 0;
}

/* Select rows with caching */
fi_array* cached_rdb_select_rows(cached_rdb_t *cached_rdb, const char *table_name, fi_array *columns, 
                                fi_array *where_conditions) {
    if (!cached_rdb || !table_name) return NULL;
    
    pthread_rwlock_rdlock(&cached_rdb->rwlock);
    
    /* Select rows from underlying database */
    fi_array *result = rdb_select_rows(cached_rdb->db, table_name, columns, where_conditions);
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return result;
}

/* Cache table metadata */
static int cached_rdb_cache_table_metadata(cached_rdb_t *cached_rdb, const char *table_name, rdb_table_t *table) {
    if (!cached_rdb || !table_name || !table) return -1;
    
    /* Serialize table metadata */
    void *data = NULL;
    size_t data_size = 0;
    
    /* For simplicity, we'll just cache the table pointer */
    /* In a real implementation, this would serialize the table structure */
    data = table;
    data_size = sizeof(rdb_table_t*);
    
    /* Cache the data */
    return cached_rdb_cache_put(cached_rdb, CACHED_RDB_KEY_TABLE, table_name, 0, NULL, data, data_size);
}

/* Cache row data */
static int cached_rdb_cache_row_data(cached_rdb_t *cached_rdb, const char *table_name, uint64_t row_id, rdb_row_t *row) {
    if (!cached_rdb || !table_name || !row) return -1;
    
    /* Serialize row data */
    void *data = NULL;
    size_t data_size = 0;
    
    /* For simplicity, we'll just cache the row pointer */
    /* In a real implementation, this would serialize the row structure */
    data = row;
    data_size = sizeof(rdb_row_t*);
    
    /* Cache the data */
    return cached_rdb_cache_put(cached_rdb, CACHED_RDB_KEY_ROW, table_name, row_id, NULL, data, data_size);
}

/* Cache put operation */
int cached_rdb_cache_put(cached_rdb_t *cached_rdb, cached_rdb_key_type_t type, const char *table_name,
                        uint64_t row_id, const char *index_name, const void *data, size_t data_size) {
    if (!cached_rdb || !data) return -1;
    
    /* Create cache key */
    cached_rdb_key_t *key = cached_rdb_create_key(type, table_name, row_id, index_name, 0);
    if (!key) return -1;
    
    /* Put in cache system */
    int result = rdb_cache_put(cached_rdb->cache_system, key, sizeof(cached_rdb_key_t), 
                              data, data_size, false);
    
    cached_rdb_free_key(key);
    return result;
}

/* Cache get operation */
int cached_rdb_cache_get(cached_rdb_t *cached_rdb, cached_rdb_key_type_t type, const char *table_name,
                        uint64_t row_id, const char *index_name, void **data, size_t *data_size) {
    if (!cached_rdb || !data || !data_size) return -1;
    
    /* Create cache key */
    cached_rdb_key_t *key = cached_rdb_create_key(type, table_name, row_id, index_name, 0);
    if (!key) return -1;
    
    /* Get from cache system */
    int result = rdb_cache_get(cached_rdb->cache_system, key, sizeof(cached_rdb_key_t), 
                              data, data_size);
    
    cached_rdb_free_key(key);
    return result;
}

/* Cache remove operation */
int cached_rdb_cache_remove(cached_rdb_t *cached_rdb, cached_rdb_key_type_t type, const char *table_name,
                           uint64_t row_id, const char *index_name) {
    if (!cached_rdb) return -1;
    
    /* Create cache key */
    cached_rdb_key_t *key = cached_rdb_create_key(type, table_name, row_id, index_name, 0);
    if (!key) return -1;
    
    /* Remove from cache system */
    int result = rdb_cache_remove(cached_rdb->cache_system, key, sizeof(cached_rdb_key_t));
    
    cached_rdb_free_key(key);
    return result;
}

/* Update statistics */
static void cached_rdb_update_stats(cached_rdb_t *cached_rdb, bool is_hit, uint64_t query_time_us) {
    pthread_mutex_lock(&cached_rdb->stats_mutex);
    
    if (is_hit) {
        cached_rdb->stats.cache_hits++;
    } else {
        cached_rdb->stats.cache_misses++;
    }
    
    /* Update hit ratio */
    uint64_t total_requests = cached_rdb->stats.cache_hits + cached_rdb->stats.cache_misses;
    if (total_requests > 0) {
        cached_rdb->stats.cache_hit_ratio = (double)cached_rdb->stats.cache_hits / total_requests;
    }
    
    /* Update average query time */
    cached_rdb->stats.average_query_time = (cached_rdb->stats.average_query_time + query_time_us) / 2;
    
    pthread_mutex_unlock(&cached_rdb->stats_mutex);
}

/* Print cached RDB statistics */
void cached_rdb_print_stats(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return;
    
    printf("=== Cached RDB Statistics ===\n");
    printf("Cache Hits: %lu\n", cached_rdb->stats.cache_hits);
    printf("Cache Misses: %lu\n", cached_rdb->stats.cache_misses);
    printf("Cache Hit Ratio: %.2f%%\n", cached_rdb->stats.cache_hit_ratio * 100);
    printf("Disk Reads: %lu\n", cached_rdb->stats.disk_reads);
    printf("Disk Writes: %lu\n", cached_rdb->stats.disk_writes);
    printf("Cache Evictions: %lu\n", cached_rdb->stats.cache_evictions);
    printf("Persistence Operations: %lu\n", cached_rdb->stats.persistence_operations);
    printf("Average Query Time: %.2f μs\n", cached_rdb->stats.average_query_time);
    
    /* Print cache system statistics */
    printf("\n");
    rdb_cache_print_stats(cached_rdb->cache_system);
    
    /* Print persistence statistics */
    printf("\n");
    rdb_persistence_print_stats(cached_rdb->persistence_manager);
}

/* Get cached RDB statistics */
cached_rdb_stats_t* cached_rdb_get_stats(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return NULL;
    
    pthread_mutex_lock(&cached_rdb->stats_mutex);
    cached_rdb_stats_t *stats = malloc(sizeof(cached_rdb_stats_t));
    if (stats) {
        memcpy(stats, &cached_rdb->stats, sizeof(cached_rdb_stats_t));
    }
    pthread_mutex_unlock(&cached_rdb->stats_mutex);
    
    return stats;
}

/* Start auto-tuning */
int cached_rdb_start_auto_tuning(cached_rdb_t *cached_rdb) {
    if (!cached_rdb || cached_rdb->tuning_thread_running) return -1;
    
    cached_rdb->tuning_thread_running = true;
    
    if (pthread_create(&cached_rdb->tuning_thread, NULL, cached_rdb_auto_tuning_thread, cached_rdb) != 0) {
        cached_rdb->tuning_thread_running = false;
        return -1;
    }
    
    return 0;
}

/* Stop auto-tuning */
int cached_rdb_stop_auto_tuning(cached_rdb_t *cached_rdb) {
    if (!cached_rdb || !cached_rdb->tuning_thread_running) return -1;
    
    cached_rdb->tuning_thread_running = false;
    
    if (pthread_join(cached_rdb->tuning_thread, NULL) != 0) {
        return -1;
    }
    
    return 0;
}

/* Auto-tuning thread function */
void* cached_rdb_auto_tuning_thread(void *arg) {
    cached_rdb_t *cached_rdb = (cached_rdb_t*)arg;
    
    while (cached_rdb->tuning_thread_running) {
        /* Sleep for tune interval */
        sleep(cached_rdb->config.tune_interval);
        
        if (!cached_rdb->tuning_thread_running) break;
        
        /* Check if we need to tune */
        time_t current_time = time(NULL);
        if (current_time - cached_rdb->last_tune_time >= cached_rdb->config.tune_interval) {
            /* Perform auto-tuning */
            rdb_cache_tune(cached_rdb->cache_system);
            cached_rdb->last_tune_time = current_time;
        }
    }
    
    return NULL;
}

/* Save database */
int cached_rdb_save(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return -1;
    
    pthread_rwlock_wrlock(&cached_rdb->rwlock);
    
    /* Save database to persistence */
    int result = rdb_persistence_save_database(cached_rdb->persistence_manager, cached_rdb->db);
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return result;
}

/* Load database */
int cached_rdb_load(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return -1;
    
    pthread_rwlock_wrlock(&cached_rdb->rwlock);
    
    /* Load database from persistence */
    int result = rdb_persistence_load_database(cached_rdb->persistence_manager, cached_rdb->db);
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return result;
}

/* Checkpoint database */
int cached_rdb_checkpoint(cached_rdb_t *cached_rdb) {
    if (!cached_rdb) return -1;
    
    pthread_rwlock_wrlock(&cached_rdb->rwlock);
    
    /* Force checkpoint */
    int result = rdb_persistence_force_checkpoint(cached_rdb->persistence_manager, cached_rdb->db);
    
    pthread_rwlock_unlock(&cached_rdb->rwlock);
    return result;
}

/* Set cache algorithm */
int cached_rdb_set_cache_algorithm(cached_rdb_t *cached_rdb, size_t level, rdb_cache_algorithm_t algorithm) {
    if (!cached_rdb || level >= cached_rdb->config.cache_levels) return -1;
    
    pthread_mutex_lock(&cached_rdb->config_mutex);
    int result = rdb_cache_set_algorithm(cached_rdb->cache_system, level, algorithm);
    pthread_mutex_unlock(&cached_rdb->config_mutex);
    
    return result;
}

/* Set auto-tuning */
int cached_rdb_set_auto_tuning(cached_rdb_t *cached_rdb, bool enable, double target_ratio) {
    if (!cached_rdb || target_ratio < 0.0 || target_ratio > 1.0) return -1;
    
    pthread_mutex_lock(&cached_rdb->config_mutex);
    cached_rdb->auto_tuning_enabled = enable;
    cached_rdb->config.enable_auto_tuning = enable;
    cached_rdb->config.target_hit_ratio = target_ratio;
    
    /* Update cache system auto-tuning */
    rdb_cache_set_auto_tune(cached_rdb->cache_system, enable, target_ratio);
    
    pthread_mutex_unlock(&cached_rdb->config_mutex);
    
    return 0;
}

/* Thread-safe operations */
int cached_rdb_insert_row_thread_safe(cached_rdb_t *cached_rdb, const char *table_name, fi_array *values) {
    return cached_rdb_insert_row(cached_rdb, table_name, values);
}

fi_array* cached_rdb_select_rows_thread_safe(cached_rdb_t *cached_rdb, const char *table_name, fi_array *columns, 
                                            fi_array *where_conditions) {
    return cached_rdb_select_rows(cached_rdb, table_name, columns, where_conditions);
}
