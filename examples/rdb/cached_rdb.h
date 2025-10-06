#ifndef __CACHED_RDB_H__
#define __CACHED_RDB_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>

/* Use mutex for thread safety (rwlock compatibility) */
#define pthread_rwlock_t pthread_mutex_t
#define pthread_rwlock_init(lock, attr) pthread_mutex_init(lock, attr)
#define pthread_rwlock_destroy(lock) pthread_mutex_destroy(lock)
#define pthread_rwlock_rdlock(lock) pthread_mutex_lock(lock)
#define pthread_rwlock_wrlock(lock) pthread_mutex_lock(lock)
#define pthread_rwlock_unlock(lock) pthread_mutex_unlock(lock)

/* Include FI data structures */
#include "../../src/include/fi.h"
#include "../../src/include/fi_map.h"
#include "../../src/include/fi_btree.h"

/* Include RDB structures */
#include "rdb.h"
#include "cache_system.h"
#include "persistence.h"

/* Cached RDB configuration */
#define CACHED_RDB_DEFAULT_CACHE_LEVELS 2
#define CACHED_RDB_DEFAULT_CACHE_SIZE 64 * 1024 * 1024  /* 64MB default cache */
#define CACHED_RDB_DEFAULT_PERSISTENCE_MODE RDB_PERSISTENCE_FULL

/* Cache key types for different RDB operations */
typedef enum {
    CACHED_RDB_KEY_TABLE = 1,    /* Table metadata cache key */
    CACHED_RDB_KEY_ROW,          /* Row data cache key */
    CACHED_RDB_KEY_INDEX,        /* Index cache key */
    CACHED_RDB_KEY_QUERY         /* Query result cache key */
} cached_rdb_key_type_t;

/* Cache key structure */
typedef struct {
    cached_rdb_key_type_t type;  /* Key type */
    char table_name[64];         /* Table name */
    uint64_t row_id;             /* Row ID (for row keys) */
    char index_name[64];         /* Index name (for index keys) */
    uint32_t query_hash;         /* Query hash (for query keys) */
    uint32_t checksum;           /* Key checksum */
} cached_rdb_key_t;

/* Cached RDB statistics */
typedef struct {
    uint64_t cache_hits;         /* Cache hits */
    uint64_t cache_misses;       /* Cache misses */
    uint64_t disk_reads;         /* Disk read operations */
    uint64_t disk_writes;        /* Disk write operations */
    uint64_t cache_evictions;    /* Cache evictions */
    uint64_t persistence_operations; /* Persistence operations */
    double cache_hit_ratio;      /* Cache hit ratio */
    double average_query_time;   /* Average query time in microseconds */
    time_t last_reset;           /* Last statistics reset time */
} cached_rdb_stats_t;

/* Cached RDB configuration */
typedef struct {
    size_t cache_levels;         /* Number of cache levels */
    rdb_cache_level_config_t *cache_configs; /* Cache level configurations */
    rdb_persistence_mode_t persistence_mode; /* Persistence mode */
    char *persistence_dir;       /* Persistence directory */
    bool enable_query_cache;     /* Whether to enable query result caching */
    size_t query_cache_size;     /* Query cache size */
    bool enable_auto_tuning;     /* Whether to enable automatic cache tuning */
    double target_hit_ratio;     /* Target cache hit ratio */
    uint64_t tune_interval;      /* Auto-tuning interval in seconds */
} cached_rdb_config_t;

/* Cached RDB instance */
typedef struct {
    rdb_database_t *db;          /* Underlying RDB database */
    rdb_cache_system_t *cache_system; /* Multi-level cache system */
    rdb_persistence_manager_t *persistence_manager; /* Persistence manager */
    cached_rdb_config_t config;  /* Configuration */
    cached_rdb_stats_t stats;    /* Statistics */
    
    /* Query cache for frequently executed queries */
    fi_map *query_cache;         /* Map of query_hash -> result */
    
    /* Thread safety */
    pthread_rwlock_t rwlock;     /* Read-write lock for cached RDB operations */
    pthread_mutex_t stats_mutex; /* Mutex for statistics updates */
    pthread_mutex_t config_mutex; /* Mutex for configuration changes */
    
    /* Auto-tuning */
    bool auto_tuning_enabled;    /* Whether auto-tuning is enabled */
    time_t last_tune_time;       /* Last auto-tuning time */
    pthread_t tuning_thread;     /* Auto-tuning thread */
    bool tuning_thread_running;  /* Whether tuning thread is running */
} cached_rdb_t;

/* Cached RDB operations */
cached_rdb_t* cached_rdb_create(const char *name, const cached_rdb_config_t *config);
void cached_rdb_destroy(cached_rdb_t *cached_rdb);
int cached_rdb_init(cached_rdb_t *cached_rdb);
int cached_rdb_shutdown(cached_rdb_t *cached_rdb);

/* Database operations */
int cached_rdb_open(cached_rdb_t *cached_rdb);
int cached_rdb_close(cached_rdb_t *cached_rdb);
bool cached_rdb_is_open(cached_rdb_t *cached_rdb);

/* Table operations */
int cached_rdb_create_table(cached_rdb_t *cached_rdb, const char *table_name, fi_array *columns);
int cached_rdb_drop_table(cached_rdb_t *cached_rdb, const char *table_name);
rdb_table_t* cached_rdb_get_table(cached_rdb_t *cached_rdb, const char *table_name);
bool cached_rdb_table_exists(cached_rdb_t *cached_rdb, const char *table_name);

/* Row operations */
int cached_rdb_insert_row(cached_rdb_t *cached_rdb, const char *table_name, fi_array *values);
int cached_rdb_update_rows(cached_rdb_t *cached_rdb, const char *table_name, fi_array *set_columns, 
                          fi_array *set_values, fi_array *where_conditions);
int cached_rdb_delete_rows(cached_rdb_t *cached_rdb, const char *table_name, fi_array *where_conditions);
fi_array* cached_rdb_select_rows(cached_rdb_t *cached_rdb, const char *table_name, fi_array *columns, 
                                fi_array *where_conditions);

/* Index operations */
int cached_rdb_create_index(cached_rdb_t *cached_rdb, const char *table_name, const char *index_name, 
                           const char *column_name);
int cached_rdb_drop_index(cached_rdb_t *cached_rdb, const char *table_name, const char *index_name);

/* Query operations with caching */
fi_array* cached_rdb_select_rows_cached(cached_rdb_t *cached_rdb, const char *table_name, 
                                       fi_array *columns, fi_array *where_conditions);
int cached_rdb_execute_query(cached_rdb_t *cached_rdb, const char *sql, fi_array **result);

/* Transaction operations */
int cached_rdb_begin_transaction(cached_rdb_t *cached_rdb, rdb_isolation_level_t isolation);
int cached_rdb_commit_transaction(cached_rdb_t *cached_rdb);
int cached_rdb_rollback_transaction(cached_rdb_t *cached_rdb);
bool cached_rdb_is_in_transaction(cached_rdb_t *cached_rdb);

/* Cache operations */
int cached_rdb_cache_put(cached_rdb_t *cached_rdb, cached_rdb_key_type_t type, const char *table_name,
                        uint64_t row_id, const char *index_name, const void *data, size_t data_size);
int cached_rdb_cache_get(cached_rdb_t *cached_rdb, cached_rdb_key_type_t type, const char *table_name,
                        uint64_t row_id, const char *index_name, void **data, size_t *data_size);
int cached_rdb_cache_remove(cached_rdb_t *cached_rdb, cached_rdb_key_type_t type, const char *table_name,
                           uint64_t row_id, const char *index_name);
int cached_rdb_cache_clear(cached_rdb_t *cached_rdb);

/* Persistence operations */
int cached_rdb_save(cached_rdb_t *cached_rdb);
int cached_rdb_load(cached_rdb_t *cached_rdb);
int cached_rdb_checkpoint(cached_rdb_t *cached_rdb);

/* Configuration operations */
int cached_rdb_set_cache_levels(cached_rdb_t *cached_rdb, size_t levels);
int cached_rdb_set_cache_size(cached_rdb_t *cached_rdb, size_t level, size_t size);
int cached_rdb_set_cache_algorithm(cached_rdb_t *cached_rdb, size_t level, rdb_cache_algorithm_t algorithm);
int cached_rdb_set_persistence_mode(cached_rdb_t *cached_rdb, rdb_persistence_mode_t mode);
int cached_rdb_set_auto_tuning(cached_rdb_t *cached_rdb, bool enable, double target_ratio);

/* Statistics and monitoring */
void cached_rdb_print_stats(cached_rdb_t *cached_rdb);
void cached_rdb_print_cache_stats(cached_rdb_t *cached_rdb);
void cached_rdb_print_persistence_stats(cached_rdb_t *cached_rdb);
cached_rdb_stats_t* cached_rdb_get_stats(cached_rdb_t *cached_rdb);

/* Utility functions */
uint32_t cached_rdb_hash_key(const cached_rdb_key_t *key);
int cached_rdb_compare_keys(const cached_rdb_key_t *key1, const cached_rdb_key_t *key2);
cached_rdb_key_t* cached_rdb_create_key(cached_rdb_key_type_t type, const char *table_name,
                                        uint64_t row_id, const char *index_name, uint32_t query_hash);
void cached_rdb_free_key(cached_rdb_key_t *key);

/* Thread-safe operations */
int cached_rdb_insert_row_thread_safe(cached_rdb_t *cached_rdb, const char *table_name, fi_array *values);
int cached_rdb_update_rows_thread_safe(cached_rdb_t *cached_rdb, const char *table_name, fi_array *set_columns, 
                                      fi_array *set_values, fi_array *where_conditions);
int cached_rdb_delete_rows_thread_safe(cached_rdb_t *cached_rdb, const char *table_name, fi_array *where_conditions);
fi_array* cached_rdb_select_rows_thread_safe(cached_rdb_t *cached_rdb, const char *table_name, fi_array *columns, 
                                            fi_array *where_conditions);

/* Auto-tuning */
int cached_rdb_start_auto_tuning(cached_rdb_t *cached_rdb);
int cached_rdb_stop_auto_tuning(cached_rdb_t *cached_rdb);
void* cached_rdb_auto_tuning_thread(void *arg);

/* Default configuration */
cached_rdb_config_t* cached_rdb_get_default_config(void);
void cached_rdb_free_config(cached_rdb_config_t *config);

/* Performance optimization */
int cached_rdb_warmup_cache(cached_rdb_t *cached_rdb, const char *warmup_queries_file);
int cached_rdb_preload_table(cached_rdb_t *cached_rdb, const char *table_name);
int cached_rdb_optimize_cache(cached_rdb_t *cached_rdb);

#endif //__CACHED_RDB_H__
