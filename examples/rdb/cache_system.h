#ifndef __CACHE_SYSTEM_H__
#define __CACHE_SYSTEM_H__

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdbool.h>
#include <stdint.h>
#include <time.h>
#include <pthread.h>
#include <sys/mman.h>
#include <fcntl.h>
#include <unistd.h>

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

/* Cache configuration */
#define RDB_CACHE_MAX_LEVELS 8
#define RDB_CACHE_DEFAULT_LEVELS 2
#define RDB_CACHE_DEFAULT_SIZE 1024 * 1024  /* 1MB default cache size */
#define RDB_CACHE_MAX_SIZE 1024 * 1024 * 1024  /* 1GB max cache size */
#define RDB_CACHE_PAGE_SIZE 4096  /* 4KB page size */
#define RDB_CACHE_WRITE_BUFFER_SIZE 64 * 1024  /* 64KB write buffer */

/* Cache algorithm types */
typedef enum {
    RDB_CACHE_LRU = 1,           /* Least Recently Used */
    RDB_CACHE_LFU,               /* Least Frequently Used */
    RDB_CACHE_ARC,               /* Adaptive Replacement Cache */
    RDB_CACHE_W_TINY_LFU,        /* W-TinyLFU (2025 best practice) */
    RDB_CACHE_AURA               /* AURA algorithm for stability */
} rdb_cache_algorithm_t;

/* Cache level configuration */
typedef struct {
    size_t level;                /* Cache level (0 = fastest, N = slowest) */
    size_t max_size;             /* Maximum size in bytes */
    size_t max_entries;          /* Maximum number of entries */
    rdb_cache_algorithm_t algorithm; /* Eviction algorithm */
    bool is_memory;              /* true for memory, false for disk */
    double hit_ratio_threshold;  /* Hit ratio threshold for auto-tuning */
    size_t write_buffer_size;    /* Write buffer size for disk caches */
} rdb_cache_level_config_t;

/* Cache entry structure */
typedef struct rdb_cache_entry {
    void *key;                   /* Cache key */
    void *value;                 /* Cached value */
    size_t key_size;             /* Key size in bytes */
    size_t value_size;           /* Value size in bytes */
    size_t level;                /* Cache level where entry resides */
    
    /* Access tracking for different algorithms */
    time_t last_access_time;     /* Last access time */
    uint32_t access_count;       /* Access frequency counter */
    uint32_t access_frequency;   /* Normalized access frequency */
    double access_score;         /* Composite access score */
    
    /* Cache metadata */
    bool is_dirty;               /* Whether entry needs to be written to disk */
    bool is_pinned;              /* Whether entry is pinned (cannot be evicted) */
    uint32_t reference_count;    /* Reference counting */
    
    /* Linked list pointers for LRU/LFU */
    struct rdb_cache_entry *prev;
    struct rdb_cache_entry *next;
    
    /* Hash table collision chain */
    struct rdb_cache_entry *hash_next;
} rdb_cache_entry_t;

/* Cache statistics */
typedef struct {
    uint64_t total_requests;     /* Total cache requests */
    uint64_t hits;               /* Cache hits */
    uint64_t misses;             /* Cache misses */
    uint64_t evictions;          /* Number of evictions */
    uint64_t writes;             /* Number of writes */
    uint64_t reads;              /* Number of reads */
    double hit_ratio;            /* Current hit ratio */
    size_t current_size;         /* Current cache size in bytes */
    size_t current_entries;      /* Current number of entries */
    time_t last_reset;           /* Last statistics reset time */
} rdb_cache_stats_t;

/* Cache level structure */
typedef struct {
    rdb_cache_level_config_t config;
    rdb_cache_stats_t stats;
    
    /* Storage structures */
    fi_map *entries;             /* Hash map of cache entries */
    fi_array *entry_list;        /* Ordered list for eviction algorithms */
    
    /* Algorithm-specific structures */
    union {
        struct {
            rdb_cache_entry_t *head;  /* LRU head */
            rdb_cache_entry_t *tail;  /* LRU tail */
        } lru;
        struct {
            fi_btree *frequency_tree; /* B-tree sorted by frequency */
        } lfu;
        struct {
            fi_map *t1;               /* T1: recently accessed items */
            fi_map *t2;               /* T2: frequently accessed items */
            fi_map *b1;               /* B1: recently evicted from T1 */
            fi_map *b2;               /* B2: recently evicted from T2 */
            size_t p;                 /* Target size of T1 */
        } arc;
        struct {
            fi_map *window_cache;     /* Window cache for recent items */
            fi_map *main_cache;       /* Main cache with TinyLFU */
            fi_btree *frequency_sketch; /* Count-min sketch for frequency */
        } wtinylfu;
        struct {
            fi_map *stability_map;    /* H-factor (stability) tracking */
            fi_map *value_map;        /* V-factor (value) tracking */
            double alpha;             /* Exploration vs exploitation balance */
        } aura;
    } algorithm_data;
    
    /* Persistence */
    char *disk_path;             /* Disk storage path */
    int disk_fd;                 /* File descriptor for disk storage */
    void *mmap_ptr;              /* Memory-mapped file pointer */
    size_t mmap_size;            /* Size of memory-mapped region */
    
    /* Thread safety */
    pthread_rwlock_t rwlock;     /* Read-write lock for cache level */
    pthread_mutex_t stats_mutex; /* Mutex for statistics */
    
    /* Write buffer for disk caches */
    void *write_buffer;
    size_t write_buffer_pos;
    pthread_mutex_t write_buffer_mutex;
} rdb_cache_level_t;

/* Multi-level cache system */
typedef struct {
    char *name;                  /* Cache system name */
    size_t num_levels;           /* Number of cache levels */
    rdb_cache_level_t **levels;   /* Array of cache level pointers */
    
    /* Configuration */
    bool auto_tune;              /* Whether to auto-tune cache parameters */
    double target_hit_ratio;     /* Target hit ratio for auto-tuning */
    size_t tune_interval;        /* Auto-tuning interval in seconds */
    
    /* Global statistics */
    rdb_cache_stats_t global_stats;
    time_t last_tune_time;       /* Last auto-tuning time */
    
    /* Thread safety */
    pthread_rwlock_t global_rwlock; /* Global read-write lock */
    pthread_mutex_t tune_mutex;     /* Mutex for auto-tuning */
    
    /* Persistence configuration */
    char *persistence_dir;       /* Directory for persistent storage */
    bool enable_persistence;     /* Whether persistence is enabled */
    size_t checkpoint_interval;  /* Checkpoint interval in seconds */
    time_t last_checkpoint;      /* Last checkpoint time */
} rdb_cache_system_t;

/* Cache system operations */
rdb_cache_system_t* rdb_cache_system_create(const char *name, size_t num_levels, 
                                           const rdb_cache_level_config_t *configs);
void rdb_cache_system_destroy(rdb_cache_system_t *cache_system);

/* Cache level operations */
rdb_cache_level_t* rdb_cache_level_create(const rdb_cache_level_config_t *config);
void rdb_cache_level_destroy(rdb_cache_level_t *level);

/* Cache operations */
int rdb_cache_get(rdb_cache_system_t *cache_system, const void *key, size_t key_size, 
                  void **value, size_t *value_size);
int rdb_cache_put(rdb_cache_system_t *cache_system, const void *key, size_t key_size,
                  const void *value, size_t value_size, bool pin_entry);
int rdb_cache_remove(rdb_cache_system_t *cache_system, const void *key, size_t key_size);
int rdb_cache_clear(rdb_cache_system_t *cache_system);

/* Cache management */
int rdb_cache_resize(rdb_cache_system_t *cache_system, size_t level, size_t new_size);
int rdb_cache_tune(rdb_cache_system_t *cache_system);
int rdb_cache_checkpoint(rdb_cache_system_t *cache_system);
int rdb_cache_warmup(rdb_cache_system_t *cache_system, const char *warmup_data_path);

/* Statistics and monitoring */
void rdb_cache_print_stats(rdb_cache_system_t *cache_system);
void rdb_cache_print_level_stats(rdb_cache_system_t *cache_system, size_t level);
rdb_cache_stats_t* rdb_cache_get_stats(rdb_cache_system_t *cache_system);
rdb_cache_stats_t* rdb_cache_get_level_stats(rdb_cache_system_t *cache_system, size_t level);

/* Configuration */
int rdb_cache_set_algorithm(rdb_cache_system_t *cache_system, size_t level, 
                           rdb_cache_algorithm_t algorithm);
int rdb_cache_set_size(rdb_cache_system_t *cache_system, size_t level, size_t new_size);
int rdb_cache_set_auto_tune(rdb_cache_system_t *cache_system, bool enable, double target_ratio);

/* Persistence operations */
int rdb_cache_enable_persistence(rdb_cache_system_t *cache_system, const char *persistence_dir);
int rdb_cache_save_to_disk(rdb_cache_system_t *cache_system);
int rdb_cache_load_from_disk(rdb_cache_system_t *cache_system);

/* Utility functions */
uint32_t rdb_cache_hash_key(const void *key, size_t key_size);
int rdb_cache_compare_keys(const void *key1, const void *key2);
void rdb_cache_entry_free(void *entry);
void rdb_cache_key_free(void *key);
void rdb_cache_value_free(void *value);

/* Algorithm-specific functions */
int rdb_cache_lru_evict(rdb_cache_level_t *level);
int rdb_cache_lfu_evict(rdb_cache_level_t *level);
int rdb_cache_arc_evict(rdb_cache_level_t *level);
int rdb_cache_wtiny_lfu_evict(rdb_cache_level_t *level);
int rdb_cache_aura_evict(rdb_cache_level_t *level);

/* Thread-safe operations */
int rdb_cache_get_thread_safe(rdb_cache_system_t *cache_system, const void *key, size_t key_size, 
                              void **value, size_t *value_size);
int rdb_cache_put_thread_safe(rdb_cache_system_t *cache_system, const void *key, size_t key_size,
                              const void *value, size_t value_size, bool pin_entry);
int rdb_cache_remove_thread_safe(rdb_cache_system_t *cache_system, const void *key, size_t key_size);

#endif //__CACHE_SYSTEM_H__
