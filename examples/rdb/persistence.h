#ifndef __PERSISTENCE_H__
#define __PERSISTENCE_H__

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

/* Include RDB structures */
#include "rdb.h"

/* Persistence configuration */
#define RDB_PERSISTENCE_MAX_PATH_LEN 512
#define RDB_PERSISTENCE_DEFAULT_DIR "./rdb_data"
#define RDB_PERSISTENCE_CHECKPOINT_INTERVAL 3600  /* 1 hour */
#define RDB_PERSISTENCE_WAL_SIZE 16 * 1024 * 1024  /* 16MB WAL */
#define RDB_PERSISTENCE_PAGE_SIZE 4096  /* 4KB pages */
#define RDB_PERSISTENCE_MAX_PAGES 1024 * 1024  /* 4GB max database size */

/* Persistence modes */
typedef enum {
    RDB_PERSISTENCE_MEMORY_ONLY = 0,    /* No persistence */
    RDB_PERSISTENCE_WAL_ONLY,           /* Write-ahead log only */
    RDB_PERSISTENCE_CHECKPOINT_ONLY,    /* Periodic checkpoints only */
    RDB_PERSISTENCE_FULL                /* WAL + checkpoints */
} rdb_persistence_mode_t;

/* WAL entry types */
typedef enum {
    RDB_WAL_INSERT = 1,
    RDB_WAL_UPDATE,
    RDB_WAL_DELETE,
    RDB_WAL_CREATE_TABLE,
    RDB_WAL_DROP_TABLE,
    RDB_WAL_CREATE_INDEX,
    RDB_WAL_DROP_INDEX,
    RDB_WAL_CHECKPOINT,
    RDB_WAL_COMMIT,
    RDB_WAL_ROLLBACK
} rdb_wal_entry_type_t;

/* WAL entry structure */
typedef struct {
    uint64_t sequence_number;    /* Monotonic sequence number */
    uint64_t timestamp;          /* Unix timestamp */
    rdb_wal_entry_type_t type;   /* Entry type */
    uint32_t transaction_id;     /* Transaction ID */
    uint32_t data_size;          /* Size of data payload */
    char table_name[64];         /* Table name */
    uint64_t row_id;             /* Row ID (for row operations) */
    char data[];                 /* Variable length data payload */
} rdb_wal_entry_t;

/* Page structure for persistent storage */
typedef struct {
    uint64_t page_id;            /* Page identifier */
    uint64_t checksum;           /* Page checksum */
    uint32_t version;            /* Page version for MVCC */
    uint32_t data_size;          /* Size of data in page */
    time_t last_modified;        /* Last modification time */
    bool is_dirty;               /* Whether page needs to be written */
    bool is_pinned;              /* Whether page is pinned in memory */
    uint32_t reference_count;    /* Reference count */
    char data[RDB_PERSISTENCE_PAGE_SIZE - 64]; /* Page data (64 bytes overhead) */
} rdb_persistent_page_t;

/* Table metadata for persistence */
typedef struct {
    char table_name[64];         /* Table name */
    uint64_t first_page_id;      /* First page containing table data */
    uint64_t last_page_id;       /* Last page containing table data */
    uint64_t row_count;          /* Number of rows in table */
    uint64_t total_pages;        /* Total pages used by table */
    time_t created_time;         /* Table creation time */
    time_t last_modified;        /* Last modification time */
    bool is_compressed;          /* Whether table data is compressed */
    uint32_t compression_type;   /* Compression algorithm used */
} rdb_persistent_table_metadata_t;

/* Database metadata for persistence */
typedef struct {
    char magic[16];              /* Magic number: "FI_RDB_PERSIST" */
    uint32_t version;            /* Database version */
    uint64_t created_time;       /* Database creation time */
    uint64_t last_checkpoint;    /* Last checkpoint time */
    uint64_t next_page_id;       /* Next available page ID */
    uint64_t total_pages;        /* Total pages in database */
    uint64_t wal_sequence;       /* Current WAL sequence number */
    uint32_t table_count;        /* Number of tables */
    uint32_t checksum;           /* Header checksum */
    char reserved[448];          /* Reserved for future use */
} rdb_persistent_header_t;

/* Write-ahead log structure */
typedef struct {
    char *wal_path;              /* Path to WAL file */
    int wal_fd;                  /* File descriptor for WAL */
    uint64_t sequence_number;    /* Current sequence number */
    uint64_t current_offset;     /* Current write offset */
    uint64_t max_size;           /* Maximum WAL size */
    void *mmap_ptr;              /* Memory-mapped WAL */
    size_t mmap_size;            /* Size of memory-mapped region */
    pthread_mutex_t wal_mutex;   /* Mutex for WAL operations */
    bool is_compressed;          /* Whether WAL is compressed */
} rdb_wal_t;

/* Page cache structure */
typedef struct {
    fi_map *page_map;            /* Map of page_id -> rdb_persistent_page_t* */
    fi_array *page_list;         /* LRU list of pages */
    size_t max_pages;            /* Maximum pages in cache */
    size_t current_pages;        /* Current pages in cache */
    uint64_t hit_count;          /* Cache hits */
    uint64_t miss_count;         /* Cache misses */
    pthread_rwlock_t cache_rwlock; /* Read-write lock for page cache */
} rdb_page_cache_t;

/* Persistence manager */
typedef struct {
    char *data_dir;              /* Data directory path */
    rdb_persistence_mode_t mode; /* Persistence mode */
    
    /* File handles */
    char *db_file_path;          /* Path to main database file */
    int db_fd;                   /* File descriptor for database */
    void *db_mmap_ptr;           /* Memory-mapped database */
    size_t db_mmap_size;         /* Size of memory-mapped region */
    
    /* WAL */
    rdb_wal_t *wal;              /* Write-ahead log */
    
    /* Page cache */
    rdb_page_cache_t *page_cache; /* Page cache */
    
    /* Metadata */
    rdb_persistent_header_t header; /* Database header */
    fi_map *table_metadata;      /* Map of table_name -> rdb_persistent_table_metadata_t* */
    
    /* Checkpointing */
    time_t last_checkpoint;      /* Last checkpoint time */
    uint64_t checkpoint_interval; /* Checkpoint interval in seconds */
    bool checkpoint_in_progress; /* Whether checkpoint is in progress */
    pthread_mutex_t checkpoint_mutex; /* Mutex for checkpoint operations */
    
    /* Thread safety */
    pthread_rwlock_t persistence_rwlock; /* Read-write lock for persistence operations */
    pthread_mutex_t metadata_mutex;      /* Mutex for metadata operations */
    
    /* Statistics */
    uint64_t total_writes;       /* Total write operations */
    uint64_t total_reads;        /* Total read operations */
    uint64_t checkpoint_count;   /* Number of checkpoints performed */
    uint64_t wal_entries;        /* Number of WAL entries written */
} rdb_persistence_manager_t;

/* Persistence manager operations */
rdb_persistence_manager_t* rdb_persistence_create(const char *data_dir, 
                                                  rdb_persistence_mode_t mode);
void rdb_persistence_destroy(rdb_persistence_manager_t *pm);
int rdb_persistence_init(rdb_persistence_manager_t *pm);
int rdb_persistence_shutdown(rdb_persistence_manager_t *pm);

/* Database operations with persistence */
int rdb_persistence_open_database(rdb_persistence_manager_t *pm, rdb_database_t *db);
int rdb_persistence_close_database(rdb_persistence_manager_t *pm, rdb_database_t *db);
int rdb_persistence_save_database(rdb_persistence_manager_t *pm, rdb_database_t *db);
int rdb_persistence_load_database(rdb_persistence_manager_t *pm, rdb_database_t *db);

/* Table operations with persistence */
int rdb_persistence_create_table(rdb_persistence_manager_t *pm, rdb_database_t *db, 
                                 const char *table_name, fi_array *columns);
int rdb_persistence_drop_table(rdb_persistence_manager_t *pm, rdb_database_t *db, 
                               const char *table_name);

/* Row operations with persistence */
int rdb_persistence_insert_row(rdb_persistence_manager_t *pm, rdb_database_t *db, 
                               const char *table_name, rdb_row_t *row);
int rdb_persistence_update_row(rdb_persistence_manager_t *pm, rdb_database_t *db, 
                               const char *table_name, rdb_row_t *old_row, rdb_row_t *new_row);
int rdb_persistence_delete_row(rdb_persistence_manager_t *pm, rdb_database_t *db, 
                               const char *table_name, uint64_t row_id);

/* WAL operations */
int rdb_wal_create(rdb_wal_t **wal, const char *wal_path, uint64_t max_size);
void rdb_wal_destroy(rdb_wal_t *wal);
int rdb_wal_append(rdb_wal_t *wal, rdb_wal_entry_type_t type, uint32_t transaction_id,
                   const char *table_name, uint64_t row_id, const void *data, uint32_t data_size);
int rdb_wal_replay(rdb_wal_t *wal, rdb_database_t *db);
int rdb_wal_truncate(rdb_wal_t *wal);

/* Page operations */
int rdb_page_cache_create(rdb_page_cache_t **cache, size_t max_pages);
void rdb_page_cache_destroy(rdb_page_cache_t *cache);
int rdb_page_cache_get(rdb_page_cache_t *cache, uint64_t page_id, 
                       rdb_persistent_page_t **page);
int rdb_page_cache_put(rdb_page_cache_t *cache, rdb_persistent_page_t *page);
int rdb_page_cache_evict(rdb_page_cache_t *cache, uint64_t page_id);

/* Checkpoint operations */
int rdb_persistence_checkpoint(rdb_persistence_manager_t *pm, rdb_database_t *db);
int rdb_persistence_force_checkpoint(rdb_persistence_manager_t *pm, rdb_database_t *db);

/* Utility functions */
int rdb_persistence_create_directory(const char *path);
int rdb_persistence_file_exists(const char *path);
uint32_t rdb_persistence_calculate_checksum(const void *data, size_t size);
int rdb_persistence_verify_checksum(const void *data, size_t size, uint32_t checksum);

/* Serialization functions */
int rdb_serialize_table(rdb_table_t *table, void **data, size_t *data_size);
int rdb_deserialize_table(const void *data, size_t data_size, rdb_table_t **table);
int rdb_serialize_row(rdb_row_t *row, void **data, size_t *data_size);
int rdb_deserialize_row(const void *data, size_t data_size, rdb_row_t **row);

/* Thread-safe operations */
int rdb_persistence_insert_row_thread_safe(rdb_persistence_manager_t *pm, rdb_database_t *db, 
                                           const char *table_name, rdb_row_t *row);
int rdb_persistence_update_row_thread_safe(rdb_persistence_manager_t *pm, rdb_database_t *db, 
                                           const char *table_name, rdb_row_t *old_row, rdb_row_t *new_row);
int rdb_persistence_delete_row_thread_safe(rdb_persistence_manager_t *pm, rdb_database_t *db, 
                                           const char *table_name, uint64_t row_id);

/* Statistics and monitoring */
void rdb_persistence_print_stats(rdb_persistence_manager_t *pm);
int rdb_persistence_get_stats(rdb_persistence_manager_t *pm, uint64_t *writes, uint64_t *reads, 
                              uint64_t *checkpoints, uint64_t *wal_entries);

/* Configuration */
int rdb_persistence_set_mode(rdb_persistence_manager_t *pm, rdb_persistence_mode_t mode);
int rdb_persistence_set_checkpoint_interval(rdb_persistence_manager_t *pm, uint64_t interval);
int rdb_persistence_set_wal_size(rdb_persistence_manager_t *pm, uint64_t max_size);

#endif //__PERSISTENCE_H__
