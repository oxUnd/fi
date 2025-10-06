#include "persistence.h"
#include <errno.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <dirent.h>

/* Magic number for database files */
#define RDB_MAGIC_NUMBER "FI_RDB_PERSIST"
#define RDB_VERSION 1

/* Forward declarations for static functions */
static int rdb_serialize_value(const rdb_value_t *value, void **data, size_t *data_size);
static int rdb_deserialize_value(const void *data, size_t data_size, rdb_value_t **value);
static int rdb_serialize_column(const rdb_column_t *column, void **data, size_t *data_size);
static int rdb_deserialize_column(const void *data, size_t data_size, rdb_column_t **column);
static int rdb_persistence_save_header(rdb_persistence_manager_t *pm, rdb_database_t *db);
static int rdb_persistence_load_header(rdb_persistence_manager_t *pm, rdb_database_t *db);
static int rdb_persistence_save_table(rdb_persistence_manager_t *pm, rdb_table_t *table);
static int rdb_persistence_load_table(rdb_persistence_manager_t *pm, rdb_database_t *db, const char *table_name);
static int rdb_persistence_load_tables(rdb_persistence_manager_t *pm, rdb_database_t *db);
static int rdb_persistence_save_foreign_keys(rdb_persistence_manager_t *pm, rdb_database_t *db);
static int rdb_persistence_load_foreign_keys(rdb_persistence_manager_t *pm, rdb_database_t *db);

/* Create persistence manager */
rdb_persistence_manager_t* rdb_persistence_create(const char *data_dir, 
                                                  rdb_persistence_mode_t mode) {
    if (!data_dir) return NULL;
    
    rdb_persistence_manager_t *pm = malloc(sizeof(rdb_persistence_manager_t));
    if (!pm) return NULL;
    
    /* Copy data directory path */
    pm->data_dir = malloc(strlen(data_dir) + 1);
    if (!pm->data_dir) {
        free(pm);
        return NULL;
    }
    strcpy(pm->data_dir, data_dir);
    
    /* Initialize configuration */
    pm->mode = mode;
    pm->checkpoint_interval = RDB_PERSISTENCE_CHECKPOINT_INTERVAL;
    pm->checkpoint_in_progress = false;
    pm->last_checkpoint = time(NULL);
    
    /* Initialize file handles */
    pm->db_file_path = NULL;
    pm->db_fd = -1;
    pm->db_mmap_ptr = NULL;
    pm->db_mmap_size = 0;
    
    /* Initialize WAL */
    pm->wal = NULL;
    
    /* Initialize page cache */
    pm->page_cache = NULL;
    
    /* Initialize header */
    memset(&pm->header, 0, sizeof(rdb_persistent_header_t));
    strcpy(pm->header.magic, RDB_MAGIC_NUMBER);
    pm->header.version = RDB_VERSION;
    pm->header.created_time = time(NULL);
    pm->header.next_page_id = 1;
    
    /* Initialize metadata */
    pm->table_metadata = fi_map_create_string_ptr(64);
    if (!pm->table_metadata) {
        free(pm->data_dir);
        free(pm);
        return NULL;
    }
    
    /* Initialize statistics */
    pm->total_writes = 0;
    pm->total_reads = 0;
    pm->checkpoint_count = 0;
    pm->wal_entries = 0;
    
    /* Initialize thread safety */
    if (pthread_rwlock_init(&pm->persistence_rwlock, NULL) != 0) {
        fi_map_destroy(pm->table_metadata);
        free(pm->data_dir);
        free(pm);
        return NULL;
    }
    
    if (pthread_mutex_init(&pm->metadata_mutex, NULL) != 0) {
        pthread_rwlock_destroy(&pm->persistence_rwlock);
        fi_map_destroy(pm->table_metadata);
        free(pm->data_dir);
        free(pm);
        return NULL;
    }
    
    if (pthread_mutex_init(&pm->checkpoint_mutex, NULL) != 0) {
        pthread_mutex_destroy(&pm->metadata_mutex);
        pthread_rwlock_destroy(&pm->persistence_rwlock);
        fi_map_destroy(pm->table_metadata);
        free(pm->data_dir);
        free(pm);
        return NULL;
    }
    
    return pm;
}

/* Destroy persistence manager */
void rdb_persistence_destroy(rdb_persistence_manager_t *pm) {
    if (!pm) return;
    
    /* Clean up WAL */
    if (pm->wal) {
        rdb_wal_destroy(pm->wal);
    }
    
    /* Clean up page cache */
    if (pm->page_cache) {
        rdb_page_cache_destroy(pm->page_cache);
    }
    
    /* Clean up memory mapping */
    if (pm->db_mmap_ptr) {
        munmap(pm->db_mmap_ptr, pm->db_mmap_size);
    }
    
    /* Close file descriptor */
    if (pm->db_fd >= 0) {
        close(pm->db_fd);
    }
    
    /* Clean up metadata */
    if (pm->table_metadata) {
        fi_map_destroy(pm->table_metadata);
    }
    
    /* Clean up paths */
    if (pm->data_dir) free(pm->data_dir);
    if (pm->db_file_path) free(pm->db_file_path);
    
    /* Clean up thread safety */
    pthread_rwlock_destroy(&pm->persistence_rwlock);
    pthread_mutex_destroy(&pm->metadata_mutex);
    pthread_mutex_destroy(&pm->checkpoint_mutex);
    
    free(pm);
}

/* Initialize persistence manager */
int rdb_persistence_init(rdb_persistence_manager_t *pm) {
    if (!pm) return -1;
    
    /* Create data directory if it doesn't exist */
    if (rdb_persistence_create_directory(pm->data_dir) != 0) {
        return -1;
    }
    
    /* Create database file path */
    pm->db_file_path = malloc(strlen(pm->data_dir) + 20);
    if (!pm->db_file_path) return -1;
    sprintf(pm->db_file_path, "%s/database.rdb", pm->data_dir);
    
    /* Create WAL if needed */
    if (pm->mode == RDB_PERSISTENCE_WAL_ONLY || pm->mode == RDB_PERSISTENCE_FULL) {
        char wal_path[512];
        sprintf(wal_path, "%s/wal.log", pm->data_dir);
        
        if (rdb_wal_create(&pm->wal, wal_path, RDB_PERSISTENCE_WAL_SIZE) != 0) {
            return -1;
        }
    }
    
    /* Create page cache */
    if (rdb_page_cache_create(&pm->page_cache, 1024) != 0) {
        return -1;
    }
    
    /* Open or create database file */
    if (rdb_persistence_file_exists(pm->db_file_path)) {
        /* Load existing database */
        pm->db_fd = open(pm->db_file_path, O_RDWR);
        if (pm->db_fd < 0) return -1;
        
        /* Read header */
        if (read(pm->db_fd, &pm->header, sizeof(rdb_persistent_header_t)) != sizeof(rdb_persistent_header_t)) {
            close(pm->db_fd);
            return -1;
        }
        
        /* Verify magic number */
        if (strcmp(pm->header.magic, RDB_MAGIC_NUMBER) != 0) {
            close(pm->db_fd);
            return -1;
        }
        
        /* Memory map the database */
        struct stat st;
        if (fstat(pm->db_fd, &st) != 0) {
            close(pm->db_fd);
            return -1;
        }
        
        pm->db_mmap_size = st.st_size;
        pm->db_mmap_ptr = mmap(NULL, pm->db_mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, pm->db_fd, 0);
        if (pm->db_mmap_ptr == MAP_FAILED) {
            close(pm->db_fd);
            return -1;
        }
        
    } else {
        /* Create new database file */
        pm->db_fd = open(pm->db_file_path, O_CREAT | O_RDWR, 0644);
        if (pm->db_fd < 0) return -1;
        
        /* Calculate checksum for new header */
        pm->header.checksum = 0; /* Clear checksum field before calculation */
        pm->header.checksum = rdb_persistence_calculate_checksum(&pm->header, sizeof(rdb_persistent_header_t) - sizeof(uint32_t));
        
        /* Write header */
        if (write(pm->db_fd, &pm->header, sizeof(rdb_persistent_header_t)) != sizeof(rdb_persistent_header_t)) {
            close(pm->db_fd);
            return -1;
        }
        
        /* Initialize memory mapping */
        pm->db_mmap_size = sizeof(rdb_persistent_header_t);
        pm->db_mmap_ptr = mmap(NULL, pm->db_mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, pm->db_fd, 0);
        if (pm->db_mmap_ptr == MAP_FAILED) {
            close(pm->db_fd);
            return -1;
        }
    }
    
    return 0;
}

/* Shutdown persistence manager */
int rdb_persistence_shutdown(rdb_persistence_manager_t *pm) {
    if (!pm) return -1;
    
    pthread_rwlock_wrlock(&pm->persistence_rwlock);
    
    /* Force checkpoint if needed */
    if (pm->mode == RDB_PERSISTENCE_CHECKPOINT_ONLY || pm->mode == RDB_PERSISTENCE_FULL) {
        /* Note: We can't do a full checkpoint here without a database reference */
        /* This would typically be called during database shutdown */
    }
    
    /* Sync WAL */
    if (pm->wal) {
        /* Ensure all WAL entries are flushed */
        fsync(pm->db_fd);
    }
    
    /* Sync database file */
    if (pm->db_fd >= 0) {
        fsync(pm->db_fd);
    }
    
    pthread_rwlock_unlock(&pm->persistence_rwlock);
    
    return 0;
}

/* Create WAL */
int rdb_wal_create(rdb_wal_t **wal, const char *wal_path, uint64_t max_size) {
    if (!wal || !wal_path) return -1;
    
    rdb_wal_t *w = malloc(sizeof(rdb_wal_t));
    if (!w) return -1;
    
    /* Copy WAL path */
    w->wal_path = malloc(strlen(wal_path) + 1);
    if (!w->wal_path) {
        free(w);
        return -1;
    }
    strcpy(w->wal_path, wal_path);
    
    /* Open WAL file */
    w->wal_fd = open(wal_path, O_CREAT | O_RDWR, 0644);
    if (w->wal_fd < 0) {
        free(w->wal_path);
        free(w);
        return -1;
    }
    
    /* Initialize WAL */
    w->sequence_number = 1;
    w->current_offset = 0;
    w->max_size = max_size;
    w->mmap_ptr = NULL;
    w->mmap_size = 0;
    w->is_compressed = false;
    
    /* Initialize mutex */
    if (pthread_mutex_init(&w->wal_mutex, NULL) != 0) {
        close(w->wal_fd);
        free(w->wal_path);
        free(w);
        return -1;
    }
    
    /* Memory map WAL file */
    w->mmap_size = max_size;
    w->mmap_ptr = mmap(NULL, w->mmap_size, PROT_READ | PROT_WRITE, MAP_SHARED, w->wal_fd, 0);
    if (w->mmap_ptr == MAP_FAILED) {
        pthread_mutex_destroy(&w->wal_mutex);
        close(w->wal_fd);
        free(w->wal_path);
        free(w);
        return -1;
    }
    
    *wal = w;
    return 0;
}

/* Destroy WAL */
void rdb_wal_destroy(rdb_wal_t *wal) {
    if (!wal) return;
    
    /* Unmap memory */
    if (wal->mmap_ptr) {
        munmap(wal->mmap_ptr, wal->mmap_size);
    }
    
    /* Close file */
    if (wal->wal_fd >= 0) {
        close(wal->wal_fd);
    }
    
    /* Clean up mutex */
    pthread_mutex_destroy(&wal->wal_mutex);
    
    /* Free path */
    if (wal->wal_path) free(wal->wal_path);
    
    free(wal);
}

/* Append to WAL */
int rdb_wal_append(rdb_wal_t *wal, rdb_wal_entry_type_t type, uint32_t transaction_id,
                   const char *table_name, uint64_t row_id, const void *data, uint32_t data_size) {
    if (!wal) return -1;
    
    pthread_mutex_lock(&wal->wal_mutex);
    
    /* Check if WAL is full */
    if (wal->current_offset + sizeof(rdb_wal_entry_t) + data_size > wal->max_size) {
        pthread_mutex_unlock(&wal->wal_mutex);
        return -1;  /* WAL is full */
    }
    
    /* Create WAL entry */
    rdb_wal_entry_t *entry = (rdb_wal_entry_t*)((char*)wal->mmap_ptr + wal->current_offset);
    entry->sequence_number = wal->sequence_number++;
    entry->timestamp = time(NULL);
    entry->type = type;
    entry->transaction_id = transaction_id;
    entry->data_size = data_size;
    entry->row_id = row_id;
    
    /* Copy table name */
    strncpy(entry->table_name, table_name, sizeof(entry->table_name) - 1);
    entry->table_name[sizeof(entry->table_name) - 1] = '\0';
    
    /* Copy data */
    if (data && data_size > 0) {
        memcpy(entry->data, data, data_size);
    }
    
    /* Update offset */
    wal->current_offset += sizeof(rdb_wal_entry_t) + data_size;
    
    pthread_mutex_unlock(&wal->wal_mutex);
    
    return 0;
}

/* Create page cache */
int rdb_page_cache_create(rdb_page_cache_t **cache, size_t max_pages) {
    if (!cache) return -1;
    
    rdb_page_cache_t *c = malloc(sizeof(rdb_page_cache_t));
    if (!c) return -1;
    
    /* Create page map */
    c->page_map = fi_map_create_with_destructors(
        max_pages / 2,
        sizeof(uint64_t),
        sizeof(rdb_persistent_page_t*),
        fi_map_hash_int64,
        fi_map_compare_int64,
        NULL,
        NULL
    );
    
    if (!c->page_map) {
        free(c);
        return -1;
    }
    
    /* Create page list for LRU */
    c->page_list = fi_array_create(max_pages, sizeof(rdb_persistent_page_t*));
    if (!c->page_list) {
        fi_map_destroy(c->page_map);
        free(c);
        return -1;
    }
    
    /* Initialize cache */
    c->max_pages = max_pages;
    c->current_pages = 0;
    c->hit_count = 0;
    c->miss_count = 0;
    
    /* Initialize read-write lock */
    if (pthread_rwlock_init(&c->cache_rwlock, NULL) != 0) {
        fi_array_destroy(c->page_list);
        fi_map_destroy(c->page_map);
        free(c);
        return -1;
    }
    
    *cache = c;
    return 0;
}

/* Destroy page cache */
void rdb_page_cache_destroy(rdb_page_cache_t *cache) {
    if (!cache) return;
    
    /* Clean up all pages */
    fi_map_iterator iter = fi_map_iterator_create(cache->page_map);
    while (fi_map_iterator_next(&iter)) {
        rdb_persistent_page_t **page_ptr = (rdb_persistent_page_t**)fi_map_iterator_value(&iter);
        if (page_ptr && *page_ptr) {
            free(*page_ptr);
        }
    }
    
    /* Clean up structures */
    if (cache->page_map) fi_map_destroy(cache->page_map);
    if (cache->page_list) fi_array_destroy(cache->page_list);
    
    /* Clean up lock */
    pthread_rwlock_destroy(&cache->cache_rwlock);
    
    free(cache);
}

/* Get page from cache */
int rdb_page_cache_get(rdb_page_cache_t *cache, uint64_t page_id, 
                       rdb_persistent_page_t **page) {
    if (!cache || !page) return -1;
    
    pthread_rwlock_rdlock(&cache->cache_rwlock);
    
    rdb_persistent_page_t **cached_page = NULL;
    if (fi_map_get(cache->page_map, &page_id, &cached_page) == 0 && cached_page) {
        *page = *cached_page;
        cache->hit_count++;
        pthread_rwlock_unlock(&cache->cache_rwlock);
        return 0;
    }
    
    cache->miss_count++;
    pthread_rwlock_unlock(&cache->cache_rwlock);
    return -1;
}

/* Put page in cache */
int rdb_page_cache_put(rdb_page_cache_t *cache, rdb_persistent_page_t *page) {
    if (!cache || !page) return -1;
    
    pthread_rwlock_wrlock(&cache->cache_rwlock);
    
    /* Check if we need to evict */
    while (cache->current_pages >= cache->max_pages) {
        /* Evict least recently used page */
        if (fi_array_count(cache->page_list) > 0) {
            rdb_persistent_page_t **lru_page_ptr = (rdb_persistent_page_t**)fi_array_get(cache->page_list, 0);
            if (lru_page_ptr && *lru_page_ptr) {
                uint64_t lru_page_id = (*lru_page_ptr)->page_id;
                fi_map_remove(cache->page_map, &lru_page_id);
                fi_array_splice(cache->page_list, 0, 1, NULL);
                free(*lru_page_ptr);
                cache->current_pages--;
            }
        } else {
            break;
        }
    }
    
    /* Add page to cache */
    rdb_persistent_page_t *page_copy = malloc(sizeof(rdb_persistent_page_t));
    if (!page_copy) {
        pthread_rwlock_unlock(&cache->cache_rwlock);
        return -1;
    }
    memcpy(page_copy, page, sizeof(rdb_persistent_page_t));
    
    if (fi_map_put(cache->page_map, &page_copy->page_id, &page_copy) == 0) {
        fi_array_push(cache->page_list, &page_copy);
        cache->current_pages++;
        pthread_rwlock_unlock(&cache->cache_rwlock);
        return 0;
    }
    
    free(page_copy);
    pthread_rwlock_unlock(&cache->cache_rwlock);
    return -1;
}

/* Create directory */
int rdb_persistence_create_directory(const char *path) {
    if (!path) return -1;
    
    /* Check if directory exists */
    struct stat st;
    if (stat(path, &st) == 0) {
        return S_ISDIR(st.st_mode) ? 0 : -1;
    }
    
    /* Create directory */
    return mkdir(path, 0755);
}

/* Check if file exists */
int rdb_persistence_file_exists(const char *path) {
    if (!path) return 0;
    
    struct stat st;
    return (stat(path, &st) == 0) ? 1 : 0;
}

/* Calculate checksum */
uint32_t rdb_persistence_calculate_checksum(const void *data, size_t size) {
    if (!data || size == 0) return 0;
    
    const uint8_t *bytes = (const uint8_t*)data;
    uint32_t checksum = 0x811c9dc5;  /* FNV offset basis */
    
    for (size_t i = 0; i < size; i++) {
        checksum ^= bytes[i];
        checksum *= 0x01000193;  /* FNV prime */
    }
    
    return checksum;
}

/* Verify checksum */
int rdb_persistence_verify_checksum(const void *data, size_t size, uint32_t checksum) {
    return (rdb_persistence_calculate_checksum(data, size) == checksum) ? 0 : -1;
}

/* Persistence operations with RDB integration */
int rdb_persistence_open_database(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    pthread_rwlock_wrlock(&pm->persistence_rwlock);
    
    /* Load database from persistent storage */
    if (rdb_persistence_load_database(pm, db) != 0) {
        pthread_rwlock_unlock(&pm->persistence_rwlock);
        return -1;
    }
    
    /* Replay WAL if it exists */
    if (pm->wal) {
        if (rdb_wal_replay(pm->wal, db) != 0) {
            pthread_rwlock_unlock(&pm->persistence_rwlock);
            return -1;
        }
    }
    
    pthread_rwlock_unlock(&pm->persistence_rwlock);
    return 0;
}

int rdb_persistence_close_database(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    pthread_rwlock_wrlock(&pm->persistence_rwlock);
    
    /* Save database to persistent storage */
    if (rdb_persistence_save_database(pm, db) != 0) {
        pthread_rwlock_unlock(&pm->persistence_rwlock);
        return -1;
    }
    
    /* Force checkpoint */
    if (pm->mode == RDB_PERSISTENCE_CHECKPOINT_ONLY || pm->mode == RDB_PERSISTENCE_FULL) {
        if (rdb_persistence_force_checkpoint(pm, db) != 0) {
            pthread_rwlock_unlock(&pm->persistence_rwlock);
            return -1;
        }
    }
    
    pthread_rwlock_unlock(&pm->persistence_rwlock);
    return 0;
}

/* Timeout-protected save database to persistent storage */
int rdb_persistence_save_database_timeout(rdb_persistence_manager_t *pm, rdb_database_t *db, int timeout_seconds) {
    if (!pm || !db) return -1;
    
    /* Set up timeout using alarm */
    alarm(timeout_seconds);
    
    /* Try to acquire lock (no timeout on lock acquisition since we use mutex) */
    if (pthread_mutex_lock(&pm->persistence_rwlock) != 0) {
        alarm(0); /* Cancel alarm */
        printf("DEBUG: Failed to acquire lock for save operation\n");
        return -1;
    }
    
    /* Serialize and save database header */
    if (rdb_persistence_save_header(pm, db) != 0) {
        alarm(0); /* Cancel alarm */
        pthread_rwlock_unlock(&pm->persistence_rwlock);
        return -1;
    }
    
    /* Serialize and save all tables */
    if (db->tables) {
        printf("DEBUG: Found %zu tables to save\n", fi_map_size(db->tables));
        
        /* Iterate through all tables and save them */
        fi_map_iterator iter = fi_map_iterator_create(db->tables);
        while (fi_map_iterator_next(&iter)) {
            const char *table_name = (const char*)fi_map_iterator_key(&iter);
            rdb_table_t **table_ptr = (rdb_table_t**)fi_map_iterator_value(&iter);
            
            if (table_ptr && *table_ptr) {
                printf("DEBUG: Saving table: %s\n", table_name);
                if (rdb_persistence_save_table(pm, *table_ptr) != 0) {
                    printf("DEBUG: Failed to save table: %s\n", table_name);
                    alarm(0); /* Cancel alarm */
                    pthread_rwlock_unlock(&pm->persistence_rwlock);
                    return -1;
                }
                printf("DEBUG: Successfully saved table: %s\n", table_name);
            }
        }
    } else {
        printf("DEBUG: No tables found in database\n");
    }
    
    /* Save foreign key constraints */
    if (rdb_persistence_save_foreign_keys(pm, db) != 0) {
        alarm(0); /* Cancel alarm */
        pthread_rwlock_unlock(&pm->persistence_rwlock);
        return -1;
    }
    
    /* Force sync to disk with timeout protection */
    if (pm->db_fd >= 0) {
        /* Use fdatasync instead of fsync for better performance */
        if (fdatasync(pm->db_fd) != 0) {
            printf("DEBUG: Warning - fdatasync failed, but continuing\n");
            /* Don't fail the entire operation for sync issues */
        }
    }
    
    pm->total_writes++;
    alarm(0); /* Cancel alarm */
    pthread_rwlock_unlock(&pm->persistence_rwlock);
    return 0;
}

/* Timeout-protected close database */
int rdb_persistence_close_database_timeout(rdb_persistence_manager_t *pm, rdb_database_t *db, int timeout_seconds) {
    if (!pm || !db) return -1;
    
    /* Set up timeout using alarm */
    alarm(timeout_seconds);
    
    /* Try to acquire lock (no timeout on lock acquisition since we use mutex) */
    if (pthread_mutex_lock(&pm->persistence_rwlock) != 0) {
        alarm(0); /* Cancel alarm */
        printf("DEBUG: Failed to acquire lock for close operation\n");
        return -1;
    }
    
    /* Save database to persistent storage with timeout protection */
    if (rdb_persistence_save_database_timeout(pm, db, timeout_seconds - 1) != 0) {
        printf("DEBUG: Warning - save operation failed during close, but continuing\n");
        /* Don't fail the entire close operation */
    }
    
    /* Force checkpoint with timeout protection */
    if (pm->mode == RDB_PERSISTENCE_CHECKPOINT_ONLY || pm->mode == RDB_PERSISTENCE_FULL) {
        /* Skip checkpoint if we're running out of time */
        if (timeout_seconds > 1) {
            if (rdb_persistence_force_checkpoint(pm, db) != 0) {
                printf("DEBUG: Warning - checkpoint failed during close, but continuing\n");
                /* Don't fail the entire close operation */
            }
        }
    }
    
    alarm(0); /* Cancel alarm */
    pthread_rwlock_unlock(&pm->persistence_rwlock);
    return 0;
}

/* Save database to persistent storage */
int rdb_persistence_save_database(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    pthread_rwlock_wrlock(&pm->persistence_rwlock);
    
    /* Serialize and save database header */
    if (rdb_persistence_save_header(pm, db) != 0) {
        pthread_rwlock_unlock(&pm->persistence_rwlock);
        return -1;
    }
    
    /* Serialize and save all tables */
    if (db->tables) {
        printf("DEBUG: Found %zu tables to save\n", fi_map_size(db->tables));
        
        /* Iterate through all tables and save them */
        fi_map_iterator iter = fi_map_iterator_create(db->tables);
        
        /* Handle first element if iterator is valid */
        if (iter.is_valid) {
            const char *table_name = (const char*)fi_map_iterator_key(&iter);
            rdb_table_t **table_ptr = (rdb_table_t**)fi_map_iterator_value(&iter);
            
            if (table_ptr && *table_ptr) {
                if (rdb_persistence_save_table(pm, *table_ptr) != 0) {
                    pthread_rwlock_unlock(&pm->persistence_rwlock);
                    return -1;
                }
            }
        }
        
        /* Handle remaining elements */
        while (fi_map_iterator_next(&iter)) {
            const char *table_name = (const char*)fi_map_iterator_key(&iter);
            rdb_table_t **table_ptr = (rdb_table_t**)fi_map_iterator_value(&iter);
            
            if (table_ptr && *table_ptr) {
                if (rdb_persistence_save_table(pm, *table_ptr) != 0) {
                    pthread_rwlock_unlock(&pm->persistence_rwlock);
                    return -1;
                }
            }
        }
    } else {
        printf("DEBUG: No tables found in database\n");
    }
    
    /* Save foreign key constraints */
    if (rdb_persistence_save_foreign_keys(pm, db) != 0) {
        pthread_rwlock_unlock(&pm->persistence_rwlock);
        return -1;
    }
    
    /* Force sync to disk */
    if (pm->db_fd >= 0) {
        fsync(pm->db_fd);
    }
    
    pm->total_writes++;
    pthread_rwlock_unlock(&pm->persistence_rwlock);
    return 0;
}

/* Load database from persistent storage */
int rdb_persistence_load_database(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    /* Load database header */
    if (rdb_persistence_load_header(pm, db) != 0) {
        return -1;
    }
    
    /* Load all tables */
    if (rdb_persistence_load_tables(pm, db) != 0) {
        return -1;
    }
    
    /* Load foreign key constraints */
    if (rdb_persistence_load_foreign_keys(pm, db) != 0) {
        return -1;
    }
    
    pm->total_reads++;
    return 0;
}

/* Replay WAL */
int rdb_wal_replay(rdb_wal_t *wal, rdb_database_t *db) {
    if (!wal || !db) return -1;
    
    pthread_mutex_lock(&wal->wal_mutex);
    
    /* Read WAL entries from the beginning */
    size_t offset = 0;
    
    while (offset < wal->current_offset) {
        /* Read WAL entry header */
        if (offset + sizeof(rdb_wal_entry_t) > wal->current_offset) {
            break; /* Incomplete entry */
        }
        
        rdb_wal_entry_t *entry = (rdb_wal_entry_t*)((char*)wal->mmap_ptr + offset);
        
        /* Validate entry */
        if (entry->sequence_number == 0 || entry->type == 0) {
            break; /* Invalid entry */
        }
        
        /* Replay the operation */
        switch (entry->type) {
            case RDB_WAL_INSERT:
                /* Replay insert operation */
                if (entry->data_size > 0) {
                    rdb_row_t *row = NULL;
                    if (rdb_deserialize_row(entry->data, entry->data_size, &row) == 0) {
                        /* Find the table and insert the row */
                        rdb_table_t *table = rdb_get_table(db, entry->table_name);
                        if (table && row) {
                            fi_array_push(table->rows, &row);
                        }
                    }
                }
                break;
                
            case RDB_WAL_UPDATE:
                /* Replay update operation */
                if (entry->data_size > 0) {
                    rdb_row_t *row = NULL;
                    if (rdb_deserialize_row(entry->data, entry->data_size, &row) == 0) {
                        /* Find the table and update the row */
                        rdb_table_t *table = rdb_get_table(db, entry->table_name);
                        if (table && row) {
                            /* Find and replace the row with matching row_id */
                            for (size_t i = 0; i < fi_array_count(table->rows); i++) {
                                rdb_row_t **existing_row = (rdb_row_t**)fi_array_get(table->rows, i);
                                if (existing_row && *existing_row && (*existing_row)->row_id == row->row_id) {
                                    rdb_row_free(*existing_row);
                                    *existing_row = row;
                                    break;
                                }
                            }
                        }
                    }
                }
                break;
                
            case RDB_WAL_DELETE:
                /* Replay delete operation */
                {
                    rdb_table_t *table = rdb_get_table(db, entry->table_name);
                    if (table) {
                        /* Find and remove the row with matching row_id */
                        for (size_t i = 0; i < fi_array_count(table->rows); i++) {
                            rdb_row_t **existing_row = (rdb_row_t**)fi_array_get(table->rows, i);
                            if (existing_row && *existing_row && (*existing_row)->row_id == entry->row_id) {
                                rdb_row_free(*existing_row);
                                fi_array_splice(table->rows, i, 1, NULL);
                                break;
                            }
                        }
                    }
                }
                break;
                
            case RDB_WAL_CREATE_TABLE:
                /* Replay table creation */
                if (entry->data_size > 0) {
                    rdb_table_t *table = NULL;
                    if (rdb_deserialize_table(entry->data, entry->data_size, &table) == 0) {
                        if (table) {
                            fi_map_put(db->tables, entry->table_name, &table);
                        }
                    }
                }
                break;
                
            case RDB_WAL_DROP_TABLE:
                /* Replay table deletion */
                {
                    rdb_table_t *table_ptr = NULL;
                    fi_map_get(db->tables, entry->table_name, &table_ptr);
                    if (table_ptr) {
                        rdb_destroy_table(table_ptr);
                        fi_map_remove(db->tables, entry->table_name);
                    }
                }
                break;
                
            case RDB_WAL_CHECKPOINT:
                /* Checkpoint reached - we can stop replaying */
                pthread_mutex_unlock(&wal->wal_mutex);
                return 0;
                
            default:
                /* Unknown entry type - skip */
                break;
        }
        
        /* Move to next entry */
        offset += sizeof(rdb_wal_entry_t) + entry->data_size;
    }
    
    pthread_mutex_unlock(&wal->wal_mutex);
    return 0;
}

/* Force checkpoint */
int rdb_persistence_force_checkpoint(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    pthread_mutex_lock(&pm->checkpoint_mutex);
    
    if (pm->checkpoint_in_progress) {
        pthread_mutex_unlock(&pm->checkpoint_mutex);
        return 0;  /* Checkpoint already in progress */
    }
    
    pm->checkpoint_in_progress = true;
    
    /* Save database state */
    if (rdb_persistence_save_database(pm, db) != 0) {
        pm->checkpoint_in_progress = false;
        pthread_mutex_unlock(&pm->checkpoint_mutex);
        return -1;
    }
    
    /* Truncate WAL */
    if (pm->wal) {
        rdb_wal_truncate(pm->wal);
    }
    
    /* Update header */
    pm->header.last_checkpoint = time(NULL);
    pm->checkpoint_count++;
    pm->last_checkpoint = pm->header.last_checkpoint;
    
    pm->checkpoint_in_progress = false;
    pthread_mutex_unlock(&pm->checkpoint_mutex);
    
    return 0;
}

/* Truncate WAL */
int rdb_wal_truncate(rdb_wal_t *wal) {
    if (!wal) return -1;
    
    pthread_mutex_lock(&wal->wal_mutex);
    
    /* Reset WAL to beginning */
    wal->current_offset = 0;
    wal->sequence_number = 1;
    
    /* Clear memory-mapped region */
    if (wal->mmap_ptr) {
        memset(wal->mmap_ptr, 0, wal->max_size);
    }
    
    /* Truncate file */
    if (ftruncate(wal->wal_fd, 0) != 0) {
        pthread_mutex_unlock(&wal->wal_mutex);
        return -1;
    }
    
    pthread_mutex_unlock(&wal->wal_mutex);
    return 0;
}

/* Print persistence statistics */
void rdb_persistence_print_stats(rdb_persistence_manager_t *pm) {
    if (!pm) return;
    
    printf("=== Persistence Statistics ===\n");
    printf("Data Directory: %s\n", pm->data_dir);
    printf("Persistence Mode: %d\n", pm->mode);
    printf("Total Writes: %lu\n", pm->total_writes);
    printf("Total Reads: %lu\n", pm->total_reads);
    printf("Checkpoints: %lu\n", pm->checkpoint_count);
    printf("WAL Entries: %lu\n", pm->wal_entries);
    printf("Last Checkpoint: %s", ctime(&pm->last_checkpoint));
    
    if (pm->page_cache) {
        printf("\nPage Cache:\n");
        printf("  Current Pages: %zu\n", pm->page_cache->current_pages);
        printf("  Max Pages: %zu\n", pm->page_cache->max_pages);
        printf("  Hit Count: %lu\n", pm->page_cache->hit_count);
        printf("  Miss Count: %lu\n", pm->page_cache->miss_count);
        if (pm->page_cache->hit_count + pm->page_cache->miss_count > 0) {
            double hit_ratio = (double)pm->page_cache->hit_count / 
                              (pm->page_cache->hit_count + pm->page_cache->miss_count);
            printf("  Hit Ratio: %.2f%%\n", hit_ratio * 100);
        }
    }
    
    if (pm->wal) {
        printf("\nWAL:\n");
        printf("  Current Offset: %lu\n", pm->wal->current_offset);
        printf("  Max Size: %lu\n", pm->wal->max_size);
        printf("  Sequence Number: %lu\n", pm->wal->sequence_number);
    }
}

/* ===== BINARY SERIALIZATION FUNCTIONS ===== */

/* Serialize a value to binary format */
static int rdb_serialize_value(const rdb_value_t *value, void **data, size_t *data_size) {
    if (!value || !data || !data_size) return -1;
    
    size_t total_size = sizeof(rdb_data_type_t) + sizeof(bool); /* type + is_null */
    size_t value_size = 0;
    
    /* Calculate size based on type */
    if (!value->is_null) {
        switch (value->type) {
            case RDB_TYPE_INT:
                value_size = sizeof(int64_t);
                break;
            case RDB_TYPE_FLOAT:
                value_size = sizeof(double);
                break;
            case RDB_TYPE_VARCHAR:
            case RDB_TYPE_TEXT:
                if (value->data.string_val) {
                    value_size = strlen(value->data.string_val) + 1; /* +1 for null terminator */
                }
                break;
            case RDB_TYPE_BOOLEAN:
                value_size = sizeof(bool);
                break;
            default:
                return -1;
        }
    }
    
    total_size += value_size;
    
    /* Allocate buffer */
    void *buffer = malloc(total_size);
    if (!buffer) return -1;
    
    char *ptr = (char*)buffer;
    
    /* Write type */
    memcpy(ptr, &value->type, sizeof(rdb_data_type_t));
    ptr += sizeof(rdb_data_type_t);
    
    /* Write is_null flag */
    memcpy(ptr, &value->is_null, sizeof(bool));
    ptr += sizeof(bool);
    
    /* Write value data */
    if (!value->is_null) {
        switch (value->type) {
            case RDB_TYPE_INT:
                memcpy(ptr, &value->data.int_val, sizeof(int64_t));
                break;
            case RDB_TYPE_FLOAT:
                memcpy(ptr, &value->data.float_val, sizeof(double));
                break;
            case RDB_TYPE_VARCHAR:
            case RDB_TYPE_TEXT:
                if (value->data.string_val) {
                    strcpy(ptr, value->data.string_val);
                }
                break;
            case RDB_TYPE_BOOLEAN:
                memcpy(ptr, &value->data.bool_val, sizeof(bool));
                break;
        }
    }
    
    *data = buffer;
    *data_size = total_size;
    return 0;
}

/* Deserialize a value from binary format */
static int rdb_deserialize_value(const void *data, size_t data_size, rdb_value_t **value) {
    if (!data || !value || data_size < sizeof(rdb_data_type_t) + sizeof(bool)) return -1;
    
    rdb_value_t *v = malloc(sizeof(rdb_value_t));
    if (!v) return -1;
    
    const char *ptr = (const char*)data;
    
    /* Read type */
    memcpy(&v->type, ptr, sizeof(rdb_data_type_t));
    ptr += sizeof(rdb_data_type_t);
    
    /* Read is_null flag */
    memcpy(&v->is_null, ptr, sizeof(bool));
    ptr += sizeof(bool);
    
    /* Read value data */
    if (!v->is_null) {
        switch (v->type) {
            case RDB_TYPE_INT:
                if (data_size < sizeof(rdb_data_type_t) + sizeof(bool) + sizeof(int64_t)) {
                    free(v);
                    return -1;
                }
                memcpy(&v->data.int_val, ptr, sizeof(int64_t));
                break;
            case RDB_TYPE_FLOAT:
                if (data_size < sizeof(rdb_data_type_t) + sizeof(bool) + sizeof(double)) {
                    free(v);
                    return -1;
                }
                memcpy(&v->data.float_val, ptr, sizeof(double));
                break;
            case RDB_TYPE_VARCHAR:
            case RDB_TYPE_TEXT:
                {
                    size_t remaining = data_size - (ptr - (const char*)data);
                    v->data.string_val = malloc(remaining);
                    if (!v->data.string_val) {
                        free(v);
                        return -1;
                    }
                    strcpy(v->data.string_val, ptr);
                }
                break;
            case RDB_TYPE_BOOLEAN:
                if (data_size < sizeof(rdb_data_type_t) + sizeof(bool) + sizeof(bool)) {
                    free(v);
                    return -1;
                }
                memcpy(&v->data.bool_val, ptr, sizeof(bool));
                break;
            default:
                free(v);
                return -1;
        }
    }
    
    *value = v;
    return 0;
}

/* Serialize a column definition */
static int rdb_serialize_column(const rdb_column_t *column, void **data, size_t *data_size) {
    if (!column || !data || !data_size) return -1;
    
    size_t total_size = sizeof(rdb_column_t);
    void *buffer = malloc(total_size);
    if (!buffer) return -1;
    
    memcpy(buffer, column, total_size);
    
    *data = buffer;
    *data_size = total_size;
    return 0;
}

/* Deserialize a column definition */
static int rdb_deserialize_column(const void *data, size_t data_size, rdb_column_t **column) {
    if (!data || !column || data_size != sizeof(rdb_column_t)) return -1;
    
    rdb_column_t *c = malloc(sizeof(rdb_column_t));
    if (!c) return -1;
    
    memcpy(c, data, sizeof(rdb_column_t));
    
    *column = c;
    return 0;
}

/* Serialize a row */
int rdb_serialize_row(rdb_row_t *row, void **data, size_t *data_size) {
    if (!row || !data || !data_size) return -1;
    
    size_t total_size = sizeof(size_t); /* row_id */
    
    /* Calculate size for values array */
    if (row->values) {
        size_t value_count = fi_array_count(row->values);
        total_size += sizeof(size_t); /* value count */
        
        for (size_t i = 0; i < value_count; i++) {
            rdb_value_t **value_ptr = (rdb_value_t**)fi_array_get(row->values, i);
            if (value_ptr && *value_ptr) {
                void *value_data = NULL;
                size_t value_data_size = 0;
                
                if (rdb_serialize_value(*value_ptr, &value_data, &value_data_size) == 0) {
                    total_size += sizeof(size_t) + value_data_size; /* size + data */
                    free(value_data);
                }
            }
        }
    }
    
    /* Allocate buffer */
    void *buffer = malloc(total_size);
    if (!buffer) return -1;
    
    char *ptr = (char*)buffer;
    
    /* Write row_id */
    memcpy(ptr, &row->row_id, sizeof(size_t));
    ptr += sizeof(size_t);
    
    /* Write values */
    if (row->values) {
        size_t value_count = fi_array_count(row->values);
        memcpy(ptr, &value_count, sizeof(size_t));
        ptr += sizeof(size_t);
        
        for (size_t i = 0; i < value_count; i++) {
            rdb_value_t **value_ptr = (rdb_value_t**)fi_array_get(row->values, i);
            if (value_ptr && *value_ptr) {
                void *value_data = NULL;
                size_t value_data_size = 0;
                
                if (rdb_serialize_value(*value_ptr, &value_data, &value_data_size) == 0) {
                    memcpy(ptr, &value_data_size, sizeof(size_t));
                    ptr += sizeof(size_t);
                    memcpy(ptr, value_data, value_data_size);
                    ptr += value_data_size;
                    free(value_data);
                }
            }
        }
    } else {
        size_t zero = 0;
        memcpy(ptr, &zero, sizeof(size_t));
    }
    
    *data = buffer;
    *data_size = total_size;
    return 0;
}

/* Deserialize a row */
int rdb_deserialize_row(const void *data, size_t data_size, rdb_row_t **row) {
    if (!data || !row || data_size < sizeof(size_t)) return -1;
    
    rdb_row_t *r = malloc(sizeof(rdb_row_t));
    if (!r) return -1;
    
    const char *ptr = (const char*)data;
    
    /* Read row_id */
    memcpy(&r->row_id, ptr, sizeof(size_t));
    ptr += sizeof(size_t);
    
    /* Read values */
    if (ptr < (const char*)data + data_size) {
        size_t value_count;
        memcpy(&value_count, ptr, sizeof(size_t));
        ptr += sizeof(size_t);
        
        r->values = fi_array_create(value_count, sizeof(rdb_value_t*));
        if (!r->values) {
            free(r);
            return -1;
        }
        
        for (size_t i = 0; i < value_count; i++) {
            if (ptr >= (const char*)data + data_size) break;
            
            size_t value_data_size;
            memcpy(&value_data_size, ptr, sizeof(size_t));
            ptr += sizeof(size_t);
            
            if (ptr + value_data_size > (const char*)data + data_size) break;
            
            rdb_value_t *value = NULL;
            if (rdb_deserialize_value(ptr, value_data_size, &value) == 0) {
                fi_array_push(r->values, &value);
            }
            ptr += value_data_size;
        }
    } else {
        r->values = NULL;
    }
    
    *row = r;
    return 0;
}

/* Serialize a table */
int rdb_serialize_table(rdb_table_t *table, void **data, size_t *data_size) {
    if (!table || !data || !data_size) return -1;
    
    size_t total_size = 0;
    
    /* Calculate total size */
    total_size += 64; /* table name */
    total_size += sizeof(size_t); /* column count */
    total_size += sizeof(size_t); /* row count */
    total_size += 64; /* primary key */
    total_size += sizeof(size_t); /* next_row_id */
    
    /* Calculate columns size */
    if (table->columns) {
        size_t column_count = fi_array_count(table->columns);
        total_size += column_count * sizeof(rdb_column_t);
    }
    
    /* Calculate rows size */
    if (table->rows) {
        size_t row_count = fi_array_count(table->rows);
        for (size_t i = 0; i < row_count; i++) {
            rdb_row_t **row_ptr = (rdb_row_t**)fi_array_get(table->rows, i);
            if (row_ptr && *row_ptr) {
                void *row_data = NULL;
                size_t row_data_size = 0;
                if (rdb_serialize_row(*row_ptr, &row_data, &row_data_size) == 0) {
                    total_size += sizeof(size_t) + row_data_size;
                    free(row_data);
                }
            }
        }
    }
    
    /* Allocate buffer */
    void *buffer = malloc(total_size);
    if (!buffer) return -1;
    
    char *ptr = (char*)buffer;
    
    /* Write table name */
    strncpy(ptr, table->name, 63);
    ptr[63] = '\0';
    ptr += 64;
    
    /* Write column count */
    size_t column_count = table->columns ? fi_array_count(table->columns) : 0;
    memcpy(ptr, &column_count, sizeof(size_t));
    ptr += sizeof(size_t);
    
    /* Write columns */
    if (table->columns) {
        for (size_t i = 0; i < column_count; i++) {
            rdb_column_t **col_ptr = (rdb_column_t**)fi_array_get(table->columns, i);
            if (col_ptr && *col_ptr) {
                memcpy(ptr, *col_ptr, sizeof(rdb_column_t));
                ptr += sizeof(rdb_column_t);
            }
        }
    }
    
    /* Write row count */
    size_t row_count = table->rows ? fi_array_count(table->rows) : 0;
    memcpy(ptr, &row_count, sizeof(size_t));
    ptr += sizeof(size_t);
    
    /* Write rows */
    if (table->rows) {
        for (size_t i = 0; i < row_count; i++) {
            rdb_row_t **row_ptr = (rdb_row_t**)fi_array_get(table->rows, i);
            if (row_ptr && *row_ptr) {
                void *row_data = NULL;
                size_t row_data_size = 0;
                if (rdb_serialize_row(*row_ptr, &row_data, &row_data_size) == 0) {
                    memcpy(ptr, &row_data_size, sizeof(size_t));
                    ptr += sizeof(size_t);
                    memcpy(ptr, row_data, row_data_size);
                    ptr += row_data_size;
                    free(row_data);
                }
            }
        }
    }
    
    /* Write primary key */
    strncpy(ptr, table->primary_key, 63);
    ptr[63] = '\0';
    ptr += 64;
    
    /* Write next_row_id */
    memcpy(ptr, &table->next_row_id, sizeof(size_t));
    
    *data = buffer;
    *data_size = total_size;
    return 0;
}

/* Deserialize a table */
int rdb_deserialize_table(const void *data, size_t data_size, rdb_table_t **table) {
    if (!data || !table) return -1;
    
    rdb_table_t *t = malloc(sizeof(rdb_table_t));
    if (!t) return -1;
    
    const char *ptr = (const char*)data;
    
    /* Read table name */
    strncpy(t->name, ptr, 63);
    t->name[63] = '\0';
    ptr += 64;
    
    /* Read column count */
    size_t column_count;
    memcpy(&column_count, ptr, sizeof(size_t));
    ptr += sizeof(size_t);
    
    /* Read columns */
    if (column_count > 0) {
        t->columns = fi_array_create(column_count, sizeof(rdb_column_t*));
        if (!t->columns) {
            free(t);
            return -1;
        }
        
        for (size_t i = 0; i < column_count; i++) {
            rdb_column_t *column = malloc(sizeof(rdb_column_t));
            if (!column) {
                fi_array_destroy(t->columns);
                free(t);
                return -1;
            }
            memcpy(column, ptr, sizeof(rdb_column_t));
            ptr += sizeof(rdb_column_t);
            fi_array_push(t->columns, &column);
        }
    } else {
        t->columns = NULL;
    }
    
    /* Read row count */
    size_t row_count;
    memcpy(&row_count, ptr, sizeof(size_t));
    ptr += sizeof(size_t);
    
    /* Read rows */
    if (row_count > 0) {
        t->rows = fi_array_create(row_count, sizeof(rdb_row_t*));
        if (!t->rows) {
            if (t->columns) fi_array_destroy(t->columns);
            free(t);
            return -1;
        }
        
        for (size_t i = 0; i < row_count; i++) {
            size_t row_data_size;
            memcpy(&row_data_size, ptr, sizeof(size_t));
            ptr += sizeof(size_t);
            
            rdb_row_t *row = NULL;
            if (rdb_deserialize_row(ptr, row_data_size, &row) == 0) {
                fi_array_push(t->rows, &row);
            }
            ptr += row_data_size;
        }
    } else {
        t->rows = NULL;
    }
    
    /* Read primary key */
    strncpy(t->primary_key, ptr, 63);
    t->primary_key[63] = '\0';
    ptr += 64;
    
    /* Read next_row_id */
    memcpy(&t->next_row_id, ptr, sizeof(size_t));
    
    /* Initialize other fields */
    t->indexes = NULL;
    pthread_mutex_init(&t->rwlock, NULL);
    pthread_mutex_init(&t->mutex, NULL);
    
    *table = t;
    return 0;
}

/* ===== DATABASE PERSISTENCE FUNCTIONS ===== */

/* Save database header to disk */
static int rdb_persistence_save_header(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    /* Update header with current database info */
    strncpy(pm->header.magic, RDB_MAGIC_NUMBER, 15);
    pm->header.magic[15] = '\0';
    pm->header.version = RDB_VERSION;
    pm->header.created_time = time(NULL);
    pm->header.last_checkpoint = time(NULL);
    pm->header.next_page_id = 1;
    pm->header.total_pages = 1; /* At least header page */
    pm->header.wal_sequence = pm->wal ? pm->wal->sequence_number : 0;
    pm->header.table_count = db->tables ? fi_map_size(db->tables) : 0;
    pm->header.checksum = 0; /* Clear checksum field before calculation */
    pm->header.checksum = rdb_persistence_calculate_checksum(&pm->header, sizeof(rdb_persistent_header_t) - sizeof(uint32_t));
    
    /* Write header to file */
    if (lseek(pm->db_fd, 0, SEEK_SET) == -1) return -1;
    if (write(pm->db_fd, &pm->header, sizeof(rdb_persistent_header_t)) != sizeof(rdb_persistent_header_t)) {
        return -1;
    }
    
    return 0;
}

/* Load database header from disk */
static int rdb_persistence_load_header(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    /* Read header from file */
    if (lseek(pm->db_fd, 0, SEEK_SET) == -1) {
        printf("DEBUG: Failed to seek to beginning of file\n");
        return -1;
    }
    if (read(pm->db_fd, &pm->header, sizeof(rdb_persistent_header_t)) != sizeof(rdb_persistent_header_t)) {
        printf("DEBUG: Failed to read header from file\n");
        return -1;
    }
    
    /* Verify magic number */
    printf("DEBUG: Read magic number: '%.15s'\n", pm->header.magic);
    printf("DEBUG: Expected magic number: '%s'\n", RDB_MAGIC_NUMBER);
    if (strcmp(pm->header.magic, RDB_MAGIC_NUMBER) != 0) {
        printf("DEBUG: Magic number verification failed\n");
        return -1;
    }
    
    /* Verify checksum */
    uint32_t stored_checksum = pm->header.checksum;
    pm->header.checksum = 0; /* Clear checksum field before calculation */
    uint32_t calculated_checksum = rdb_persistence_calculate_checksum(&pm->header, sizeof(rdb_persistent_header_t) - sizeof(uint32_t));
    pm->header.checksum = stored_checksum; /* Restore checksum field */
    printf("DEBUG: Calculated checksum: %u, Stored checksum: %u\n", calculated_checksum, stored_checksum);
    if (calculated_checksum != stored_checksum) {
        printf("DEBUG: Checksum verification failed\n");
        return -1;
    }
    
    printf("DEBUG: Header loaded successfully\n");
    return 0;
}

/* Save a single table to disk */
static int rdb_persistence_save_table(rdb_persistence_manager_t *pm, rdb_table_t *table) {
    if (!pm || !table) return -1;
    
    /* Serialize table */
    void *table_data = NULL;
    size_t table_data_size = 0;
    
    if (rdb_serialize_table(table, &table_data, &table_data_size) != 0) {
        return -1;
    }
    
    /* Create table file path */
    char table_file_path[512];
    sprintf(table_file_path, "%s/table_%s.rdb", pm->data_dir, table->name);
    
    /* Open table file */
    int table_fd = open(table_file_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (table_fd < 0) {
        free(table_data);
        return -1;
    }
    
    /* Write table data */
    ssize_t written = write(table_fd, table_data, table_data_size);
    close(table_fd);
    free(table_data);
    
    if (written != (ssize_t)table_data_size) {
        return -1;
    }
    
    /* Update table metadata */
    rdb_persistent_table_metadata_t *metadata = malloc(sizeof(rdb_persistent_table_metadata_t));
    if (!metadata) return -1;
    
    strncpy(metadata->table_name, table->name, 63);
    metadata->table_name[63] = '\0';
    metadata->first_page_id = 1;
    metadata->last_page_id = 1;
    metadata->row_count = table->rows ? fi_array_count(table->rows) : 0;
    metadata->total_pages = 1;
    metadata->created_time = time(NULL);
    metadata->last_modified = time(NULL);
    metadata->is_compressed = false;
    metadata->compression_type = 0;
    
    /* Store metadata */
    const char *table_name_ptr = table->name;
    fi_map_put(pm->table_metadata, &table_name_ptr, &metadata);
    
    return 0;
}

/* Load all tables from disk */
static int rdb_persistence_load_tables(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    /* Open data directory to discover all table files */
    DIR *dir = opendir(pm->data_dir);
    if (!dir) {
        printf("DEBUG: Could not open data directory: %s\n", pm->data_dir);
        return -1;
    }
    
    struct dirent *entry;
    int tables_loaded = 0;
    
    while ((entry = readdir(dir)) != NULL) {
        /* Look for table files with pattern "table_*.rdb" */
        if (strncmp(entry->d_name, "table_", 6) == 0 && 
            strcmp(entry->d_name + strlen(entry->d_name) - 4, ".rdb") == 0) {
            
            /* Extract table name from filename */
            char table_name[256];
            size_t name_len = strlen(entry->d_name) - 10; /* "table_" (6) + ".rdb" (4) */
            if (name_len > 0 && name_len < sizeof(table_name)) {
                strncpy(table_name, entry->d_name + 6, name_len);
                table_name[name_len] = '\0';
                
                printf("DEBUG: Loading table: %s\n", table_name);
                if (rdb_persistence_load_table(pm, db, table_name) != 0) {
                    printf("DEBUG: Failed to load table: %s\n", table_name);
                    /* Continue loading other tables even if one fails */
                    continue;
                }
                printf("DEBUG: Successfully loaded table: %s\n", table_name);
                tables_loaded++;
            }
        }
    }
    
    closedir(dir);
    printf("DEBUG: Loaded %d tables from disk\n", tables_loaded);
    return 0;
}

/* Load a single table from disk */
static int rdb_persistence_load_table(rdb_persistence_manager_t *pm, rdb_database_t *db, const char *table_name) {
    if (!pm || !db || !table_name) return -1;
    
    /* Create table file path */
    char table_file_path[512];
    sprintf(table_file_path, "%s/table_%s.rdb", pm->data_dir, table_name);
    
    /* Open table file */
    int table_fd = open(table_file_path, O_RDONLY);
    if (table_fd < 0) return -1;
    
    /* Get file size */
    struct stat st;
    if (fstat(table_fd, &st) != 0) {
        close(table_fd);
        return -1;
    }
    
    size_t file_size = st.st_size;
    
    /* Read table data */
    void *table_data = malloc(file_size);
    if (!table_data) {
        close(table_fd);
        return -1;
    }
    
    ssize_t read_bytes = read(table_fd, table_data, file_size);
    close(table_fd);
    
    if (read_bytes != (ssize_t)file_size) {
        free(table_data);
        return -1;
    }
    
    /* Deserialize table */
    rdb_table_t *table = NULL;
    if (rdb_deserialize_table(table_data, file_size, &table) != 0) {
        free(table_data);
        return -1;
    }
    
    free(table_data);
    
    /* Allocate memory for table name to ensure it persists */
    char *table_name_copy = malloc(strlen(table_name) + 1);
    if (!table_name_copy) {
        rdb_destroy_table(table);
        return -1;
    }
    strcpy(table_name_copy, table_name);
    
    /* Add table to database */
    if (fi_map_put(db->tables, &table_name_copy, &table) != 0) {
        free(table_name_copy);
        rdb_destroy_table(table);
        return -1;
    }
    
    return 0;
}

/* Save foreign key constraints */
static int rdb_persistence_save_foreign_keys(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    /* Create foreign keys file path */
    char fk_file_path[512];
    sprintf(fk_file_path, "%s/foreign_keys.rdb", pm->data_dir);
    
    /* Open foreign keys file */
    int fk_fd = open(fk_file_path, O_CREAT | O_WRONLY | O_TRUNC, 0644);
    if (fk_fd < 0) return -1;
    
    /* Write foreign key count */
    size_t fk_count = db->foreign_keys ? fi_map_size(db->foreign_keys) : 0;
    if (write(fk_fd, &fk_count, sizeof(size_t)) != sizeof(size_t)) {
        close(fk_fd);
        return -1;
    }
    
    /* Write foreign keys */
    if (db->foreign_keys && fk_count > 0) {
        fi_map_iterator iter = fi_map_iterator_create(db->foreign_keys);
        while (fi_map_iterator_next(&iter)) {
            const char *constraint_name = (const char*)fi_map_iterator_key(&iter);
            rdb_foreign_key_t **fk_ptr = (rdb_foreign_key_t**)fi_map_iterator_value(&iter);
            
            if (fk_ptr && *fk_ptr) {
                /* Write constraint name length and name */
                size_t name_len = strlen(constraint_name);
                if (write(fk_fd, &name_len, sizeof(size_t)) != sizeof(size_t)) {
                    close(fk_fd);
                    return -1;
                }
                if (write(fk_fd, constraint_name, name_len) != (ssize_t)name_len) {
                    close(fk_fd);
                    return -1;
                }
                
                /* Write foreign key structure */
                if (write(fk_fd, *fk_ptr, sizeof(rdb_foreign_key_t)) != sizeof(rdb_foreign_key_t)) {
                    close(fk_fd);
                    return -1;
                }
            }
        }
    }
    
    close(fk_fd);
    return 0;
}

/* Load foreign key constraints */
static int rdb_persistence_load_foreign_keys(rdb_persistence_manager_t *pm, rdb_database_t *db) {
    if (!pm || !db) return -1;
    
    /* Create foreign keys file path */
    char fk_file_path[512];
    sprintf(fk_file_path, "%s/foreign_keys.rdb", pm->data_dir);
    
    /* Check if file exists */
    if (!rdb_persistence_file_exists(fk_file_path)) {
        return 0; /* No foreign keys to load */
    }
    
    /* Open foreign keys file */
    int fk_fd = open(fk_file_path, O_RDONLY);
    if (fk_fd < 0) return -1;
    
    /* Read foreign key count */
    size_t fk_count;
    if (read(fk_fd, &fk_count, sizeof(size_t)) != sizeof(size_t)) {
        close(fk_fd);
        return -1;
    }
    
    /* Read foreign keys */
    for (size_t i = 0; i < fk_count; i++) {
        /* Read constraint name length and name */
        size_t name_len;
        if (read(fk_fd, &name_len, sizeof(size_t)) != sizeof(size_t)) {
            close(fk_fd);
            return -1;
        }
        
        char *constraint_name = malloc(name_len + 1);
        if (!constraint_name) {
            close(fk_fd);
            return -1;
        }
        
        if (read(fk_fd, constraint_name, name_len) != (ssize_t)name_len) {
            free(constraint_name);
            close(fk_fd);
            return -1;
        }
        constraint_name[name_len] = '\0';
        
        /* Read foreign key structure */
        rdb_foreign_key_t *fk = malloc(sizeof(rdb_foreign_key_t));
        if (!fk) {
            free(constraint_name);
            close(fk_fd);
            return -1;
        }
        
        if (read(fk_fd, fk, sizeof(rdb_foreign_key_t)) != sizeof(rdb_foreign_key_t)) {
            free(constraint_name);
            free(fk);
            close(fk_fd);
            return -1;
        }
        
        /* Add to database */
        if (fi_map_put(db->foreign_keys, constraint_name, &fk) != 0) {
            free(constraint_name);
            free(fk);
            close(fk_fd);
            return -1;
        }
        
        free(constraint_name);
    }
    
    close(fk_fd);
    return 0;
}
