#include "cache_system.h"
#include <errno.h>
#include <sys/stat.h>
#include <float.h>

/* Forward declarations */
static int rdb_cache_level_evict_entry(rdb_cache_level_t *level, rdb_cache_entry_t *entry);
static rdb_cache_entry_t* rdb_cache_find_entry(rdb_cache_system_t *cache_system, 
                                               const void *key, size_t key_size);
/* Removed unused function declarations */
static void rdb_cache_update_stats(rdb_cache_level_t *level, bool is_hit);
static int rdb_cache_should_evict(rdb_cache_level_t *level);

/* Hash function for cache keys */
uint32_t rdb_cache_hash_key(const void *key, size_t key_size) {
    const uint8_t *data = (const uint8_t*)key;
    uint32_t hash = 0x811c9dc5;  /* FNV offset basis */
    
    for (size_t i = 0; i < key_size; i++) {
        hash ^= data[i];
        hash *= 0x01000193;  /* FNV prime */
    }
    
    return hash;
}

/* Key comparison function */
int rdb_cache_compare_keys(const void *key1, const void *key2) {
    const rdb_cache_entry_t *entry1 = (const rdb_cache_entry_t*)key1;
    const rdb_cache_entry_t *entry2 = (const rdb_cache_entry_t*)key2;
    
    if (entry1->key_size != entry2->key_size) {
        return entry1->key_size - entry2->key_size;
    }
    
    return memcmp(entry1->key, entry2->key, entry1->key_size);
}

/* Free functions */
void rdb_cache_entry_free(void *entry) {
    if (!entry) return;
    
    rdb_cache_entry_t *e = (rdb_cache_entry_t*)entry;
    if (e->key) free(e->key);
    if (e->value) free(e->value);
    free(e);
}

void rdb_cache_key_free(void *key) {
    if (key) free(key);
}

void rdb_cache_value_free(void *value) {
    if (value) free(value);
}

/* Create cache entry */
static rdb_cache_entry_t* rdb_cache_create_entry(const void *key, size_t key_size,
                                                 const void *value, size_t value_size,
                                                 size_t level) {
    rdb_cache_entry_t *entry = malloc(sizeof(rdb_cache_entry_t));
    if (!entry) return NULL;
    
    /* Allocate and copy key */
    entry->key = malloc(key_size);
    if (!entry->key) {
        free(entry);
        return NULL;
    }
    memcpy(entry->key, key, key_size);
    
    /* Allocate and copy value */
    entry->value = malloc(value_size);
    if (!entry->value) {
        free(entry->key);
        free(entry);
        return NULL;
    }
    memcpy(entry->value, value, value_size);
    
    /* Initialize entry fields */
    entry->key_size = key_size;
    entry->value_size = value_size;
    entry->level = level;
    entry->last_access_time = time(NULL);
    entry->access_count = 1;
    entry->access_frequency = 1;
    entry->access_score = 1.0;
    entry->is_dirty = true;
    entry->is_pinned = false;
    entry->reference_count = 1;
    entry->prev = NULL;
    entry->next = NULL;
    entry->hash_next = NULL;
    
    return entry;
}

/* Create cache level */
rdb_cache_level_t* rdb_cache_level_create(const rdb_cache_level_config_t *config) {
    if (!config) return NULL;
    
    rdb_cache_level_t *level = malloc(sizeof(rdb_cache_level_t));
    if (!level) return NULL;
    
    /* Copy configuration */
    memcpy(&level->config, config, sizeof(rdb_cache_level_config_t));
    
    /* Initialize statistics */
    memset(&level->stats, 0, sizeof(rdb_cache_stats_t));
    level->stats.last_reset = time(NULL);
    
    /* Create hash map for entries */
    level->entries = fi_map_create_with_destructors(
        config->max_entries / 2,  /* Initial capacity */
        sizeof(rdb_cache_entry_t*),
        sizeof(rdb_cache_entry_t*),
        rdb_cache_hash_key,
        rdb_cache_compare_keys,
        rdb_cache_entry_free,
        NULL
    );
    
    if (!level->entries) {
        free(level);
        return NULL;
    }
    
    /* Create entry list for eviction algorithms */
    level->entry_list = fi_array_create(config->max_entries, sizeof(rdb_cache_entry_t*));
    if (!level->entry_list) {
        fi_map_destroy(level->entries);
        free(level);
        return NULL;
    }
    
    /* Initialize algorithm-specific data */
    switch (config->algorithm) {
        case RDB_CACHE_LRU:
            level->algorithm_data.lru.head = NULL;
            level->algorithm_data.lru.tail = NULL;
            break;
            
        case RDB_CACHE_LFU:
            level->algorithm_data.lfu.frequency_tree = fi_btree_create(
                sizeof(uint32_t), NULL
            );
            break;
            
        case RDB_CACHE_ARC:
            level->algorithm_data.arc.t1 = fi_map_create_string_ptr(1024);
            level->algorithm_data.arc.t2 = fi_map_create_string_ptr(1024);
            level->algorithm_data.arc.b1 = fi_map_create_string_ptr(1024);
            level->algorithm_data.arc.b2 = fi_map_create_string_ptr(1024);
            level->algorithm_data.arc.p = config->max_entries / 2;
            break;
            
        case RDB_CACHE_W_TINY_LFU:
            level->algorithm_data.wtinylfu.window_cache = fi_map_create_string_ptr(1024);
            level->algorithm_data.wtinylfu.main_cache = fi_map_create_string_ptr(1024);
            level->algorithm_data.wtinylfu.frequency_sketch = fi_btree_create(
                sizeof(uint32_t), NULL
            );
            break;
            
        case RDB_CACHE_AURA:
            level->algorithm_data.aura.stability_map = fi_map_create_string_ptr(1024);
            level->algorithm_data.aura.value_map = fi_map_create_string_ptr(1024);
            level->algorithm_data.aura.alpha = 0.5;  /* Balanced exploration/exploitation */
            break;
    }
    
    /* Initialize persistence */
    level->disk_path = NULL;
    level->disk_fd = -1;
    level->mmap_ptr = NULL;
    level->mmap_size = 0;
    
    /* Initialize write buffer for disk caches */
    if (!config->is_memory) {
        level->write_buffer = malloc(config->write_buffer_size);
        if (!level->write_buffer) {
            rdb_cache_level_destroy(level);
            return NULL;
        }
        level->write_buffer_pos = 0;
        
        if (pthread_mutex_init(&level->write_buffer_mutex, NULL) != 0) {
            free(level->write_buffer);
            rdb_cache_level_destroy(level);
            return NULL;
        }
    } else {
        level->write_buffer = NULL;
        level->write_buffer_pos = 0;
    }
    
    /* Initialize thread safety */
    if (pthread_rwlock_init(&level->rwlock, NULL) != 0) {
        rdb_cache_level_destroy(level);
        return NULL;
    }
    
    if (pthread_mutex_init(&level->stats_mutex, NULL) != 0) {
        pthread_rwlock_destroy(&level->rwlock);
        rdb_cache_level_destroy(level);
        return NULL;
    }
    
    return level;
}

/* Destroy cache level */
void rdb_cache_level_destroy(rdb_cache_level_t *level) {
    if (!level) return;
    
    /* Clean up algorithm-specific data */
    switch (level->config.algorithm) {
        case RDB_CACHE_LRU:
            /* LRU cleanup is handled by entry list */
            break;
            
        case RDB_CACHE_LFU:
            if (level->algorithm_data.lfu.frequency_tree) {
                fi_btree_destroy(level->algorithm_data.lfu.frequency_tree);
            }
            break;
            
        case RDB_CACHE_ARC:
            if (level->algorithm_data.arc.t1) fi_map_destroy(level->algorithm_data.arc.t1);
            if (level->algorithm_data.arc.t2) fi_map_destroy(level->algorithm_data.arc.t2);
            if (level->algorithm_data.arc.b1) fi_map_destroy(level->algorithm_data.arc.b1);
            if (level->algorithm_data.arc.b2) fi_map_destroy(level->algorithm_data.arc.b2);
            break;
            
        case RDB_CACHE_W_TINY_LFU:
            if (level->algorithm_data.wtinylfu.window_cache) {
                fi_map_destroy(level->algorithm_data.wtinylfu.window_cache);
            }
            if (level->algorithm_data.wtinylfu.main_cache) {
                fi_map_destroy(level->algorithm_data.wtinylfu.main_cache);
            }
            if (level->algorithm_data.wtinylfu.frequency_sketch) {
                fi_btree_destroy(level->algorithm_data.wtinylfu.frequency_sketch);
            }
            break;
            
        case RDB_CACHE_AURA:
            if (level->algorithm_data.aura.stability_map) {
                fi_map_destroy(level->algorithm_data.aura.stability_map);
            }
            if (level->algorithm_data.aura.value_map) {
                fi_map_destroy(level->algorithm_data.aura.value_map);
            }
            break;
    }
    
    /* Clean up storage structures */
    if (level->entries) fi_map_destroy(level->entries);
    if (level->entry_list) fi_array_destroy(level->entry_list);
    
    /* Clean up persistence */
    if (level->disk_path) free(level->disk_path);
    if (level->mmap_ptr) munmap(level->mmap_ptr, level->mmap_size);
    if (level->disk_fd >= 0) close(level->disk_fd);
    
    /* Clean up write buffer */
    if (level->write_buffer) {
        pthread_mutex_destroy(&level->write_buffer_mutex);
        free(level->write_buffer);
    }
    
    /* Clean up thread safety */
    pthread_rwlock_destroy(&level->rwlock);
    pthread_mutex_destroy(&level->stats_mutex);
    
    free(level);
}

/* Create cache system */
rdb_cache_system_t* rdb_cache_system_create(const char *name, size_t num_levels, 
                                           const rdb_cache_level_config_t *configs) {
    if (!name || num_levels == 0 || num_levels > RDB_CACHE_MAX_LEVELS || !configs) {
        return NULL;
    }
    
    rdb_cache_system_t *cache_system = malloc(sizeof(rdb_cache_system_t));
    if (!cache_system) return NULL;
    
    /* Copy name */
    cache_system->name = malloc(strlen(name) + 1);
    if (!cache_system->name) {
        free(cache_system);
        return NULL;
    }
    strcpy(cache_system->name, name);
    
    /* Initialize configuration */
    cache_system->num_levels = num_levels;
    cache_system->auto_tune = true;
    cache_system->target_hit_ratio = 0.8;  /* 80% target hit ratio */
    cache_system->tune_interval = 300;     /* 5 minutes */
    
    /* Initialize global statistics */
    memset(&cache_system->global_stats, 0, sizeof(rdb_cache_stats_t));
    cache_system->global_stats.last_reset = time(NULL);
    cache_system->last_tune_time = time(NULL);
    
    /* Initialize persistence */
    cache_system->persistence_dir = NULL;
    cache_system->enable_persistence = false;
    cache_system->checkpoint_interval = 3600;  /* 1 hour */
    cache_system->last_checkpoint = time(NULL);
    
    /* Create cache levels */
    cache_system->levels = malloc(sizeof(rdb_cache_level_t*) * num_levels);
    if (!cache_system->levels) {
        free(cache_system->name);
        free(cache_system);
        return NULL;
    }
    
    for (size_t i = 0; i < num_levels; i++) {
        cache_system->levels[i] = rdb_cache_level_create(&configs[i]);
        if (!cache_system->levels[i]) {
            /* Clean up previously created levels */
            for (size_t j = 0; j < i; j++) {
                rdb_cache_level_destroy(cache_system->levels[j]);
            }
            free(cache_system->levels);
            free(cache_system->name);
            free(cache_system);
            return NULL;
        }
    }
    
    /* Initialize thread safety */
    if (pthread_rwlock_init(&cache_system->global_rwlock, NULL) != 0) {
        for (size_t i = 0; i < num_levels; i++) {
            rdb_cache_level_destroy(cache_system->levels[i]);
        }
        free(cache_system->levels);
        free(cache_system->name);
        free(cache_system);
        return NULL;
    }
    
    if (pthread_mutex_init(&cache_system->tune_mutex, NULL) != 0) {
        pthread_rwlock_destroy(&cache_system->global_rwlock);
        for (size_t i = 0; i < num_levels; i++) {
            rdb_cache_level_destroy(cache_system->levels[i]);
        }
        free(cache_system->levels);
        free(cache_system->name);
        free(cache_system);
        return NULL;
    }
    
    return cache_system;
}

/* Destroy cache system */
void rdb_cache_system_destroy(rdb_cache_system_t *cache_system) {
    if (!cache_system) return;
    
    /* Destroy all cache levels */
    for (size_t i = 0; i < cache_system->num_levels; i++) {
        rdb_cache_level_destroy(cache_system->levels[i]);
    }
    
    /* Clean up */
    if (cache_system->levels) free(cache_system->levels);
    if (cache_system->name) free(cache_system->name);
    if (cache_system->persistence_dir) free(cache_system->persistence_dir);
    
    /* Clean up thread safety */
    pthread_rwlock_destroy(&cache_system->global_rwlock);
    pthread_mutex_destroy(&cache_system->tune_mutex);
    
    free(cache_system);
}

/* Update cache statistics */
static void rdb_cache_update_stats(rdb_cache_level_t *level, bool is_hit) {
    pthread_mutex_lock(&level->stats_mutex);
    
    level->stats.total_requests++;
    if (is_hit) {
        level->stats.hits++;
    } else {
        level->stats.misses++;
    }
    
    /* Update hit ratio */
    if (level->stats.total_requests > 0) {
        level->stats.hit_ratio = (double)level->stats.hits / level->stats.total_requests;
    }
    
    pthread_mutex_unlock(&level->stats_mutex);
}

/* Check if cache level should evict entries */
static int rdb_cache_should_evict(rdb_cache_level_t *level) {
    return (level->stats.current_size >= level->config.max_size ||
            level->stats.current_entries >= level->config.max_entries);
}

/* Evict entry from cache level */
static int rdb_cache_level_evict_entry(rdb_cache_level_t *level, rdb_cache_entry_t *entry) {
    if (!level || !entry) return -1;
    
    /* Don't evict pinned entries */
    if (entry->is_pinned) return -1;
    
    /* Remove from hash map */
    fi_map_remove(level->entries, &entry);
    
    /* Remove from entry list */
    fi_array *entry_list = level->entry_list;
    for (size_t i = 0; i < fi_array_count(entry_list); i++) {
        rdb_cache_entry_t **entry_ptr = (rdb_cache_entry_t**)fi_array_get(entry_list, i);
        if (entry_ptr && *entry_ptr == entry) {
            fi_array_splice(entry_list, i, 1, NULL);
            break;
        }
    }
    
    /* Update statistics */
    level->stats.evictions++;
    level->stats.current_size -= entry->key_size + entry->value_size;
    level->stats.current_entries--;
    
    /* Free entry */
    rdb_cache_entry_free(entry);
    
    return 0;
}

/* LRU eviction algorithm */
int rdb_cache_lru_evict(rdb_cache_level_t *level) {
    if (!level || level->config.algorithm != RDB_CACHE_LRU) return -1;
    
    /* Find least recently used entry */
    rdb_cache_entry_t *lru_entry = level->algorithm_data.lru.tail;
    while (lru_entry && lru_entry->is_pinned) {
        lru_entry = lru_entry->prev;
    }
    
    if (!lru_entry) return -1;
    
    /* Update LRU list */
    if (lru_entry->prev) {
        lru_entry->prev->next = lru_entry->next;
    } else {
        level->algorithm_data.lru.head = lru_entry->next;
    }
    
    if (lru_entry->next) {
        lru_entry->next->prev = lru_entry->prev;
    } else {
        level->algorithm_data.lru.tail = lru_entry->prev;
    }
    
    return rdb_cache_level_evict_entry(level, lru_entry);
}

/* LFU eviction algorithm */
int rdb_cache_lfu_evict(rdb_cache_level_t *level) {
    if (!level || level->config.algorithm != RDB_CACHE_LFU) return -1;
    
    /* Simple LFU implementation - find entry with lowest frequency in entry list */
    if (fi_array_count(level->entry_list) == 0) return -1;
    
    rdb_cache_entry_t *lfu_entry = NULL;
    uint32_t lowest_frequency = UINT32_MAX;
    
    /* Find entry with lowest access frequency */
    for (size_t i = 0; i < fi_array_count(level->entry_list); i++) {
        rdb_cache_entry_t **entry_ptr = (rdb_cache_entry_t**)fi_array_get(level->entry_list, i);
        if (entry_ptr && *entry_ptr && !(*entry_ptr)->is_pinned) {
            if ((*entry_ptr)->access_frequency < lowest_frequency) {
                lowest_frequency = (*entry_ptr)->access_frequency;
                lfu_entry = *entry_ptr;
            }
        }
    }
    
    if (!lfu_entry) return -1;
    
    return rdb_cache_level_evict_entry(level, lfu_entry);
}

/* W-TinyLFU eviction algorithm (2025 best practice) */
int rdb_cache_wtiny_lfu_evict(rdb_cache_level_t *level) {
    if (!level || level->config.algorithm != RDB_CACHE_W_TINY_LFU) return -1;
    
    /* W-TinyLFU uses a window cache for recent items and main cache with TinyLFU */
    fi_map *window_cache = level->algorithm_data.wtinylfu.window_cache;
    fi_map *main_cache = level->algorithm_data.wtinylfu.main_cache;
    
    /* First try to evict from window cache */
    if (fi_map_size(window_cache) > 0) {
        fi_map_iterator iter = fi_map_iterator_create(window_cache);
        if (iter.is_valid) {
            const char *key = (const char*)fi_map_iterator_key(&iter);
            fi_map_remove(window_cache, key);
            return 0;
        }
    }
    
    /* Then evict from main cache using TinyLFU */
    if (fi_map_size(main_cache) > 0) {
        fi_map_iterator iter = fi_map_iterator_create(main_cache);
        if (iter.is_valid) {
            const char *key = (const char*)fi_map_iterator_key(&iter);
            fi_map_remove(main_cache, key);
            return 0;
        }
    }
    
    return -1;
}

/* AURA eviction algorithm (2025 stability-aware) */
int rdb_cache_aura_evict(rdb_cache_level_t *level) {
    if (!level || level->config.algorithm != RDB_CACHE_AURA) return -1;
    
    fi_map *stability_map = level->algorithm_data.aura.stability_map;
    fi_map *value_map = level->algorithm_data.aura.value_map;
    
    /* Find entry with lowest AURA score (combination of stability and value) */
    double lowest_score = DBL_MAX;
    const char *evict_key = NULL;
    
    fi_map_iterator iter = fi_map_iterator_create(stability_map);
    while (fi_map_iterator_next(&iter)) {
        const char *key = (const char*)fi_map_iterator_key(&iter);
        double *stability = (double*)fi_map_iterator_value(&iter);
        
        double *value = NULL;
        fi_map_get(value_map, key, &value);
        
        if (stability && value) {
            double aura_score = level->algorithm_data.aura.alpha * (*stability) + 
                              (1.0 - level->algorithm_data.aura.alpha) * (*value);
            
            if (aura_score < lowest_score) {
                lowest_score = aura_score;
                evict_key = key;
            }
        }
    }
    
    if (evict_key) {
        fi_map_remove(stability_map, evict_key);
        fi_map_remove(value_map, evict_key);
        return 0;
    }
    
    return -1;
}

/* Generic eviction function */
static int rdb_cache_evict(rdb_cache_level_t *level) {
    if (!level) return -1;
    
    switch (level->config.algorithm) {
        case RDB_CACHE_LRU:
            return rdb_cache_lru_evict(level);
            
        case RDB_CACHE_LFU:
            return rdb_cache_lfu_evict(level);
            
        case RDB_CACHE_W_TINY_LFU:
            return rdb_cache_wtiny_lfu_evict(level);
            
        case RDB_CACHE_AURA:
            return rdb_cache_aura_evict(level);
            
        default:
            return rdb_cache_lru_evict(level);  /* Default to LRU */
    }
}

/* Find cache entry across all levels */
static rdb_cache_entry_t* rdb_cache_find_entry(rdb_cache_system_t *cache_system, 
                                               const void *key, size_t key_size) {
    if (!cache_system || !key) return NULL;
    
    for (size_t i = 0; i < cache_system->num_levels; i++) {
        rdb_cache_level_t *level = cache_system->levels[i];
        pthread_rwlock_rdlock(&level->rwlock);
        
        /* Create temporary entry for search */
        rdb_cache_entry_t temp_entry;
        temp_entry.key = (void*)key;
        temp_entry.key_size = key_size;
        
        rdb_cache_entry_t *entry = NULL;
        if (fi_map_get(level->entries, &temp_entry, &entry) == 0 && entry) {
            pthread_rwlock_unlock(&level->rwlock);
            return entry;
        }
        
        pthread_rwlock_unlock(&level->rwlock);
    }
    
    return NULL;
}

/* Get value from cache */
int rdb_cache_get(rdb_cache_system_t *cache_system, const void *key, size_t key_size, 
                  void **value, size_t *value_size) {
    if (!cache_system || !key || !value || !value_size) return -1;
    
    pthread_rwlock_rdlock(&cache_system->global_rwlock);
    
    rdb_cache_entry_t *entry = rdb_cache_find_entry(cache_system, key, key_size);
    if (!entry) {
        pthread_rwlock_unlock(&cache_system->global_rwlock);
        return -1;  /* Cache miss */
    }
    
    /* Update access statistics */
    entry->last_access_time = time(NULL);
    entry->access_count++;
    entry->access_frequency = (entry->access_frequency * 7 + entry->access_count) / 8;
    
    /* Copy value */
    *value = malloc(entry->value_size);
    if (!*value) {
        pthread_rwlock_unlock(&cache_system->global_rwlock);
        return -1;
    }
    memcpy(*value, entry->value, entry->value_size);
    *value_size = entry->value_size;
    
    /* Update statistics */
    rdb_cache_level_t *level = cache_system->levels[entry->level];
    rdb_cache_update_stats(level, true);
    /* Update global stats manually since we don't have a level for it */
    pthread_mutex_lock(&cache_system->tune_mutex);
    cache_system->global_stats.total_requests++;
    cache_system->global_stats.hits++;
    if (cache_system->global_stats.total_requests > 0) {
        cache_system->global_stats.hit_ratio = (double)cache_system->global_stats.hits / cache_system->global_stats.total_requests;
    }
    pthread_mutex_unlock(&cache_system->tune_mutex);
    
    pthread_rwlock_unlock(&cache_system->global_rwlock);
    return 0;
}

/* Put value into cache */
int rdb_cache_put(rdb_cache_system_t *cache_system, const void *key, size_t key_size,
                  const void *value, size_t value_size, bool pin_entry) {
    if (!cache_system || !key || !value) return -1;
    
    pthread_rwlock_wrlock(&cache_system->global_rwlock);
    
    /* Check if entry already exists */
    rdb_cache_entry_t *existing_entry = rdb_cache_find_entry(cache_system, key, key_size);
    if (existing_entry) {
        /* Update existing entry */
        if (existing_entry->value_size != value_size) {
            free(existing_entry->value);
            existing_entry->value = malloc(value_size);
            if (!existing_entry->value) {
                pthread_rwlock_unlock(&cache_system->global_rwlock);
                return -1;
            }
        }
        memcpy(existing_entry->value, value, value_size);
        existing_entry->value_size = value_size;
        existing_entry->is_dirty = true;
        existing_entry->is_pinned = pin_entry;
        existing_entry->last_access_time = time(NULL);
        existing_entry->access_count++;
        
        pthread_rwlock_unlock(&cache_system->global_rwlock);
        return 0;
    }
    
    /* Create new entry at level 0 (fastest level) */
    rdb_cache_level_t *target_level = cache_system->levels[0];
    rdb_cache_entry_t *entry = rdb_cache_create_entry(key, key_size, value, value_size, 0);
    if (!entry) {
        pthread_rwlock_unlock(&cache_system->global_rwlock);
        return -1;
    }
    
    entry->is_pinned = pin_entry;
    
    pthread_rwlock_wrlock(&target_level->rwlock);
    
    /* Check if we need to evict */
    while (rdb_cache_should_evict(target_level)) {
        if (rdb_cache_evict(target_level) != 0) {
            /* No more entries can be evicted */
            break;
        }
    }
    
    /* Add to cache */
    if (fi_map_put(target_level->entries, &entry, &entry) == 0) {
        fi_array_push(target_level->entry_list, &entry);
        
        /* Update statistics */
        target_level->stats.current_size += key_size + value_size;
        target_level->stats.current_entries++;
        target_level->stats.writes++;
        
        /* Update LRU list if using LRU algorithm */
        if (target_level->config.algorithm == RDB_CACHE_LRU) {
            if (!target_level->algorithm_data.lru.head) {
                target_level->algorithm_data.lru.head = entry;
                target_level->algorithm_data.lru.tail = entry;
            } else {
                entry->next = target_level->algorithm_data.lru.head;
                target_level->algorithm_data.lru.head->prev = entry;
                target_level->algorithm_data.lru.head = entry;
            }
        }
        
        pthread_rwlock_unlock(&target_level->rwlock);
        pthread_rwlock_unlock(&cache_system->global_rwlock);
        return 0;
    }
    
    pthread_rwlock_unlock(&target_level->rwlock);
    rdb_cache_entry_free(entry);
    pthread_rwlock_unlock(&cache_system->global_rwlock);
    return -1;
}

/* Remove value from cache */
int rdb_cache_remove(rdb_cache_system_t *cache_system, const void *key, size_t key_size) {
    if (!cache_system || !key) return -1;
    
    pthread_rwlock_wrlock(&cache_system->global_rwlock);
    
    /* Find entry across all levels */
    rdb_cache_entry_t *entry = rdb_cache_find_entry(cache_system, key, key_size);
    if (!entry) {
        pthread_rwlock_unlock(&cache_system->global_rwlock);
        return -1;
    }
    
    /* Remove from the level where it was found */
    rdb_cache_level_t *level = cache_system->levels[entry->level];
    pthread_rwlock_wrlock(&level->rwlock);
    
    /* Remove from hash map */
    fi_map_remove(level->entries, &entry);
    
    /* Remove from entry list */
    for (size_t i = 0; i < fi_array_count(level->entry_list); i++) {
        rdb_cache_entry_t **entry_ptr = (rdb_cache_entry_t**)fi_array_get(level->entry_list, i);
        if (entry_ptr && *entry_ptr == entry) {
            fi_array_splice(level->entry_list, i, 1, NULL);
            break;
        }
    }
    
    /* Update statistics */
    level->stats.current_size -= entry->key_size + entry->value_size;
    level->stats.current_entries--;
    
    /* Free entry */
    rdb_cache_entry_free(entry);
    
    pthread_rwlock_unlock(&level->rwlock);
    pthread_rwlock_unlock(&cache_system->global_rwlock);
    
    return 0;
}

/* Clear all cache levels */
int rdb_cache_clear(rdb_cache_system_t *cache_system) {
    if (!cache_system) return -1;
    
    pthread_rwlock_wrlock(&cache_system->global_rwlock);
    
    for (size_t i = 0; i < cache_system->num_levels; i++) {
        rdb_cache_level_t *level = cache_system->levels[i];
        pthread_rwlock_wrlock(&level->rwlock);
        
        /* Clear hash map */
        fi_map_clear(level->entries);
        
        /* Clear entry list */
        fi_array_destroy(level->entry_list);
        level->entry_list = fi_array_create(level->config.max_entries, sizeof(rdb_cache_entry_t*));
        
        /* Reset statistics */
        level->stats.current_size = 0;
        level->stats.current_entries = 0;
        
        pthread_rwlock_unlock(&level->rwlock);
    }
    
    pthread_rwlock_unlock(&cache_system->global_rwlock);
    
    return 0;
}

/* Auto-tuning function */
int rdb_cache_tune(rdb_cache_system_t *cache_system) {
    if (!cache_system) return -1;
    
    pthread_mutex_lock(&cache_system->tune_mutex);
    
    /* Simple auto-tuning: adjust cache sizes based on hit ratios */
    for (size_t i = 0; i < cache_system->num_levels; i++) {
        rdb_cache_level_t *level = cache_system->levels[i];
        
        if (level->stats.hit_ratio < cache_system->target_hit_ratio) {
            /* Increase cache size if hit ratio is too low */
            level->config.max_size = (size_t)(level->config.max_size * 1.1);
        } else if (level->stats.hit_ratio > cache_system->target_hit_ratio + 0.05) {
            /* Decrease cache size if hit ratio is too high */
            level->config.max_size = (size_t)(level->config.max_size * 0.95);
        }
    }
    
    pthread_mutex_unlock(&cache_system->tune_mutex);
    
    return 0;
}

/* Thread-safe versions */
int rdb_cache_get_thread_safe(rdb_cache_system_t *cache_system, const void *key, size_t key_size, 
                              void **value, size_t *value_size) {
    return rdb_cache_get(cache_system, key, key_size, value, value_size);
}

int rdb_cache_put_thread_safe(rdb_cache_system_t *cache_system, const void *key, size_t key_size,
                              const void *value, size_t value_size, bool pin_entry) {
    return rdb_cache_put(cache_system, key, key_size, value, value_size, pin_entry);
}

int rdb_cache_remove_thread_safe(rdb_cache_system_t *cache_system, const void *key, size_t key_size) {
    return rdb_cache_remove(cache_system, key, key_size);
}

/* Print cache statistics */
void rdb_cache_print_stats(rdb_cache_system_t *cache_system) {
    if (!cache_system) return;
    
    printf("=== Cache System Statistics: %s ===\n", cache_system->name);
    printf("Total Requests: %lu\n", cache_system->global_stats.total_requests);
    printf("Total Hits: %lu\n", cache_system->global_stats.hits);
    printf("Total Misses: %lu\n", cache_system->global_stats.misses);
    printf("Hit Ratio: %.2f%%\n", cache_system->global_stats.hit_ratio * 100);
    printf("Total Evictions: %lu\n", cache_system->global_stats.evictions);
    
    printf("\n=== Cache Level Statistics ===\n");
    for (size_t i = 0; i < cache_system->num_levels; i++) {
        rdb_cache_level_t *level = cache_system->levels[i];
        printf("\nLevel %zu:\n", i);
        printf("  Algorithm: %d\n", level->config.algorithm);
        printf("  Max Size: %zu bytes\n", level->config.max_size);
        printf("  Current Size: %zu bytes\n", level->stats.current_size);
        printf("  Max Entries: %zu\n", level->config.max_entries);
        printf("  Current Entries: %zu\n", level->stats.current_entries);
        printf("  Hit Ratio: %.2f%%\n", level->stats.hit_ratio * 100);
        printf("  Evictions: %lu\n", level->stats.evictions);
    }
}

/* Configuration functions */
int rdb_cache_set_algorithm(rdb_cache_system_t *cache_system, size_t level, 
                           rdb_cache_algorithm_t algorithm) {
    if (!cache_system || level >= cache_system->num_levels) return -1;
    
    pthread_rwlock_wrlock(&cache_system->global_rwlock);
    cache_system->levels[level]->config.algorithm = algorithm;
    pthread_rwlock_unlock(&cache_system->global_rwlock);
    
    return 0;
}

int rdb_cache_set_size(rdb_cache_system_t *cache_system, size_t level, size_t new_size) {
    if (!cache_system || level >= cache_system->num_levels || new_size == 0) return -1;
    
    pthread_rwlock_wrlock(&cache_system->global_rwlock);
    cache_system->levels[level]->config.max_size = new_size;
    pthread_rwlock_unlock(&cache_system->global_rwlock);
    
    return 0;
}

int rdb_cache_set_auto_tune(rdb_cache_system_t *cache_system, bool enable, double target_ratio) {
    if (!cache_system || target_ratio < 0.0 || target_ratio > 1.0) return -1;
    
    pthread_mutex_lock(&cache_system->tune_mutex);
    cache_system->auto_tune = enable;
    cache_system->target_hit_ratio = target_ratio;
    pthread_mutex_unlock(&cache_system->tune_mutex);
    
    return 0;
}
