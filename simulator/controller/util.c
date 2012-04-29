#include "util.h"

/* Move one key from source server to destination server
   Source server hostname and port are in src_host and src_port
   Destination server hostname and port are in dest_host and dest_port
   Return 1 on success
   Return 0 on failure
*/
int move_key(char *key, char *src_host, in_port_t src_port, char *dest_host,     in_port_t dest_port) {
    memcached_server_st *src_server = NULL;
    memcached_server_st *dest_server = NULL;
    memcached_st *src_memc;
    memcached_st *dest_memc;
    memcached_return rc;
    size_t value_len;
    uint32_t flags;
    char *value = NULL;

    src_memc = memcached_create(NULL);
    dest_memc = memcached_create(NULL);

    src_server = memcached_server_list_append(src_server, src_host, src_port, &rc);
    rc = memcached_server_push(src_memc, src_server);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't add source server: %s\n", memcached_strerror(src_memc, rc));
        memcached_free(src_memc);
        return 0;
    }

    dest_server = memcached_server_list_append(dest_server, dest_host, dest_port, &rc);
    rc = memcached_server_push(dest_memc, dest_server);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't add destination server: %s\n", memcached_strerror(dest_memc, rc));
        memcached_free(src_memc);
        memcached_free(dest_memc);
        return 0;
    }

    value = memcached_get(src_memc, key, strlen(key), &value_len, &flags, &rc);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't get key from source server: %s\n", memcached_strerror(src_memc, rc));
        memcached_free(src_memc);
        memcached_free(dest_memc);
        return 0;
    }

    rc = memcached_set(dest_memc, key, strlen(key), value, strlen(value), (time_t)0, (uint32_t)0);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't set key value to destination server: %s\n", memcached_strerror(dest_memc, rc));
        memcached_free(src_memc);
        memcached_free(dest_memc);
        return 0;
    }

    printf("Key moved!\n");

    memcached_free(src_memc);
    memcached_free(dest_memc);
    free(value);

    return 1;
}

/* Move multiple keys from source server to destination server
   source server hostname and port are in src_host and src_port
   Destination server hostname and port are in dest_host and dest_port
   Key number is in key_num
   Return 1 on success
   Return 0 on failure
*/
int mmove_key(char **key, char *src_host, in_port_t src_port, char *dest_host, in_port_t dest_port, int key_num) {
    memcached_st *src_memc;
    memcached_st *dest_memc;
    memcached_server_st *src_server = NULL;
    memcached_server_st *dest_server = NULL;
    memcached_return rc;
    
    size_t *key_len;
    uint32_t flags;
    int i = 0;
    char *value;
    char return_key[MEMCACHED_MAX_KEY];
    size_t return_key_len;
    size_t return_value_len;
    
    src_memc = memcached_create(NULL);
    dest_memc = memcached_create(NULL);

    src_server = memcached_server_list_append(src_server, src_host, src_port, &rc);
    rc = memcached_server_push(src_memc, src_server);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't add source server: %s\n", memcached_strerror(src_memc, rc));
        memcached_free(src_memc);
        return 0;
    }

    dest_server = memcached_server_list_append(dest_server, dest_host, dest_port, &rc);
    rc = memcached_server_push(dest_memc, dest_server);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't add destination server: %s\n", memcached_strerror(dest_memc, rc));
        memcached_free(src_memc);
        memcached_free(dest_memc);
        return 0;
    }

    key_len = malloc(key_num * sizeof(size_t));

    for (i = 0; i < key_num; i++) {
        key_len[i] = strlen(key[i]);
    }

    rc = memcached_mget(src_memc, key, key_len, key_num);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't get values from source server: %s\n", memcached_strerror(src_memc, rc));
        memcached_free(src_memc);
        memcached_free(dest_memc);
        free(key_len);
        return 0;
    }

    while ((value = memcached_fetch(src_memc, return_key, &return_key_len, &return_value_len, &flags, &rc)) != NULL) {
        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr, "Couldn't fetch value: %s\n", memcached_strerror(src_memc, rc));
            memcached_free(src_memc);
            memcached_free(dest_memc);
            free(key_len);
            free(value);
            return 0;
        }

        rc = memcached_set(dest_memc, return_key, return_key_len, value, return_value_len, (time_t)0, (uint32_t)0);

        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr, "Couldn't set value: %s\n", memcached_strerror(dest_memc, rc));
            memcached_free(src_memc);
            memcached_free(dest_memc);
            free(key_len);
            free(value);
            return 0;
        }
        
        free(value);
    }

    printf("Keys moved!\n");
    memcached_free(src_memc);
    memcached_free(dest_memc);

    return 1;
}

        



            


       

