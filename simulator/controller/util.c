#include "util.h"
#include <string.h>
#include <stdlib.h>

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
int mmove_key(const char * const* key, char *src_host, in_port_t src_port, char *dest_host, in_port_t dest_port, int key_num) {
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
        memcached_free(dest_memc);
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

        
/* Move one key from one source server to multiple destination servers
   Source server hostname and port are in src_host and src_port
   Destination server hostname and port are in dest_host_ip and 
   They are splited by ":"
   Return 1 on success
   Return 0 on failure
*/
int move_key_mserver(char *key, char *src_host, in_port_t src_port, UT_array *dest_host_ip) {
    memcached_server_st *src_server = NULL;
    memcached_server_st **dest_server = NULL;
    memcached_st *src_memc;
    memcached_st **dest_memc;
    memcached_return rc;
    size_t value_len;
    uint32_t flags;
    char *value = NULL;
    int dest_len = utarray_len(dest_host_ip);
    int i = 0;
    int j = 0;
    char **line = NULL;
    char *dest_host;
    char *dest_port_str;
    in_port_t dest_port;

    src_memc = memcached_create(NULL);
    dest_server = malloc(dest_len * sizeof(memcached_server_st*));
    dest_memc = malloc(dest_len * sizeof(memcached_st*));

    for (i = 0; i < dest_len; i++) {
        dest_memc[i] = memcached_create(NULL);
    }

    src_server = memcached_server_list_append(src_server, src_host, src_port, &rc);
    rc = memcached_server_push(src_memc, src_server);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't add source server: %s\n", memcached_strerror(src_memc, rc));
        memcached_free(src_memc);
        for (j = 0; j < dest_len; j++) {
            memcached_free(dest_memc[j]);
        }
        free(dest_memc);
        free(dest_server);
        return 0;
    }

    i = 0;
    while ((line = (char**)utarray_next(dest_host_ip, line)) != NULL) {
        dest_host = strtok(*line, ":");
        dest_port_str = strtok(NULL, ":");
        dest_port = atoi(dest_port_str);

        dest_server[i] = memcached_server_list_append(dest_server[i], dest_host, dest_port, &rc);
        rc = memcached_server_push(dest_memc[i], dest_server[i]);

        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr, "Couldn't add destination server: %s\n", memcached_strerror(dest_memc[i], rc));
            memcached_free(src_memc);
            for (j = 0; j < dest_len; j++) {
                memcached_free(dest_memc[j]);
            }
            free(dest_server);
            free(dest_memc);
            return 0;
        }
        i++;
    }
    value = memcached_get(src_memc, key, strlen(key), &value_len, &flags, &rc);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't get key from source server: %s\n", memcached_strerror(src_memc, rc));
        memcached_free(src_memc);
        for (j = 0; j < dest_len; j++) {
            memcached_free(dest_memc[j]);
        }
        free(dest_memc);
        free(dest_server);
        return 0;
    }

    for (i = 0; i < dest_len; i++) {
        rc = memcached_set(dest_memc[i], key, strlen(key), value, strlen(value), (time_t)0, (uint32_t)0);

        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr, "Couldn't set key value to destination server: %s\n", memcached_strerror(dest_memc[i], rc));
            memcached_free(src_memc);
            for (j = 0; j < dest_len; j++) {
                memcached_free(dest_memc[j]);
            }
            free(dest_memc);
            free(dest_server);
            return 0;
        }
    }
    printf("Key moved!\n");

    memcached_free(src_memc);
    for (j = 0; j < dest_len; j++) {
        memcached_free(dest_memc[j]);
    }
    free(dest_memc);
    free(dest_server);
    free(value);

    return 1;
}


/* Move one key from one source server to multiple destination servers */
int move_key_mserver_index(char *key, UT_array *dest_host_ip, in_port_t port, int index) {
    memcached_server_st *src_server = NULL;
    memcached_server_st **dest_server = NULL;
    memcached_st *src_memc;
    memcached_st **dest_memc;
    memcached_return rc;
    size_t value_len;
    uint32_t flags;
    char *value = NULL;
    int dest_len = utarray_len(dest_host_ip) - index - 1;
    int i = 0;
    int j = 0;
    char **line = NULL;
    char *dest_host; 

    printf("Dest len is %d\n", dest_len);

    src_memc = memcached_create(NULL);
    dest_server = malloc(dest_len * sizeof(memcached_server_st*));
    dest_memc = malloc(dest_len * sizeof(memcached_st*));

    for (i = 0; i < dest_len; i++) {
        dest_memc[i] = memcached_create(NULL);
    }

    i = 0;
    while ((line = (char**)utarray_next(dest_host_ip, line)) != NULL) {
        if (i == index) {
            dest_host = strtok(*line, ":");
            printf("Source ip is %s\n", dest_host);

            src_server = memcached_server_list_append(src_server, dest_host, port, &rc);

            rc = memcached_server_push(src_memc, src_server);

            if (rc != MEMCACHED_SUCCESS) {
                fprintf(stderr, "Couldn't add source server: %s\n", memcached_strerror(src_memc, rc));
                memcached_free(src_memc);
                for (j = 0; j < dest_len; j++) {
                    memcached_free(dest_memc[j]);
                }
                free(dest_memc);
                free(dest_server);
                return 0;
            }
        } else if (i > index) {
            dest_host = strtok(*line, ":");

            printf("Dest ip is %s\n", dest_host);

            dest_server[i - 1 - index] = memcached_server_list_append(dest_server[i - 1 - index], dest_host, port, &rc);
            rc = memcached_server_push(dest_memc[i - 1 - index], dest_server[i - 1 - index]);

            if (rc != MEMCACHED_SUCCESS) {
                fprintf(stderr, "Couldn't add destination server: %s\n", memcached_strerror(dest_memc[i - 1 - index], rc));
                memcached_free(src_memc);
                for (j = 0; j < dest_len; j++) {
                    memcached_free(dest_memc[j]);
                }
                free(dest_server);
                free(dest_memc);
                return 0;
            }
        }
        i++;
    }

    printf("Get %s\n", key);

    value = memcached_get(src_memc, key, strlen(key), &value_len, &flags, &rc);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't get key from source server: %s\n", memcached_strerror(src_memc, rc));
        memcached_free(src_memc);
        for (j = 0; j < dest_len; j++) {
            memcached_free(dest_memc[j]);
        }
        free(dest_memc);
        free(dest_server);
        return 0;
    }

    for (i = 0; i < dest_len; i++) {
        rc = memcached_set(dest_memc[i], key, strlen(key), value, strlen(value), (time_t)0, (uint32_t)0);

        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr, "Couldn't set key value to destination server: %s\n", memcached_strerror(dest_memc[i], rc));
            memcached_free(src_memc);
            for (j = 0; j < dest_len; j++) {
                memcached_free(dest_memc[j]);
            }
            free(dest_memc);
            free(dest_server);
            return 0;
        }
    }
    printf("Key moved!\n");

    memcached_free(src_memc);
    for (j = 0; j < dest_len; j++) {
        memcached_free(dest_memc[j]);
    }
    free(dest_memc);
    free(dest_server);
    free(value);

    return 1;
}

/* Move multiple keys from source server to multiple destination servers
   Source server hostname and port are in src_host and src_port
   Destination server hostname and port are in dest_host and dest_port
   Key number is in key_num
   Return 1 on success
   Return 0 on failure
*/
/*
int mmove_key_mserver(char **key, char *src_host, in_port_t src_port, UT_array *dest_host_ip, int key_num) {
    memcached_st *src_memc;
    memcached_st **dest_memc;
    memcached_server_st *src_server = NULL;
    memcached_server_st **dest_server = NULL;
    memcached_return rc;
    
    size_t *key_len;
    uint32_t flags;
    int i = 0;
    int j = 0;
    char *value;
    char return_key[MEMCACHED_MAX_KEY];
    size_t return_key_len;
    size_t return_value_len;
    char **line = NULL;
    char *dest_host;
    char *dest_port_str;
    in_port_t dest_port;
    int dest_len = utarray_len(dest_host_ip);

    src_memc = memcached_create(NULL);
    dest_server = malloc(dest_len * sizeof(memcached_server_st*));
    dest_memc = malloc(dest_len * sizeof(memcached_st*));

    for (i = 0; i < dest_len; i++) {
        dest_memc[i] = memcached_create(NULL);
    }

    src_server = memcached_server_list_append(src_server, src_host, src_port, &rc);
    rc = memcached_server_push(src_memc, src_server);

    if (rc != MEMCACHED_SUCCESS) {
        fprintf(stderr, "Couldn't add source server: %s\n", memcached_strerror(src_memc, rc));
        memcached_free(src_memc);
        
        return 0;
    }

    i = 0;
    while ((line = (char**)utarray_next(dest_host_ip, line)) != NULL) {
        dest_host = strtok(*line, ":");
        dest_port_str = strtok(NULL, ":");
        dest_port = atoi(dest_port_str);

        dest_server[i] = memcached_server_list_append(dest_server[i], dest_host, dest_port, &rc);
        rc = memcached_server_push(dest_memc[i], dest_server[i]);

        if (rc != MEMCACHED_SUCCESS) {
            fprintf(stderr, "Couldn't add destination server: %s\n", memcached_strerror(dest_memc[i], rc));
            memcached_free(src_memc);
            for (j = 0; j < dest_len; j++) {
                memcached_free(dest_memc[j]);
            }
            free(dest_server);
            free(dest_memc);
            return 0;
        }
        i++;
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
*/
           


       

