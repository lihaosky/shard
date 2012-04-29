#include "util.h"

int main() {
    memcached_st *src_memc;
    memcached_st *dest_memc;
    memcached_server_st *src_server = NULL;
    memcached_server_st *dest_server = NULL;
    char *value = NULL;
    size_t value_len;
    uint32_t flags;
    memcached_return rc;
    char *key[] = {"abcd", "efgh", "ijkl"};
    int i = 0;

    src_memc = memcached_create(NULL);
    dest_memc = memcached_create(NULL);

    src_server = memcached_server_list_append(src_server, "localhost", 1234, &rc);
    rc = memcached_server_push(src_memc, src_server);


    for (i = 0; i < 3; i++) {
        rc = memcached_set(src_memc, key[i], strlen(key[i]), "good", strlen("good"), (time_t)0, (uint32_t)0);
    }
    mmove_key(key, "localhost", 1234, "localhost", 4321, 3);

    dest_server = memcached_server_list_append(dest_server, "localhost", 4321, &rc);

    rc = memcached_server_push(dest_memc, dest_server);

    value = memcached_get(dest_memc, "abcd", strlen("abcd"), &value_len, &flags, &rc);
    printf("Value get from destination is %s\n", value);

    return 0;
}

 




