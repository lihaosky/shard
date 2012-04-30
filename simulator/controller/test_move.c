#include "util.h"
#include "uthash/utarray.h"

int main() {
    memcached_st *src_memc;
    memcached_st *dest_memc;
    memcached_server_st *src_server = NULL;
    memcached_server_st *dest_server = NULL;
    char *value = NULL;
    size_t value_len;
    uint32_t flags;
    memcached_return rc;
    char *s;
    UT_array *utarray;

    utarray_new(utarray, &ut_str_icd);

    s = "someid:kdh";
    utarray_push_back(utarray, &s);
    s = "someelse:kdf";
    utarray_push_back(utarray, &s);
    s = "128.84.98.141:port";
    utarray_push_back(utarray, &s);
    s = "128.84.97.238:port";
    utarray_push_back(utarray, &s);

    src_memc = memcached_create(NULL);
    dest_memc = memcached_create(NULL);

    src_server = memcached_server_list_append(src_server, "128.84.98.141", 1234, &rc);
    rc = memcached_server_push(src_memc, src_server);

    rc = memcached_set(src_memc, "lihaosky", strlen("lihaosky"), "good", strlen("good"), (time_t)0, (uint32_t)0);

    move_key_mserver_index("lihaosky", utarray, 1234, 2);

    dest_server = memcached_server_list_append(dest_server, "128.84.97.238", 1234, &rc);

    rc = memcached_server_push(dest_memc, dest_server);

    value = memcached_get(dest_memc, "lihaosky", strlen("lihaosky"), &value_len, &flags, &rc);
    printf("Value get from destination is %s\n", value);

    return 0;
}

 




