#ifndef _UTIL_H_
#define _UTIL_H_
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <libmemcached/memcached.h>
#include "uthash/utarray.h"

/* Move one key from source server to destination server
   Source server hostname and port are in src_host and src_port
   Destination server hostname and port are in dest_host and dest_port
   Return 1 on success
   Return 0 on failure
*/
int move_key(char *key, char *src_host, in_port_t src_port, char *dest_host, in_port_t dest_port);

/* Move multiple keys from source server to destination server
   Source server hostname and port are in src_host and src_port
   Destination server hostname and port are in dest_host and dest_port
   Key number is in key_num
   Return 1 on success
   Return 0 on failure
*/

int mmove_key(const char * const*key, char *src_host, in_port_t src_port, char *dest_host, in_port_t dest_port, int key_num);

/* Move one key from one source server to multiple destination servers
   Source server hostname and port are in src_host and src_port
   Destination server hostname and port are in dest_host_ip and 
   They are splited by ":"
   Return 1 on success
   Return 0 on failure
*/
int move_key_mserver(char *key, char *src_host, in_port_t src_port, UT_array *dest_host_ip); 

/* Move multiple keys from source server to multiple destination servers
   Source server hostname and port are in src_host and src_port
   Destination server hostname and port are in dest_host and dest_port
   Key number is in key_num
   Return 1 on success
   Return 0 on failure
*/
int mmove_key_mserver(char **key, char *src_host, in_port_t src_port, UT_array *dest_host_ip, int key_num);

/* Move one key from one source server to multiple destination servers */
int move_key_mserver_index(char *key, UT_array *dest_host_ip, in_port_t port, int index);
#endif
