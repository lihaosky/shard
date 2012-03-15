#ifndef __COMMON_UTILITIES_MODULE
#define __COMMON_UTILITIES_MODULE

// include windows file first if needed
#ifdef _WIN32
#ifndef _WINDOWS_
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#define _WIN32_WINNT 0x0501
#include <winsock2.h>
#include <ws2tcpip.h>
#include <windows.h>
#include <winbase.h>
#include <time.h>
#endif
#endif

// standard C99 include files
#include <stdlib.h>
#include <stdint.h>
#include <assert.h>

// platform specific include files
#ifndef _WIN32
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netdb.h>

typedef int SOCKET;
#endif

//#include "output.h"

// COMMON UTILITIES MODULE
// -----------------------
//
// KNOWN BUGS
// cu_timed_connect() is not implemented yet.
//

#ifndef IP_ADDRESS_LENGTH
#define IP_ADDRESS_LENGTH 16
#endif

struct _cu_sock_info 
{
	int socket_id;
	int bytes_received;
	int bytes_sent;

	struct timeval uptime;

	struct _cu_sock_info* next;
};
typedef struct _cu_sock_info _cu_sockinfo;

/////////// socket and internet-related calls /////////////////////

// basics
void cu_initialize_network();
int cu_connect(int* socket_id, const char* ip_address, uint16_t port);
int cu_timed_connect(int* socket_id, const char* ip_address, uint16_t port, double* timeout_interval);		// not implemented
int cu_listen(int* socket_id, uint16_t port);
int cu_accept(int* client_socket, int listening_socket);
int cu_close(int socket);

// ip management
void cu_copy_ip_address(char* dest_addr_buf, const char* src_addr_buf);
int cu_validate_ip_address(const char* ip_addr);
int cu_resolve_hostname_to_ip_address(const void* hostname, void* ip_addr_dest_buf);
int cu_get_remote_ip_address(int socket_id, char* addr_dest_buf, uint16_t* port_dest_buf);
int cu_get_sockaddr(int socket_id, struct sockaddr_in* sockaddr_dest_buf);
int cu_get_local_ip_address(char* addr_dest_buf);

// read/write 
int cu_read_data_item(int socket_id, void* buf, int buf_len);			// replaces read_binary_package
int cu_read_data_item_auto_alloc(int socket_id, char** buf);			// replaces read_binary_package_auto_alloc
int cu_write_data_item(int socket_id, const void* buf, int buf_len);	// replaces send_binary_package

int cu_read_string_from_socket(int socket_id, char* buf, int buf_len);
int cu_write_string_to_socket(int socket_id, const char* buf);

int cu_read_int_from_socket(int socket_id, int* dest);
int cu_write_int_to_socket(int socket_id, int value);

int cu_read_uint32_from_socket(int client_socket, uint32_t* result);
int cu_write_uint32_to_socket(int client_socket, uint32_t value);

int cu_read_bool_from_socket(int client_socket, bool* result);
int cu_write_bool_to_socket(int client_socket, bool value);

int cu_read_float_from_socket(int socket_id, float* result);
int cu_write_float_to_socket(int socket_id, float value);

int cu_read_double_from_socket(int socket_id, double* result);
int cu_write_double_from_socket(int socket_id, double value);

int cu_read_fixed_bytes_from_socket(int socket_id, int number_of_bytes_to_read, void* buf);

int cu_send(int socket_id, const char* buf, int buf_len);
int cu_recv(int socket_id, char* buf, int buf_len);

int cu_read_crlfcrlf_terminated_block_from_socket(int client_socket, char* buffer, int buffer_size);

int cu_read_command_from_socket(int socket_id, int* data_length, long* arrival_time, int* command_type);

// timer related
void cu_improved_sleep(double interval);
double cu_calculate_time_difference(struct timeval end_time, struct timeval start_time);
double cu_get_time_elapsed_to_now(struct timeval start_time);
int cu_generate_random_number(int lower_bound, int upper_bound);
void cu_reseed_random_number_generator();

// debugging related
void cu_error(const void* error_message, ...);
void cu_message(const void* message, ...);

// system statistics reporting
int cu_get_total_connections_open();
int cu_get_total_bytes_sent();
int cu_get_total_bytes_received();
int cu_get_bytes_sent(int socket_id);
int cu_get_bytes_received(int socket_id);
void cu_get_socket_data(int* total_sockets, _cu_sockinfo** dest);

// system related
double cu_get_cpu_utilization();		// windows only
void* cu_alloc(uint32_t number_of_bytes);


// output device related
void get_console_dimensions(uint32_t* width, uint32_t* height);

// WINDOWS emulation of socket calls and gettimeofday
#ifdef _WIN32

void close(SOCKET socket_id);
const char *inet_ntop(int af, const void *src, char *dst, uint32_t cnt);

#if defined(_MSC_VER) || defined(_MSC_EXTENSIONS)
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000Ui64
#else
#define DELTA_EPOCH_IN_MICROSECS  11644473600000000ULL
#endif 

#ifndef __STRUCT_TIMEZONE
#define __STRUCT_TIMEZONE
struct timezone
{
	int tz_minuteswest;
	int tz_dsttime;
};
#endif

int gettimeofday(struct timeval *timeval_structure, struct timezone *set_to_NULL);

ULONGLONG __cu_internal_subtract_times(const FILETIME& final_time, const FILETIME& initial_time);

#endif
#endif
