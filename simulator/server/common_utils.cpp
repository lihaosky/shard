// standard C99 include files
#include <stdlib.h>
#include <stdio.h>
#include <pthread.h>
#include <string.h>
#include <time.h>
#include <errno.h>
#include <stdarg.h>
#include <stdint.h>

// internal include files
#include "common_utils.h"

// platform specific include files
#ifndef _WIN32
#include <unistd.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <sys/ioctl.h>
#include <netdb.h>
#include <sys/ioctl.h>
#include <net/if.h>
#include <sys/time.h>
#endif

// WINDOWS portability: this code has been ported.

int cu_debug_file_count = 0;
pthread_mutex_t host_lookup_lock = PTHREAD_MUTEX_INITIALIZER;

_cu_sockinfo* _cu_sockinfo_head = NULL;
int _total_bytes_sent = 0;
int _total_bytes_received = 0;
int _connections_open = 0;
pthread_mutex_t _sock_info_lock = PTHREAD_MUTEX_INITIALIZER;

// adds a connection to the linked list
static void add_connection(int socket_id)
{
	_cu_sockinfo* new_sockinfo = (_cu_sockinfo*) malloc(sizeof(_cu_sockinfo));
	memset(new_sockinfo, 0, sizeof(_cu_sockinfo));
	new_sockinfo->socket_id = socket_id;
	new_sockinfo->bytes_received = 0;
	new_sockinfo->bytes_sent = 0;
	new_sockinfo->next = NULL;
	gettimeofday(&(new_sockinfo->uptime), NULL);
	pthread_mutex_lock(&_sock_info_lock);
	new_sockinfo->next = _cu_sockinfo_head;
	_cu_sockinfo_head = new_sockinfo;
	pthread_mutex_unlock(&_sock_info_lock);
	_connections_open++;
}

// removes a connection from the linked list
static void remove_connection(int socket_id)
{
	_cu_sockinfo* current;
	_cu_sockinfo* prev;

	pthread_mutex_lock(&_sock_info_lock);
	current = _cu_sockinfo_head;
	prev = NULL;
	while (current != NULL)
	{
		if (current->socket_id == socket_id)
		{
			_connections_open--;
			if (current == _cu_sockinfo_head)
				_cu_sockinfo_head = current->next;
			else
				prev->next = current->next;

			free(current);
			break;
		}
		else
		{
			prev = current;
			current = current->next;
		}
	}
	pthread_mutex_unlock(&_sock_info_lock);
}

// update bytes sent/received
// operation == 0: bytes sent
// operation == 1: bytes received
static void update_connection_stat(int socket_id, int bytes_to_adjust, int operation)
{
	_cu_sockinfo* current;

	pthread_mutex_lock(&_sock_info_lock);
	for (current = _cu_sockinfo_head; current != NULL; current = current->next)
	{
		if (current->socket_id == socket_id)
		{
			if (operation == 0)
			{
				_total_bytes_sent += bytes_to_adjust;
				current->bytes_sent += bytes_to_adjust;
			}
			else
			{
				_total_bytes_received += bytes_to_adjust;
				current->bytes_received += bytes_to_adjust;
			}

			break;
		}
	}
	pthread_mutex_unlock(&_sock_info_lock);
}

// returns the bytes sent/received on this socket
// operation == 0: bytes sent
// operation == 1: bytes received
static int get_connection_stat(int socket_id, int operation)
{
	_cu_sockinfo* current;
	int result = -1;

	pthread_mutex_lock(&_sock_info_lock);
	for (current = _cu_sockinfo_head; current != NULL; current = current->next)
	{
		if (current->socket_id == socket_id)
			if (operation == 0)
				result = current->bytes_sent;
			else
				result = current->bytes_received;
		break;
	}
	pthread_mutex_unlock(&_sock_info_lock);

	return result;
}

// creates and sets a socket to connect to an address/port given
// assigns the socket via socket_id through the socket_structaddr
int cu_connect(int* socket_id, const char* address, uint16_t port)
{
	int return_value;
	struct sockaddr_in remote_sockaddr;
	SOCKET client_socket;

	*socket_id = -1;

	client_socket = socket(AF_INET, SOCK_STREAM, 0);
	remote_sockaddr.sin_family = AF_INET;
	remote_sockaddr.sin_addr.s_addr = inet_addr(address);
	remote_sockaddr.sin_port = htons(port);

	// parameters set, now attempt to connect
	if (connect(client_socket, (struct sockaddr*) &remote_sockaddr, sizeof(remote_sockaddr)) == -1)
	{
		close(client_socket);
		return_value = -1;
	}
	else
	{
		add_connection(client_socket);

		return_value = 0;
		*socket_id = (int) client_socket;
	}

	return return_value;
}

// timed connect -- will enforce a timeout setting for connect() calls
// warning -- not implemented
int cu_timed_connect(int* socket_id, const char* address, uint16_t port, double* timeout_interval)
{
	return -1;
}

// creates and sets a TCP socket into listen mode
// assigns the socket via socket_id
int cu_listen(int* socket_id, uint16_t port)
{
	int one = 1;
	SOCKET server_socket = -1;
	struct sockaddr_in local_sockaddr;

	if ((server_socket = socket(AF_INET, SOCK_STREAM, 0)) == -1)
		goto cu_listen_failure;

	// setup sockaddr
	local_sockaddr.sin_family = AF_INET;
	local_sockaddr.sin_addr.s_addr = htonl(INADDR_ANY);
	local_sockaddr.sin_port = htons(port);

	// set socket options
#ifndef _WIN32
	if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one)) == -1)
#else
	if (setsockopt(server_socket, SOL_SOCKET, SO_REUSEADDR, (const char*) &one, sizeof(one)) == -1)
#endif
		goto cu_listen_failure;

	// bind socket to sockaddr
	if (bind(server_socket, (struct sockaddr*) &local_sockaddr, sizeof(local_sockaddr)) == -1)
		goto cu_listen_failure;

	// set to listen mode
	if (listen(server_socket, 15) == -1)
		goto cu_listen_failure;

	*socket_id = (int) server_socket;
	return 0;

cu_listen_failure:
	close (server_socket);
	*socket_id = -1;
	return -1;
}

// accepts an incoming connection on a TCP listening socket
// fills up the socket_id with the accepted client socket if successful, -1 otherwise
int cu_accept(int *client_socket, int listening_socket)
{
	struct sockaddr_in client_sockaddr;
	socklen_t address_length = sizeof(struct sockaddr_in);
	SOCKET accepted_socket;

	accepted_socket = accept(listening_socket, (struct sockaddr*) &client_sockaddr, &address_length);
	if (accepted_socket == -1)
	{
		*client_socket = -1;
		return -1;
	}

	add_connection((int)accepted_socket);
	*client_socket = (int) accepted_socket;
	return 0;
}

// closes a connection
int cu_close(int socket_id)
{
	int result = close(socket_id);
	remove_connection(socket_id);

	return result;
}


// gets the remote ip address from a socket number
// fills up the address buffer if the remote address can be gotten
int cu_get_remote_ip_address(int socket_id, char* address_buffer, uint16_t* port_buffer)
{
	struct sockaddr_in remote_sockaddr;
	socklen_t struct_size = sizeof(remote_sockaddr);

	// get the sockaddr struct from the socket
	if (getpeername(socket_id, (struct sockaddr*) &remote_sockaddr, &struct_size) == -1)
		goto fail;

	if (port_buffer != NULL)
		*port_buffer = remote_sockaddr.sin_port;

	// get the ip address from the struct sockaddr
	if (address_buffer != NULL)
	{		
		if (inet_ntop(AF_INET, &remote_sockaddr.sin_addr.s_addr, address_buffer, 16) == NULL)
			goto fail;
	}

	return 0;

fail:
	if (address_buffer != NULL)
		memset(address_buffer, 0, 16);
	if (port_buffer != NULL)
		*port_buffer = 0;

	return -1;
}


// gets the sockaddr given the socket id
// sets the destination sockaddr if successful, otherwise it is emptied out
int cu_get_sockaddr(int socket_id, struct sockaddr_in *destination)
{
	socklen_t sock_len = sizeof(struct sockaddr_in);

	if (getpeername(socket_id, (struct sockaddr*) destination, &sock_len) == -1)
	{
		memset(destination, 0, sizeof(struct sockaddr_in));
		return -1;
	}

	return 0;
}

// supporting function for reads
int cu_read_data_item(int socket_id, void* buffer, int buf_len)
{
	uint32_t item_size = 0;
	int read_status = 0;
	int offset = 0;

	if (buf_len <= 0 || buffer == NULL)
		return -1;

	// read out item size
	while(1)
	{
		read_status = recv(socket_id, ((char*)&item_size)+offset, sizeof(uint32_t)-offset, 0);
		if (read_status == -1)
		{
			if (errno == EINTR)
				continue;
			else
				goto cu_read_data_item_failure;
		}
		else if (read_status == 0)
			goto cu_read_data_item_failure;
		else
			offset += read_status;

		if (offset == sizeof(uint32_t))
			break;
	}

	// item size read out
	item_size = ntohl(item_size);
	if (item_size > (unsigned int) buf_len)
		return -1;
	else if (item_size == 0)
		return 0;

	// read in data item
	offset = 0;
	while(1)
	{
		read_status = recv(socket_id, ((char*) buffer)+offset, item_size-offset, 0);

		if (read_status == -1)
		{
			if (errno == EINTR)
				continue;
			else
				goto cu_read_data_item_failure;
		}
		else if (read_status == 0)
			goto cu_read_data_item_failure;
		else
			offset += read_status;

		if (offset == (int) item_size)
			break;
	}

	// successful, return item size
	update_connection_stat(socket_id, item_size, 1);
	return item_size;

cu_read_data_item_failure:
	return -1;
}

// supporting function for reads, automatic allocation
int cu_read_data_item_auto_alloc(int socket_id, char** buf)
{
	uint32_t item_size = 0;
	int read_status = 0;
	int offset = 0;
	bool allocated = false;

	// read out item size
	while(1)
	{
		read_status = recv(socket_id, ((char*)&item_size)+offset, sizeof(uint32_t)-offset, 0);
		if (read_status == -1)
		{
			if (errno == EINTR)
				continue;
			else
				goto cu_read_data_item_auto_alloc_failure;
		}
		else if (read_status == 0)
			goto cu_read_data_item_auto_alloc_failure;
		else
			offset += read_status;

		if (offset == sizeof(uint32_t))
			break;
	}

	// item size read out
	item_size = ntohl(item_size);
	*buf = (char*) malloc(item_size);
	if (*buf == NULL)
		goto cu_read_data_item_auto_alloc_failure;
	else
		allocated = true;

	// read in data item
	offset = 0;
	while(1)
	{
		read_status = recv(socket_id, ((char*)(*buf))+offset, item_size-offset, 0);

		if (read_status == -1)
		{
			if (errno == EINTR)
				continue;
			else
				goto cu_read_data_item_auto_alloc_failure;
		}
		else if (read_status == 0)
			goto cu_read_data_item_auto_alloc_failure;
		else
			offset += read_status;

		if (offset == (int) item_size)
			break;
	}

	// successful, retrn item size
	update_connection_stat(socket_id, item_size, 1);
	return item_size;

cu_read_data_item_auto_alloc_failure:
	
	if (allocated)
	{
		free(*buf);
		*buf = NULL;
	}

	return -1;
}

// supporting function for writes
int cu_write_data_item(int socket_id, const void* buf, int buf_len)
{
	uint32_t item_size = htonl((unsigned int) buf_len);
	int write_status;
	int offset = 0;

	if (buf_len <= 0)
		goto fail;

	// write number of bytes of data item
	while(1)
	{
		write_status = send(socket_id, ((char*) &item_size)+offset, sizeof(uint32_t)-offset, MSG_NOSIGNAL);
		if (write_status == -1)
		{
			if (errno == EINTR)
				continue;
			else
				goto fail;
		}
		else if (write_status == 0)
			goto fail;
		else
			offset += write_status;

		if (offset == sizeof(uint32_t))
			break;
	}

	// write data item
	if (buf_len == 0)
		return 0;
	offset = 0;
	while(1)
	{
		write_status = send(socket_id, ((const char*) buf)+offset, buf_len-offset, MSG_NOSIGNAL);
		if (write_status == -1)
		{
			if (errno == EINTR)
				continue;
			else
				goto fail;
		}
		else if (write_status == 0)
			goto fail;
		else
			offset += write_status;

		if (offset == buf_len)
			break;
	}

	update_connection_stat(socket_id, buf_len, 0);
	return buf_len;

fail:
	return -1;
}

// reads a string from the socket. this string will be null terminated.
int cu_read_string_from_socket(int socket_id, char* buf, int buf_len)
{
	int read_status;
	
	read_status = cu_read_data_item(socket_id, buf, buf_len);

	if (read_status > 0 && buf[read_status-1] == '\0')
		return read_status-1;
	else if (read_status == 0)
		return 0;
	else
		return -1;
}

// writes a string to the socket
int cu_write_string_to_socket(int socket_id, const char* buf)
{
	int write_status;
	int bytes_to_write;

	// if no string or an empty string, write an empty string
	if (buf == NULL)
		write_status = cu_write_data_item(socket_id, NULL, 0);

	// bytes to write includes the NULL char
	bytes_to_write = strlen(buf)+1;
	write_status = cu_write_data_item(socket_id, buf, bytes_to_write);

	return write_status;
}
// reads in an integer from the stream
int cu_read_int_from_socket(int client_socket, int* result)
{
	char buf[20];
	int return_value;

	// read in a string
	if ((return_value = cu_read_string_from_socket(client_socket, buf, sizeof(buf))) < 0)
		goto cu_read_int_from_socket_failure;

	// parse numerical value from string
	if (sscanf((char*) buf, "%d", result) != 1)
		goto cu_read_int_from_socket_failure;
	
	// successful
	return 0;

cu_read_int_from_socket_failure:
	*result = 0;
	return -1;
}

// writes an integer to the socket
int cu_write_int_to_socket(int client_socket, int value)
{
	char buf[20];

	sprintf(buf, "%d", value);
	return cu_write_string_to_socket(client_socket, buf);
}

// reads in an unsigned integer from the stream
int cu_read_uint32_from_socket(int client_socket, uint32_t* result)
{
	char buf[20];
	int return_value;

	// read in a string
	if ((return_value = cu_read_string_from_socket(client_socket, buf, sizeof(buf))) < 0)
		goto fail;

	// parse numerical value from string
	if (sscanf(buf, "%u", result) != 1)
		goto fail;

	return 0;

fail:
	*result = 0;
	return -1;
}

// writes an unsigned integer to a given socket
int cu_write_uint32_to_socket(int client_socket, uint32_t value)
{
	char buf[20];

	sprintf(buf, "%u", value);
	return cu_write_string_to_socket(client_socket, buf);
}


// reads a fixed number of bytes from a socket, efficient implementation
int cu_read_fixed_bytes_from_socket(int socket_id, int number_of_bytes, void* buffer)
{
	int status = 0;
	int total_bytes_read = 0;

	if (number_of_bytes == 0)
		return 0;
	else if (number_of_bytes < 0)
		goto fail;

	while(1)
	{
		status = recv(socket_id, ((char*) buffer) + total_bytes_read, number_of_bytes-total_bytes_read, 0);
		if (status == 0)
			goto fail;
		else if (status == -1)
		{
			if (errno == EINTR)
				continue;
			else
				goto fail;
		}
		else
			total_bytes_read += status;
		
		if (total_bytes_read == number_of_bytes)
			break;
	}

	update_connection_stat(socket_id, number_of_bytes, 1);
	return total_bytes_read;

fail:
	return -1;
}

// reads in a bool from the stream
int cu_read_bool_from_socket(int client_socket, bool* result)
{
	char buf[20];
	int return_value;

	if ((return_value = cu_read_string_from_socket(client_socket, buf, sizeof(buf))) < 0)
		goto fail;

	// parse bool value
	if (buf[0] == 't')
		*result = true;
	else if (buf[0] == 'f')
		*result = false;
	else
		goto fail;

	// successful
	return 0;

fail:
	return -1;
}

// writes a boolean to the socket
int cu_write_bool_to_socket(int client_socket, bool value)
{
	if (value)
		return cu_write_string_to_socket(client_socket, "t");
	else
		return cu_write_string_to_socket(client_socket, "f");
}

// reads in a float from the stream
int cu_read_float_from_socket(int socket_id, float* result)
{
	char buf[20];
	int return_value;

	// read in a string
	if ((return_value = cu_read_string_from_socket(socket_id, buf, sizeof(buf))) < 0)
		goto fail;

	// parse numerical value from string
	if (sscanf((char*) buf, "%f", result) != 1)
		goto fail;
	
	// successful
	return 0;

fail:
	return -1;
}

// writes a float to the socket
int cu_write_float_to_socket(int socket_id, float value)
{
	char buf[20];

	sprintf(buf, "%f", value);
	return cu_write_string_to_socket(socket_id, buf);
}

// reads in a double from the stream
int cu_read_double_from_socket(int socket_id, double* result)
{
	char buf[20];
	int return_value;

	// read in a string
	if ((return_value = cu_read_string_from_socket(socket_id, buf, sizeof(buf))) < 0)
		goto fail;

	// parse numerical value from string
	if (sscanf((char*) buf, "%lf", result) != 1)
		goto fail;
	
	// successful
	return 0;

fail:
	return -1;
}

// writes a float to the socket
int cu_write_double_to_socket(int socket_id, double value)
{
	char buf[20];

	sprintf(buf, "%lf", value);
	return cu_write_string_to_socket(socket_id, buf);
}

// puts the program to sleep for a fixed amount of time
void cu_improved_sleep(double interval)
{
#ifndef _WIN32
// UNIX CODE
	struct timespec sleep_interval;
	struct timespec sleep_remaining;

	sleep_interval.tv_sec = (int) interval;
	sleep_interval.tv_nsec = (int) ((interval - ((int) interval)) * 1000000000);

	while (nanosleep(&sleep_interval, &sleep_remaining) == -1 && errno == EINTR)
		sleep_interval = sleep_remaining;
#else
// WINDOWS CODE

	DWORD milliseconds = (DWORD) (interval * 1000.0);
	Sleep(milliseconds);

#endif
}

int cu_read_crlfcrlf_terminated_block_from_socket(int client_socket, char* buffer, int buffer_size)
{
	int offset = 0;
	int return_value;
	memset(buffer, 0, buffer_size);

	while(1)
	{
		if (offset >= buffer_size-1)
			goto fail;

		if ((return_value = cu_recv(client_socket, buffer+offset, 1)) < 0)
			goto fail;

		if (offset >=3)
		{
			if (buffer[offset-3] == '\r' && buffer[offset-2] == '\n' && buffer[offset-1] == '\r' && buffer[offset] == '\n')
				break;
		}

		offset++;
	}

	return offset+1;

fail:
	return -1;
}

// displays an error message and exits (windows)
// or uses the output manager to display an error message and then exit (unix)
void cu_error(const char* error_message, ...)
{
	char message_buffer[2048];

	va_list args;
	va_start(args, error_message);
	vsnprintf(message_buffer, sizeof(message_buffer), (char*) error_message, args);
	va_end(args);

	#ifdef _WIN32
	MessageBoxA(NULL, message_buffer, "Error", MB_ICONSTOP | MB_SETFOREGROUND);
	#endif

	exit(1);
}

void cu_message(const void* message, ...)
{
	char message_buffer[2048];

	va_list args;
	va_start(args, message);
	vsnprintf(message_buffer, sizeof(message_buffer), (char*) message, args);
	va_end(args);

	#ifdef _WIN32
	MessageBoxA(NULL, message_buffer, "Message", 0);
	#endif
}

// get number of connections open
int cu_get_total_connections_open()
{
	return _connections_open;
}

// get total bytes sent (monitored)
int cu_get_total_bytes_sent()
{
	return _total_bytes_sent;
}

// get total bytes received (monitored)
int cu_get_total_bytes_received()
{
	return _total_bytes_received;
}

// get bytes sent on a socket
int cu_get_bytes_sent(int socket_id)
{
	return get_connection_stat(socket_id, 0);
}

// get total bytes received on a socket
int cu_get_bytes_received(int socket_id)
{
	return get_connection_stat(socket_id, 1);
}

// get aggregate socket statistics
void cu_get_socket_data(int* total_sockets, _cu_sockinfo** dest)
{
	int counter = 0;
	_cu_sockinfo* current;

	pthread_mutex_lock(&_sock_info_lock);
	for (current = _cu_sockinfo_head; current != NULL; current = current->next)
		counter++;

	if (counter == 0)
	{
		*total_sockets = 0;
		*dest = NULL;
	}
	else
	{
		*dest = (_cu_sockinfo*) malloc(counter*sizeof(_cu_sockinfo));
		memset(*dest, 0, counter*sizeof(_cu_sockinfo));
		*total_sockets = counter;
	
		counter = 0;
		for (current = _cu_sockinfo_head; current != NULL; current = current->next)
		{
			memcpy(*dest+counter, current, sizeof(_cu_sockinfo));
			(*dest)[counter++].next = NULL;
		}
	}
	pthread_mutex_unlock(&_sock_info_lock);
} 


// auto-restarting socket send
int cu_send(int socket_id, const char* buf, int buf_len)
{
	int bytes_out=0;
	int offset=0;

	if (buf_len <= 0 || buf == NULL)
		goto fail;

	while(1)
	{
		bytes_out = send(socket_id, buf+offset, buf_len-offset, MSG_NOSIGNAL);
		if (bytes_out == -1)
		{
			if (errno == EINTR)
				continue;
			else
				goto fail;
		}
		else if (bytes_out == 0)
			goto fail;
		else
			offset += bytes_out;

		if (offset == buf_len)
			break;
	}

	update_connection_stat(socket_id, buf_len, 0);
	return buf_len;

fail:
	return -1;
}

// performs an recv call in lieu of calling the sockets function directly
// this function is more platform portable and recovers from signals
int cu_recv(int socket_id, char* buffer, int buffer_length)
{
	int bytes_recv = 0;
	
	if (buffer_length < 0)
		goto fail;
	else if (buffer_length == 0)
		return 0;

	while ((bytes_recv = recv(socket_id, buffer, buffer_length, 0)) == -1 && errno == EINTR);

	if (bytes_recv == 0)
		return -1;

	update_connection_stat(socket_id, bytes_recv, 1);
	return bytes_recv;

fail:
	return -1;
}

double cu_calculate_time_difference(struct timeval end_time, struct timeval start_time)
{
	return ((double)end_time.tv_sec + ((double)end_time.tv_usec)/1000000.0) - ((double)start_time.tv_sec + ((double)start_time.tv_usec/1000000.0));
}

double cu_get_time_elapsed_to_now(struct timeval start_time)
{
	struct timeval current_time;
	gettimeofday(&current_time, NULL);

	return cu_calculate_time_difference(current_time, start_time);
}

int cu_generate_random_number(int lower_bound, int upper_bound)
{
	int difference;

	// get random number
	difference = upper_bound-lower_bound+1;
	return (rand()%difference)+lower_bound;
}

void cu_reseed_random_number_generator()
{
	struct timeval current_time;
	uint32_t seed_value;

	// seed the random number generator
	gettimeofday(&current_time, NULL);
	seed_value = (uint32_t) current_time.tv_sec + (uint32_t) current_time.tv_usec;
	srand(seed_value);
}

void cu_copy_ip_address(char* dest_addr_buf, const char* src_addr_buf)
{
	memcpy(dest_addr_buf, src_addr_buf, 16);
}

int cu_validate_ip_address(const char* ip_addr)
{
	int buffer_size;
	int dots_seen = 0;
	int numbers_seen = 0;
	int offset = 0;
	bool previous_numeric = false;

	buffer_size = strlen(ip_addr);
	if (buffer_size >= 16)
		goto fail;
	else
	{
		for(offset = 0; offset < buffer_size; offset++)
		{
			if (ip_addr[offset] >= '0' && ip_addr[offset] <= '9')
			{
				if (previous_numeric == false)
				{
					previous_numeric = true;
					numbers_seen++;
				}
			}
			else if (ip_addr[offset] == '.')
			{
				dots_seen++;
				previous_numeric = false;
			}
			else
				goto fail;
		}
	}

	if (numbers_seen != 4 || dots_seen != 3)
		goto fail;

	// all checks passed
	return 0;

fail:
	return -1;
}

int cu_resolve_hostname_to_ip_address(const char* hostname, char* ip_addr_buf)
{
	struct hostent* resolver;

	// clear out the ip address buffer first
	memset(ip_addr_buf, 0, 16);

	pthread_mutex_lock(&host_lookup_lock);
	resolver = gethostbyname(hostname);
	if (resolver == NULL)
	{
		pthread_mutex_unlock(&host_lookup_lock);
		strcpy(ip_addr_buf, "0.0.0.0");
		return -1;
	}
	else
	{
		memcpy(ip_addr_buf, resolver->h_addr_list[0], resolver->h_length);
		pthread_mutex_unlock(&host_lookup_lock);
	}

	return 0;
}

// stores the local ip address into the address buffer
// attempts two interfaces -- eth0 and wlan0
int cu_get_local_ip_address(char* addr_buf)
{
#ifndef _WIN32
// UNIX CODE

	struct ifreq ifr;
	struct sockaddr_in saddr;
	int fd;
	uint8_t unparsed_ip_address[4];

	fd = socket(AF_INET, SOCK_STREAM, 0);
	strcpy(ifr.ifr_name, "eth0");
	ioctl(fd, SIOCGIFADDR, &ifr);
	saddr = *((struct sockaddr_in*) (&(ifr.ifr_addr)));

	unparsed_ip_address[0] = (uint8_t) ((saddr.sin_addr.s_addr << 24) >> 24);
	unparsed_ip_address[1] = (uint8_t) (((saddr.sin_addr.s_addr >> 8) << 24) >> 24);
	unparsed_ip_address[2] = (uint8_t) (((saddr.sin_addr.s_addr >> 16) << 24) >> 24);
	unparsed_ip_address[3] = (uint8_t) (saddr.sin_addr.s_addr >> 24);
	
	sprintf(addr_buf, "%d.%d.%d.%d", unparsed_ip_address[0], unparsed_ip_address[1], unparsed_ip_address[2], unparsed_ip_address[3]);

	if (strcmp(addr_buf, "0.0.0.0") == 0 || strcmp(addr_buf, "127.0.0.1") == 0)
	{
		strcpy(ifr.ifr_name, "wlan0");
		ioctl(fd, SIOCGIFADDR, &ifr);
		saddr = *((struct sockaddr_in*) (&(ifr.ifr_addr)));

		unparsed_ip_address[0] = (uint8_t) ((saddr.sin_addr.s_addr << 24) >> 24);
		unparsed_ip_address[1] = (uint8_t) (((saddr.sin_addr.s_addr >> 8) << 24) >> 24);
		unparsed_ip_address[2] = (uint8_t) (((saddr.sin_addr.s_addr >> 16) << 24) >> 24);
		unparsed_ip_address[3] = (uint8_t) (saddr.sin_addr.s_addr >> 24);
	
		sprintf(addr_buf, "%d.%d.%d.%d", unparsed_ip_address[0], unparsed_ip_address[1], unparsed_ip_address[2], unparsed_ip_address[3]);
	}

	close(fd);

	if (strcmp(addr_buf, "0.0.0.0") == 0 || strcmp(addr_buf, "127.0.0.1") == 0)
		return -1;

	return 0;

#else
// WINDOWS CODE

	char host_name[1024];
	struct sockaddr_in local_address;
	struct hostent* host = NULL;

	sprintf(addr_buf, "0.0.0.0");

	if (gethostname(host_name, sizeof(host_name)))
		goto fail;

	host = gethostbyname(host_name);
	if (host == NULL)
		goto fail;

	memcpy(&local_address.sin_addr, host->h_addr_list[0], host->h_length);
	strcpy(addr_buf, inet_ntoa(local_address.sin_addr));
	if (strcmp(addr_buf, "0.0.0.0") == 0)
		goto fail;

	// successful
	return 0;

fail:
	return -1;

#endif
}

void* cu_alloc(uint32_t number_of_bytes)
{
	void* ret = malloc(number_of_bytes);
	if (ret == NULL)
		assert(0);
	return ret;
}

void get_console_dimensions(uint32_t* width, uint32_t* height)
{
	*width = 0;
	*height = 0;

#ifndef _WIN32

	struct winsize w;
	ioctl(0, TIOCGWINSZ, &w);

	*width = w.ws_row;
	*height = w.ws_col;

#else

	CONSOLE_SCREEN_BUFFER_INFO csbi;
	int ret;
	ret = GetConsoleScreenBufferInfo(GetStdHandle( STD_OUTPUT_HANDLE ),&csbi);
	if(ret)
	{
		*width = csbi.dwSize.X;
		*height = csbi.dwSize.Y;
	}
#endif
}


// performs initialization, more applicable for Windows sockets
void cu_initialize_network()
{
#ifndef _WIN32
// UNIX CODE
	// no initialization required to use sockets
#else
// WINDOWS CODE
	WSADATA wsa_data;
	WSAStartup(MAKEWORD(2,0), &wsa_data);
#endif
}

#ifdef _WIN32

// performs the UNIX equivalent of close() for sockets
void close(SOCKET socket_id)
{
	shutdown(socket_id, 2);
	closesocket(socket_id);
}

// performs the UNIX equivalent of inet_ntop() for sockets
const char *inet_ntop(int af, const void *src, char *dst, uint32_t cnt)
{
	if (af == AF_INET)
	{
		struct sockaddr_in in;
		memset(&in, 0, sizeof(in));
		in.sin_family = AF_INET;
		memcpy(&in.sin_addr, src, sizeof(struct in_addr));
		getnameinfo((struct sockaddr *)&in, sizeof(struct sockaddr_in), dst, cnt, NULL, 0, NI_NUMERICHOST);
		return dst;
	}
	else if (af == AF_INET6)
	{
		struct sockaddr_in6 in;
		memset(&in, 0, sizeof(in));
		in.sin6_family = AF_INET6;
		memcpy(&in.sin6_addr, src, sizeof(struct in_addr6));
		getnameinfo((struct sockaddr *)&in, sizeof(struct sockaddr_in6), dst, cnt, NULL, 0, NI_NUMERICHOST);
		return dst;
	}
	return NULL;
}

// WINDOWS port of gettimeofday
int gettimeofday(struct timeval *tv, struct timezone *tz)
{
	FILETIME ft;
	uint64_t tmpres = 0;

	if (tv != NULL)
	{
		GetSystemTimeAsFileTime(&ft);

		tmpres |= ft.dwHighDateTime;
		tmpres <<= 32;
		tmpres |= ft.dwLowDateTime;

		tmpres /= 10;
		tmpres -= DELTA_EPOCH_IN_MICROSECS;
		tv->tv_sec  = (long) (tmpres / 1000000UL);
		tv->tv_usec = (long) (tmpres % 1000000UL);
	}

	return 0;
}

#endif

double cu_get_cpu_utilization()
{
	double cpu_utilization = 0.0;

#ifdef _WIN32

	static bool first_run = false;
	
	// windows implementation
	FILETIME system_idle_time, system_kernel_time, system_user_time;
	FILETIME process_creation_time, process_exit_time, process_in_kernel_time, process_in_user_time;

	static FILETIME previous_system_kernel_time, previous_system_user_time, previous_process_kernel_time, previous_process_user_time;

	GetSystemTimes(&system_idle_time, &system_kernel_time, &system_user_time);
	GetProcessTimes(GetCurrentProcess(), &process_creation_time, &process_exit_time, &process_in_kernel_time, &process_in_user_time);

	if (!first_run)
	{
		ULONGLONG system_kernel_difference, system_user_difference, process_kernel_difference, process_user_difference;
		ULONGLONG total_kernel_time, total_process_time;
		system_kernel_difference = __cu_internal_subtract_times(system_kernel_time, previous_system_kernel_time);
		system_user_difference = __cu_internal_subtract_times(system_user_time, previous_system_user_time);
		process_kernel_difference = __cu_internal_subtract_times(process_in_kernel_time, previous_process_kernel_time);
		process_user_difference = __cu_internal_subtract_times(process_in_user_time, previous_process_user_time);

		total_kernel_time = system_kernel_difference + system_user_difference;
		total_process_time = process_kernel_difference + process_user_difference;
		cpu_utilization = (100.0*total_process_time)/total_kernel_time;
		if (cpu_utilization > 100.0)
			cpu_utilization = 100.0;
	}

	previous_system_kernel_time = system_kernel_time;
	previous_system_user_time = system_user_time;
	previous_process_kernel_time = process_in_kernel_time;
	previous_process_user_time = process_in_user_time;

	first_run = false;

#else

	// linux implementation
	static bool first_run = true;
	static char filename[200];
	static unsigned long last_utime, last_stime, last_total_time;
	FILE* fp;

	// fields in stat/proc
	char state;
	int pid, ppid, pgrp, session, tpgid;
	char command[100], cpu_id[100];
	unsigned long flags, minflt, cminflt, majflt, cmajflt, utime, stime;
	unsigned long user, nice, system, idle, iowait, irq, softirq;
	unsigned long current_total_time;
	
	if (first_run)
	{
		memset(filename, 0, sizeof(filename));
		sprintf(filename, "/proc/%ld/stat", (unsigned long) getpid());
	}

	fp = fopen(filename, "r");
	fscanf(fp, "%d %s %c %d %d %d %d %lu %lu %lu %lu %lu %lu %lu", &pid, command, &state, &ppid, &pgrp, &session, &tpgid, &flags, &minflt, &cminflt, &majflt,
			&cmajflt, &utime, &stime);
	fclose(fp);
	fp = fopen("/proc/stat", "r");
	fscanf(fp, "%s %lu %lu %lu %lu %lu %lu %lu", cpu_id, &user, &nice, &system, &idle, &iowait, &irq, &softirq);
	fclose(fp);

	if (first_run)
	{
		first_run = false;
		last_utime = utime;
		last_stime = stime;
		last_total_time = user+nice+system+idle+iowait+irq+softirq;
		
		cpu_utilization = 0.0;
	}
	else
	{
		current_total_time = user+nice+system+idle+iowait+irq+softirq;
		cpu_utilization = 100.0*(double)(utime+stime-last_utime-last_stime) / (double)(current_total_time-last_total_time);
		if (cpu_utilization > 100.0)
			cpu_utilization = 100.0;

		last_utime = utime;
		last_stime = stime;
		last_total_time = current_total_time;
	}

#endif

	return cpu_utilization;
}

#ifdef _WIN32

ULONGLONG __cu_internal_subtract_times(const FILETIME& final_time, const FILETIME& initial_time)
{
	LARGE_INTEGER a, b;
	a.LowPart = final_time.dwLowDateTime;
	a.HighPart = final_time.dwHighDateTime;

	b.LowPart = initial_time.dwLowDateTime;
	b.HighPart = initial_time.dwHighDateTime;

	return a.QuadPart - b.QuadPart;
}

#endif
