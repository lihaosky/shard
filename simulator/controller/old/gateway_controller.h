#define GATEWAY_CONTROL_PORT 9000

void startup_gateway_control();
void* gateway_control_thread(void* args);
void process_client_messages(int client_socket);
void set_primary_server(const char* primary_server_ip);
void transfer_connection(const char* src_ip, unsigned short src_port, const char* dest_ip, unsigned short dest_port);
