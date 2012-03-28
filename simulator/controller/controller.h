#ifndef _CONTROLLER_H_
#define _CONTROLLER_H_

void start_client_control();
void start_server_control();
void* client_control_thread(void* args);
void* server_control_thread(void* args);

#endif
