#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include "gateway_controller.h"
#include "gateway_commands.h"
#include "common_utils.h"

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>
using namespace std;

int main(int argc, char** argv)
{
    cout<<"Controller starts listening"<<endl;
    startup_gateway_control();
    cout<<"Press anykey to exit"<<endl;
    getchar();
    cout<<"Bye bye"<<endl;
	return 0;
}

// function to start up the listening thread
void startup_gateway_control()
{
	pthread_t tid;
	assert(pthread_create(&tid, NULL, gateway_control_thread, NULL) == 0);
}

// thread code that actually processes messages from the session controller
void* gateway_control_thread(void* args)
{
	int server_socket, client_socket;
	pthread_detach(pthread_self());

	// set socket to listen	
	assert(cu_listen(&server_socket, GATEWAY_CONTROL_PORT) == 0);

	while(1)
	{
		while(cu_accept(&client_socket, server_socket) != 0);
	
		// client connected
		process_client_messages(client_socket);
		close(client_socket);
	}

	return NULL;
}

// code that processes gateway control commands
void process_client_messages(int client_socket)
{
	_gateway_command command;

	// reading loop
	while(1)
	{
		if (cu_read_fixed_bytes_from_socket(client_socket, sizeof(command), &command) < 0)
			return;

		switch(command.command_id)
		{
			case SET_PRIMARY_SERVER:
				set_primary_server(command.src_ip);
				break;

			case TRANSFER_CONNECTION:
				transfer_connection(command.src_ip, ntohs(*((unsigned short*) command.src_port)), command.dest_ip, ntohs(*((unsigned short*) command.dest_port)));
				break;

			// command not recognized -- return to close connection
			default:
				return;
		}
	}
}

// code to direct all initial requests to some server
void set_primary_server(const char* primary_server_ip)
{

	// Run the change_primary bash script
    char cmd[64];
    sprintf(cmd, "./change_primay.sh %s %d", primary_server_ip, 80);
    cout << "Called set primary server" << endl;    
    if (system(cmd) == -1)
        cout << "Failed" << endl;
}

// code to redirect all requests from one server to another
// don't worry about application-level state, this will be handled by the session controller
void transfer_connection(const char* src_ip, unsigned short src_port, const char* dest_ip, unsigned short dest_port)
{
	// Run the connection transfer bash script
    char cmd[64];
    sprintf(cmd, "./migrate_session.sh %s %d %s %d", src_ip, src_port, dest_ip, dest_port);
    cout << "Called connection transfer" << endl;
    if (system(cmd) == -1)
        cout << "Failed" << endl;
}


