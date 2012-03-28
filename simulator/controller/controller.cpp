#include <pthread.h>
#include <unistd.h>
#include <assert.h>
#include "gateway_controller.h"
#include "gateway_commands.h"
#include "common_utils.h"
#include "controller.h"

#include <cstdlib>
#include <cstdio>
#include <cstring>
#include <iostream>

#define CONTROLLER_PORT 4321
#define SERVER_PORT 4322

using namespace std;

int main(int argc, char** argv)
{
    cout << "Starting controller..." << endl;
    
    start_client_control();
    cout << "Starting to listen to client..." << endl;

    start_server_control();
    cout << "Starting to listen to server..." << endl;

    getchar();

    return 0;

}

// function to start up the listening thread
void start_client_control() {
	pthread_t tid;
	assert(pthread_create(&tid, NULL, client_control_thread, NULL) == 0);
}

// thread code that actually processes messages from the session controller
void* client_control_thread(void* args) {
	int server_socket, client_socket;
	pthread_detach(pthread_self());

	// set socket to listen	
	assert(cu_listen(&server_socket, CONTROLLER_PORT) == 0);

	while(1)
	{
		while(cu_accept(&client_socket, server_socket) != 0);
	
        int data_length, command_type;
        long arrival_time;
        char key[128];

        if (cu_read_command_from_socket(client_socket, &data_length, &arrival_time, &command_type, key) != 0) {
            cout << "Something wrong!" << endl;
        }

        cout << "Data length is " << data_length << endl;
        cout << "Arrival_time is " << arrival_time << endl;
        cout << "Command type is " << command_type << endl;
        cout << "Key is " << endl;

		close(client_socket);
	}

	return NULL;
}

void start_server_control() {
    pthread_t tid;
    assert(pthread_create(&tid, NULL, server_control_thread, NULL) == 0);
}

void* server_control_thread(void* args) {
    int server_socket, client_socket;
    pthread_detach(pthread_self());

    assert(cu_listen(&server_socket, SERVER_PORT) == 0);

    while (1) {
        while (cu_accept(&client_socket, server_socket) != 0);

        cout << "Got server connection!" << endl;
    }
}

