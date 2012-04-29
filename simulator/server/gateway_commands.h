#ifndef __GATEWAY_COMMANDS
#define __GATEWAY_COMMANDS

typedef struct
{
	char command_id;

	char src_ip[16];
	char src_port[2];

	char dest_ip[16];
	char dest_port[2];

} _gateway_command;

enum { SET_PRIMARY_SERVER=1, TRANSFER_CONNECTION };


#endif
