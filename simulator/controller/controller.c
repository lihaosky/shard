/* For sockaddr_in */
#include <netinet/in.h>
/* For socket functions */
#include <sys/socket.h>
/* For inet_ntoa */
#include <arpa/inet.h>
/* For fcntl */
#include <fcntl.h>

#include <event2/event.h>
#include <event2/buffer.h>
#include <event2/bufferevent.h>

#include <assert.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>

/* For hash map */
#include "uthash/uthash.h"

#define MAX_LINE 16384

void do_read(evutil_socket_t fd, short events, void *arg);
void do_write(evutil_socket_t fd, short events, void *arg);

/* Managing the server status in hashmap */
typedef struct server {
    struct bufferevent *bev;
    int hit;    
} SERVER;

typedef struct server_item {
    char name[24];          /* ip:port makes the key */
    SERVER *serv;
    UT_hash_handle hh;      /* makes this structure hashable */
} SERV_IT;

SERV_IT *servers = NULL;    /* Declared hashmap */

void
readcb(struct bufferevent *bev, void *ctx)
{
    struct evbuffer *input, *output;
    char *line, *ip_port;
    size_t n;
    int i;
    input = bufferevent_get_input(bev);
    output = bufferevent_get_output(bev);

    ip_port = (char *)ctx;
    
    while ((line = evbuffer_readln(input, &n, EVBUFFER_EOL_CRLF))) {
        // TODO: record report
        SERV_IT *serv_item;
        HASH_FIND_STR( servers, ip_port, serv_item);
        if (serv_item) {
            serv_item->serv->hit += 1;
        }        
        printf("Get report from %s on key %s\n", ip_port, line);
        free(line);
    }

    if (evbuffer_get_length(input) >= MAX_LINE) {
        /* Too long; just process what there is and go on so that the buffer
         * doesn't grow infinitely long. */
        char buf[1024];
        while (evbuffer_get_length(input)) {
            int n = evbuffer_remove(input, buf, sizeof(buf));
            // TODO: process data in line
        }
    }
}

void
errorcb(struct bufferevent *bev, short error, void *ctx)
{
    if (error & BEV_EVENT_EOF) {
        /* connection has been closed, do any clean up here */
        /* ... */
    } else if (error & BEV_EVENT_ERROR) {
        /* check errno to see what error occurred */
        /* ... */
    } else if (error & BEV_EVENT_TIMEOUT) {
        /* must be a timeout event handle, handle it */
        /* ... */
    }
    bufferevent_free(bev);
}

void
do_accept(evutil_socket_t listener, short event, void *arg)
{
    struct event_base *base = arg;
    struct sockaddr_in sa_in;
    socklen_t slen = sizeof(sa_in);
    int fd = accept(listener, (struct sockaddr *)&sa_in, &slen);
    if (fd < 0) {
        perror("accept");
    } else if (fd > FD_SETSIZE) {
        close(fd);
    } else {
        /* get address formed in ip:port */
        char ip_port[24];        
        if (sa_in.sin_family == AF_INET) {
            sprintf(ip_port, "%s:%d", inet_ntoa(sa_in.sin_addr), 
                ntohs(sa_in.sin_port));
            printf("Got connection from %s\n", ip_port);            
        }
        else {
            printf("Remote host has invalid address family, close\n");
            close(fd);
            return;
        }
        
        struct bufferevent *bev;
        evutil_make_socket_nonblocking(fd);
        bev = bufferevent_socket_new(base, fd, BEV_OPT_CLOSE_ON_FREE);
        
        /* add new server into hashtable */
        SERVER *mem_serv = malloc(sizeof(SERVER));
        mem_serv->bev = bev;
        mem_serv->hit = 0;
        SERV_IT *mem_serv_it = malloc(sizeof(SERV_IT));
        mem_serv_it->serv = mem_serv;
        memcpy(mem_serv_it->name, ip_port, 24);
        HASH_ADD_STR(servers, name, mem_serv_it);
        
        /* setup event callback for the new connection */
        bufferevent_setcb(bev, readcb, NULL, errorcb, ip_port);
        bufferevent_setwatermark(bev, EV_READ, 0, MAX_LINE);
        bufferevent_enable(bev, EV_READ|EV_WRITE);
    }
}

void
run(void)
{
    evutil_socket_t listener;
    struct sockaddr_in sin;
    struct event_base *base;
    struct event *listener_event;

    base = event_base_new();
    if (!base)
        return; /*XXXerr*/

    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = 0;
    sin.sin_port = htons(40713);

    listener = socket(AF_INET, SOCK_STREAM, 0);
    evutil_make_socket_nonblocking(listener);

#ifndef WIN32
    {
        int one = 1;
        setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));
    }
#endif

    if (bind(listener, (struct sockaddr*)&sin, sizeof(sin)) < 0) {
        perror("bind");
        return;
    }

    if (listen(listener, 16)<0) {
        perror("listen");
        return;
    }

    listener_event = event_new(base, listener, EV_READ|EV_PERSIST, do_accept, (void*)base);
    /*XXX check it */
    event_add(listener_event, NULL);

    event_base_dispatch(base);
}

int
main(int c, char **v)
{
    setvbuf(stdout, NULL, _IONBF, 0);

    run();
    return 0;
}
