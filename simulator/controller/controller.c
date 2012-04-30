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
#include <pthread.h>

/* For hashtable, list and dynamic array */
#include "uthash/uthash.h"
#include "uthash/utlist.h"
#include "uthash/utarray.h"

/* For libmemcached */
#include "util.h"

#define MAX_LINE 16384

/* Traffic head for differen use */
enum head 
{
    REPORT = 1,     /* arbitrary value. */
    FETCH
};

#define STAT_KEY_LEN 128
#define SERV_ID_LEN 24
#define SERV_PORT 9000

/* Managing replica status of each object */
typedef struct replica_stat
{
    char kname[STAT_KEY_LEN];
    int replica_cnt;
    int old_index;
    UT_array *replicas;
    UT_hash_handle hh;
} REPLICA_STATUS;
REPLICA_STATUS *replica_stats = NULL;

/* Managing the key report of that server */
typedef struct key_report
{
    char kname[STAT_KEY_LEN];
    int hit;
    UT_hash_handle hh;
} KV_REPORT;

/* Managing the server status in hashtable */
typedef struct server 
{
    struct bufferevent *bev;
    KV_REPORT *reports;         /* hashtable for key report */
    int hit;
} SERVER;
typedef struct server_item 
{
    char name[SERV_ID_LEN];     /* ip:port makes the key */
    SERVER *serv;
    UT_hash_handle hh;          /* makes this structure hashable */
} SERV_IT;

pthread_rwlock_t lock;          /* read_write lock for server hashtable */
SERV_IT *servers = NULL;        /* declared hashtable for all servers*/
//KV_REPORT *objects = NULL;      /* decalred hashtable for all objects*/

int total_hit = 0;              /* total hit of all servers */
//int min_hit = 0;                /* hit of the least loaded server */
//SERV_IT *min_serv_it = NULL;    /* the least loaded server item in hashtable */

SERV_IT *
addserv(struct bufferevent *bev, char *ip_port) 
{
    /* add new server into hashtable */
    SERVER *mem_serv = malloc(sizeof(SERVER));
    mem_serv->bev = bev;
    mem_serv->hit = 0;
    mem_serv->reports = NULL;
    SERV_IT *mem_serv_it = malloc(sizeof(SERV_IT));
    mem_serv_it->serv = mem_serv;
    memcpy(mem_serv_it->name, ip_port, SERV_ID_LEN);
    HASH_ADD_STR(servers, name, mem_serv_it);
    return mem_serv_it;
}

void
addreport(SERV_IT *serv_item, char *key, int hit) 
{
    /* record report for relevant server and key */
    serv_item->serv->hit += hit;
    printf("Get report from %s on key %s for %dth hit\n", serv_item->name, key, serv_item->serv->hit);
    KV_REPORT *kv_rep;
    HASH_FIND_STR(serv_item->serv->reports, key, kv_rep);
    if (!kv_rep) {
        kv_rep = malloc(sizeof(KV_REPORT));
        memcpy(kv_rep->kname, key, STAT_KEY_LEN);
        HASH_ADD_STR(serv_item->serv->reports, kname, kv_rep);
    }
    kv_rep->hit += hit;
    total_hit += hit;
    printf("Total hit number is %d\n", total_hit);
}

void
process_report(struct bufferevent *bev, char *tokens, char *ip_port) 
{
    SERV_IT *serv_item;
    HASH_FIND_STR(servers, ip_port, serv_item);
    if (!serv_item) {
        serv_item = addserv(bev, ip_port);
    }
    char *key = strtok(NULL, ":");
    //TODO: multi key report case
    addreport(serv_item, key, 1);
}

void
process_fetch(struct bufferevent *bev, char *tokens, char *ip_port) 
{
    //TODO: send replication info
}

/* Callback functions for events */
void
readcb(struct bufferevent *bev, void *ctx) 
{
    struct evbuffer *input, *output;
    char *line, *ip_port;
    size_t n;
    input = bufferevent_get_input(bev);
    output = bufferevent_get_output(bev);

    ip_port = (char *)ctx;
    while ((line = evbuffer_readln(input, &n, EVBUFFER_EOL_CRLF))) {
        char *tokens = strtok(line, ":");
        switch (atoi(tokens)) {
            case REPORT:
                process_report(bev, tokens, ip_port);
                break;
            case FETCH:
                process_fetch(bev, tokens, ip_port);
                break;
        }
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
        char *ip_port = (char *)ctx;

        /* remove server from hashmap */
        SERV_IT *serv_item;
        HASH_FIND_STR(servers, ip_port, serv_item);
        if (serv_item) {
            HASH_DEL(servers, serv_item);
            //free(serv_item->serv);
            //free(serv_item->name);
            free(serv_item);
        }
        free(ip_port);

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
        char *ip_port = malloc(SERV_ID_LEN * sizeof(char));        
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

        /* setup event callback for the new connection */
        bufferevent_setcb(bev, readcb, NULL, errorcb, ip_port);
        bufferevent_setwatermark(bev, EV_READ, 0, MAX_LINE);
        bufferevent_enable(bev, EV_READ|EV_WRITE);
    }
}

int server_hit_sort(SERV_IT *a, SERV_IT *b) 
{
    //make the hashmap sorted with highest hit value first
    return -(a->serv->hit - b->serv->hit);
}

int key_hit_sort(KV_REPORT *a, KV_REPORT *b)
{
    return -(a->hit - b->hit);
}

int
replicate(char *key, UT_array *stats, int last_src_index)
{
    move_key_mserver_index(key, stats, SERV_PORT, last_src_index);
    return 1;
}

void
rep_adjust(int fd, short event, void *arg) 
{
    printf("Rep adjusted!\n");

    //Calculate and adjust replicas:
    //1. get average load of servers, keep replicating the most popular items
    //from the most popular server to least popular server until no one is
    //10% higher than average
    //2. in every step calculation the number of replicas is doubled
    int server_count = HASH_COUNT(servers);
    
    if (server_count == 0) {
        /* no server to maintain */
        return;
    }
    
    int average_hit = total_hit / server_count;
    printf("Average hit per server is %d\n", average_hit);
    int replica_bar = (int)((float)average_hit * 1.1);
    printf("Replicate bar is hit %d\n", replica_bar);

    /* sort servers based on their hit cnt */
    HASH_SORT(servers, server_hit_sort); 

    SERV_IT *busy_item, *casual_item;
    int pop_obj_cnt = 0;
    /* find pop object, and calculate number of replicas in need*/
    for(busy_item = servers; busy_item != NULL; busy_item = busy_item->hh.next) {
        
        float diff = busy_item->serv->hit - replica_bar;
        if (diff > 0) {
            printf("Server %s crossed the bar!\n", busy_item->name);
            //potentially very inefficient for now
            //TODO: optimize it later for performance
            HASH_SORT(busy_item->serv->reports, key_hit_sort);
            KV_REPORT * pop_obj;
            for(pop_obj = busy_item->serv->reports; pop_obj != NULL; 
                pop_obj=pop_obj->hh.next) {
                if (pop_obj->hit <= average_hit) {
                    break;      /* done with this busy server */
                }
                else {
                    int need_replica = pop_obj->hit/average_hit; /* at least one */
                    REPLICA_STATUS *pop_status;
                    HASH_FIND_STR(replica_stats, pop_obj->kname, pop_status);
                    if (!pop_status)
                    {
                        pop_status = malloc(sizeof(REPLICA_STATUS));
                        memcpy(pop_status->kname, pop_obj->kname, STAT_KEY_LEN);
                        pop_status->old_index = 0;
                        pop_status->replica_cnt = 1;
                        utarray_new(pop_status->replicas,&ut_str_icd);
                        char *rep_name = malloc(SERV_ID_LEN * sizeof(char));
                        memcpy(rep_name, busy_item->name, SERV_ID_LEN);
                        utarray_push_back(pop_status->replicas, rep_name);
                        HASH_ADD_STR(replica_stats, kname, pop_status);
                    }
                    pop_status->replica_cnt += need_replica;
                    pop_obj_cnt += 1;
                }
            }
        }
        casual_item = busy_item;
    }
    
    printf("Need to replicate %d popular objects!\n", pop_obj_cnt);
    
    /* exit if no need to do replication */
    if (pop_obj_cnt == 0) return;
    
    /* now to find approriate casual servers to serve as replicas */
    REPLICA_STATUS *pop_status;
    bool can_replicate = true;
    for(pop_status = replica_stats; pop_status != NULL; 
        pop_status = pop_status->hh.next) {
        if ( can_replicate == true &&
            (casual_item == NULL || casual_item->serv->hit >= replica_bar)) {
            /* if casual_item has been used up */
            can_replicate = false;
        }
        SERV_IT *candidate_item = casual_item;
        int need = pop_status->replica_cnt-pop_status->old_index-1;
        while (need != 0 && can_replicate == true) {
            if ((float)(candidate_item->serv->hit) < replica_bar)
            {
                /* add candidate and adjust its hit*/
                candidate_item->serv->hit += average_hit;
                char *rep_name = malloc(SERV_ID_LEN * sizeof(char));
                memcpy(rep_name, candidate_item->name, SERV_ID_LEN);
                utarray_push_back(pop_status->replicas, rep_name);
                need--;
            } else
            {
                /* need to update the casual_item to point to less replicated */
                casual_item = casual_item->hh.prev;
            }
            candidate_item = candidate_item->hh.prev;
            if (candidate_item == NULL || candidate_item->serv->hit >= replica_bar)
            {
                /* used up candidates */
                casual_item = candidate_item;
                break;
            }
        }
        
        /* adjust old replica's hit based on new replication */
        int new_replica = pop_status->replica_cnt-pop_status->old_index-1-need;
        /*adjust replica count*/
        pop_status->replica_cnt = pop_status->old_index + new_replica;
        if (new_replica != 0) {
            int i;
            for(i=0; i<=pop_status->old_index; i++) {
                char **p = (char **)utarray_eltptr(pop_status->replicas, i);
                SERV_IT *old_item = NULL;
                HASH_FIND_STR(servers, *p, old_item);
                if (old_item != NULL) {
                    old_item->serv->hit -= average_hit / (pop_status->old_index + 1);
                }
                else {
                    /* sth going wrong!! */
                    printf("Server %s not known, error!!\n", *p);
                    exit(-1);
                }
            }
            /* do replication */
            replicate(pop_status->kname, pop_status->replicas, pop_status->old_index);
            pop_status->old_index = pop_status->replica_cnt - 1;
        }
    }
}

/* Setup listener and timer */
void
run(void) {
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

    /* set up a listening event */
    listener = socket(AF_INET, SOCK_STREAM, 0);
    evutil_make_socket_nonblocking(listener);

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
    
    /* setup timer driver event */
    struct event *timer_ev; 
    timer_ev = event_new(base, -1, EV_PERSIST|EV_TIMEOUT, rep_adjust, NULL);
    struct timeval tv = {5,0};
    evtimer_add(timer_ev, &tv);

    /* start the main levent loop */
    event_base_dispatch(base);
}

int
main(int c, char **v) {
    setvbuf(stdout, NULL, _IONBF, 0);

    run();
    return 0;
}
