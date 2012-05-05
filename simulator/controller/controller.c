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
    int target_hit;
} SERVER;
typedef struct server_item 
{
    char name[SERV_ID_LEN];     /* ip:port makes the key */
    SERVER *serv;
    UT_hash_handle hh;          /* makes this structure hashable */
} SERV_IT;

pthread_rwlock_t lock;          /* read_write lock for server hashtable */
SERV_IT *servers = NULL;        /* declared hashtable for all servers*/

int total_hit = 0;              /* total hit of all servers */

SERV_IT *
addserv(struct bufferevent *bev, char *ip_port) 
{
    /* add new server into hashtable */
    SERVER *mem_serv = malloc(sizeof(SERVER));
    mem_serv->bev = bev;
    mem_serv->hit = 0;
    mem_serv->target_hit = 0;   /* used by load calculation for replication */
    mem_serv->reports = NULL;
    SERV_IT *mem_serv_it = malloc(sizeof(SERV_IT));
    mem_serv_it->serv = mem_serv;
    memcpy(mem_serv_it->name, ip_port, SERV_ID_LEN);
    HASH_ADD_STR(servers, name, mem_serv_it);
    return mem_serv_it;
}

/* Return 1: added new, 0: just updated */
int
add_report(SERV_IT *serv_item, char *key, int hit) 
{
    int ret = 0;
    /* record report for relevant server and key */
    serv_item->serv->hit += hit;
    KV_REPORT *kv_rep;
    HASH_FIND_STR(serv_item->serv->reports, key, kv_rep);
    if (!kv_rep) {
        kv_rep = malloc(sizeof(KV_REPORT));
        memcpy(kv_rep->kname, key, STAT_KEY_LEN);
        kv_rep->hit = 0;
        HASH_ADD_STR(serv_item->serv->reports, kname, kv_rep);
        ret = 1;
    }
    kv_rep->hit += hit;
    total_hit += hit;
    if (hit > 0) {
        printf("....on key %.5s", key);
    }
    return ret;
}

void
process_report(struct bufferevent *bev, char *tokens, char *ip_port) 
{
    SERV_IT *serv_item;
    
    if (pthread_rwlock_wrlock(&lock) != 0) {
        fprintf(stderr,"can't acquire write lock\n");
        exit(-1);
    }
    
    HASH_FIND_STR(servers, ip_port, serv_item);
    if (!serv_item) {
        serv_item = addserv(bev, ip_port);
    }
    
    printf("Report from %s for %dth hit\n", serv_item->name, 
        serv_item->serv->hit);
    
    char *key = strtok(NULL, ":");
    add_report(serv_item, key, 1);
    
    pthread_rwlock_unlock(&lock);
    //printf("Total hit number is %d\n", total_hit);
}

void
process_fetch(struct bufferevent *bev, char *tokens, char *ip_port) 
{
    printf("Process Fetch request from client %s\n", ip_port);
    
    if (pthread_rwlock_rdlock(&lock) != 0) {
        fprintf(stderr,"can't acquire read lock\n");
        exit(-1);
    }
    
    struct evbuffer *output;
    output = bufferevent_get_output(bev);
    
    REPLICA_STATUS *stats;
    /* every key is fetched as key:rep1,rep2,...,repN\r\n */
    for (stats = replica_stats; stats != NULL; stats = stats->hh.next) {
        evbuffer_add_printf(output, "%s:", stats->kname);
        char **p = NULL;
        int i = 0;
        while ( (p=(char**)utarray_next(stats->replicas,p))) {
            if (i != 0) {
                evbuffer_add_printf(output, ",");
            }
            evbuffer_add_printf(output, "%s", *p);
            i++;
        }
        evbuffer_add_printf(output, "\r\n");
    }
    /* empty line indicates terminate */
    evbuffer_add_printf(output, "\r\n");
    pthread_rwlock_unlock(&lock);
}

/* Callback functions for events */
void
readcb(struct bufferevent *bev, void *ctx) 
{
    struct evbuffer *input;
    char *line, *ip_port;
    size_t n;
    input = bufferevent_get_input(bev);

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
            // Do nothing means toss this data
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
        printf("========Server %s failed!========\n", ip_port);

        /* remove server from hashmap */
        if (pthread_rwlock_rdlock(&lock) != 0) {
            fprintf(stderr,"can't acquire read lock\n");
            exit(-1);
        }
        SERV_IT *serv_item;
        HASH_FIND_STR(servers, ip_port, serv_item);
        if (serv_item) {
            total_hit -= serv_item->serv->hit;
            HASH_DEL(servers, serv_item);
            free(serv_item->serv);
            free(serv_item);
        }
        free(ip_port);
        pthread_rwlock_unlock(&lock);

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
    printf("Replicating object %s\n", key);
    move_key_mserver_index(key, stats, SERV_PORT, last_src_index);
    return 1;
}

void
rep_adjust(int fd, short event, void *arg) 
{
    printf("Entering rep_adjust function\n");

    //Calculate and adjust replicas:
    //1. get average load of servers, keep replicating the most popular items
    //from the most popular server to least popular server until no one is
    //10% higher than average
    //2. in every step calculation the number of replicas is doubled
    
    if (pthread_rwlock_wrlock(&lock) != 0) {
        fprintf(stderr,"can't acquire write lock\n");
        exit(-1);
    }
    
    int server_count = HASH_COUNT(servers);
    
    if (server_count == 0) {
        /* no server to maintain */
        printf("----No server to maintain\n");
        pthread_rwlock_unlock(&lock);
        return;
    }
    printf("----Server #: %d\n", server_count);
    
    int average_hit = total_hit / server_count;
    printf("----Server average hit: %d\n", average_hit);
    int replica_bar = (int)((float)average_hit * 1.5);
    printf("----Imbalance bar hit: %d\n", replica_bar);

    /* sort servers based on their hit cnt */
    HASH_SORT(servers, server_hit_sort); 

    SERV_IT *busy_item, *casual_item;
    int pop_obj_cnt = 0;
    /* find pop object, and calculate number of replicas in need*/
    for(busy_item = servers; busy_item != NULL; busy_item = busy_item->hh.next) {
        
        float diff = busy_item->serv->hit - replica_bar;
        printf("--------Server %s hit %d\n", busy_item->name, busy_item->serv->hit);
        if (diff > 0) {
            printf("--------Crossed the bar!\n");
            HASH_SORT(busy_item->serv->reports, key_hit_sort);
            KV_REPORT * pop_obj;
            int pop_rank = 0;
            for(pop_obj = busy_item->serv->reports; pop_obj != NULL; 
                pop_obj=pop_obj->hh.next) {
                pop_rank++;
                printf("--------Key %.5s hit %d\n", pop_obj->kname, pop_obj->hit);
                int need_replica = 0;
                if (pop_obj->hit <= average_hit) {
                    if (pop_rank > 1) {
                        break;              /* done with this busy server */
                    }
                    else {
                        need_replica = 1;   /* still replicate the most pop one*/
                    }
                }
                else {
                    /* replicate based on the load, at least one */
                    need_replica = pop_obj->hit/average_hit; 
                }
                printf("------------Key %.5s needs %d more replicas\n",
                       pop_obj->kname, need_replica);
                REPLICA_STATUS *pop_status;
                HASH_FIND_STR(replica_stats, pop_obj->kname, pop_status);
                if (!pop_status)
                {
                    pop_status = malloc(sizeof(REPLICA_STATUS));
                    memcpy(pop_status->kname, pop_obj->kname, STAT_KEY_LEN);
                    pop_status->old_index = 0;
                    pop_status->replica_cnt = 1;
                    utarray_new(pop_status->replicas, &ut_str_icd);
                    char *rep_name = malloc(SERV_ID_LEN * sizeof(char));
                    memcpy(rep_name, busy_item->name, SERV_ID_LEN);
                    utarray_push_back(pop_status->replicas, &rep_name);
                    HASH_ADD_STR(replica_stats, kname, pop_status);
                }
                printf("------------Key %.5s has %d old replicas\n",
                       pop_obj->kname, pop_status->old_index);
                pop_status->replica_cnt += need_replica;
                pop_obj_cnt += 1;
                
            }
        }
        busy_item->serv->target_hit = 0;
        casual_item = busy_item;
    }
    
    printf("----Need to replicate %d popular objects!\n", pop_obj_cnt);
    
    /* exit if no need to do replication */
    if (pop_obj_cnt == 0) {
        pthread_rwlock_unlock(&lock);
        return;
    }
    
    /* now to find approriate casual servers to serve as replicas */
    REPLICA_STATUS *pop_status;
    bool can_replicate = true;
    for(pop_status = replica_stats; pop_status != NULL; 
        pop_status = pop_status->hh.next) {
        if ( can_replicate == true &&
            (casual_item == NULL || casual_item->serv->hit >= replica_bar)) {
            /* if casual_item has been used up */
            printf("--------No more load capacity for replication!\n");
            can_replicate = false;
        }
        SERV_IT *candidate_item = casual_item;
        int need = pop_status->replica_cnt-pop_status->old_index-1;
        printf("----Object %.5s starts finding %d more replicas\n", 
            pop_status->kname, need);
        while (need != 0 && can_replicate == true) {
            if (candidate_item->serv->hit + candidate_item->serv->target_hit 
                < replica_bar) {
                /* check whether this candidate has replica already*/
                int added = add_report(candidate_item, pop_status->kname, 0);
                if (added == 1) {
                    /* add candidate and adjust its extra target hit*/
                    candidate_item->serv->target_hit += average_hit;
                    char *rep_name = malloc(SERV_ID_LEN * sizeof(char));
                    memcpy(rep_name, candidate_item->name, SERV_ID_LEN);
                    utarray_push_back(pop_status->replicas, &rep_name);
                    need--;
                    printf("--------Added replica %s\n", rep_name);
                }
                else {
                    printf("--------***WARNING: Imbalance even for the same key***\n");
                }
            } else
            {
                /* need to update the casual_item to point to less replicated */
                casual_item = casual_item->hh.prev;
            }
            candidate_item = candidate_item->hh.prev;
            if (candidate_item == NULL || 
                candidate_item->serv->hit + candidate_item->serv->target_hit 
                >= replica_bar)
            {
                /* used up candidates */
                casual_item = candidate_item;
                break;
            }
        }
        
        /* adjust replica's hit based on new replication */
        int new_replica = pop_status->replica_cnt-pop_status->old_index-1-need;
        printf("----Object %.5s found %d more replicas\n", pop_status->kname, 
            new_replica);
        /*adjust replica count*/
        pop_status->replica_cnt = pop_status->old_index + 1 + new_replica;
        if (new_replica != 0) {
            int i;
            int old_hit;
            int new_hit;
            for(i=0; i<pop_status->replica_cnt; i++) {
                char **p = (char **)utarray_eltptr(pop_status->replicas, i);
                SERV_IT *item = NULL;
                HASH_FIND_STR(servers, *p, item);
                if (item != NULL) {
                    KV_REPORT *kv_report = NULL;
                    HASH_FIND_STR(item->serv->reports, pop_status->kname, 
                        kv_report);
                    if (kv_report == NULL) {
                        printf("--------Server %s does not have key %.5s, error!!\n", 
                            *p, pop_status->kname);
                        exit (-1);
                    }
                    if (i == 0) {
                        /*set the old hit record*/
                        old_hit = kv_report->hit;
                        new_hit = old_hit * (pop_status->old_index + 1) / pop_status->replica_cnt;
                    }
                    if (i <= pop_status->old_index){
                        /* decrease hit of old replicas */
                        item->serv->hit -= (old_hit - new_hit);
                    }
                    else {
                        /* increase hit of new replicas */
                        item->serv->hit += (old_hit - new_hit);
                    }
                    kv_report->hit = new_hit;
                }
                else {
                    /* sth going wrong!! */
                    printf("--------Server %s not known, error!!\n", *p);
                    exit(-1);
                }
            }
            /* do replication */
            replicate(pop_status->kname, pop_status->replicas, pop_status->old_index);
            pop_status->old_index = pop_status->replica_cnt - 1;
        }
    }
    
    pthread_rwlock_unlock(&lock);
}

/* Setup listener and timer */
void
run(void) {
    evutil_socket_t listener;
    struct sockaddr_in sin;
    struct event_base *base;
    struct event *listener_event;
    int flags = 1;

    base = event_base_new();
    if (!base)
        return; /*XXXerr*/

    sin.sin_family = AF_INET;
    sin.sin_addr.s_addr = 0;
    sin.sin_port = htons(40713);

    /* set up a listening event */
    listener = socket(AF_INET, SOCK_STREAM, 0);
    setsockopt(listener, SOL_SOCKET, SO_REUSEADDR, (void *)&flags, sizeof(flags));
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
    struct timeval tv = {10,0};
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
