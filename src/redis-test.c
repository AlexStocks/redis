/* Redis benchmark utility.
 *
 * Copyright (c) 2009-2012, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include "fmacros.h"

#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <unistd.h>
#include <errno.h>
#include <time.h>
#include <sys/time.h>
#include <signal.h>
#include <assert.h>

#include <sds.h> /* Use hiredis sds. */
#include "ae.h"
#include "hiredis.h"
#include "adlist.h"

#include "zmalloc.h"

static char * keyprefix =  "__rand_int__";

void genRandom(char *s, size_t len)
{
    static const char alphanum[] = {
        "0123456789"
        "!@#$%^&*"
        "ABCDEFGHIJKLMNOPQRSTUVWXYZ"
        "abcdefghijklmnopqrstuvwxyz"
    };
    static size_t alphanumLen = sizeof(alphanum) - 1;

    for (size_t i = 0; i < len-1; ++i) {
        s[i] = alphanum[random() % alphanumLen];
    }
    // s[len-1] = '\0';
}

#define UNUSED(V) ((void) V)
#define RANDPTR_INITIAL_SIZE 8

static struct config {
    aeEventLoop *el;
    const char *hostip;
    int hostport;
    const char *hostsocket;
    int numclients;
    int liveclients;
    int requests;
    int requests_issued;
    int requests_finished;
    int keysize;
    int datasize;
    int randomkeys;
    int randomkeys_keyspacelen;
    int keepalive;
    int pipeline;
    int showerrors;
    long long start;
    long long totlatency;
    long long maxlatency;
    long long *latency;
    const char *title;
    list *clients;
    int quiet;
    int csv;
    int loop;
    int idlemode;
    int dbnum;
    sds dbnumstr;
    char *tests;

    char *keyprefix;
    size_t keyprefixlen;
    char* value;
} config;

typedef struct _client {
    redisContext *context;
    sds obuf;
    char **randptr;         /* Pointers to :rand: strings inside the command buf */
    size_t randlen;         /* Number of pointers in client->randptr */
    size_t randfree;        /* Number of unused pointers in client->randptr */
    size_t written;         /* Bytes of 'obuf' already written */
    long long start;        /* Start time of a request */
    long long latency;      /* Request latency */
    int pending;            /* Number of pending requests (replies to consume) */
    int prefix_pending;     /* If non-zero, number of pending prefix commands. Commands
                               such as auth and select are prefixed to the pipeline of
                               benchmark commands and discarded after the first send. */
    int prefixlen;          /* Size in bytes of the pending prefix commands */
} *client;

/* Prototypes */
static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask);
static void createMissingClients(client c);

/* Implementation */
static long long ustime(void) {
    struct timeval tv;
    long long ust;

    gettimeofday(&tv, NULL);
    ust = ((long)tv.tv_sec)*1000000;
    ust += tv.tv_usec;
    return ust;
}

static long long mstime(void) {
    struct timeval tv;
    long long mst;

    gettimeofday(&tv, NULL);
    mst = ((long long)tv.tv_sec)*1000;
    mst += tv.tv_usec/1000;
    return mst;
}

static void freeClient(client c) {
    listNode *ln;
    aeDeleteFileEvent(config.el,c->context->fd,AE_WRITABLE);
    aeDeleteFileEvent(config.el,c->context->fd,AE_READABLE);
    redisFree(c->context);
    sdsfree(c->obuf);
    zfree(c->randptr);
    zfree(c);
    config.liveclients--;
    ln = listSearchKey(config.clients,c);
    assert(ln != NULL);
    listDelNode(config.clients,ln);
}

static void freeAllClients(void) {
    listNode *ln = config.clients->head, *next;

    while(ln) {
        next = ln->next;
        freeClient(ln->value);
        ln = next;
    }
}

static void resetClient(client c) {
    aeDeleteFileEvent(config.el,c->context->fd,AE_WRITABLE);
    aeDeleteFileEvent(config.el,c->context->fd,AE_READABLE);
    aeCreateFileEvent(config.el,c->context->fd,AE_WRITABLE,writeHandler,c);
    c->written = 0;
    c->pending = config.pipeline;
}

static void randomizeClientKey(client c) {
    size_t i;

    for (i = 0; i < c->randlen; i++) {
        char *p = c->randptr[i]+config.keyprefixlen;
        // size_t r = random() % config.randomkeys_keyspacelen;
        // size_t j;

        // for (j = 0; j < 12; j++) {
        //     *p = '0'+r%10;
        //     r/=10;
        //     p--;
        // }

        genRandom(p, config.randomkeys_keyspacelen);
    }
}

static void clientDone(client c) {
    if (config.requests_finished == config.requests) {
        freeClient(c);
        aeStop(config.el);
        return;
    }
    if (config.keepalive) {
        resetClient(c);
    } else {
        config.liveclients--;
        createMissingClients(c);
        config.liveclients++;
        freeClient(c);
    }
}

static void readHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    void *reply = NULL;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Calculate latency only for the first read event. This means that the
     * server already sent the reply and we need to parse it. Parsing overhead
     * is not part of the latency, so calculate it only once, here. */
    if (c->latency < 0) c->latency = ustime()-(c->start);

    if (redisBufferRead(c->context) != REDIS_OK) {
        fprintf(stderr,"Error: %s\n",c->context->errstr);
        exit(1);
    } else {
        while(c->pending) {
            if (redisGetReply(c->context,&reply) != REDIS_OK) {
                fprintf(stderr,"Error: %s\n",c->context->errstr);
                exit(1);
            }
            if (reply != NULL) {
                if (reply == (void*)REDIS_REPLY_ERROR) {
                    fprintf(stderr,"Unexpected error reply, exiting...\n");
                    exit(1);
                }

                if (config.showerrors) {
                    static time_t lasterr_time = 0;
                    time_t now = time(NULL);
                    redisReply *r = reply;
                    if (r->type == REDIS_REPLY_ERROR && lasterr_time != now) {
                        lasterr_time = now;
                        printf("Error from server: %s\n", r->str);
                    }
                }

                freeReplyObject(reply);
                /* This is an OK for prefix commands such as auth and select.*/
                if (c->prefix_pending > 0) {
                    c->prefix_pending--;
                    c->pending--;
                    /* Discard prefix commands on first response.*/
                    if (c->prefixlen > 0) {
                        size_t j;
                        sdsrange(c->obuf, c->prefixlen, -1);
                        /* We also need to fix the pointers to the strings
                        * we need to randomize. */
                        for (j = 0; j < c->randlen; j++)
                            c->randptr[j] -= c->prefixlen;
                        c->prefixlen = 0;
                    }
                    continue;
                }

                if (config.requests_finished < config.requests)
                    config.latency[config.requests_finished++] = c->latency;
                c->pending--;
                if (c->pending == 0) {
                    clientDone(c);
                    break;
                }
            } else {
                break;
            }
        }
    }
}

static void writeHandler(aeEventLoop *el, int fd, void *privdata, int mask) {
    client c = privdata;
    UNUSED(el);
    UNUSED(fd);
    UNUSED(mask);

    /* Initialize request when nothing was written. */
    if (c->written == 0) {
        /* Enforce upper bound to number of requests. */
        if (config.requests_issued++ >= config.requests) {
            freeClient(c);
            return;
        }

        /* Really initialize: randomize keys and set start time. */
        if (config.randomkeys) randomizeClientKey(c);
        c->start = ustime();
        c->latency = -1;
    }

    if (sdslen(c->obuf) > c->written) {
        void *ptr = c->obuf+c->written;
        ssize_t nwritten = write(c->context->fd,ptr,sdslen(c->obuf)-c->written);
        if (nwritten == -1) {
            if (errno != EPIPE)
                fprintf(stderr, "Writing to socket: %s\n", strerror(errno));
            freeClient(c);
            return;
        }
        c->written += nwritten;
        if (sdslen(c->obuf) == c->written) {
            aeDeleteFileEvent(config.el,c->context->fd,AE_WRITABLE);
            aeCreateFileEvent(config.el,c->context->fd,AE_READABLE,readHandler,c);
        }
    }
}

/* Create a benchmark client, configured to send the command passed as 'cmd' of
 * 'len' bytes.
 *
 * The command is copied N times in the client output buffer (that is reused
 * again and again to send the request to the server) accordingly to the configured
 * pipeline size.
 *
 * Also an initial SELECT command is prepended in order to make sure the right
 * database is selected, if needed. The initial SELECT will be discarded as soon
 * as the first reply is received.
 *
 * To create a client from scratch, the 'from' pointer is set to NULL. If instead
 * we want to create a client using another client as reference, the 'from' pointer
 * points to the client to use as reference. In such a case the following
 * information is take from the 'from' client:
 *
 * 1) The command line to use.
 * 2) The offsets of the __rand_int__ elements inside the command line, used
 *    for arguments randomization.
 *
 * Even when cloning another client, prefix commands are applied if needed.*/
static client createClient(char *cmd, size_t len, client from) {
    int j;
    client c = zmalloc(sizeof(struct _client));

    if (config.hostsocket == NULL) {
        c->context = redisConnectNonBlock(config.hostip,config.hostport);
    } else {
        c->context = redisConnectUnixNonBlock(config.hostsocket);
    }
    if (c->context->err) {
        fprintf(stderr,"Could not connect to Redis at ");
        if (config.hostsocket == NULL)
            fprintf(stderr,"%s:%d: %s\n",config.hostip,config.hostport,c->context->errstr);
        else
            fprintf(stderr,"%s: %s\n",config.hostsocket,c->context->errstr);
        exit(1);
    }
    /* Suppress hiredis cleanup of unused buffers for max speed. */
    c->context->reader->maxbuf = 0;

    /* Build the request buffer:
     * Queue N requests accordingly to the pipeline size, or simply clone
     * the example client buffer. */
    c->obuf = sdsempty();
    /* Prefix the request buffer with AUTH and/or SELECT commands, if applicable.
     * These commands are discarded after the first response, so if the client is
     * reused the commands will not be used again. */
    c->prefix_pending = 0;

    /* If a DB number different than zero is selected, prefix our request
     * buffer with the SELECT command, that will be discarded the first
     * time the replies are received, so if the client is reused the
     * SELECT command will not be used again. */
    if (config.dbnum != 0) {
        c->obuf = sdscatprintf(c->obuf,"*2\r\n$6\r\nSELECT\r\n$%d\r\n%s\r\n",
            (int)sdslen(config.dbnumstr),config.dbnumstr);
        c->prefix_pending++;
    }
    c->prefixlen = sdslen(c->obuf);
    /* Append the request itself. */
    if (from) {
        c->obuf = sdscatlen(c->obuf,
            from->obuf+from->prefixlen,
            sdslen(from->obuf)-from->prefixlen);
    } else {
        for (j = 0; j < config.pipeline; j++)
            c->obuf = sdscatlen(c->obuf,cmd,len);
    }

    c->written = 0;
    c->pending = config.pipeline+c->prefix_pending;
    c->randptr = NULL;
    c->randlen = 0;

    /* Find substrings in the output buffer that need to be randomized. */
    if (config.randomkeys) {
        if (from) {
            c->randlen = from->randlen;
            c->randfree = 0;
            c->randptr = zmalloc(sizeof(char*)*c->randlen);
            /* copy the offsets. */
            for (j = 0; j < (int)c->randlen; j++) {
                c->randptr[j] = c->obuf + (from->randptr[j]-from->obuf);
                /* Adjust for the different select prefix length. */
                c->randptr[j] += c->prefixlen - from->prefixlen;
            }
        } else {
            char *p = c->obuf;

            c->randlen = 0;
            c->randfree = RANDPTR_INITIAL_SIZE;
            c->randptr = zmalloc(sizeof(char*)*c->randfree);
            // while ((p = strstr(p,"__rand_int__")) != NULL) {
            size_t inclen = config.keyprefixlen;
            if (config.keysize) {
                inclen = config.keysize;
            }
            while ((p = strstr(p, config.keyprefix)) != NULL) {
                if (c->randfree == 0) {
                    c->randptr = zrealloc(c->randptr,sizeof(char*)*c->randlen*2);
                    c->randfree += c->randlen;
                }
                c->randptr[c->randlen++] = p;
                c->randfree--;
                // p += config.keyprefixlen;
                p += inclen;
            }
        }
    }
    if (config.idlemode == 0)
        aeCreateFileEvent(config.el,c->context->fd,AE_WRITABLE,writeHandler,c);
    listAddNodeTail(config.clients,c);
    config.liveclients++;
    return c;
}

static void createMissingClients(client c) {
    int n = 0;

    while(config.liveclients < config.numclients) {
        createClient(NULL,0,c);

        /* Listen backlog is quite limited on most systems */
        if (++n > 64) {
            usleep(50000);
            n = 0;
        }
    }
}

static int compareLatency(const void *a, const void *b) {
    return (*(long long*)a)-(*(long long*)b);
}

static void showLatencyReport(void) {
    int i, curlat = 0;
    float perc, reqpersec;
    long long totlatency = 0;
    long long maxlatency = config.maxlatency * 1e3;
    long long beyondnum = 0;

    reqpersec = (float)config.requests_finished/((float)config.totlatency/1000);
    if (!config.quiet && !config.csv) {
        printf("====== %s ======\n", config.title);
        // printf("  %d requests completed in %.2f seconds\n", config.requests_finished,
        //    (float)config.totlatency/1000);
        // printf("  %d parallel clients\n", config.numclients);
        // printf("  %d bytes payload\n", config.datasize);
        // printf("  keep alive: %d\n", config.keepalive);
        // printf("\n");

        qsort(config.latency,config.requests,sizeof(long long),compareLatency);
        beyondnum = 0;
        for (i = 0; i < config.requests; i++) {
            if (config.latency[i]/1000 != curlat || i == (config.requests-1)) {
                curlat = config.latency[i]/1000;
                perc = ((float)(i+1)*100)/config.requests;
                printf("%.2f%% <= %d milliseconds\n", perc, curlat);
            }
            totlatency += config.latency[i];
            if (maxlatency < config.latency[i]) {
                beyondnum ++;
            }
        }
        printf("%d requests latency > %d milliseconds\n", beyondnum, config.maxlatency);

        // totol latency 只能是各个 request 的 latency 之和，不能使用 config.totlancy，因为它是错略的aeMain前后的时间，
        // 把计算随机 key 的计算时间也计算在内了
        reqpersec = (float)config.requests_finished/((float)totlatency/1e6);
        printf("\n");
        printf("  %d parallel clients\n", config.numclients);
        printf("  %d bytes payload\n", config.datasize);
        printf("  keep alive: %d\n", config.keepalive);
        printf("  %d requests completed in %.2f seconds\n", config.requests_finished, (float)totlatency/1e6);
        printf("  %.2f requests per second\n\n", reqpersec);
    } else if (config.csv) {
        printf("\"%s\",\"%.2f\"\n", config.title, reqpersec);
    } else {
        printf("%s: %.2f requests per second\n", config.title, reqpersec);
    }
}

static void benchmark(char *title, char *cmd, int len) {
    client c;

    config.title = title;
    config.requests_issued = 0;
    config.requests_finished = 0;

    c = createClient(cmd,len,NULL);
    createMissingClients(c);

    config.start = mstime();
    aeMain(config.el);
    config.totlatency = mstime()-config.start;

    showLatencyReport();
    freeAllClients();
}

/* Returns number of consumed options. */
int parseOptions(int argc, const char **argv) {
    int i;
    int lastarg;
    int exit_status = 1;

    for (i = 1; i < argc; i++) {
        lastarg = (i == (argc-1));

        if (!strcmp(argv[i],"-c")) {
            if (lastarg) goto invalid;
            config.numclients = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-n")) {
            if (lastarg) goto invalid;
            config.requests = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-k")) {
            if (lastarg) goto invalid;
            config.keepalive = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-h")) {
            if (lastarg) goto invalid;
            config.hostip = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-p")) {
            if (lastarg) goto invalid;
            config.hostport = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-s")) {
            if (lastarg) goto invalid;
            config.hostsocket = strdup(argv[++i]);
        } else if (!strcmp(argv[i],"-d")) {
            if (lastarg) goto invalid;
            config.datasize = atoi(argv[++i]);
            if (config.datasize < 1) config.datasize=1;
            if (config.datasize > 1024*1024*1024) config.datasize = 1024*1024*1024;
        } else if (!strcmp(argv[i],"-P")) {
            if (lastarg) goto invalid;
            config.pipeline = atoi(argv[++i]);
            if (config.pipeline <= 0) config.pipeline=1;
        } else if (!strcmp(argv[i],"-r")) {
            if (lastarg) goto invalid;
            config.randomkeys = 1;
            config.randomkeys_keyspacelen = atoi(argv[++i]);
            if (config.randomkeys_keyspacelen < 0)
                config.randomkeys_keyspacelen = 0;
        } else if (!strcmp(argv[i],"-q")) {
            config.quiet = 1;
        } else if (!strcmp(argv[i],"--csv")) {
            config.csv = 1;
        } else if (!strcmp(argv[i],"--kp")) {
            config.keyprefix = argv[++i];
            config.keyprefixlen = strlen(config.keyprefix);
            if (config.keyprefixlen == 0) {
                goto invalid;
            }
        } else if (!strcmp(argv[i],"-l")) {
            config.loop = 1;
        } else if (!strcmp(argv[i],"-I")) {
            config.idlemode = 1;
        } else if (!strcmp(argv[i],"-e")) {
            config.showerrors = 1;
        } else if (!strcmp(argv[i],"-v")) {
            if (lastarg) goto invalid;
            config.value = argv[++i];
        } else if (!strcmp(argv[i],"-m")) {
            if (lastarg) goto invalid;
            config.maxlatency = atoi(argv[++i]);
        } else if (!strcmp(argv[i],"-t")) {
            if (lastarg) goto invalid;
            /* We get the list of tests to run as a string in the form
             * get,set,lrange,...,test_N. Then we add a comma before and
             * after the string in order to make sure that searching
             * for ",testname," will always get a match if the test is
             * enabled. */
            config.tests = sdsnew(",");
            config.tests = sdscat(config.tests,(char*)argv[++i]);
            config.tests = sdscat(config.tests,",");
            sdstolower(config.tests);
        } else if (!strcmp(argv[i],"--dbnum")) {
            if (lastarg) goto invalid;
            config.dbnum = atoi(argv[++i]);
            config.dbnumstr = sdsfromlonglong(config.dbnum);
        } else if (!strcmp(argv[i],"--help")) {
            exit_status = 0;
            goto usage;
        } else {
            /* Assume the user meant to provide an option when the arg starts
             * with a dash. We're done otherwise and should use the remainder
             * as the command and arguments for running the benchmark. */
            if (argv[i][0] == '-') goto invalid;
            return i;
        }
    }

    return i;

invalid:
    printf("Invalid option \"%s\" or option argument missing\n\n",argv[i]);

usage:
    printf(
"Usage: redis-benchmark [-h <host>] [-p <port>] [-c <clients>] [-n <requests>] [-k <boolean>]\n\n"
" -h <hostname>      Server hostname (default 127.0.0.1)\n"
" -p <port>          Server port (default 6379)\n"
" -s <socket>        Server socket (overrides host and port)\n"
" -c <clients>       Number of parallel connections (default 50)\n"
" -m <maxlatency>    Max latency in millisecond (default 10)\n"
" -n <requests>      Total number of requests (default 100000)\n"
" -d <size>          Data size of SET/GET value in bytes (default 3).\n"
" --dbnum <db>       SELECT the specified db number (default 0)\n"
" -k <boolean>       1=keep alive 0=reconnect (default 1)\n"
" --kf <string>      Key prefix\n"
" -r <keyspacelen>   Use random keys for SET/GET/INCR, random values for SADD\n"
"  Using this option the benchmark will expand the string __rand_int__\n"
"  inside an argument with a 12 digits number in the specified range\n"
"  from 0 to keyspacelen-1. The substitution changes every time a command\n"
"  is executed. Default tests use this to hit random keys in the\n"
"  specified range.\n"
" -P <numreq>        Pipeline <numreq> requests. Default 1 (no pipeline).\n"
" -e                 If server replies with errors, show them on stdout.\n"
"                    (no more than 1 error per second is displayed)\n"
" -q                 Quiet. Just show query/sec values\n"
" --csv              Output in CSV format\n"
" -l                 Loop. Run the tests forever\n"
" -t <tests>         Only run the comma separated list of tests. The test\n"
"                    names are the same as the ones produced as output.\n"
" -I                 Idle mode. Just open N idle connections and wait.\n\n"
" -v                 Value of INCRBY\n"
"Examples:\n\n"
" Run the benchmark with the default configuration against 127.0.0.1:6379:\n"
"   $ redis-benchmark\n\n"
" Use 20 parallel clients, for a total of 100k requests, against 192.168.1.1:\n"
"   $ redis-benchmark -h 192.168.1.1 -p 6379 -n 100000 -c 20\n\n"
" Fill 127.0.0.1:6379 with about 1 million keys only using the SET test:\n"
"   $ redis-benchmark -t set -n 1000000 -r 100000000\n\n"
" Benchmark 127.0.0.1:6379 for a few commands producing CSV output:\n"
"   $ redis-benchmark -t ping,set,get -n 100000 --csv\n\n"
" Benchmark a specific command line:\n"
"   $ redis-benchmark -r 10000 -n 10000 eval 'return redis.call(\"ping\")' 0\n\n"
" Fill a list with 10000 random elements:\n"
"   $ redis-benchmark -r 10000 -n 10000 lpush mylist __rand_int__\n\n"
" On user specified command lines __rand_int__ is replaced with a random integer\n"
" with a range of values selected by the -r option.\n"
    );
    exit(exit_status);
}

int showThroughput(struct aeEventLoop *eventLoop, long long id, void *clientData) {
    UNUSED(eventLoop);
    UNUSED(id);
    UNUSED(clientData);

    if (config.liveclients == 0 && config.requests_finished != config.requests) {
        fprintf(stderr,"All clients disconnected... aborting.\n");
        exit(1);
    }
    if (config.csv) return 250;
    if (config.idlemode == 1) {
        printf("clients: %d\r", config.liveclients);
        fflush(stdout);
    return 250;
    }
    float dt = (float)(mstime()-config.start)/1000.0;
    float rps = (float)config.requests_finished/dt;
    printf("%s: %.2f\r", config.title, rps);
    fflush(stdout);
    return 250; /* every 250ms */
}

/* Return true if the named test was selected using the -t command line
 * switch, or if all the tests are selected (no -t passed by user). */
int test_is_selected(char *name) {
    char buf[256];
    int l = strlen(name);

    if (config.tests == NULL) return 1;
    buf[0] = ',';
    memcpy(buf+1,name,l);
    buf[l+1] = ',';
    buf[l+2] = '\0';
    return strstr(config.tests,buf) != NULL;
}

void test_set(char* data) {
    sds cmdstr = sdsnew("SET ");
    if (config.keyprefix != keyprefix) {
        cmdstr = sdscat(cmdstr, config.keyprefix);
        config.keysize = config.keyprefixlen;
    } else {
        const char* key = "key:__rand_int__";
        cmdstr = sdscat(cmdstr, key);
        config.keysize = strlen(key);
    }
    if (config.randomkeys_keyspacelen) {
        for (size_t idx = 0; idx < config.randomkeys_keyspacelen; idx++) {
            cmdstr = sdscat(cmdstr, "z");
        }
        config.keysize += config.randomkeys_keyspacelen;
    }
    cmdstr = sdscat(cmdstr, " %s");
    char* cmd;
    printf("cmd: %s\n", cmdstr);
    int len = redisFormatCommand(&cmd, cmdstr, data);
    benchmark("SET", cmd, len);
    free(cmd);
    sdsfree(cmdstr);
}

void test_incr() {
    sds cmdstr = sdsnew("INCR ");
    if (config.keyprefix != keyprefix) {
        cmdstr = sdscat(cmdstr, config.keyprefix);
        config.keysize = config.keyprefixlen;
    } else {
        const char* key = "counter:__rand_int__";
        cmdstr = sdscat(cmdstr, key);
        config.keysize = strlen(key);
    }
    if (config.randomkeys_keyspacelen) {
        for (size_t idx = 0; idx < config.randomkeys_keyspacelen; idx++) {
            cmdstr = sdscat(cmdstr, "z");
        }
        config.keysize += config.randomkeys_keyspacelen;
    }
    char* cmd;
    printf("cmd: %s\n", cmdstr);
    int len = redisFormatCommand(&cmd, cmdstr);
    benchmark("INCR", cmd, len);
    free(cmd);
    sdsfree(cmdstr);
}

void test_decr() {
    sds cmdstr = sdsnew("DECR ");
    if (config.keyprefix != keyprefix) {
        cmdstr = sdscat(cmdstr, config.keyprefix);
        config.keysize = config.keyprefixlen;
    } else {
        const char* key = "counter:__rand_int__";
        cmdstr = sdscat(cmdstr, key);
        config.keysize = strlen(key);
    }
    if (config.randomkeys_keyspacelen) {
        for (size_t idx = 0; idx < config.randomkeys_keyspacelen; idx++) {
            cmdstr = sdscat(cmdstr, "z");
        }
        config.keysize += config.randomkeys_keyspacelen;
    }
    char* cmd;
    printf("cmd: %s\n", cmdstr);
    int len = redisFormatCommand(&cmd, cmdstr);
    benchmark("DECR", cmd, len);
    free(cmd);
    sdsfree(cmdstr);
}

void test_incrby() {
    sds cmdstr = sdsnew("INCRBY ");
    int incv = 0;
    if (NULL != config.value) {
        incv = atoi(config.value);
    }
    if (incv == 0) {
        printf("Error: incby value is 0\n");
        exit(1);
    }
    if (config.keyprefix != keyprefix) {
        cmdstr = sdscat(cmdstr, config.keyprefix);
        config.keysize = config.keyprefixlen;
    } else {
        const char* key = "counter:__rand_int__";
        cmdstr = sdscat(cmdstr, key);
        config.keysize = strlen(key);
    }
    if (config.randomkeys_keyspacelen) {
        for (size_t idx = 0; idx < config.randomkeys_keyspacelen; idx++) {
            cmdstr = sdscat(cmdstr, "z");
        }
        config.keysize += config.randomkeys_keyspacelen;
    }
    // cmdstr = sdscatfmt(cmdstr, " %d", incv);
    // printf("cmd %s\n", cmdstr);
    cmdstr = sdscatprintf(cmdstr, " %d", incv);
    printf("cmd: %s\n", cmdstr);

    char* cmd;
    int len = redisFormatCommand(&cmd, cmdstr);
    benchmark("INCRBY", cmd, len);
    free(cmd);
    sdsfree(cmdstr);
}

void test_hset(char* data) {
    sds cmdstr = sdsnew("HSET ");
    if (config.keyprefix != keyprefix) {
        cmdstr = sdscat(cmdstr, config.keyprefix);
        config.keysize = config.keyprefixlen;
    } else {
        const char* key = "myset:__rand_int__";
        cmdstr = sdscat(cmdstr, key);
        config.keysize = strlen(key);
    }
    if (config.randomkeys_keyspacelen) {
        for (size_t idx = 0; idx < config.randomkeys_keyspacelen; idx++) {
            cmdstr = sdscat(cmdstr, "z");
        }
        config.keysize += config.randomkeys_keyspacelen;
    }
    cmdstr = sdscat(cmdstr, " element:__rand_field__ %s");
    printf("cmd: %s\n", cmdstr);

    char* cmd;
    int len = redisFormatCommand(&cmd, cmdstr, data);
    benchmark("HSET", cmd, len);
    free(cmd);
    sdsfree(cmdstr);
}

void test_hmset(char* data) {
    sds cmdstr = sdsnew("HMSET ");

    // key
    if (config.keyprefix != keyprefix) {
        cmdstr = sdscat(cmdstr, config.keyprefix);
        config.keysize = config.keyprefixlen;
    } else {
        const char* key = "myset:__rand_int__";
        cmdstr = sdscat(cmdstr, key);
        config.keysize = strlen(key);
    }

    if (config.randomkeys_keyspacelen) {
        for (size_t idx = 0; idx < config.randomkeys_keyspacelen; idx++) {
            cmdstr = sdscat(cmdstr, "z");
        }
        config.keysize += config.randomkeys_keyspacelen;
    }

    // field, value
    for (int i = 0; i < 10; i+=1) {
        cmdstr = sdscatprintf(cmdstr, " element:__rand_field__%d %s ", i, data);
    }
    printf("cmd: %s\n", cmdstr);

    char* cmd;
    int len = redisFormatCommand(&cmd, cmdstr, data);
    benchmark("HMSET", cmd, len);
    free(cmd);
    sdsfree(cmdstr);
}


int main(int argc, const char **argv) {
    int i;
    char *data, *cmd;
    int len;

    client c;

    srandom(time(NULL));
    signal(SIGHUP, SIG_IGN);
    signal(SIGPIPE, SIG_IGN);

    config.numclients = 50;
    config.requests = 100000;
    config.liveclients = 0;
    config.el = aeCreateEventLoop(1024*10);
    aeCreateTimeEvent(config.el,1,showThroughput,NULL,NULL);
    config.keepalive = 1;
    config.keysize = 0;
    config.datasize = 3;
    config.pipeline = 1;
    config.showerrors = 0;
    config.randomkeys = 0;
    config.randomkeys_keyspacelen = 0;
    config.keyprefix = keyprefix;
    config.keyprefixlen = strlen(config.keyprefix);
    config.quiet = 0;
    config.csv = 0;
    config.loop = 0;
    config.idlemode = 0;
    config.latency = NULL;
    config.maxlatency = 10;
    config.clients = listCreate();
    config.hostip = "127.0.0.1";
    config.hostport = 6379;
    config.hostsocket = NULL;
    config.tests = NULL;
    config.dbnum = 0;
    config.value = NULL;

    i = parseOptions(argc,argv);
    argc -= i;
    argv += i;

    config.latency = zmalloc(sizeof(long long)*config.requests);

    if (config.keepalive == 0) {
        printf("WARNING: keepalive disabled, you probably need 'echo 1 > /proc/sys/net/ipv4/tcp_tw_reuse' for Linux "
        "and 'sudo sysctl -w net.inet.tcp.msl=1000' for Mac OS X in order to use a lot of clients/requests\n");
    }

    if (config.idlemode) {
        printf("Creating %d idle connections and waiting forever (Ctrl+C when done)\n", config.numclients);
        c = createClient("",0,NULL); /* will never receive a reply */
        createMissingClients(c);
        aeMain(config.el);
        /* and will wait for every */
    }

    /* Run benchmark with command in the remainder of the arguments. */
    if (argc) {
        sds title = sdsnew(argv[0]);
        for (i = 1; i < argc; i++) {
            title = sdscatlen(title, " ", 1);
            title = sdscatlen(title, (char*)argv[i], strlen(argv[i]));
        }

        do {
            len = redisFormatCommandArgv(&cmd,argc,argv,NULL);
            benchmark(title,cmd,len);
            free(cmd);
        } while(config.loop);

        return 0;
    }

    /* Run default benchmark suite. */
    data = zmalloc(config.datasize+1);
    do {
        memset(data,'x',config.datasize);
        data[config.datasize] = '\0';

        if (test_is_selected("ping_inline") || test_is_selected("ping"))
            benchmark("PING_INLINE","PING\r\n",6);

        if (test_is_selected("ping_mbulk") || test_is_selected("ping")) {
            len = redisFormatCommand(&cmd,"PING");
            benchmark("PING_BULK",cmd,len);
            free(cmd);
        }

        if (test_is_selected("set")) {
            test_set(data);
        }

        if (test_is_selected("get")) {
            len = redisFormatCommand(&cmd,"GET key:__rand_int__");
            benchmark("GET",cmd,len);
            free(cmd);
        }

        if (test_is_selected("incr")) {
            test_incr();
        }

        if (test_is_selected("incrby")) {
            test_incrby();
        }

        if (test_is_selected("lpush")) {
            len = redisFormatCommand(&cmd,"LPUSH mylist %s",data);
            benchmark("LPUSH",cmd,len);
            free(cmd);
        }

        if (test_is_selected("rpush")) {
            len = redisFormatCommand(&cmd,"RPUSH mylist %s",data);
            benchmark("RPUSH",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lpop")) {
            len = redisFormatCommand(&cmd,"LPOP mylist");
            benchmark("LPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("rpop")) {
            len = redisFormatCommand(&cmd,"RPOP mylist");
            benchmark("RPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("sadd")) {
            len = redisFormatCommand(&cmd,
                "SADD myset element:__rand_int__");
            benchmark("SADD",cmd,len);
            free(cmd);
        }

        if (test_is_selected("hset")) {
            test_hset(data);
        }

        if (test_is_selected("hmset")) {
            test_hmset(data);
        }

        if (test_is_selected("spop")) {
            len = redisFormatCommand(&cmd,"SPOP myset");
            benchmark("SPOP",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") ||
            test_is_selected("lrange_100") ||
            test_is_selected("lrange_300") ||
            test_is_selected("lrange_500") ||
            test_is_selected("lrange_600"))
        {
            len = redisFormatCommand(&cmd,"LPUSH mylist %s",data);
            benchmark("LPUSH (needed to benchmark LRANGE)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_100")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 99");
            benchmark("LRANGE_100 (first 100 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_300")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 299");
            benchmark("LRANGE_300 (first 300 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_500")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 449");
            benchmark("LRANGE_500 (first 450 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("lrange") || test_is_selected("lrange_600")) {
            len = redisFormatCommand(&cmd,"LRANGE mylist 0 599");
            benchmark("LRANGE_600 (first 600 elements)",cmd,len);
            free(cmd);
        }

        if (test_is_selected("mset")) {
            const char *argv[21];
            argv[0] = "MSET";
            for (i = 1; i < 21; i += 2) {
                argv[i] = "key:__rand_int__";
                argv[i+1] = data;
            }
            len = redisFormatCommandArgv(&cmd,21,argv,NULL);
            benchmark("MSET (10 keys)",cmd,len);
            free(cmd);
        }

        if (!config.csv) printf("\n");
    } while(config.loop);

    return 0;
}
