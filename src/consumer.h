/* SPDX-License-Identifier: BSD-3-Clause */

#ifndef __SO_CONSUMER_H__
#define __SO_CONSUMER_H__

#include "ring_buffer.h"
#include "packet.h"

#define THRESHOLD 500

typedef struct so_consumer_ctx_t {
	struct so_ring_buffer_t *producer_rb;
	int fd;
	int numConsumers;
	int finishedConsumers;
    /* TODO: add synchronization primitives for timestamp ordering */
} so_consumer_ctx_t;

// used to pass remaining logs to firewall.c
extern logInfo_t *logs;

extern int nr;

// used to pass remaining log cnts to firewall.c
extern int cnt;

// used to pass the fd to firewall.c
extern so_consumer_ctx_t ctx;

// used for qsort based on timers
int cmp(const void *p1, const void *p2);

int create_consumers(pthread_t *tids,
					int num_consumers,
					so_ring_buffer_t *rb,
					const char *out_filename);

#endif /* __SO_CONSUMER_H__ */
