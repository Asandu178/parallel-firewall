// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"


int cmp(const void *p1, const void *p2) {
	logInfo_t a = *(logInfo_t*)p1;
	logInfo_t b = *(logInfo_t*)p2;
	return (a.timestamp - b.timestamp);
}

logInfo_t *logs = NULL;

so_consumer_ctx_t ctx;

int cnt = 0;

int fd;

void consumer_thread(so_consumer_ctx_t *ctx)
{
	/* TODO: implement consumer thread */
	so_ring_buffer_t *rb = ctx->producer_rb;
	so_packet_t pkt;
	ssize_t sz;
	char out_buf[PKT_SZ];


	while (1) {
		ssize_t r = ring_buffer_dequeue(rb, &pkt, PKT_SZ);

		if (r == -1)
			break;
		if (r == 0)
			continue;

		logInfo_t log;
		log.action = process_packet(&pkt);
		log.hash = packet_hash(&pkt);
		log.timestamp = pkt.hdr.timestamp;

		pthread_mutex_lock(&rb->locklog);

		logs[cnt] = log;
		cnt = cnt + 1;

		if (cnt == THRESHOLD) {

			qsort(logs, THRESHOLD, sizeof(logInfo_t), cmp);

			for (int i = 0 ; i < cnt ; i++) {
				int len = snprintf(out_buf, 256, "%s %016lx %lu\n",
				RES_TO_STR(logs[i].action), logs[i].hash, logs[i].timestamp);
				write(ctx->fd, out_buf, len);
			}

			cnt = 0;

		}

		pthread_mutex_unlock(&rb->locklog);

	}
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{

	fd = open(out_filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
	ctx.fd = fd;
	ctx.producer_rb = rb;
	ctx.numConsumers = num_consumers;

	logs = malloc(sizeof(logInfo_t) * 300);

	for (int i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/
		pthread_create(&tids[i], NULL, consumer_thread, &ctx);
	}

	return num_consumers;
}
