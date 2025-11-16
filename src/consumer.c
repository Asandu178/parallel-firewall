// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"

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

		int action = process_packet(&pkt);
		unsigned long hash = packet_hash(&pkt);
		unsigned long timestamp = pkt.hdr.timestamp;


		pthread_mutex_lock(&rb->locklog);

		int len = snprintf(out_buf, 256, "%s %016lx %lu\n",
			RES_TO_STR(action), hash, timestamp);
		write(ctx->fd, out_buf, len);

		pthread_mutex_unlock(&rb->locklog);

	}
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{

	int fd = open(out_filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
	so_consumer_ctx_t *ctx = malloc(sizeof(so_consumer_ctx_t) * num_consumers);
	if (!ctx)
		return -1;

	for (int i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/
		ctx[i].fd = fd;
		ctx[i].producer_rb = rb;
		pthread_create(&tids[i], NULL, consumer_thread, &ctx[i]);
	}

	return num_consumers;
}
