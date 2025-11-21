// SPDX-License-Identifier: BSD-3-Clause

#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "consumer.h"
#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"
#include <stdatomic.h>


int cmp(const void *p1, const void *p2) {
	logInfo_t a = *(logInfo_t*)p1;
	logInfo_t b = *(logInfo_t*)p2;
	return (a.timestamp - b.timestamp);
}

logInfo_t *logs = NULL;

so_consumer_ctx_t ctx;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t waitForElement = PTHREAD_COND_INITIALIZER;

int cnt = 0;

void addLogInfo(logInfo_t log) {

	pthread_mutex_lock(&lock);

	logs[cnt++] = log;

	if (cnt == THRESHOLD) {
		pthread_cond_signal(&waitForElement);
	}

	pthread_mutex_unlock(&lock);
	
}

void logHandler(void *args) {

	size_t len;
	char out_buf[256];

	// waits until we have enough logs or we have finished all consumers
	while (1) {
		pthread_mutex_lock(&lock);
		while (cnt < THRESHOLD && ctx.numConsumers != ctx.finishedConsumers)
			pthread_cond_wait(&waitForElement, &lock);

		// acquire lock

		if (ctx.numConsumers == ctx.finishedConsumers) {
			pthread_mutex_unlock(&lock);
			break;
		}

		qsort(logs, cnt, sizeof(logInfo_t), cmp);

		for (int i = 0 ; i < cnt ; i++) {
			len = snprintf(out_buf, 256, "%s %016lx %lu\n",
			RES_TO_STR(logs[i].action), logs[i].hash, logs[i].timestamp);
			write(ctx.fd, out_buf, len);
		}

		cnt = 0;

		pthread_mutex_unlock(&lock);
	}

	if (cnt > 0) {
		qsort(logs, cnt, sizeof(logInfo_t), cmp);
		for (int i = 0 ; i < cnt ; i++) {
			len = snprintf(out_buf, 256, "%s %016lx %lu\n",
				RES_TO_STR(logs[i].action), logs[i].hash, logs[i].timestamp);
				write(ctx.fd, out_buf, len);
		}
	}
}

void consumer_thread(void *argv)
{
	/* TODO: implement consumer thread */
	so_packet_t pkt;
	ssize_t sz;
	char out_buf[PKT_SZ];


	while (1) {
		ssize_t r = ring_buffer_dequeue(ctx.producer_rb, &pkt, PKT_SZ);


		// if thread is finished
		if (r == -1) {
			atomic_fetch_add(&ctx.finishedConsumers, 1);
			if (ctx.finishedConsumers == ctx.numConsumers)
				pthread_cond_signal(&waitForElement);
			break;
		}

		logInfo_t log;
		log.action = process_packet(&pkt);
		log.hash = packet_hash(&pkt);
		log.timestamp = pkt.hdr.timestamp;
		addLogInfo(log);

	}
}

int create_consumers(pthread_t *tids,
					 int num_consumers,
					 struct so_ring_buffer_t *rb,
					 const char *out_filename)
{

	ctx.fd = open(out_filename, O_RDWR | O_CREAT | O_TRUNC, 0644);
	ctx.producer_rb = rb;
	ctx.numConsumers = num_consumers;
	ctx.finishedConsumers = 0;

	logs = malloc(sizeof(logInfo_t) * 300);

	int i;

	for (i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/
		pthread_create(&tids[i], NULL, consumer_thread, NULL);
	}

	pthread_create(&tids[i], NULL, logHandler, NULL);

	return num_consumers;
}
