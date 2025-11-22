// SPDX-License-Identifier: BSD-3-Clause

#include "consumer.h"
#include <pthread.h>
#include <fcntl.h>
#include <unistd.h>

#include "ring_buffer.h"
#include "packet.h"
#include "utils.h"


int cmp(const void *p1, const void *p2)
{
	return ((*(logInfo_t *)p1).timestamp - (*(logInfo_t *)p2).timestamp);
}

logInfo_t *logs;

so_consumer_ctx_t ctx;

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_cond_t waitForElement = PTHREAD_COND_INITIALIZER;
pthread_cond_t waitForInsert = PTHREAD_COND_INITIALIZER;

int cnt;

void addLogInfo(logInfo_t log)
{
	pthread_mutex_lock(&lock);

	while (cnt >= THRESHOLD)
		pthread_cond_wait(&waitForInsert, &lock);

	printf("BEFORE: Adding log #%d with timestamp %lu\n", cnt, log.timestamp);
	logs[cnt++] = log;
	printf("AFTER: cnt now = %d\n", cnt);

	DIE(cnt > THRESHOLD, "THRESHOLD");

	if (cnt == THRESHOLD)
		pthread_cond_signal(&waitForElement);

	pthread_mutex_unlock(&lock);
}

void *logHandler(void *argv)
{
	size_t len;
	char out_buf[256];
	(void) argv;

	// waits until we have enough logs or we have finished all consumers
	while (1) {
		DIE(pthread_mutex_lock(&lock) != 0, "Mutex lock failed");
		while (cnt < THRESHOLD && ctx.numConsumers != ctx.finishedConsumers)
			pthread_cond_wait(&waitForElement, &lock);

		if (ctx.numConsumers == ctx.finishedConsumers) {
			DIE(pthread_mutex_unlock(&lock) != 0, "Mutex unlock failed");
			break;
		}

		qsort(logs, cnt, sizeof(logInfo_t), cmp);

		for (int i = 0 ; i < cnt ; i++) {
			len = snprintf(out_buf, 256, "%s %016lx %lu\n",
			RES_TO_STR(logs[i].action), logs[i].hash, logs[i].timestamp);
			write(ctx.fd, out_buf, len);
		}

		cnt = 0;
		pthread_cond_broadcast(&waitForInsert);

		DIE(pthread_mutex_unlock(&lock) != 0, "Mutex unlock failed");
	}

	if (cnt > 0) {
		qsort(logs, cnt, sizeof(logInfo_t), cmp);
		for (int i = 0 ; i < cnt ; i++) {
			len = snprintf(out_buf, 256, "%s %016lx %lu\n",
				RES_TO_STR(logs[i].action), logs[i].hash, logs[i].timestamp);
				write(ctx.fd, out_buf, len);
		}
	}

	return NULL;
}

void *consumer_thread(void *argv)
{
	/* TODO: implement consumer thread */
	so_packet_t pkt;
	ssize_t r;
	(void) argv;

	while (1) {
		r = ring_buffer_dequeue(ctx.producer_rb, &pkt, PKT_SZ);

		// if thread is finished
		if (r == -1) {
			DIE(pthread_mutex_lock(&lock) != 0, "Mutex lock failed");
			ctx.finishedConsumers++;
			if (ctx.finishedConsumers == ctx.numConsumers)
				pthread_cond_signal(&waitForElement);
			DIE(pthread_mutex_unlock(&lock) != 0, "Mutex unlock failed");
			break;
		}

		logInfo_t log;

		log.action = process_packet(&pkt);
		log.hash = packet_hash(&pkt);
		log.timestamp = pkt.hdr.timestamp;
		addLogInfo(log);
	}
	return NULL;
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
	cnt = 0;

	logs = malloc(sizeof(logInfo_t) * THRESHOLD * 2);

	int i;

	for (i = 0; i < num_consumers; i++) {
		/*
		 * TODO: Launch consumer threads
		 **/
		DIE(pthread_create(&tids[i], NULL, consumer_thread, NULL) != 0, "Error creating consumer threads");
	}

	DIE(pthread_create(&tids[i], NULL, logHandler, NULL) != 0, "Error creating writer thread");

	return num_consumers;
}
