// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include <stdlib.h>

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
	/* TODO: implement ring_buffer_init */
	ring->cap = cap;
	ring->data = calloc(ring->cap, sizeof(char));

	if (!ring->data)
		return -1;

	ring->read_pos = 0;
	ring->write_pos = 0;
	ring->len = 0;
	ring->stopped = 0;

	pthread_mutex_init(&ring->lock, NULL);
	pthread_mutex_init(&ring->locklog, NULL);
	pthread_cond_init(&ring->notFull, NULL);
	pthread_cond_init(&ring->notEmpty, NULL);

	return 1;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: implement ring_buffer_enqueue */


	if (ring == NULL || data == NULL || size == 0)
		return -1;

	pthread_mutex_lock(&ring->lock);

	char *byte = (char *)data;

	// not enough space to write but the ring hasnt stopped so we wait
	while ((ring->cap - ring->len < size) && !ring->stopped)
		pthread_cond_wait(&ring->notFull, &ring->lock);
	// ring stopped so we stop
	if (ring->stopped || ring->cap < size) {
		pthread_mutex_unlock(&ring->lock);
		return -1;
	}

	for (size_t i = 0 ; i < size ; i++) {
		ring->data[ring->write_pos] = *byte;
		ring->write_pos = (ring->write_pos + 1) % ring->cap;
		byte++;
	}

	ring->len += size;

	// signal to the other consumer threads that the ring has new data written

	pthread_cond_broadcast(&ring->notEmpty);

	pthread_mutex_unlock(&ring->lock);

	return size;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
	/* TODO: Implement ring_buffer_dequeue */

	if (ring == NULL || data == NULL || size == 0)
		return -1;

	pthread_mutex_lock(&ring->lock);

	char *byte = (char *)data;

	// waiting until we have enough to read as the ring is not stopped
	while (ring->len < size && !ring->stopped)
		pthread_cond_wait(&ring->notEmpty, &ring->lock);
	// if ring stops we also stop
	if (ring->stopped && ring->len == 0) {
		pthread_mutex_unlock(&ring->lock);
		return -1;
	}

	for (size_t i = 0 ; i < size ; i++) {
		*byte = ring->data[ring->read_pos];
		ring->read_pos = (ring->read_pos + 1) % ring->cap;
		byte++;
	}

	ring->len -= size;

	// signal to the producer thread that we have made space for new data

	pthread_cond_broadcast(&ring->notFull);

	pthread_mutex_unlock(&ring->lock);

	return size;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_destroy */
	ring->len = 0;
	ring->cap = 0;
	ring->write_pos = 0;
	ring->read_pos = 0;
	free(ring->data);
	ring->data = NULL;
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
	/* TODO: Implement ring_buffer_stop */
	pthread_mutex_lock(&ring->lock);
	ring->stopped = 1;

	pthread_cond_broadcast(&ring->notEmpty);
	pthread_cond_broadcast(&ring->notFull);
	pthread_mutex_unlock(&ring->lock);
}
