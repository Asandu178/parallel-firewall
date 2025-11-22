// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include <stdlib.h>
#include <string.h>
#include "utils.h"

#define PACKAGE_SIZE 256

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
ring->cap = cap;
ring->data = malloc(ring->cap);
pthread_mutex_init(&ring->lock, NULL);
sem_init(&ring->fullSlots, 0, 0);
sem_init(&ring->emptySlots, 0, ring->cap / PACKAGE_SIZE);
ring->read_pos = 0;
ring->write_pos = 0;
ring->stopped = 0;
ring->len = 0;
return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
sem_wait(&ring->emptySlots);
pthread_mutex_lock(&ring->lock);
if (ring->stopped) {
	pthread_mutex_unlock(&ring->lock);
	sem_post(&ring->emptySlots);
	return -1;
}
memcpy(ring->data + ring->write_pos, data, PACKAGE_SIZE);
ring->write_pos = (ring->write_pos + PACKAGE_SIZE) % ring->cap;
ring->len += PACKAGE_SIZE;
pthread_mutex_unlock(&ring->lock);
sem_post(&ring->fullSlots);
return PACKAGE_SIZE;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
sem_wait(&ring->fullSlots);
pthread_mutex_lock(&ring->lock);
if (ring->stopped && ring->len == 0) {
	pthread_mutex_unlock(&ring->lock);
	sem_post(&ring->fullSlots);
	return -1;
}
memcpy((char *)data, ring->data + ring->read_pos, PACKAGE_SIZE);
ring->read_pos = (ring->read_pos + PACKAGE_SIZE) % ring->cap;
ring->len -= PACKAGE_SIZE;
pthread_mutex_unlock(&ring->lock);
sem_post(&ring->emptySlots);
return PACKAGE_SIZE;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
free(ring->data);
sem_destroy(&ring->emptySlots);
sem_destroy(&ring->fullSlots);
pthread_mutex_destroy(&ring->lock);
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
pthread_mutex_lock(&ring->lock);
ring->stopped = 1;
pthread_mutex_unlock(&ring->lock);
// Wake up potential waiters
sem_post(&ring->emptySlots);
sem_post(&ring->fullSlots);
}
