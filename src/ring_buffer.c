// SPDX-License-Identifier: BSD-3-Clause

#include "ring_buffer.h"
#include <stdlib.h>
#include <string.h>

#define PACKAGE_SIZE 256

int ring_buffer_init(so_ring_buffer_t *ring, size_t cap)
{
    ring->cap = cap;
    ring->data = malloc(ring->cap);
    
    if (!ring->data)
        return -1;

    ring->read_pos = 0;
    ring->write_pos = 0;
    ring->len = 0;
    ring->stopped = 0;

    pthread_mutex_init(&ring->lock, NULL);
    
    // Since we always use 256 bytes, we can optimize semaphore counts
    sem_init(&ring->fullSlots, 0, 0);
    sem_init(&ring->emptySlots, 0, ring->cap / PACKAGE_SIZE);
    
    return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
    if (size != PACKAGE_SIZE)
        return -1;

    // Single semaphore wait for entire package
    if (sem_wait(&ring->emptySlots) == -1) {
        return -1;
    }

    pthread_mutex_lock(&ring->lock);

    if (ring->stopped) {
        pthread_mutex_unlock(&ring->lock);
        sem_post(&ring->emptySlots);
        return -1;
    }

    // Single memcpy operation
    size_t first_chunk = ring->cap - ring->write_pos;
    if (first_chunk >= PACKAGE_SIZE) {
        memcpy(ring->data + ring->write_pos, data, PACKAGE_SIZE);
    } else {
        memcpy(ring->data + ring->write_pos, data, first_chunk);
        memcpy(ring->data, (char*)data + first_chunk, PACKAGE_SIZE - first_chunk);
    }

    ring->write_pos = (ring->write_pos + PACKAGE_SIZE) % ring->cap;
    ring->len += PACKAGE_SIZE;

    pthread_mutex_unlock(&ring->lock);

    // Single semaphore post
    sem_post(&ring->fullSlots);

    return PACKAGE_SIZE;
}

ssize_t ring_buffer_dequeue(so_ring_buffer_t *ring, void *data, size_t size)
{
    if (size < PACKAGE_SIZE)
        return -1;

    // Single semaphore wait for entire package
    if (sem_wait(&ring->fullSlots) == -1) {
        return -1;
    }

    pthread_mutex_lock(&ring->lock);

    if (ring->stopped && ring->len == 0) {
        pthread_mutex_unlock(&ring->lock);
        sem_post(&ring->fullSlots);
        return -1;
    }

    // Single memcpy operation
    size_t first_chunk = ring->cap - ring->read_pos;
    if (first_chunk >= PACKAGE_SIZE) {
        memcpy(data, ring->data + ring->read_pos, PACKAGE_SIZE);
    } else {
        memcpy(data, ring->data + ring->read_pos, first_chunk);
        memcpy((char*)data + first_chunk, ring->data, PACKAGE_SIZE - first_chunk);
    }

    ring->read_pos = (ring->read_pos + PACKAGE_SIZE) % ring->cap;
    ring->len -= PACKAGE_SIZE;

    pthread_mutex_unlock(&ring->lock);

    // Single semaphore post
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