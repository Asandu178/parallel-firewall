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
    
    if (!ring->data)
        return -1;

    ring->read_pos = 0;
    ring->write_pos = 0;
    ring->len = 0;
    ring->stopped = 0;

    DIE(pthread_mutex_init(&ring->lock, NULL) != 0, "Error init mutex");
    
    // Since we always use 256 bytes, we can optimize semaphore counts
    DIE(sem_init(&ring->fullSlots, 0, 0) != 0, "Error init sem");
    DIE(sem_init(&ring->emptySlots, 0, ring->cap / PACKAGE_SIZE) != 0, "Error init sem");
    
    return 0;
}

ssize_t ring_buffer_enqueue(so_ring_buffer_t *ring, void *data, size_t size)
{
    if (size != PACKAGE_SIZE) {
        return -1;
	}

	DIE(size != PACKAGE_SIZE, "Invalid package size");

    // Single semaphore wait for entire package
    if (sem_wait(&ring->emptySlots) == -1) {
		DIE(1 == 1, "SEMWAIT");
        return -1;
    }

    DIE(pthread_mutex_lock(&ring->lock) != 0, "Mutex lock failed");

    if (ring->stopped) {
        DIE(pthread_mutex_unlock(&ring->lock) != 0, "Mutex unlock failed");
        DIE(sem_post(&ring->emptySlots) != 0, "SEMPOST");
        return -1;
    }

	memcpy(ring->data + ring->write_pos, data, PACKAGE_SIZE);

    ring->write_pos = (ring->write_pos + PACKAGE_SIZE) % ring->cap;
    ring->len += PACKAGE_SIZE;

    DIE(pthread_mutex_unlock(&ring->lock) != 0, "Mutex unlock failed");

    // Single semaphore post
    DIE(sem_post(&ring->fullSlots) != 0, "SEMPOST");

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

    DIE(pthread_mutex_lock(&ring->lock) != 0, "Mutex lock failed");

    if (ring->stopped && ring->len == 0) {
        DIE(pthread_mutex_unlock(&ring->lock) != 0, "Mutex unlock failed");
        DIE(sem_post(&ring->fullSlots) != 0, "SEMPOST");
        return -1;
    }

	memcpy((char*)data, ring->data + ring->read_pos, PACKAGE_SIZE);

    ring->read_pos = (ring->read_pos + PACKAGE_SIZE) % ring->cap;
    ring->len -= PACKAGE_SIZE;

    DIE(pthread_mutex_unlock(&ring->lock) != 0, "Mutex unlock failed");

    // Single semaphore post
    DIE(sem_post(&ring->emptySlots) != 0, "SEMPOST");

    return PACKAGE_SIZE;
}

void ring_buffer_destroy(so_ring_buffer_t *ring)
{
    free(ring->data);
    DIE(sem_destroy(&ring->emptySlots) != 0, "SEMDESTORY");
    DIE(sem_destroy(&ring->fullSlots) != 0, "SEMDESTORY");
    DIE(pthread_mutex_destroy(&ring->lock) != 0, "MUTEXDESTORY");
}

void ring_buffer_stop(so_ring_buffer_t *ring)
{
    DIE(pthread_mutex_lock(&ring->lock) != 0, "Mutex lock failed");
    ring->stopped = 1;
    DIE(pthread_mutex_unlock(&ring->lock) != 0, "Mutex unlock failed");
    
    // Wake up potential waiters
    DIE(sem_post(&ring->emptySlots) != 0, "SEMPOST");
    DIE(sem_post(&ring->fullSlots) != 0, "SEMPOST");
}