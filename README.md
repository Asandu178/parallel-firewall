# Parallel Firewall

## Overview

This project was developed as a homework assignment for the Operating Systems course. I decided to make the repository public to showcase some of the concepts I worked with, particularly multithreading and synchronization using POSIX threads.

The assignment is based on the classic Producer-Consumer problem, where a producer generates packets and multiple consumers process them using a shared circular buffer.

## Implementation

Working on this project helped me understand several concepts related to parallel programming, such as:

- Using POSIX threads to run multiple consumers concurrently.
- Implementing a circular buffer as a shared data structure between producer and consumer threads.
- Synchronizing access to shared resources using pthread synchronization primitives.
- Handling thread synchronization without busy waiting.
- Processing packets concurrently while keeping the output logs ordered by timestamp.
- Managing thread termination and coordinating the producer and consumer threads.

This assignment gave me practical experience with multithreaded programming and the challenges of coordinating multiple threads that access shared data.