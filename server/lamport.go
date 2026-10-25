package main

import "sync"

type LamportClock struct {
	mu   sync.Mutex
	time int64
}

func (c *LamportClock) Tick() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.time++
	return c.time
}

func (c *LamportClock) Update(received int64) int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	if received > c.time {
		c.time = received
	}
	c.time++
	return c.time
}

func (c *LamportClock) Now() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.time
}
