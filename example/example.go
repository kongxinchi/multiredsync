package main

import (
	"context"
	"fmt"
	"time"

	"github.com/go-redis/redis/v8"
	"github.com/go-redsync/redsync/v4/redis/goredis/v8"
	"github.com/kongxinchi/multiredsync"
)

func main() {
	// 执行
	ctx := context.Background()

	opt := &redis.Options{
		Addr:     "127.0.0.1:6379",
		DB:       0,
		PoolSize: 10,
	}
	client := redis.NewClient(opt)
	redsync := multiredsync.New(goredis.NewPool(client))

	keys := make([]string, 0)
	for i := 0; i < 100; i++ {
		keys = append(keys, fmt.Sprintf("key%d", i))
	}

	mutex := redsync.NewMutex(
		keys,
		multiredsync.WithExpiry(8*time.Second),
		multiredsync.WithRetryDelay(100*time.Millisecond),
	)
	begin := time.Now()
	err := mutex.LockContext(ctx)
	fmt.Printf("cost: %+v, err: %+v, until: %+v\n", time.Now().Sub(begin), err, mutex.Until())

	time.Sleep(1 * time.Second)

	begin = time.Now()
	ok, err := mutex.ExtendContext(ctx)
	fmt.Printf("cost: %+v, ok: %+v, err: %+v, until: %+v\n", time.Now().Sub(begin), ok, err, mutex.Until())

	begin = time.Now()
	ok, err = mutex.UnlockContext(ctx)
	fmt.Printf("cost: %+v, ok: %+v, err: %+v\n", time.Now().Sub(begin), ok, err)
}
