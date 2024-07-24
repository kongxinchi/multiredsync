# multiredsync
批量版本的 redsync，可以针对一批分布式锁进行Lock/Unlock

## Features
1. 支持一次性对多个key进行加锁，操作具有原子性（任一未上锁成功则会阻塞等待）
2. 拥有和 redsync 相同的使用方式，仅构造方法有锁差异
3. 当仅对一个key加锁时，会退化为与 redsync 相同的操作过程（redlock），完全兼容 redsync 的使用场景&性能

## Usage
除了初始化方案需要传入多个key以外，其他的使用方式与redsync无二 
```go
func main() {
    opt := &redis.Options{
        Addr:     "127.0.0.1:6379",
    }
    client := redis.NewClient(opt)
    redsync := multiredsync.New(goredis.NewPool(client))
	
    mutex := redsync.NewMutex(
        []string{"key1", "key2", "key3", "key4", "key4"},
        multiredsync.WithExpiry(8*time.Second),
        multiredsync.WithRetryDelay(100*time.Millisecond),
    )
	
    if err := mutex.Lock(); err != nil {
        panic(err)
    }

    if ok, err := mutex.Unlock(); !ok || err != nil {
        panic("unlock failed")
    }
}
```