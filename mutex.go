package multiredsync

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"time"

	"github.com/go-redsync/redsync/v4/redis"
	"github.com/hashicorp/go-multierror"
)

// A DelayFunc is used to decide the amount of time to wait between retries.
type DelayFunc func(tries int) time.Duration

// A Mutex is a distributed mutual exclusion lock.
type Mutex struct {
	// name   string
	names  []string
	expiry time.Duration

	tries     int
	delayFunc DelayFunc

	driftFactor   float64
	timeoutFactor float64

	quorum int

	genValueFunc  func() (string, error)
	value         string
	until         time.Time
	shuffle       bool
	failFast      bool
	setNXOnExtend bool

	pools []redis.Pool
}

// Names returns mutex names (i.e. the Redis key).
func (m *Mutex) Names() []string {
	return m.names
}

// Value returns the current random value. The value will be empty until a lock is acquired (or WithValue option is used).
func (m *Mutex) Value() string {
	return m.value
}

// Until returns the time of validity of acquired lock. The value will be zero value until a lock is acquired.
func (m *Mutex) Until() time.Time {
	return m.until
}

// TryLock only attempts to lock m once and returns immediately regardless of success or failure without retrying.
func (m *Mutex) TryLock() error {
	return m.TryLockContext(context.Background())
}

// TryLockContext only attempts to lock m once and returns immediately regardless of success or failure without retrying.
func (m *Mutex) TryLockContext(ctx context.Context) error {
	return m.lockContext(ctx, 1)
}

// Lock locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) Lock() error {
	return m.LockContext(context.Background())
}

// LockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) LockContext(ctx context.Context) error {
	return m.lockContext(ctx, m.tries)
}

// lockContext locks m. In case it returns an error on failure, you may retry to acquire the lock by calling this method again.
func (m *Mutex) lockContext(ctx context.Context, tries int) error {
	if ctx == nil {
		ctx = context.Background()
	}

	value, err := m.genValueFunc()
	if err != nil {
		return err
	}

	var timer *time.Timer
	for i := 0; i < tries; i++ {
		if i != 0 {
			if timer == nil {
				timer = time.NewTimer(m.delayFunc(i))
			} else {
				timer.Reset(m.delayFunc(i))
			}

			select {
			case <-ctx.Done():
				timer.Stop()
				// Exit early if the context is done.
				return ErrFailed
			case <-timer.C:
				// Fall-through when the delay timer completes.
			}
		}

		start := time.Now()

		n, err := func() (int, error) {
			ctx, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(m.expiry)*m.timeoutFactor)))
			defer cancel()
			return m.actOnPoolsAsync(
				func(pool redis.Pool) (bool, error) {
					return m.acquire(ctx, pool, value)
				},
			)
		}()

		now := time.Now()
		until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
		if n >= m.quorum && now.Before(until) {
			m.value = value
			m.until = until
			return nil
		}
		_, _ = func() (int, error) {
			ctx, cancel := context.WithTimeout(ctx, time.Duration(int64(float64(m.expiry)*m.timeoutFactor)))
			defer cancel()
			return m.actOnPoolsAsync(
				func(pool redis.Pool) (bool, error) {
					return m.release(ctx, pool, value)
				},
			)
		}()
		if i == tries-1 && err != nil {
			return err
		}
	}

	return ErrFailed
}

// Unlock unlocks m and returns the status of unlock.
func (m *Mutex) Unlock() (bool, error) {
	return m.UnlockContext(context.Background())
}

// UnlockContext unlocks m and returns the status of unlock.
func (m *Mutex) UnlockContext(ctx context.Context) (bool, error) {
	n, err := m.actOnPoolsAsync(
		func(pool redis.Pool) (bool, error) {
			return m.release(ctx, pool, m.value)
		},
	)
	if n < m.quorum {
		return false, err
	}
	return true, nil
}

// Extend resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) Extend() (bool, error) {
	return m.ExtendContext(context.Background())
}

// ExtendContext resets the mutex's expiry and returns the status of expiry extension.
func (m *Mutex) ExtendContext(ctx context.Context) (bool, error) {
	start := time.Now()
	n, err := m.actOnPoolsAsync(
		func(pool redis.Pool) (bool, error) {
			return m.touch(ctx, pool, m.value, int(m.expiry/time.Millisecond))
		},
	)
	if n < m.quorum {
		return false, err
	}
	now := time.Now()
	until := now.Add(m.expiry - now.Sub(start) - time.Duration(int64(float64(m.expiry)*m.driftFactor)))
	if now.Before(until) {
		m.until = until
		return true, nil
	}
	return false, ErrExtendFailed
}

func genValue() (string, error) {
	b := make([]byte, 16)
	_, err := rand.Read(b)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(b), nil
}

func acquireScript(keyCount int) *redis.Script {
	return redis.NewScript(
		keyCount, `
		local params = {};
		for k, v in pairs(KEYS) do
			table.insert(params, v);
			table.insert(params, ARGV[1]);
        end
		if redis.call("MSETNX", unpack(params)) == 1 then
			for k, v in pairs(KEYS) do
				redis.call("PEXPIRE", v, ARGV[2]);
			end
			return 1;
		else
			return 0;
		end
	`,
	)
}

func (m *Mutex) acquire(ctx context.Context, pool redis.Pool, value string) (bool, error) {
	if len(m.names) == 0 {
		return true, nil
	}

	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	// 如果只有一个Key，退化成直接SetNX
	if len(m.names) == 1 {
		reply, err := conn.SetNX(m.names[0], value, m.expiry)
		if err != nil {
			return false, err
		}
		return reply, nil
	}

	keysAndArgs := make([]interface{}, 0)
	for _, name := range m.names {
		keysAndArgs = append(keysAndArgs, name)
	}
	keysAndArgs = append(keysAndArgs, value, int(m.expiry/time.Millisecond))

	status, err := conn.Eval(acquireScript(len(m.names)), keysAndArgs...)
	if err != nil {
		return false, err
	}
	return status != int64(0), nil
}

func deleteScript(keyCount int) *redis.Script {
	if keyCount == 1 {
		return redis.NewScript(
			1, `
			local val = redis.call("GET", KEYS[1])
			if val == ARGV[1] then
				return redis.call("DEL", KEYS[1])
			elseif val == false then
				return -1
			else
				return 0
			end
		`,
		)
	}

	return redis.NewScript(
		keyCount, `
		local values = redis.call("MGET", unpack(KEYS));
		local delete_keys = {};
		local expired_keys = {};
		for i, val in pairs(values) do
			if val == ARGV[1] then
				table.insert(delete_keys, KEYS[i]);
			elseif val == false then
				table.insert(expired_keys, KEYS[i]);
			end
		end

		if #delete_keys > 0 then
			return redis.call("DEL", unpack(delete_keys));
		elseif #expired_keys > 0 then
			return -1;
		else
			return 0;
		end
	`,
	)
}

func (m *Mutex) release(ctx context.Context, pool redis.Pool, value string) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	keysAndArgs := make([]interface{}, 0)
	for _, name := range m.names {
		keysAndArgs = append(keysAndArgs, name)
	}
	keysAndArgs = append(keysAndArgs, value)

	status, err := conn.Eval(deleteScript(len(m.names)), keysAndArgs...)
	if err != nil {
		return false, err
	}
	if status == int64(-1) {
		return false, ErrLockAlreadyExpired
	}
	return status != int64(0), nil
}

func touchScript(keyCount int) *redis.Script {
	return redis.NewScript(
		keyCount, `
		local values = redis.call("MGET", unpack(KEYS));
		local result = 1;
		for i, val in pairs(values) do
			if val == ARGV[1] then
				if redis.call("PEXPIRE", KEYS[i], ARGV[2]) == 0 then
					result = 0;
				end
			else
				result = 0;
			end
		end
		return result;
	`,
	)
}

func touchWithSetNXScript(keyCount int) *redis.Script {
	return redis.NewScript(
		keyCount, `
		local values = redis.call("MGET", unpack(KEYS));
		local result = 1;
		for i, val in pairs(values) do
			if val == ARGV[1] then
				if redis.call("PEXPIRE", KEYS[i], ARGV[2]) == 0 then
					result = 0;
				end
			else
				if redis.call("SET", KEYS[i], ARGV[1], "PX", ARGV[2], "NX") == 0 then
					result = 0;
				end
			end
		end
		return result;
	`,
	)
}

func (m *Mutex) touch(ctx context.Context, pool redis.Pool, value string, expiry int) (bool, error) {
	conn, err := pool.Get(ctx)
	if err != nil {
		return false, err
	}
	defer conn.Close()

	script := touchScript(len(m.names))
	if m.setNXOnExtend {
		script = touchWithSetNXScript(len(m.names))
	}

	keysAndArgs := make([]interface{}, 0)
	for _, name := range m.names {
		keysAndArgs = append(keysAndArgs, name)
	}
	keysAndArgs = append(keysAndArgs, value, expiry)

	status, err := conn.Eval(script, keysAndArgs...)
	if err != nil {
		return false, err
	}
	return status != int64(0), nil
}

func (m *Mutex) actOnPoolsAsync(actFn func(redis.Pool) (bool, error)) (int, error) {
	type result struct {
		node     int
		statusOK bool
		err      error
	}

	ch := make(chan result, len(m.pools))
	for node, pool := range m.pools {
		go func(node int, pool redis.Pool) {
			r := result{node: node}
			r.statusOK, r.err = actFn(pool)
			ch <- r
		}(node, pool)
	}

	var (
		n     = 0
		taken []int
		err   error
	)

	for range m.pools {
		r := <-ch
		if r.statusOK {
			n++
		} else if r.err == ErrLockAlreadyExpired {
			err = multierror.Append(err, ErrLockAlreadyExpired)
		} else if r.err != nil {
			err = multierror.Append(err, &RedisError{Node: r.node, Err: r.err})
		} else {
			taken = append(taken, r.node)
			err = multierror.Append(err, &ErrNodeTaken{Node: r.node})
		}

		if m.failFast {
			// fast retrun
			if n >= m.quorum {
				return n, err
			}

			// fail fast
			if len(taken) >= m.quorum {
				return n, &ErrTaken{Nodes: taken}
			}
		}
	}

	if len(taken) >= m.quorum {
		return n, &ErrTaken{Nodes: taken}
	}
	return n, err
}
