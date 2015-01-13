package sherlock

import (
	"fmt"
	"github.com/coreos/go-etcd/etcd"
	"time"
)

type ILock interface {
	Acquire(blocking bool) (bool, error)
	Release()
	Locked() bool
}

type BaseLock struct {
	lockName string

	// Namespace to use for setting lock keys in the backend store
	namespace string

	// Lock expiration time. If explicitly set to -1, lock will not expire
	expire int64

	// Timeout to acquire lock, in milliseconds
	timeout int64

	// Retry interval to retry acquiring a lock if previous attempts failed
	retryInterval int64

	// just a few milliseconds in order to compensate for clock drift between processes
	driftFactor float64

	// lock value for key
	owner string
}

var DefaultOptions = map[string]interface{}{
	"namespace":     "LockDefault",
	"expire":        int64(60000),
	"timeout":       int64(10000),
	"retryInterval": int64(100),
	"driftFactor":   0.001,
	"owner":         "dfltowner",
}

func NewBaseLock(lockName string) *BaseLock {
	l := &BaseLock{
		lockName: lockName,
	}
	l.namespace = DefaultOptions["namespace"].(string)
	l.expire = DefaultOptions["expire"].(int64)
	l.timeout = DefaultOptions["timeout"].(int64)
	l.retryInterval = DefaultOptions["retryInterval"].(int64)
	l.driftFactor = DefaultOptions["driftFactor"].(float64)
	l.owner = DefaultOptions["owner"].(string)

	return l
}

func (l *BaseLock) SetNamespace(namespace string) {
	l.namespace = namespace
}

func (l *BaseLock) SetExpire(expire int64) {
	l.expire = expire
}

func (l *BaseLock) SetTimeout(timeout int64) {
	l.timeout = timeout
}

func (l *BaseLock) SetRetryInterval(retryInterval int64) {
	l.retryInterval = retryInterval
}

func (l *BaseLock) SetDriftFactor(driftFactor float64) {
	l.driftFactor = driftFactor
}

func (l *BaseLock) SetOwner(owner string) {
	l.owner = owner
}

type EtcdLock struct {
	BaseLock
	client *etcd.Client
}

func NewEtcdLock(lockName string, client *etcd.Client) *EtcdLock {
	if client == nil {
		client = etcd.NewClient([]string{"http://127.0.0.1:4001"})
	}
	baseLock := NewBaseLock(lockName)
	l := &EtcdLock{*baseLock, client}

	return l
}

func (l *EtcdLock) keyName() string {
	return fmt.Sprintf("/%s/%s", l.namespace, l.lockName)
}

func (l *EtcdLock) Locked() bool {
	if _, err := l.client.Get(l.keyName(), false, false); err != nil {
		return false
	} else {
		return true
	}
}

func (l *EtcdLock) acquire() (bool, error) {
	if _, err := l.client.Get(l.keyName(), false, false); err != nil {
		if _, err := l.client.Set(l.keyName(), l.owner, uint64(l.expire)); err != nil {
			return false, err
		}
		return true, nil
	} else {
		return false, fmt.Errorf("lock has been acquired by other node")
	}
}

func (l *EtcdLock) Acquire(blocking bool) (int64, error) {
	start := time.Now().UnixNano()
	if blocking {
		timeout := l.timeout
		for timeout > 0 {
			if ok, _ := l.acquire(); !ok {
				timeout -= l.retryInterval
				if timeout > 0 {
					time.Sleep(time.Millisecond * time.Duration(l.retryInterval))
				}
			} else {
				break
			}
		}
		if timeout <= 0 {
			return -1, fmt.Errorf(
				"Timeout elapsed after %.3f seconds while trying to acquiring lock.",
				float64(l.timeout)/1000)
		}
	} else {
		if ok, err := l.acquire(); !ok {
			return -1, err
		}
	}
	cost := int64(time.Now().UnixNano()-start) / 1e6
	drift := int64(float64(l.expire) * l.driftFactor)
	validity := l.expire - cost - drift
	return validity, nil
}

func (l *EtcdLock) Release() error {
	_, err := l.client.Delete(l.keyName(), true)
	return err
}
