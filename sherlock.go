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
	expire int

	// Timeout to acquire lock
	timeout float64

	// Retry interval to retry acquiring a lock if previous attempts failed
	retryInterval float64

	// lock value for key
	owner string
}

var DefaultOptions = map[string]interface{}{
	"namespace":     "LockDefault",
	"expire":        60,
	"timeout":       10.0,
	"retryInterval": 0.1,
	"owner":         "dfltowner",
}

func NewBaseLock(lockName string) *BaseLock {
	l := &BaseLock{
		lockName: lockName,
	}
	l.namespace = DefaultOptions["namespace"].(string)
	l.expire = DefaultOptions["expire"].(int)
	l.timeout = DefaultOptions["timeout"].(float64)
	l.retryInterval = DefaultOptions["retryInterval"].(float64)
	l.owner = DefaultOptions["owner"].(string)

	return l
}

func (l *BaseLock) SetNamespace(namespace string) {
	l.namespace = namespace
}

func (l *BaseLock) SetExpire(expire int) {
	l.expire = expire
}

func (l *BaseLock) SetTimeout(timeout float64) {
	l.timeout = timeout
}

func (l *BaseLock) SetRetryInterval(retryInterval float64) {
	l.retryInterval = retryInterval
}

func (l *BaseLock) SetDfltOwner(owner string) {
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
		return false, nil
	}
}

func (l *EtcdLock) Acquire(blocking bool) (bool, error) {
	if blocking {
		timeout := l.timeout
		for timeout >= 0 {
			if ok, _ := l.acquire(); !ok {
				timeout -= l.retryInterval
				if timeout > 0 {
					time.Sleep(time.Second * time.Duration(l.retryInterval))
				}
			} else {
				return true, nil
			}
		}
		return false, fmt.Errorf("Timeout elapsed after %s seconds while "+
			"trying to acquiring lock.", l.timeout)
	} else {
		return l.acquire()
	}
}

func (l *EtcdLock) Release() error {
	_, err := l.client.Delete(l.keyName(), true)
	return err
}
