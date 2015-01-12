## sherlock-go

Sherlock is a simple distributed lock with pluginable backends.

It is inspired by [vaidik/sherlock](https://github.com/vaidik/sherlock)

## Basic Usage

```go

l := NewEtcdLock("TestEtcdLock", nil)
l.SetNamespace("lock")
if _, err := l.Acquire(false); err != nil {
    // error handling
}
if l.Locked() != true {
    // error handling
}
if err := l.Release(); err != nil {
    // error handling
}

```

