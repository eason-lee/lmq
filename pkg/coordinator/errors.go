package coordinator

import "errors"

var (
    ErrNotLeader     = errors.New("not leader")
    ErrLockNotHeld   = errors.New("lock not held")
    ErrSessionExpired = errors.New("session expired")
)