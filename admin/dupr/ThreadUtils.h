#ifndef THREADUTILS_H
#define THREADUTILS_H

#include <pthread.h>
#include <stdexcept>

#ifndef CACHE_LINE_SIZE
#   define CACHE_LINE_SIZE 64
#endif


namespace dupr {

class Condition;
class Lock;


/** A POSIX mutual exclusion lock. Use the Lock class for acquisition/release.
  */
class Mutex {
public:
    Mutex() {
        int err = ::pthread_mutex_init(&_mutex, 0);
        if (err != 0) {
            throw std::runtime_error("pthread_mutex_init() failed");
        }
    }

    ~Mutex() {
        int result = ::pthread_mutex_destroy(&_mutex);
        assert(result == 0);
    }

private:
    // Disable copy construction and assignment.
    Mutex(Mutex const &);
    Mutex & operator=(Mutex const &);

    void acquire() {
        int result = ::pthread_mutex_lock(&_mutex);
        assert(result == 0);
    }

    void release() {
        int result = ::pthread_mutex_unlock(&_mutex);
        assert(result == 0);
    }

    ::pthread_mutex_t _mutex;

    friend class Lock;
    friend class Condition;
};


/** Mutex acquisition and release using the RAII idiom.
  */
class Lock {
public:
    explicit Lock(Mutex & m) : _mutex(&m) { m.acquire(); }

    ~Lock() { release(); }

    void release() {
        if (_mutex != 0) {
            _mutex->release();
            _mutex = 0;
        }
    }

private:
    // Disable copy construction and assignment.
    Lock(Lock const &);
    Lock & operator=(Lock const &);

    Mutex * _mutex;

    friend class Condition;
};


/** A POSIX condition variable.
  */
class Condition {
public:
    Condition() {
        int result = ::pthread_cond_init(&_condition, 0);
        if (result != 0) {
            throw std::runtime_error("pthread_cond_init() failed");
        }
    }

    ~Condition() {
        int result = ::pthread_cond_destroy(&_condition);
        assert(result == 0);
    }

    /// Wait on the condition variable until the caller is woken as a result
    /// of another thread calling notify() or notifyAll(). Spurious wakeup
    /// can occur!
    void wait(Lock & lock) {
        assert(lock._mutex != 0);
        int result = ::pthread_cond_wait(&_condition, &(lock._mutex->_mutex));
        assert(result == 0);
    }

    /// Wake up at least one thread waiting on the condition.
    void notify() {
        int result = ::pthread_cond_signal(&_condition);
        assert(result == 0);
    }

    /// Wake up all threads waiting on the condition.
    void notifyAll() {
        int result = ::pthread_cond_broadcast(&_condition);
        assert(result == 0);
    }

private:
    ::pthread_cond_t _condition;
};

} // namespace dupr

#endif // THREADUTILS_H
