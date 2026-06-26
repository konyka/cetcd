#ifndef CETCD_IO_H
#define CETCD_IO_H

#include "cetcd/base.h"

/* Forward declarations — opaque types */
typedef struct cetcd_loop cetcd_loop;
typedef struct cetcd_co   cetcd_co;

/* Loop lifecycle */
cetcd_loop *cetcd_loop_new(void);
void        cetcd_loop_free(cetcd_loop *loop);
int         cetcd_loop_run(cetcd_loop *loop);       /* blocks until stopped */
void        cetcd_loop_stop(cetcd_loop *loop);

/* Coroutine creation and scheduling */
typedef void (*cetcd_co_fn)(void *arg);

/* Default coroutine stack size (advisory; libco uses a shared stack). */
#define CETCD_CO_DEFAULT_STACK_SIZE 4096

/* Create a coroutine without starting it. stack_size is advisory. */
CETCD_API cetcd_co *cetcd_co_create(cetcd_loop *loop, cetcd_co_fn fn, void *arg,
                                     size_t stack_size);

/* Create and immediately start a coroutine (convenience wrapper). */
CETCD_API cetcd_co *cetcd_co_spawn(cetcd_loop *loop, cetcd_co_fn fn, void *arg);

/* Yield current coroutine back to the event loop. */
CETCD_API void      cetcd_co_yield(void);

/* Resume a specific coroutine. */
CETCD_API void      cetcd_co_resume(cetcd_co *co);

/* Get the currently running coroutine (NULL if none). */
CETCD_API cetcd_co *cetcd_co_current(void);

/* Check if a coroutine has finished executing. Returns 1 if dead. */
CETCD_API int       cetcd_co_dead(cetcd_co *co);

/* Set a human-readable name for a coroutine (used by profiling/diagnostics). */
CETCD_API void cetcd_co_set_name(cetcd_co *co, const char *name);

/* Free a coroutine handle and release owned resources (e.g. its name).
 * Safe to call whether or not the coroutine has finished. After this call
 * the pointer must not be used. */
CETCD_API void cetcd_co_free(cetcd_co *co);

/* Coroutine info for profiling — populated by cetcd_co_walk callback. */
typedef struct cetcd_co_info {
    int         id;
    int         state;          /* COROUTINE_DEAD/READY/RUNNING/SUSPEND */
    const char *name;           /* optional name set via cetcd_co_set_name */
    void       *fn_addr;        /* function pointer (for symbol resolution) */
} cetcd_co_info;

typedef void (*cetcd_co_walk_fn)(const cetcd_co_info *info, void *ud);
CETCD_API void cetcd_co_walk(cetcd_co_walk_fn fn, void *ud);

/* Schedule a coroutine for resumption from a libuv callback.
 * The coroutine will be resumed in the next event loop iteration. */
CETCD_API void cetcd_loop_schedule_resume(cetcd_loop *loop, cetcd_co *co);

/* TCP server/client */
typedef struct cetcd_tcp cetcd_tcp;
typedef void (*cetcd_tcp_conn_cb)(cetcd_tcp *server, cetcd_tcp *client, void *arg);

cetcd_tcp *cetcd_tcp_new(cetcd_loop *loop);
void       cetcd_tcp_free(cetcd_tcp *tcp);
int        cetcd_tcp_bind(cetcd_tcp *tcp, const char *addr, uint16_t port);
int        cetcd_tcp_listen(cetcd_tcp *tcp, cetcd_tcp_conn_cb cb, void *arg);

/* Stream I/O (read/write with coroutine yield) */
int  cetcd_tcp_read(cetcd_tcp *tcp, void *buf, size_t len);   /* yields until data available */
int  cetcd_tcp_write(cetcd_tcp *tcp, const void *buf, size_t len); /* yields until written */
void cetcd_tcp_close(cetcd_tcp *tcp);

/* Timer (one-shot and repeating, coroutine-aware) */
typedef struct cetcd_timer cetcd_timer;
cetcd_timer *cetcd_timer_new(cetcd_loop *loop);
void         cetcd_timer_free(cetcd_timer *timer);
void         cetcd_timer_start(cetcd_timer *timer, uint64_t timeout_ms, uint64_t repeat_ms,
                                  cetcd_co_fn cb, void *arg);
void         cetcd_timer_stop(cetcd_timer *timer);

/* Async (cross-thread signal) */
typedef struct cetcd_async cetcd_async;
typedef void (*cetcd_async_cb)(cetcd_async *async, void *arg);
cetcd_async *cetcd_async_new(cetcd_loop *loop, cetcd_async_cb cb, void *arg);
void         cetcd_async_free(cetcd_async *async);
void         cetcd_async_send(cetcd_async *async);  /* thread-safe */

#endif /* CETCD_IO_H */
