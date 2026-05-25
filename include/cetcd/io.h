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
cetcd_co *cetcd_co_spawn(cetcd_loop *loop, cetcd_co_fn fn, void *arg);
void      cetcd_co_yield(cetcd_loop *loop);         /* yield back to scheduler */
void      cetcd_co_resume(cetcd_co *co);            /* resume a specific coroutine */

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
