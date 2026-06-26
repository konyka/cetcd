/*
 * Coroutine support backed by libco (ucontext-based stackful coroutines).
 *
 * cetcd_co_create creates a coroutine without starting it.
 * cetcd_co_spawn creates and immediately resumes a coroutine.
 * cetcd_co_yield suspends the current coroutine; control returns to
 *   whoever last called cetcd_co_resume (or the event loop).
 * cetcd_co_resume resumes a specific suspended coroutine.
 *
 * A global pointer (g_co_current) tracks the currently-running coroutine
 * so that cetcd_co_yield() and cetcd_co_current() need no explicit
 * loop parameter.
 *
 * NOTE: ASan does not fully support makecontext/swapcontext.  Yield/resume
 * tests may need to run without sanitizers.  Basic spawn (no yield) works
 * correctly under ASan.
 */
#include <stdlib.h>
#include <string.h>
#include "cetcd/io.h"
#include "io_internal.h"

/* Persistent context stored on the heap so it survives across yield/resume. */
typedef struct cetcd_co {
    struct schedule *sched;
    int              id;
    cetcd_co_fn      fn;
    void            *arg;
    cetcd_loop      *loop;
    char            *name;    /* optional human-readable name for profiling */
} cetcd_co;

/* ── Global tracking of the currently-running coroutine ────────────────── */
static cetcd_co *g_co_current = NULL;

/* ── Global coroutine registry for profiling ───────────────────────────── */

typedef struct co_registry_node_ {
    cetcd_co                  *co;
    struct co_registry_node_  *next;
} co_registry_node_;

static co_registry_node_ *g_co_registry = NULL;

static void co_registry_add_(cetcd_co *co) {
    co_registry_node_ *node = (co_registry_node_ *)malloc(sizeof(*node));
    if (!node) return;
    node->co = co;
    node->next = g_co_registry;
    g_co_registry = node;
}

static void co_registry_remove_(cetcd_co *co) {
    co_registry_node_ **pp = &g_co_registry;
    while (*pp) {
        if ((*pp)->co == co) {
            co_registry_node_ *del = *pp;
            *pp = del->next;
            free(del);
            return;
        }
        pp = &(*pp)->next;
    }
}

/* Adapter: libco passes (schedule*, ud), we forward to cetcd_co_fn(ud) */
static void co_entry_(struct schedule *S, void *ud) {
    (void)S;
    cetcd_co *co = (cetcd_co *)ud;
    co->fn(co->arg);
    /* When the function returns the coroutine is dead; libco handles the
     * context switch back to the resumer.  g_co_current will be restored
     * by cetcd_co_resume / cetcd_co_spawn after coroutine_resume returns. */
    co_registry_remove_(co);
}

/* ── Public API ────────────────────────────────────────────────────────── */

cetcd_co *cetcd_co_create(cetcd_loop *loop, cetcd_co_fn fn, void *arg,
                           size_t stack_size) {
    if (!loop || !fn) return NULL;
    struct schedule *S = cetcd_loop_sched(loop);
    if (!S) return NULL;
    (void)stack_size;  /* libco uses a shared stack; stack_size is advisory */

    cetcd_co *co = (cetcd_co *)calloc(1, sizeof(*co));
    if (!co) return NULL;
    co->fn   = fn;
    co->arg  = arg;
    co->loop = loop;

    int id = coroutine_new(S, co_entry_, co);
    if (id < 0) {
        free(co);
        return NULL;
    }
    co->sched = S;
    co->id    = id;
    co_registry_add_(co);
    return co;
}

cetcd_co *cetcd_co_spawn(cetcd_loop *loop, cetcd_co_fn fn, void *arg) {
    cetcd_co *co = cetcd_co_create(loop, fn, arg, CETCD_CO_DEFAULT_STACK_SIZE);
    if (!co) return NULL;

    /* Run the coroutine immediately.  If it yields, it suspends here. */
    cetcd_co *prev = g_co_current;
    g_co_current = co;
    coroutine_resume(co->sched, co->id);
    g_co_current = prev;   /* restore after resume returns */
    return co;
}

void cetcd_co_yield(void) {
    cetcd_co *co = g_co_current;
    if (!co || !co->sched) return;
    if (coroutine_running(co->sched) >= 0) {
        g_co_current = NULL;
        coroutine_yield(co->sched);
        /* Resumed — g_co_current was set by cetcd_co_resume before
         * calling coroutine_resume, so it is already correct here. */
    }
}

void cetcd_co_resume(cetcd_co *co) {
    if (!co || !co->sched) return;
    if (coroutine_status(co->sched, co->id) != COROUTINE_DEAD) {
        cetcd_co *prev = g_co_current;
        g_co_current = co;
        coroutine_resume(co->sched, co->id);
        g_co_current = prev;
    }
}

cetcd_co *cetcd_co_current(void) {
    return g_co_current;
}

int cetcd_co_dead(cetcd_co *co) {
    if (!co || !co->sched) return 1;
    return coroutine_status(co->sched, co->id) == COROUTINE_DEAD;
}

void cetcd_co_set_name(cetcd_co *co, const char *name) {
    if (!co || !name) return;
    if (co->name) free(co->name);
#ifdef _WIN32
    co->name = _strdup(name);
#else
    co->name = strdup(name);
#endif
}

void cetcd_co_free(cetcd_co *co) {
    if (!co) return;
    co_registry_remove_(co);
    if (co->name) free(co->name);
    free(co);
}

void cetcd_co_walk(cetcd_co_walk_fn fn, void *ud) {
    if (!fn) return;
    for (co_registry_node_ *n = g_co_registry; n; n = n->next) {
        cetcd_co *co = n->co;
        cetcd_co_info info;
        memset(&info, 0, sizeof(info));
        info.id      = co->id;
        info.state   = co->sched ? coroutine_status(co->sched, co->id) : COROUTINE_DEAD;
        info.name    = co->name;
        info.fn_addr = (void *)(uintptr_t)co->fn;
        fn(&info, ud);
    }
}
