/*
 * Copyright (c) 2013 Mellanox Technologies®. All rights reserved.
 *
 * This software is available to you under a choice of one of two licenses.
 * You may choose to be licensed under the terms of the GNU General Public
 * License (GPL) Version 2, available from the file COPYING in the main
 * directory of this source tree, or the Mellanox Technologies® BSD license
 * below:
 *
 *      - Redistribution and use in source and binary forms, with or without
 *        modification, are permitted provided that the following conditions
 *        are met:
 *
 *      - Redistributions of source code must retain the above copyright
 *        notice, this list of conditions and the following disclaimer.
 *
 *      - Redistributions in binary form must reproduce the above
 *        copyright notice, this list of conditions and the following
 *        disclaimer in the documentation and/or other materials
 *        provided with the distribution.
 *
 *      - Neither the name of the Mellanox Technologies® nor the names of its
 *        contributors may be used to endorse or promote products derived from
 *        this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

#include <xio_os.h>
#include "libxio.h"
#include "xio_log.h"
#include "xio_common.h"
#include "xio_observer.h"
#include "get_clock.h"
#include "xio_ev_data.h"
#include "xio_ev_loop.h"
#include "xio_idr.h"
#include "xio_objpool.h"
#include "xio_workqueue.h"
#include <xio_env_adv.h>
#include "xio_timers_list.h"
#include "xio_protocol.h"
#include "xio_mbuf.h"
#include "xio_task.h"
#include "xio_transport.h"
#include "xio_context.h"
#include "xio_usr_utils.h"
#include "xio_init.h"
#include "xio_mem.h"

#ifdef XIO_THREAD_SAFE_DEBUG
#include <execinfo.h>
#endif

#define MSGPOOL_INIT_NR	8
#define MSGPOOL_GROW_NR	64

#ifdef XIO_CFLAG_STAT_COUNTERS
int xio_netlink(struct xio_context *ctx);
#endif

/*---------------------------------------------------------------------------*/
/* xio_context_reg_observer						     */
/*---------------------------------------------------------------------------*/
int xio_context_reg_observer(struct xio_context *ctx,
			     struct xio_observer *observer)
{
	xio_observable_reg_observer(&ctx->observable, observer);

	return 0;
}
EXPORT_SYMBOL(xio_context_reg_observer);

/*---------------------------------------------------------------------------*/
/* xio_context_unreg_observer		                                     */
/*---------------------------------------------------------------------------*/
void xio_context_unreg_observer(struct xio_context *ctx,
				struct xio_observer *observer)
{
	xio_observable_unreg_observer(&ctx->observable, observer);
}
EXPORT_SYMBOL(xio_context_unreg_observer);

/*---------------------------------------------------------------------------*/
/* xio_context_create                                                        */
/*---------------------------------------------------------------------------*/
struct xio_context *xio_context_create(struct xio_context_params *ctx_params,
				       int polling_timeout_us, int cpu_hint)
{
	struct xio_context		*ctx = NULL;
	struct xio_transport		*transport;
	int				cpu;
	size_t				msgpool_grow_nr;

	/* check if user called xio_init() */
	if (!xio_inited()) {
		ERROR_LOG("xio_init() must be called before any accelio func\n");
		return NULL;
	}

	if (cpu_hint == -1) {
		cpu = xio_get_cpu();
		if (cpu == -1) {
			xio_set_error(errno);
			return NULL;
		}
	} else {
		cpu = cpu_hint;
	}
	/*pin the process to cpu */
	xio_pin_to_cpu(cpu);
	/* pin to the numa node of the cpu */
	if (0)
		if (-1 == xio_pin_to_node(cpu)) {
			xio_set_error(errno);
			ERROR_LOG("could not set affinity to cpu. %m\n");
		}
	ctx = (struct xio_context *)xio_context_ucalloc(
			NULL, 1, sizeof(struct xio_context));
	if (!ctx) {
		xio_set_error(ENOMEM);
		ERROR_LOG("xio_context_ucalloc failed. %m\n");
		return NULL;
	}
	if (ctx_params && ctx_params->allocator_assigned) {
		ctx->allocator_assigned = 1;
		memcpy(&ctx->mem_allocator,
		       &ctx_params->mem_allocator,
		       sizeof(ctx->mem_allocator));
	}

	ctx->ev_loop		= xio_ev_loop_create(ctx);
	ctx->run_private	= 0;

	ctx->cpuid		= cpu;
	ctx->nodeid		= xio_numa_node_of_cpu(cpu);
	ctx->polling_timeout	= polling_timeout_us;
	ctx->worker		= xio_get_current_thread_id();

	if (ctx_params) {
		ctx->user_context = ctx_params->user_context;
		ctx->prealloc_xio_inline_bufs =
				!!ctx_params->prealloc_xio_inline_bufs;
		ctx->max_conns_per_ctx =
				max(ctx_params->max_conns_per_ctx, 2);
                ctx->register_internal_mempool =
                        !!ctx_params->register_internal_mempool;
		ctx->rq_depth = ctx_params->rq_depth;
		ctx->mempool_stats_cbs = ctx_params->mempool_stats;
	}
	if (!ctx->max_conns_per_ctx)
		ctx->max_conns_per_ctx = 100;

	XIO_OBSERVABLE_INIT(&ctx->observable, ctx);
	INIT_LIST_HEAD(&ctx->ctx_list);

	ctx->workqueue = xio_workqueue_create(ctx);
	if (!ctx->workqueue) {
		xio_set_error(ENOMEM);
		ERROR_LOG("context's workqueue create failed. %m\n");
		goto cleanup;
	}
	/* 2 messages per connection should suffice  */
	msgpool_grow_nr = 2 * ctx->max_conns_per_ctx;
	ctx->msg_pool = xio_objpool_create(ctx, sizeof(struct xio_msg),
					   MSGPOOL_INIT_NR, msgpool_grow_nr);
	if (!ctx->msg_pool) {
		xio_set_error(ENOMEM);
		ERROR_LOG("context's msg_pool create failed. %m\n");
		goto cleanup1;
	}

#ifdef XIO_CFLAG_STAT_COUNTERS
	if (-1 == xio_netlink(ctx))
		goto cleanup2;
#endif

	/* initialize rdma pools only */
	transport = xio_get_transport("rdma");
	if (transport && ctx->prealloc_xio_inline_bufs) {
		int retval = xio_ctx_pool_create(ctx, XIO_PROTO_RDMA,
					         XIO_CONTEXT_POOL_CLASS_INITIAL);
		if (retval) {
			ERROR_LOG("Failed to create initial pool. ctx:%p\n", ctx);
			goto cleanup2;
		}
		retval = xio_ctx_pool_create(ctx, XIO_PROTO_RDMA,
					     XIO_CONTEXT_POOL_CLASS_PRIMARY);
		if (retval) {
			ERROR_LOG("Failed to create primary pool. ctx:%p\n", ctx);
			goto cleanup2;
		}
	}
#ifdef XIO_THREAD_SAFE_DEBUG
	pthread_mutex_init(&ctx->dbg_thread_mutex, NULL);
#endif
	spin_lock_init(&ctx->ctx_list_lock);

	DEBUG_LOG("context created. context:%p\n", ctx);

	xio_idr_add_uobj(usr_idr, ctx, "xio_context");
	return ctx;

cleanup2:
	xio_objpool_destroy(ctx->msg_pool);
cleanup1:
	xio_workqueue_destroy(ctx->workqueue);
cleanup:
	xio_context_ufree(ctx, ctx);
	return NULL;
}
EXPORT_SYMBOL(xio_context_create);

/*---------------------------------------------------------------------------*/
/* xio_context_reset_stop						     */
/*---------------------------------------------------------------------------*/
static inline void xio_context_reset_stop(struct xio_context *ctx)
{
	xio_ev_loop_reset_stop(ctx->ev_loop);
}

/*---------------------------------------------------------------------------*/
/* xio_ctx_task_pools_orphans_or_on_hold_list_empty			     */
/*---------------------------------------------------------------------------*/
static inline int xio_ctx_task_pools_orphans_or_on_hold_list_empty(
							struct xio_context *ctx)
{
	int i;

	for (i = 0; i < XIO_PROTO_LAST; i++) {
		if (ctx->initial_tasks_pool[i]) {
			if (!xio_tasks_pool_orphans_or_on_hold_list_empty(
						ctx->initial_tasks_pool[i]))
				return 0;
		}
		if (ctx->primary_tasks_pool[i]) {
			if (!xio_tasks_pool_orphans_or_on_hold_list_empty(
						ctx->primary_tasks_pool[i]))
				return 0;
		}
	}
	return 1;
}

/*---------------------------------------------------------------------------*/
/* xio_ctx_tasks_pools_destroy						     */
/*---------------------------------------------------------------------------*/
static void xio_ctx_task_pools_destroy(struct xio_context *ctx)
{
	int i;

	for (i = 0; i < XIO_PROTO_LAST; i++) {
		if (ctx->initial_tasks_pool[i]) {
			xio_tasks_pool_free_tasks(ctx->initial_tasks_pool[i]);
			xio_tasks_pool_orphan_tasks_clear_unassign(ctx->initial_tasks_pool[i]);
			xio_tasks_pool_destroy(ctx->initial_tasks_pool[i]);
			ctx->initial_tasks_pool[i] = NULL;
		}
		if (ctx->primary_tasks_pool[i]) {
			xio_tasks_pool_free_tasks(ctx->primary_tasks_pool[i]);
			xio_tasks_pool_orphan_tasks_clear_unassign(ctx->primary_tasks_pool[i]);
			xio_tasks_pool_destroy(ctx->primary_tasks_pool[i]);
			ctx->primary_tasks_pool[i] = NULL;
		}
	}
}

/*---------------------------------------------------------------------------*/
/* xio_context_destroy	                                                     */
/*---------------------------------------------------------------------------*/
int xio_context_destroy(struct xio_context *ctx)
{
	int i;
	int found;
	int retval = 0;

	if (unlikely(!ctx)) {
		xio_set_error(EINVAL);
		return -1;
	}

	if (unlikely(ctx->is_running && !ctx->defered_destroy)) {
		ctx->defered_destroy = 1;
		xio_ev_loop_stop(ctx->ev_loop);
		return retval;
	}

	found = xio_idr_lookup_uobj(usr_idr, ctx);
	if (found) {
		xio_idr_remove_uobj(usr_idr, ctx);
	} else {
		ERROR_LOG("context not found:%p\n", ctx);
		xio_set_error(XIO_E_USER_OBJ_NOT_FOUND);
		return -1;
	}
	ctx->run_private = 0;
	xio_observable_notify_all_observers(&ctx->observable,
					    XIO_CONTEXT_EVENT_CLOSE, NULL);

	/* allow internally to run the loop for final cleanup */
	if (ctx->run_private) {
		xio_context_reset_stop(ctx);
		xio_context_run_loop(ctx, XIO_INFINITE);
	}

	if (ctx->run_private)
		ERROR_LOG("not all observers finished! run_private=%d\n",
			  ctx->run_private);

	xio_observable_notify_all_observers(&ctx->observable,
					    XIO_CONTEXT_EVENT_POST_CLOSE, NULL);

	if (!xio_observable_is_empty(&ctx->observable))
		ERROR_LOG("context destroy: observers leak - %p\n", ctx);

	xio_observable_unreg_all_observers(&ctx->observable);

	if (ctx->netlink_sock) {
		int fd = (int)(long)ctx->netlink_sock;

		xio_ev_loop_del(ctx->ev_loop, fd);
		close(fd);
		ctx->netlink_sock = NULL;
	}
	for (i = 0; i < XIO_STAT_LAST; i++)
		if (ctx->stats.name[i])
			free(ctx->stats.name[i]);

	xio_workqueue_destroy(ctx->workqueue);

	xio_objpool_destroy(ctx->msg_pool);

	xio_ev_loop_destroy(ctx->ev_loop);
	ctx->ev_loop = NULL;

	if (!xio_ctx_task_pools_orphans_or_on_hold_list_empty(ctx)) {
		xio_set_error(EBUSY);
		ERROR_LOG("%s failed. orphans or on_hold tasks still exists\n",
			  __func__);
		retval = -1;
	}

	xio_ctx_task_pools_destroy(ctx);

	if (ctx->mempool) {
		xio_mempool_destroy((struct xio_mempool *)ctx->mempool);
		ctx->mempool = NULL;
	}
#ifdef XIO_THREAD_SAFE_DEBUG
	pthread_mutex_destroy(&ctx->dbg_thread_mutex);
#endif

	XIO_OBSERVABLE_DESTROY(&ctx->observable);
	xio_context_ufree(NULL, ctx);

	return retval;
}
EXPORT_SYMBOL(xio_context_destroy);

/*---------------------------------------------------------------------------*/
/* xio_ctx_add_delayed_work						     */
/*---------------------------------------------------------------------------*/
int xio_ctx_add_delayed_work(struct xio_context *ctx,
			     int msec_duration, void *data,
			     void (*timer_fn)(int actual_timeout_ms, void *data),
			     xio_ctx_delayed_work_t *work)
{
	int retval;

	/* test if delayed work is pending */
	if (xio_is_delayed_work_pending(work))
		return 0;

	retval = xio_workqueue_add_delayed_work(ctx->workqueue,
						msec_duration, data,
						timer_fn, work);
	if (retval) {
		xio_set_error(errno);
		ERROR_LOG("xio_workqueue_add_delayed_work failed. %m\n");
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_ctx_del_delayed_work						     */
/*---------------------------------------------------------------------------*/
int xio_ctx_del_delayed_work(struct xio_context *ctx,
			     xio_ctx_delayed_work_t *work)
{
	int retval;

	/* test if delayed work is pending */
	if (!xio_is_delayed_work_pending(work))
		return 0;

	retval = xio_workqueue_del_delayed_work(ctx->workqueue, work);
	if (retval) {
		xio_set_error(errno);
		ERROR_LOG("xio_workqueue_del_delayed_work failed. %m\n");
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_add_counter							     */
/*---------------------------------------------------------------------------*/
int xio_add_counter(struct xio_context *ctx, char *name)
{
	int i;

	for (i = XIO_STAT_USER_FIRST; i < XIO_STAT_LAST; i++) {
		if (!ctx->stats.name[i]) {
			ctx->stats.name[i] = strdup(name);
			if (!ctx->stats.name[i]) {
				ERROR_LOG("stddup failed. %m");
				return -1;
			}
			ctx->stats.counter[i] = 0;
			return i;
		}
	}

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_del_counter							     */
/*---------------------------------------------------------------------------*/
int xio_del_counter(struct xio_context *ctx, int counter)
{
	if (counter < XIO_STAT_USER_FIRST || counter >= XIO_STAT_LAST) {
		ERROR_LOG("counter(%d) out of range\n", counter);
		return -1;
	}

	/* free the name and mark as free for reuse */
	free(ctx->stats.name[counter]);
	ctx->stats.name[counter] = NULL;

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_modify_context							     */
/*---------------------------------------------------------------------------*/
int xio_modify_context(struct xio_context *ctx,
		       struct xio_context_attr *attr,
		       int attr_mask)
{
	if (!ctx || !attr) {
		xio_set_error(EINVAL);
		ERROR_LOG("invalid parameters\n");
		return -1;
	}

	if (attr_mask & XIO_CONTEXT_ATTR_USER_CTX)
		ctx->user_context = attr->user_context;

	return 0;
}
EXPORT_SYMBOL(xio_modify_context);

/*---------------------------------------------------------------------------*/
/* xio_query_context							     */
/*---------------------------------------------------------------------------*/
int xio_query_context(struct xio_context *ctx,
		      struct xio_context_attr *attr,
		      int attr_mask)
{
	if (!ctx || !attr) {
		xio_set_error(EINVAL);
		ERROR_LOG("invalid parameters\n");
		return -1;
	}

	if (attr_mask & XIO_CONTEXT_ATTR_USER_CTX)
		attr->user_context = ctx->user_context;

	return 0;
}
EXPORT_SYMBOL(xio_query_context);

/*---------------------------------------------------------------------------*/
/* xio_context_get_poll_fd						     */
/*---------------------------------------------------------------------------*/
int xio_context_get_poll_fd(struct xio_context *ctx)
{
	return xio_ev_loop_get_poll_fd(ctx->ev_loop);
}
EXPORT_SYMBOL(xio_context_get_poll_fd);

/*---------------------------------------------------------------------------*/
/* xio_context_poll_wait						     */
/*---------------------------------------------------------------------------*/
int xio_context_poll_wait(struct xio_context *ctx, int timeout_ms)
{
	return xio_ev_loop_poll_wait(ctx->ev_loop, timeout_ms);
}
EXPORT_SYMBOL(xio_context_poll_wait);

/*---------------------------------------------------------------------------*/
/* xio_context_add_ev_handler						     */
/*---------------------------------------------------------------------------*/
int xio_context_add_ev_handler(struct xio_context *ctx,
			       int fd, int events,
			       xio_ev_handler_t handler,
			       void *data)
{
	return xio_ev_loop_add(ctx->ev_loop,
			       fd, events, handler, data);
}
EXPORT_SYMBOL(xio_context_add_ev_handler);

/*---------------------------------------------------------------------------*/
/* xio_context_modify_ev_handler					     */
/*---------------------------------------------------------------------------*/
int xio_context_modify_ev_handler(struct xio_context *ctx,
				  int fd, int events)
{
	return xio_ev_loop_modify(ctx->ev_loop, fd, events);
}

/*---------------------------------------------------------------------------*/
/* xio_context_del_ev_handler						     */
/*---------------------------------------------------------------------------*/
int xio_context_del_ev_handler(struct xio_context *ctx,
			       int fd)
{
	return xio_ev_loop_del(ctx->ev_loop, fd);
}

/*---------------------------------------------------------------------------*/
/* xio_context_run_loop							     */
/*---------------------------------------------------------------------------*/
int xio_context_run_loop(struct xio_context *ctx, int timeout_ms)
{
	int retval;
#ifdef XIO_THREAD_SAFE_DEBUG
	xio_ctx_debug_thread_lock(ctx);
#endif

	ctx->is_running = 1;
	retval = (timeout_ms == XIO_INFINITE) ? xio_ev_loop_run(ctx->ev_loop) :
		  xio_ev_loop_run_timeout(ctx->ev_loop, timeout_ms);
	ctx->is_running = 0;

	if (unlikely(ctx->defered_destroy))
		xio_context_destroy(ctx);

#ifdef XIO_THREAD_SAFE_DEBUG
	xio_ctx_debug_thread_unlock(ctx);
#endif

	return retval;
}
EXPORT_SYMBOL(xio_context_run_loop);

/*---------------------------------------------------------------------------*/
/* xio_context_stop_loop						     */
/*---------------------------------------------------------------------------*/
void xio_context_stop_loop(struct xio_context *ctx)
{
	xio_ev_loop_stop(ctx->ev_loop);
}
EXPORT_SYMBOL(xio_context_stop_loop);

/*---------------------------------------------------------------------------*/
/* xio_context_is_loop_stopping						     */
/*---------------------------------------------------------------------------*/
inline int xio_context_is_loop_stopping(struct xio_context *ctx)
{
	return xio_ev_loop_is_stopping(ctx->ev_loop);
}

/*---------------------------------------------------------------------------*/
/* xio_ctx_add_work							     */
/*---------------------------------------------------------------------------*/
int xio_ctx_add_work(struct xio_context *ctx,
		     void *data,
		     void (*function)(int actual_timeout_ms, void *data),
		     xio_ctx_work_t *work)
{
	int retval;

	/* test if work is pending */
	if (xio_is_work_pending(work))
		return 0;

	retval = xio_workqueue_add_work(ctx->workqueue,
					data, function, work);
	if (retval) {
		xio_set_error(errno);
		ERROR_LOG("xio_workqueue_add_work failed. %m\n");
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_ctx_set_work_destructor						     */
/*---------------------------------------------------------------------------*/
int xio_ctx_set_work_destructor(
		     struct xio_context *ctx, void *data,
		     void (*destructor)(void *data),
		     xio_ctx_work_t *work)
{
	int retval;

	/* test if work is pending */
	if (xio_is_work_pending(work))
		return 0;

	retval = xio_workqueue_set_work_destructor(
					ctx->workqueue,
					data, destructor, work);
	if (retval) {
		xio_set_error(errno);
		ERROR_LOG("xio_workqueue_set_work_destructor failed. %m\n");
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_ctx_is_work_in_handler						     */
/*---------------------------------------------------------------------------*/
int xio_ctx_is_work_in_handler(struct xio_context *ctx, xio_ctx_work_t *work)
{
	/* test if work is pending */
	if (xio_is_work_pending(work))
		return 0;

	return xio_workqueue_is_work_in_handler(ctx->workqueue, work);
}

/*---------------------------------------------------------------------------*/
/* xio_ctx_del_work							     */
/*---------------------------------------------------------------------------*/
int xio_ctx_del_work(struct xio_context *ctx,
		     xio_ctx_work_t *work)

{
	int retval;

	/* test if work is pending */
	if (!xio_is_work_pending(work))
		return 0;

	retval = xio_workqueue_del_work(ctx->workqueue, work);
	if (retval) {
		xio_set_error(errno);
		ERROR_LOG("xio_workqueue_del_work failed. %m\n");
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_context_add_event						     */
/*---------------------------------------------------------------------------*/
int xio_context_add_event(struct xio_context *ctx, struct xio_ev_data *data)
{
	xio_ev_loop_add_event(ctx->ev_loop, data);
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_context_disable_event						     */
/*---------------------------------------------------------------------------*/
void xio_context_disable_event(struct xio_ev_data *data)
{
	xio_ev_loop_remove_event(data);
}

/*---------------------------------------------------------------------------*/
/* xio_context_is_pending_event						     */
/*---------------------------------------------------------------------------*/
int xio_context_is_pending_event(struct xio_ev_data *data)
{
	return  xio_ev_loop_is_pending_event(data);
}

/*---------------------------------------------------------------------------*/
/* xio_context_poll_completions		                                     */
/*---------------------------------------------------------------------------*/
int xio_context_poll_completions(struct xio_context *ctx, int timeout_us)
{
	if (ctx->poll_completions_fn)
		return ctx->poll_completions_fn(ctx->poll_completions_ctx,
					       timeout_us);

	return 0;
}
EXPORT_SYMBOL(xio_context_poll_completions);

/*
 * should be called only from loop context
 */
/*---------------------------------------------------------------------------*/
/* xio_context_destroy_resume	                                             */
/*---------------------------------------------------------------------------*/
void xio_context_destroy_resume(struct xio_context *ctx)
{
	if (ctx->run_private) {
		if (!--ctx->run_private) {
			xio_context_stop_loop(ctx);
		}
	}
}
EXPORT_SYMBOL(xio_context_destroy_resume);

/*---------------------------------------------------------------------------*/
/* xio_context_set_poll_completions_fn	                                     */
/*---------------------------------------------------------------------------*/
void xio_context_set_poll_completions_fn(
		struct xio_context *ctx,
		poll_completions_fn_t poll_completions_fn,
		void *poll_completions_ctx)
{
	ctx->poll_completions_ctx = poll_completions_ctx;
	ctx->poll_completions_fn =  poll_completions_fn;
}
EXPORT_SYMBOL(xio_context_set_poll_completions_fn);

/*---------------------------------------------------------------------------*/
/* xio_ctx_pool_create							     */
/*-------------------------------------/--------------------------------------*/
int xio_ctx_pool_create(struct xio_context *ctx, enum xio_proto proto,
		        enum xio_context_pool_class pool_cls)
{
	struct xio_tasks_pool_ops	*pool_ops;
	struct xio_tasks_pool		**tasks_pool;
	struct xio_transport		*transport;
	struct xio_tasks_pool_params	params;
	char				pool_name[64];
	const char			*proto_str = xio_proto_str(proto);

	/* get the transport's proto */
	transport = xio_get_transport(proto_str);
	if (!transport) {
		ERROR_LOG("failed to load %s transport layer.\n", proto_str);
		ERROR_LOG("validate that your system support %s " \
			  "and the accelio's %s module is loaded\n",
			  proto_str, proto_str);
		xio_set_error(ENOPROTOOPT);
		return -1;
	}

	if (transport->get_pools_setup_ops) {
		if (!ctx->primary_pool_ops[proto] ||
		    !ctx->initial_pool_ops[proto])
			transport->get_pools_setup_ops(
					NULL,
					&ctx->initial_pool_ops[proto],
					&ctx->primary_pool_ops[proto]);
	} else {
		ERROR_LOG("transport does not implement " \
			  "\"get_pools_setup_ops\"\n");
		return -1;
	}

	switch (pool_cls) {
	case XIO_CONTEXT_POOL_CLASS_INITIAL:
		tasks_pool = &ctx->initial_tasks_pool[proto];
		pool_ops = ctx->initial_pool_ops[proto];
		sprintf(pool_name, "ctx:%p - initial_pool_%s", ctx, proto_str);

	break;
	case XIO_CONTEXT_POOL_CLASS_PRIMARY:
		tasks_pool = &ctx->primary_tasks_pool[proto];
		pool_ops = ctx->primary_pool_ops[proto];
		sprintf(pool_name, "ctx:%p - primary_pool_%s", ctx, proto_str);
	break;
	default:
		xio_set_error(EINVAL);
		ERROR_LOG("unknown pool class\n");
		return -1;
	};

	/* if already exist */
	if (*tasks_pool)
		return 0;

	if (!pool_ops)
		return -1;

	if (!pool_ops->pool_get_params ||
	    !pool_ops->slab_pre_create ||
	    !pool_ops->slab_init_task ||
	    !pool_ops->pool_post_create ||
	    !pool_ops->slab_destroy)
		return -1;

	/* get pool properties from the transport */
	memset(&params, 0, sizeof(params));
	params.xio_context = ctx;

	pool_ops->pool_get_params(NULL,
				  (int *)&params.start_nr,
				  (int *)&params.max_nr,
				  (int *)&params.alloc_nr,
				  (int *)&params.pool_dd_data_sz,
				  (int *)&params.slab_dd_data_sz,
				  (int *)&params.task_dd_data_sz);
	params.max_nr = params.max_nr * ctx->max_conns_per_ctx;
	if (ctx->prealloc_xio_inline_bufs) {
		params.start_nr = params.max_nr;
		params.alloc_nr = 0;
	}
	params.pool_hooks.slab_pre_create  =
		(int (*)(void *, struct xio_context *, int, void *, void *))
				pool_ops->slab_pre_create;
	params.pool_hooks.slab_post_create = (int (*)(void *, void *, void *))
				pool_ops->slab_post_create;
	params.pool_hooks.slab_destroy	   = (int (*)(void *, void *, void *))
				pool_ops->slab_destroy;
	params.pool_hooks.slab_init_task   =
		(int (*)(void *, void *, void *, int,  struct xio_task *))
				pool_ops->slab_init_task;
	params.pool_hooks.slab_uninit_task =
		(int (*)(void *, void *, void *, struct xio_task *))
				pool_ops->slab_uninit_task;
	params.pool_hooks.slab_remap_task =
		(int (*)(void *, void *, void *, void *, struct xio_task *))
				pool_ops->slab_remap_task;
	params.pool_hooks.pool_pre_create  = (int (*)(void *, void *, void *))
				pool_ops->pool_pre_create;
	params.pool_hooks.pool_post_create = (int (*)(void *, void *, void *))
				pool_ops->pool_post_create;
	params.pool_hooks.pool_destroy	   = (int (*)(void *, void *, void *))
				pool_ops->pool_destroy;
	params.pool_hooks.task_pre_put  = (int (*)(void *, struct xio_task *))
		pool_ops->task_pre_put;
	params.pool_hooks.task_post_get = (int (*)(void *, struct xio_task *))
		pool_ops->task_post_get;

	params.pool_name = xio_context_kstrdup(ctx, pool_name, GFP_KERNEL);

	/* initialize the tasks pool */
	*tasks_pool = xio_tasks_pool_create(&params);
	if (!*tasks_pool) {
		ERROR_LOG("xio_tasks_pool_create failed\n");
		return -1;
	}

	return 0;
}

void xio_context_mempool_dump(struct xio_context *ctx)
{
	xio_mempool_dump((struct xio_mempool*)ctx->mempool);
}

#ifdef XIO_THREAD_SAFE_DEBUG

void xio_ctx_debug_thread_print_stack(int frames, void * const *callstack)
{
	char **strs;
	int i;
	ERROR_LOG("\tstack trace is\n");
	strs = backtrace_symbols(callstack, frames);
	for (i = 0; i < frames; ++i) {
		 ERROR_LOG("%s\n", strs[i]);
	}
	free(strs);
}

int xio_ctx_debug_thread_lock(struct xio_context *ctx)
{
	if (!pthread_mutex_trylock(&ctx->dbg_thread_mutex)) {
		/* mutex was acquired - saving the current stacktrace */
		ctx->nptrs = backtrace(ctx->buffer, BACKTRACE_BUFFER_SIZE);
		return 1;
	}
	ERROR_LOG("trying to lock an already locked lock for ctx %p\n", ctx);
	xio_ctx_debug_thread_print_stack(ctx->nptrs, ctx->buffer);
	ctx->nptrs = backtrace(ctx->buffer, BACKTRACE_BUFFER_SIZE);
	xio_ctx_debug_thread_print_stack(ctx->nptrs, ctx->buffer);

	/*since lock was unsuccessful, wait until the lock becomes available */
	pthread_mutex_lock(&ctx->dbg_thread_mutex);
	return 0;
}

int xio_ctx_debug_thread_unlock(struct xio_context *ctx)
{
	pthread_mutex_unlock(&ctx->dbg_thread_mutex);
	return 0;
}

#endif

void *xio_context_umalloc(struct xio_context *ctx, size_t size)
{
	if (ctx && ctx->allocator_assigned &&
	    ctx->mem_allocator.allocate)
		return ctx->mem_allocator.allocate(ctx, size,
				ctx->mem_allocator.user_context);
	else
		return umalloc(size);
}

void *xio_context_ucalloc(struct xio_context *ctx, size_t nmemb, size_t size)
{
	void *ptr;

	if (ctx && ctx->allocator_assigned &&
	    ctx->mem_allocator.allocate) {
		ptr = ctx->mem_allocator.allocate(ctx, nmemb*size,
					      ctx->mem_allocator.user_context);
		if (ptr)
			memset(ptr, 0, nmemb*size);
	} else {
		ptr = ucalloc(nmemb, size);
	}
	return ptr;
}

void *xio_context_umemalign(struct xio_context *ctx, size_t boundary, size_t size)
{
	void *ptr;

	if (ctx && ctx->allocator_assigned &&
	    ctx->mem_allocator.memalign) {
		ptr = ctx->mem_allocator.memalign(ctx, boundary, size,
					      ctx->mem_allocator.user_context);
		if (ptr)
			memset(ptr, 0, size);
	} else {
		ptr = umemalign(boundary, size);
	}

	return ptr;
}

void *xio_context_umalloc_huge_pages(struct xio_context *ctx, size_t size)
{
	void *ptr;

	if (ctx && ctx->allocator_assigned &&
	    ctx->mem_allocator.malloc_huge_pages) {
		ptr = ctx->mem_allocator.malloc_huge_pages(ctx, size,
					      ctx->mem_allocator.user_context);
		if (ptr)
			memset(ptr, 0, size);
	} else {
		ptr = umalloc_huge_pages(size);
	}

	return ptr;
}

void *xio_context_unuma_alloc(struct xio_context *ctx, size_t size, int node)
{
	void *ptr;

	if (ctx && ctx->allocator_assigned &&
	    ctx->mem_allocator.numa_alloc) {
		ptr = ctx->mem_allocator.numa_alloc(ctx, size, node,
					      ctx->mem_allocator.user_context);
		if (ptr)
			memset(ptr, 0, size);
	} else {
		ptr = unuma_alloc(size, node);
	}
	return ptr;
}

void xio_context_ufree(struct xio_context *ctx, void *ptr)
{
	if (ctx && ctx->allocator_assigned &&
	    ctx->mem_allocator.free)
		ctx->mem_allocator.free(ctx, ptr, ctx->mem_allocator.user_context);
	else
		ufree(ptr);
}

char *xio_context_ustrdup(struct xio_context *ctx, char const *s)
{
	size_t len = strlen(s) + 1;
	char *new1 = (char*)xio_context_umalloc(ctx, len);

	if (new1 == NULL)
		return NULL;

	return (char*)memcpy(new1, s, len);
}

char *xio_context_ustrndup(struct xio_context *ctx, char const *s, size_t n)
{
	size_t len = strnlen(s, n);
	char *new1 = (char*)xio_context_umalloc(ctx, len + 1);

	if (new1 == NULL)
		return NULL;

	new1[len] = '\0';
	return (char*)memcpy(new1, s, len);
}

void xio_context_kfree(struct xio_context *ctx, const void *ptr)
{
	xio_context_ufree(ctx, (void *) ptr);
}

void *xio_context_kmalloc(struct xio_context *ctx, size_t size, gfp_t flags)
{
	/* Make sure code transferred to kernel will work as expected */
	assert(flags == GFP_KERNEL);
	return xio_context_umalloc(ctx, size);
}

void *xio_context_kcalloc(struct xio_context *ctx, size_t n, size_t size, gfp_t flags)
{
	/* Make sure code transferred to kernel will work as expected */
	assert(flags == GFP_KERNEL);
	return xio_context_ucalloc(ctx, n, size);
}

void *xio_context_kzalloc(struct xio_context *ctx, size_t size, gfp_t flags)
{
	/* Make sure code transferred to kernel will work as expected */
	assert(flags == GFP_KERNEL);
	return xio_context_ucalloc(ctx, 1, size);
}

void *xio_context_vmalloc(struct xio_context *ctx, unsigned long size)
{
	return xio_context_umalloc(ctx, size);
}

void *xio_context_vzalloc(struct xio_context *ctx, unsigned long size)
{
	return xio_context_ucalloc(ctx, 1, size);
}

void xio_context_vfree(struct xio_context *ctx, const void *addr)
{
	xio_context_ufree(ctx, (void *) addr);
}

char *xio_context_kstrdup(struct xio_context *ctx, const char *s, gfp_t gfp)
{
	/* Make sure code transferred to kernel will work as expected */
	assert(gfp == GFP_KERNEL);
	return xio_context_ustrdup(ctx, s);
}

char *xio_context_kstrndup(struct xio_context *ctx,const char *s, size_t len, gfp_t gfp)
{
	/* Make sure code transferred to kernel will work as expected */
	assert(gfp == GFP_KERNEL);
	return xio_context_ustrndup(ctx, s, len);
}

void xio_context_ufree_huge_pages(struct xio_context *ctx, void *ptr)
{
	if (ctx && ctx->allocator_assigned &&
	    ctx->mem_allocator.free_huge_pages)
		ctx->mem_allocator.free_huge_pages(ctx, ptr,
					      ctx->mem_allocator.user_context);
	else
		ufree_huge_pages(ptr);
}

void xio_context_unuma_free(struct xio_context *ctx, void *ptr)
{
	if (ctx && ctx->allocator_assigned &&
	    ctx->mem_allocator.numa_free)
		ctx->mem_allocator.numa_free(ctx, ptr, ctx->mem_allocator.user_context);
	else
		unuma_free(ptr);
}

