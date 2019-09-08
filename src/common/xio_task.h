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
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */
#ifndef XIO_TASK_H
#define XIO_TASK_H

#ifndef list_last_entry
#define list_last_entry(ptr, type, member) \
	list_entry((ptr)->prev, type, member)
#endif

enum xio_task_state {
	XIO_TASK_STATE_INIT,
	XIO_TASK_STATE_DELIVERED,
	XIO_TASK_STATE_READ,
	XIO_TASK_STATE_RESPONSE_RECV,  /* mark the sender task */
	XIO_TASK_STATE_CANCEL_PENDING,      /* mark for rdma read task */
};

/*---------------------------------------------------------------------------*/
/* forward declarations							     */
/*---------------------------------------------------------------------------*/
struct xio_tasks_pool;
struct xio_task;

extern void xio_connection_reset_task(struct xio_task *task);

/*---------------------------------------------------------------------------*/
/* structs								     */
/*---------------------------------------------------------------------------*/
struct xio_task {
	struct list_head	tasks_list_entry;
	void			*dd_data;
	struct xio_task		*sender_task;  /* client only on receiver */
	struct xio_mbuf		mbuf;

	enum xio_task_state	state;		/* task state enum	*/
	struct kref		kref;
	uint64_t		stag;		/* session unique tag */
	uint32_t                tlv_type;
	uint16_t                is_control;
	uint16_t                omsg_flags;
	uint16_t                imsg_flags;
	uint16_t                last_in_rxq;
	uint32_t                ltid;           /* local task id        */
	uint32_t                rtid;           /* remote task id       */
	uint32_t                magic;
	int32_t                 status;
	uint32_t                on_hold:1;
	uint32_t                is_assigned:1;
	uint32_t		ka_probes:1;
	uint32_t                pad:29;

	void			*pool;
	void			*slab;
	void			*context;
	struct xio_session	*session;
	struct xio_connection	*connection;
	struct xio_nexus	*nexus;

	struct xio_vmsg		in_receipt;     /* save in of message with */
						/* receipt */
	struct xio_msg		*omsg;		/* pointer from user */
	struct xio_msg		imsg;		/* message to the user */

	int 			(*unassign_data_in_buf)(struct xio_msg *msg, void *user_context);
	void			*unassign_user_context;
};

struct xio_tasks_pool_hooks {
	void	*context;
	int	(*slab_pre_create)(void *context,
				   struct xio_context *ctx,
				   int alloc_nr,
				   void *pool_dd_data,
				   void *slab_dd_data);
	int	(*slab_destroy)(void *context,
				void *pool_dd_data,
				void *slab_dd_data);
	int	(*slab_init_task)(void *context,
				  void *pool_dd_data,
				  void *slab_dd_data,
				  int tid, struct xio_task *task);
	int	(*slab_uninit_task)(void *context,
				    void *pool_dd_data,
				    void *slab_dd_data,
				    struct xio_task *task);
	int	(*slab_remap_task)(void *old_context,
				   void *new_context,
				   void *pool_dd_data,
				   void *slab_dd_data,
				   struct xio_task *task);
	int	(*slab_post_create)(void *context,
				    void *pool_dd_data,
				    void *slab_dd_data);
	int	(*pool_pre_create)(void *context, void *pool,
				   void *pool_dd_data);
	int	(*pool_post_create)(void *context, void *pool,
				    void *pool_dd_data);
	int	(*pool_destroy)(void *context, void *pool,
				void *pool_dd_data);
	int	(*task_pre_put)(void *context, struct xio_task *task);
	int	(*task_post_get)(void *context, struct xio_task *task);
};

struct xio_tasks_pool_params {
	struct xio_context		*xio_context;
	struct xio_tasks_pool_hooks	pool_hooks;
	char				*pool_name;
	unsigned int			start_nr;
	unsigned int			max_nr;
	unsigned int			alloc_nr;
	int				pool_dd_data_sz;
	int				slab_dd_data_sz;
	int				task_dd_data_sz;
};

struct xio_tasks_slab {
	struct list_head		slabs_list_entry;
	/* pool of tasks */
	struct xio_task			**array;
	uint32_t			start_idx;
	uint32_t			end_idx;
	uint32_t			nr;
	uint32_t			huge_alloc;
	void				*dd_data;
};

struct xio_tasks_pool {
	/* LIFO */
	struct xio_tasks_pool_params	params;
	struct list_head		stack;
	unsigned int			curr_used;
	unsigned int			curr_alloced;
	unsigned int			max_used;
	unsigned int			curr_idx;
	unsigned int			node_id; /* numa node id */
	unsigned int			pad;
	struct list_head		slabs_list;
	struct list_head		on_hold_list;
	struct list_head		orphans_list;
	void				*dd_data;
};

/*---------------------------------------------------------------------------*/
/* xio_task_reset							     */
/*---------------------------------------------------------------------------*/
static inline void xio_task_reset(struct xio_task *task)
{
	/* user responsibility to reset after receive */
	/*
	task->imsg.flags		= 0;
	task->tlv_type			= 0xdead;
	task->omsg_flags		= 0;
	task->state			= XIO_TASK_STATE_INIT;
	xio_mbuf_reset(&task->mbuf);
	*/

	xio_connection_reset_task(task);

	task->sender_task	= NULL;
	task->tlv_type		= 0xdead;
	task->omsg		= NULL;
	task->status		= 0;
	task->context		= NULL;
	task->dd_data		= ((char *)task) +  sizeof(struct xio_task);

	if (task->imsg.user_context)
		task->imsg.user_context	= NULL;

	task->unassign_data_in_buf = NULL;
	task->unassign_user_context = NULL;
}

/*---------------------------------------------------------------------------*/
/* xio_task_addref							     */
/*---------------------------------------------------------------------------*/
static inline void xio_task_addref(
			struct xio_task *t)
{
	kref_get(&t->kref);
}

/*---------------------------------------------------------------------------*/
/* xio_task_reinit							     */
/*---------------------------------------------------------------------------*/
static int xio_task_reinit(void *context, struct xio_task *task)
{
	struct xio_tasks_pool	*pool = (struct xio_tasks_pool *)task->pool;
	struct xio_tasks_slab	*slab = (struct xio_tasks_slab *)task->slab;
	int			retval = -1;
	int			i = task->ltid - slab->start_idx;

	if (pool->params.pool_hooks.slab_init_task && i >= 0)
		retval = pool->params.pool_hooks.slab_init_task(context,
							      pool->dd_data,
							      slab->dd_data,
							      i,
							      task);
	task->context = context;

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_task_release							     */
/*---------------------------------------------------------------------------*/
static inline void xio_task_release(struct kref *kref)
{
	struct xio_task *task = container_of(kref, struct xio_task, kref);
	struct xio_tasks_pool *pool;

	assert(task->pool);

	pool = (struct xio_tasks_pool *)task->pool;

	if (pool->params.pool_hooks.task_pre_put)
		pool->params.pool_hooks.task_pre_put(task->context, task);

	xio_task_reset(task);

	pool->curr_used--;

	list_move(&task->tasks_list_entry, &pool->stack);
}

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_create						     */
/*---------------------------------------------------------------------------*/
struct xio_tasks_pool *xio_tasks_pool_create(
		struct xio_tasks_pool_params *params);

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_destroy						     */
/*---------------------------------------------------------------------------*/
void xio_tasks_pool_destroy(struct xio_tasks_pool *q);

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_remap							     */
/*---------------------------------------------------------------------------*/
void xio_tasks_pool_remap(struct xio_tasks_pool *q, void *new_context);

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_alloc_slab						     */
/*---------------------------------------------------------------------------*/
int xio_tasks_pool_alloc_slab(struct xio_tasks_pool *q, void *context);

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_dump_used						     */
/*---------------------------------------------------------------------------*/
void xio_tasks_pool_dump_used(struct xio_tasks_pool *q);

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_dump_tasks_queues					     */
/*---------------------------------------------------------------------------*/
void xio_tasks_pool_dump_tasks_queues(struct xio_tasks_pool *q);

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_get							     */
/*---------------------------------------------------------------------------*/
static inline struct xio_task *xio_tasks_pool_get(
		struct xio_tasks_pool *q,  void *context)
{
	struct xio_task *t;

	t = list_first_entry_or_null(&q->stack, struct xio_task,
				     tasks_list_entry);

	if (unlikely(!t || list_is_last(&t->tasks_list_entry, &q->stack))) {
		if (q->curr_used == q->params.max_nr - 1)
			goto pool_exhausted;

		xio_tasks_pool_alloc_slab(q, context);
		if (unlikely(list_empty(&q->stack)))
			goto pool_exhausted;
		t = list_last_entry(&q->stack, struct xio_task,
				    tasks_list_entry);
	} else {
		t = list_first_entry(&q->stack, struct xio_task,
				     tasks_list_entry);
	}
	list_del_init(&t->tasks_list_entry);
	q->curr_used++;
	if (q->curr_used > q->max_used)
		q->max_used = q->curr_used;

	kref_init(&t->kref);
	t->tlv_type	= 0xbeef;  /* poison the type */

	xio_task_reinit(context, t);

	if (q->params.pool_hooks.task_post_get)
		q->params.pool_hooks.task_post_get(context, t);

	return t;

pool_exhausted:
	ERROR_LOG("%s - pool exhausted. used:%d max_nr:%d\n",
		  q->params.pool_name,
		  q->curr_used , q->params.max_nr);
	xio_tasks_pool_dump_used(q);
	return NULL;
}

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_put							     */
/*---------------------------------------------------------------------------*/
static inline void xio_tasks_pool_put(struct xio_task *task)
{
      if (unlikely(atomic_read(&task->kref.refcount) == 0))
	       return;

      if (unlikely(task->on_hold &&
		    atomic_read(&task->kref.refcount) == 1))
	       return;

	kref_put(&task->kref, xio_task_release);
}

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_put_on_hold						     */
/*---------------------------------------------------------------------------*/
static inline void xio_tasks_pool_put_on_hold(struct xio_task *task)
{
	if (!task->on_hold || atomic_read(&task->kref.refcount) == 0)
		return;

	if (unlikely(atomic_read(&task->kref.refcount) == 1)) {
		struct xio_tasks_pool *pool;
		assert(task->pool);
		pool = (struct xio_tasks_pool *)task->pool;
		list_move(&task->tasks_list_entry, &pool->on_hold_list);
		return;
	}
	kref_put(&task->kref, xio_task_release);
}

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_free_tasks						     */
/*---------------------------------------------------------------------------*/
static inline int xio_tasks_pool_free_tasks(
			struct xio_tasks_pool *q)
{
	if (!q)
		return 0;

	if (q->curr_used) {
		ERROR_LOG("tasks inventory: %d/%d = missing:%d\n",
			  q->curr_alloced - q->curr_used, q->curr_alloced,
			  q->curr_used);
		xio_tasks_pool_dump_used(q);
	}

	return q->curr_alloced - q->curr_used;
}

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_lookup						     */
/*---------------------------------------------------------------------------*/
static inline struct xio_task *xio_tasks_pool_lookup(
			struct xio_tasks_pool *q,
			unsigned int id)
{
	struct xio_tasks_slab	*slab;
	struct xio_task		*task = NULL;

	list_for_each_entry(slab, &q->slabs_list, slabs_list_entry) {
		if (id >= slab->start_idx && id <= slab->end_idx) {
			task = slab->array[id - slab->start_idx];
			break;
		}
	}
	if (likely(task && task->ltid == id))
		return task;

	return NULL;
}

/*---------------------------------------------------------------------------*/
/* xio_task_swap_mbuf							     */
/*---------------------------------------------------------------------------*/
static inline int xio_task_swap_mbuf(struct xio_task *t1,
				     struct xio_task *t2)
{
	struct xio_mbuf	mbuf;

	if (unlikely(t1->pool != t2->pool))
		return -1;

	mbuf = t1->mbuf;
	t1->mbuf = t2->mbuf;
	t2->mbuf = mbuf;

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_detach_connection					     */
/*---------------------------------------------------------------------------*/
static inline void xio_tasks_pool_detach_connection(
						struct xio_tasks_pool *q,
						struct xio_connection *conn)
{
	struct xio_tasks_slab	*pslab;
	struct xio_task		*task;
	unsigned int i;

	if (unlikely(!q || !conn))
		return;

	list_for_each_entry(pslab, &q->slabs_list, slabs_list_entry) {
		for (i = 0; i < pslab->nr; i++) {
			task = pslab->array[i];
			if (task->connection == conn) {
				if (task->is_assigned) {
					if (task->unassign_data_in_buf)
						task->unassign_data_in_buf(&task->imsg,
								task->unassign_user_context);
					task->is_assigned = 0;
					task->unassign_data_in_buf = NULL;
					task->unassign_user_context = NULL;
					clr_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF,
						 &task->imsg.hints);
				}
				task->connection = NULL;
				task->session = NULL;
				task->nexus = NULL;
				/* finally check if any internal tasks refs left and 
				 * put them back to pool */
				if (IS_XIO_MSG(task->tlv_type) && !task->on_hold) {
					while (atomic_read(&task->kref.refcount) > 0)
						xio_tasks_pool_put(task);

				}
			}
		}
	}
}

/*---------------------------------------------------------------------------*/
/* xio_tasks_list_flush							     */
/*---------------------------------------------------------------------------*/
static inline int xio_tasks_list_flush(struct list_head *list)
{
	while (!list_empty(list)) {
		struct xio_task *ptask = list_entry(list->next,
				struct xio_task, tasks_list_entry);
		/*
		   DEBUG_LOG("flushing task %p type 0x%x\n",
		   ptask, ptask->tlv_type);
		   */
		if (ptask->sender_task && !ptask->on_hold) {
			xio_tasks_pool_put(ptask->sender_task);
			ptask->sender_task = NULL;
		}
		if (ptask->on_hold)
			xio_tasks_pool_put_on_hold(ptask);
		else
			xio_tasks_pool_put(ptask);
	}

	return 0;
}

static inline struct xio_context *xio_task_get_xio_context(struct xio_task *task)
{
	struct xio_tasks_pool	*pool = (struct xio_tasks_pool *)task->pool;

	return pool->params.xio_context;
}

static inline void xio_tasks_pool_add_orphan_task(struct xio_task *task)
{
	struct xio_tasks_pool *pool =
		((struct xio_tasks_pool *)task->pool);

	list_move(&task->tasks_list_entry, &pool->orphans_list);
}

static inline void xio_tasks_pool_free_orphan_task(struct xio_task *task)
{
	if (task->sender_task && !task->on_hold) {
		xio_tasks_pool_put(task->sender_task);
		task->sender_task = NULL;
	}
	xio_tasks_pool_put(task);
}

static inline int xio_tasks_pool_is_orphan_task(struct xio_task *task)
{
	struct xio_task *ptask;
	struct xio_tasks_pool *pool =
		((struct xio_tasks_pool *)task->pool);

	list_for_each_entry(ptask,
			    &pool->orphans_list,
			    tasks_list_entry) {
		if (ptask == task)
			return 1;
	}
	return 0;
}

static inline void xio_tasks_pool_flush_orphan_tasks(struct xio_tasks_pool *pool)
{
	struct xio_task *ptask;
	int cnt = 0;

	list_for_each_entry(ptask,
			    &pool->orphans_list,
			    tasks_list_entry) {
		cnt++;
	}
	if (cnt)
		ERROR_LOG("%s - ctx:%p, flushing %d orphan tasks\n",
			  pool->params.pool_name,
			  pool->params.xio_context, cnt);

	xio_tasks_list_flush(&pool->orphans_list);
}

static inline void xio_tasks_pool_orphan_tasks_clear_unassign(struct xio_tasks_pool *pool)
{
	struct xio_task *ptask;

	list_for_each_entry(ptask,
			    &pool->orphans_list,
			    tasks_list_entry) {
		ptask->unassign_data_in_buf = NULL;
	}
}

static inline int xio_tasks_pool_orphans_or_on_hold_list_empty(struct xio_tasks_pool *pool)
{
	return list_empty(&pool->orphans_list) && list_empty(&pool->on_hold_list);
}

#endif
