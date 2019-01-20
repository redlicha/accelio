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
#include <xio_os.h>
#include "libxio.h"
#include "xio_log.h"
#include "xio_common.h"
#include "xio_protocol.h"
#include "xio_mbuf.h"
#include "xio_task.h"
#include "xio_mem.h"
#include <xio_env_adv.h>
#include "xio_ev_data.h"
#include "xio_ev_loop.h"
#include "xio_objpool.h"
#include "xio_workqueue.h"
#include "xio_observer.h"
#include "xio_context.h"

#define XIO_TASK_MAGIC   0x58494f54 /* Hex of 'XIOT' */

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_alloc_slab						     */
/*---------------------------------------------------------------------------*/
int xio_tasks_pool_alloc_slab(struct xio_tasks_pool *q, void *context)
{
	int			alloc_nr;
	size_t			slab_alloc_sz;
	size_t			tasks_alloc_sz;
	void			*buf;
	void			*data, *ptr;
	struct xio_tasks_slab	*s;
	struct xio_task		*task;
	int			retval = 0, i;
	int			tot_sz;
	int			huge_alloc = 0;
	LIST_HEAD(tmp_list);
	INIT_LIST_HEAD(&tmp_list);

	if ((int)q->params.start_nr < 0  || (int)q->params.max_nr < 0 ||
	    (int)q->params.alloc_nr < 0) {
		xio_set_error(EINVAL);
		return -1;
	}
	if (q->params.start_nr && q->curr_alloced < q->params.start_nr)
		alloc_nr = min(q->params.start_nr, q->params.max_nr);
	else
		alloc_nr = min(q->params.alloc_nr,
			       q->params.max_nr - q->curr_alloced);

	if (alloc_nr == 0)
		return 0;

	/* slab + private data */
	slab_alloc_sz = sizeof(struct xio_tasks_slab) +
			q->params.slab_dd_data_sz +
			alloc_nr * sizeof(struct xio_task *);

	/* slab data */
	tasks_alloc_sz = alloc_nr * (sizeof(struct xio_task) +
			g_options.max_in_iovsz * sizeof(struct xio_iovec_ex) +
			g_options.max_out_iovsz * sizeof(struct xio_iovec_ex) +
			q->params.task_dd_data_sz);

	tot_sz = slab_alloc_sz + tasks_alloc_sz;

	if (tot_sz > 1 << 20) {
		buf = xio_context_umalloc_huge_pages(q->params.xio_context, tot_sz);
		huge_alloc = 1;
	} else {
		buf = xio_context_umemalign(q->params.xio_context, 64, tot_sz);
	}
	if (!buf) {
		xio_set_error(ENOMEM);
		ERROR_LOG("allocation failed\n");
		return -1;
	}
	data = buf;
	ptr = buf;

	/* slab */
	s = (struct xio_tasks_slab *)((char *)buf + tasks_alloc_sz);
	s->dd_data = (void *)((char *)s + sizeof(struct xio_tasks_slab));

	/* array */
	s->array = (struct xio_task **)
			((char *)(s->dd_data) + q->params.slab_dd_data_sz);

	/* fix indexes */
	s->start_idx = q->curr_idx;
	s->end_idx = s->start_idx + alloc_nr - 1;
	q->curr_idx = s->end_idx + 1;
	s->nr = alloc_nr;
	s->huge_alloc = huge_alloc;

	if (q->params.pool_hooks.slab_pre_create) {
		retval = q->params.pool_hooks.slab_pre_create(
				context,
				q->params.xio_context,
				alloc_nr,
				q->dd_data,
				s->dd_data);
		if (retval)
			goto cleanup;
	}

	for (i = 0; i < alloc_nr; i++) {
		s->array[i]	= (struct xio_task *)data;
		task		= s->array[i];
		task->tlv_type	= 0xdead;
		task->ltid	= s->start_idx + i;
		task->magic	= XIO_TASK_MAGIC;
		task->pool	= (void *)q;
		task->slab	= (void *)s;
		task->dd_data	= ((char *)data) +
						sizeof(struct xio_task);

		data = ((char *)data) + sizeof(struct xio_task) +
					q->params.task_dd_data_sz;

		task->imsg.in.sgl_type		= XIO_SGL_TYPE_IOV_PTR;
		task->imsg.in.pdata_iov.sglist	= (struct xio_iovec_ex *)data;
		task->imsg.in.pdata_iov.max_nents = g_options.max_in_iovsz;

		data = ((char *)data) +
			g_options.max_in_iovsz * sizeof(struct xio_iovec_ex);

		task->imsg.out.sgl_type		= XIO_SGL_TYPE_IOV_PTR;
		task->imsg.out.pdata_iov.sglist	= (struct xio_iovec_ex *)data;
		task->imsg.out.pdata_iov.max_nents =
						g_options.max_out_iovsz;

		data = ((char *)data) +
			g_options.max_out_iovsz * sizeof(struct xio_iovec_ex);

		if (q->params.pool_hooks.slab_init_task && context) {
			retval = q->params.pool_hooks.slab_init_task(
				context,
				q->dd_data,
				s->dd_data,
				i,
				task);
			if (retval)
				goto cleanup;
		}
		list_add_tail(&task->tasks_list_entry, &tmp_list);
	}
	q->curr_alloced += alloc_nr;

	list_add_tail(&s->slabs_list_entry, &q->slabs_list);
	list_splice_tail(&tmp_list, &q->stack);

	if (q->params.pool_hooks.slab_post_create && context) {
		retval = q->params.pool_hooks.slab_post_create(
				context,
				q->dd_data,
				s->dd_data);
		if (retval)
			goto cleanup;
	}
	return retval;

cleanup:
	if (huge_alloc)
		xio_context_ufree_huge_pages(q->params.xio_context, ptr);
	else
		xio_context_ufree(q->params.xio_context, ptr);

	return -1;
}
EXPORT_SYMBOL(xio_tasks_pool_alloc_slab);

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_create						     */
/*---------------------------------------------------------------------------*/
struct xio_tasks_pool *xio_tasks_pool_create(
		struct xio_tasks_pool_params *params)
{
	struct xio_tasks_pool	*q;
	char			*buf;
	int			retval;

	/* pool */
	buf = (char *)xio_context_ucalloc(params->xio_context,
					  1, sizeof(*q) + params->pool_dd_data_sz);
	if (!buf) {
		xio_set_error(ENOMEM);
		ERROR_LOG("xio_context_ucalloc failed\n");
		return NULL;
	}
	q		= (struct xio_tasks_pool *)buf;
	if (params->pool_dd_data_sz)
		q->dd_data = (void *)(q + 1);
	else
		q->dd_data = NULL;

	INIT_LIST_HEAD(&q->stack);
	INIT_LIST_HEAD(&q->slabs_list);
	INIT_LIST_HEAD(&q->on_hold_list);
	INIT_LIST_HEAD(&q->orphans_list);

	memcpy(&q->params, params, sizeof(*params));

	if (q->params.pool_hooks.pool_pre_create) {
		retval = q->params.pool_hooks.pool_pre_create(
				q->params.pool_hooks.context, q, q->dd_data);
		if (unlikely(retval)) {
			xio_context_ufree(q->params.xio_context, q);
			return NULL;
		}
	}

	if (q->params.start_nr) {
		xio_tasks_pool_alloc_slab(q, q->params.pool_hooks.context);
		if (list_empty(&q->stack)) {
			xio_context_ufree(q->params.xio_context, q);
			return NULL;
		}
	}
	if (q->params.pool_hooks.pool_post_create) {
		retval = q->params.pool_hooks.pool_post_create(
				q->params.pool_hooks.context, q, q->dd_data);

		if (unlikely(retval)) {
			xio_context_ufree(q->params.xio_context, q);
			return NULL;
		}
	}
	return q;
}
EXPORT_SYMBOL(xio_tasks_pool_create);

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_destroy						     */
/*---------------------------------------------------------------------------*/
void xio_tasks_pool_destroy(struct xio_tasks_pool *q)
{
	struct xio_tasks_slab	*pslab, *next_pslab;
	unsigned int		i;


	if (!list_empty(&q->on_hold_list) ||
	    !list_empty(&q->orphans_list))
		xio_tasks_pool_dump_tasks_queues(q);

	xio_tasks_pool_flush_orphan_tasks(q);

	list_for_each_entry_safe(pslab, next_pslab, &q->slabs_list,
				 slabs_list_entry) {
		list_del(&pslab->slabs_list_entry);

		if (q->params.pool_hooks.slab_uninit_task) {
			for (i = 0; i < pslab->nr; i++)
				q->params.pool_hooks.slab_uninit_task(
						pslab->array[i]->context,
						q->dd_data,
						pslab->dd_data,
						pslab->array[i]);
		}

		if (q->params.pool_hooks.slab_destroy)
			q->params.pool_hooks.slab_destroy(
				q->params.pool_hooks.context,
				q->dd_data,
				pslab->dd_data);

		/* the tmp tasks are returned back to pool */

		if (pslab->huge_alloc)
			xio_context_ufree_huge_pages(q->params.xio_context,
						     pslab->array[0]);
		else
			xio_context_ufree(q->params.xio_context, pslab->array[0]);
	}

	if (q->params.pool_hooks.pool_destroy)
		q->params.pool_hooks.pool_destroy(
				q->params.pool_hooks.context,
				q, q->dd_data);

	kfree(q->params.pool_name);

	xio_context_ufree(q->params.xio_context, q);
}
EXPORT_SYMBOL(xio_tasks_pool_destroy);

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_remap							     */
/*---------------------------------------------------------------------------*/
void xio_tasks_pool_remap(struct xio_tasks_pool *q, void *new_context)
{
	struct xio_tasks_slab	*pslab, *next_pslab;
	unsigned int		i;

	if (!q)
		return;

	list_for_each_entry_safe(pslab, next_pslab, &q->slabs_list,
				 slabs_list_entry) {
		if (q->params.pool_hooks.slab_post_create)
			q->params.pool_hooks.slab_post_create(
					new_context,
					q->dd_data,
					pslab->dd_data);

		if (q->params.pool_hooks.slab_remap_task) {
			for (i = 0; i < pslab->nr; i++)
				q->params.pool_hooks.slab_remap_task(
						q->params.pool_hooks.context,
						new_context,
						q->dd_data,
						pslab->dd_data,
						pslab->array[i]);
		}
	}
	q->params.pool_hooks.context = new_context;
}

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_dump_used						     */
/*---------------------------------------------------------------------------*/
void xio_tasks_pool_dump_used(struct xio_tasks_pool *q)
{
	struct xio_tasks_slab	*pslab;
	unsigned int		i;
	char			*pool_name;

	list_for_each_entry(pslab, &q->slabs_list, slabs_list_entry) {
		for (i = 0; i < pslab->nr; i++)
			if (pslab->array[i]->tlv_type != 0xdead ||
			    pslab->array[i]->tlv_type != 0xbeef) {
				pool_name = q->params.pool_name ?
					q->params.pool_name : "unknown";
				ERROR_LOG("pool_name:%s: [%d] - task:%p, " \
					  "type:0x%x, on_hold:%d\n",
					  pool_name,
					  i,
					  pslab->array[i],
					  pslab->array[i]->tlv_type,
					  pslab->array[i]->on_hold);
			}
	}
}

/*---------------------------------------------------------------------------*/
/* xio_tasks_pool_dump_tasks_queues					     */
/*---------------------------------------------------------------------------*/
void xio_tasks_pool_dump_tasks_queues(struct xio_tasks_pool *q)
{
	DEBUG_LOG("#################################################################\n");
	if (!list_empty(&q->on_hold_list)) {
		xio_dump_task_list("tasks_pool", q,
				   &q->on_hold_list,
				   "on_hold_list");
	}
	if (!list_empty(&q->orphans_list)) {
		xio_dump_task_list("tasks_pool", q,
				   &q->orphans_list,
				   "orphans_list");
	}
	DEBUG_LOG("#################################################################\n");
}
