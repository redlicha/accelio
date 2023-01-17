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
#include <infiniband/verbs.h>
#include <rdma/rdma_cma.h>

#include "libxio.h"
#include "xio_log.h"
#include "xio_common.h"
#include "xio_observer.h"
#include "xio_protocol.h"
#include "xio_mbuf.h"
#include "xio_task.h"
#include "xio_usr_transport.h"
#include "xio_transport.h"
#include "xio_protocol.h"
#include "get_clock.h"
#include "xio_mem.h"
#include "xio_rdma_utils.h"
#include "xio_ev_data.h"
#include "xio_sg_table.h"
#include "xio_objpool.h"
#include "xio_workqueue.h"
#include "xio_context.h"
#include "xio_rdma_transport.h"

/*---------------------------------------------------------------------------*/
/* forward declarations							     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_on_recv_req(struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task);
static int xio_rdma_on_recv_rsp(struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task);
static int xio_rdma_on_setup_msg(struct xio_rdma_transport *rdma_hndl,
				 struct xio_task *task);
static int xio_rdma_on_rsp_send_comp(struct xio_rdma_transport *rdma_hndl,
				     struct xio_task *task);
static int xio_rdma_on_req_send_comp(struct xio_rdma_transport *rdma_hndl,
				     struct xio_task *task);
static int xio_rdma_on_direct_rdma_comp(struct xio_rdma_transport *rdma_hndl,
					struct xio_task *task,
					enum xio_wc_op op);
static int xio_rdma_on_recv_nop(struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task);
#ifndef XIO_SRQ_ENABLE
static int xio_rdma_send_nop(struct xio_rdma_transport *rdma_hndl);
#endif
static int xio_sched_rdma_wr_req(struct xio_rdma_transport *rdma_hndl,
				 struct xio_task *task);
static void xio_sched_consume_cq(void *data);
static void xio_sched_poll_cq(void *data);

static int xio_rdma_send_rdma_read_ack(struct xio_rdma_transport *rdma_hndl,
				       int rtid);
static int xio_rdma_on_recv_rdma_read_ack(struct xio_rdma_transport *rdma_hndl,
					  struct xio_task *task);
static int xio_sched_rdma_rd(struct xio_rdma_transport *rdma_hndl,
			     struct xio_task *task);
static int xio_rdma_post_recv_rsp(struct xio_task *task);
static void xio_free_rdma_rd_mem(struct xio_rdma_transport *rdma_hndl,
				 struct xio_task *task);

/*---------------------------------------------------------------------------*/
/* xio_post_recv							     */
/*---------------------------------------------------------------------------*/
int xio_post_recv(struct xio_rdma_transport *rdma_hndl,
		  struct xio_task *task, int num_recv_bufs)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct ibv_recv_wr	*bad_wr	= NULL, *wr;
	int			retval, nr_posted;

#ifdef XIO_SRQ_ENABLE
	retval = ibv_post_srq_recv(rdma_hndl->tcq->srq->srq,
			&rdma_task->rxd.recv_wr, &bad_wr);
#else
	retval = ibv_post_recv(rdma_hndl->qp, &rdma_task->rxd.recv_wr, &bad_wr);
#endif
	if (likely(!retval)) {
		nr_posted = num_recv_bufs;
		for (wr = &rdma_task->rxd.recv_wr; wr != NULL; wr = wr->next) {
			struct xio_task *task =
				(struct xio_task *)ptr_from_int64(wr->wr_id);
			xio_task_addref(task);
		}
	} else {
		nr_posted = 0;
		for (wr = &rdma_task->rxd.recv_wr; wr != bad_wr; wr = wr->next) {
			struct xio_task *task =
				(struct xio_task *)ptr_from_int64(wr->wr_id);
			xio_task_addref(task);
			nr_posted++;
		}

		xio_set_error(retval);
		ERROR_LOG("ibv_post_recv failed. (errno=%d %s)\n",
			  retval, strerror(retval));
	}
#ifdef XIO_SRQ_ENABLE
	rdma_hndl->tcq->srq->rqe_avail += nr_posted;
#else
	rdma_hndl->rqe_avail += nr_posted;
#endif

	/* credit updates */
	rdma_hndl->credits += nr_posted;

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_post_send                                                           */
/*---------------------------------------------------------------------------*/
static int xio_post_send(struct xio_rdma_transport *rdma_hndl,
			 struct xio_work_req *xio_send,
			 int num_send_reqs)
{
	struct ibv_send_wr	*bad_wr, *wr;
	int			retval, nr_posted;
	struct xio_task		*task = (struct xio_task *)
				ptr_from_int64(xio_send->send_wr.wr_id);


	/* send it */
	if (IS_KEEPALIVE(task->tlv_type)) {
		if (task->ka_probes)
			DEBUG_LOG("%s - ka on wire: tlv_type:0x%x, session:%p, connection:%p, rdma_hndl:%p\n",
				  __func__, task->tlv_type, task->session, task->connection, rdma_hndl);
	} else {
		if (!IS_NOP(task->tlv_type) && !IS_APPLICATION_MSG(task->tlv_type))
			DEBUG_LOG("%s - control on wire: tlv_type:0x%x, session:%p, connection:%p, rdma_hndl:%p\n",
				  __func__, task->tlv_type, task->session, task->connection, rdma_hndl);
	}

	/*
	TRACE_LOG("num_sge:%d, len1:%d, len2:%d, send_flags:%d\n",
		  xio_send->send_wr.num_sge,
		  xio_send->send_wr.sg_list[0].length,
		  xio_send->send_wr.sg_list[1].length,
		  xio_send->send_wr.send_flags);
	*/
	retval = ibv_post_send(rdma_hndl->qp, &xio_send->send_wr, &bad_wr);
	if (likely(!retval)) {
		for (wr = &xio_send->send_wr; wr != NULL; wr = wr->next) {
			if (wr->send_flags & IBV_SEND_SIGNALED) {
				struct xio_task *task =
					(struct xio_task *)ptr_from_int64(wr->wr_id);
				xio_task_addref(task);
			}
		}
		nr_posted = num_send_reqs;
	} else {
		nr_posted = 0;
		for (wr = &xio_send->send_wr; wr != bad_wr; wr = wr->next) {
			if (wr->send_flags & IBV_SEND_SIGNALED) {
				struct xio_task *task =
					(struct xio_task *)ptr_from_int64(wr->wr_id);
				xio_task_addref(task);
			}
			nr_posted++;
		}
		xio_set_error(retval);

		ERROR_LOG("ibv_post_send failed. (errno=%d %s)  posted:%d/%d " \
			  "sge_sz:%d, sqe_avail:%d\n", retval, strerror(retval),
			  nr_posted, num_send_reqs, xio_send->send_wr.num_sge,
			  rdma_hndl->sqe_avail);
	}
	rdma_hndl->sqe_avail -= nr_posted;

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_write_sn							     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_write_sn(struct xio_task *task,
			     uint16_t sn, uint16_t ack_sn, uint16_t credits)
{
	uint16_t		*psn;
	struct xio_mbuf		*mbuf = &task->mbuf;

	/* save the current place */
	xio_mbuf_push(mbuf);
	/* goto the first transport header*/
	xio_mbuf_set_trans_hdr(mbuf);

	/* jump over the first uint32_t */
	xio_mbuf_inc(mbuf, sizeof(uint32_t));

	/* and set serial number */
	psn = (uint16_t *)xio_mbuf_get_curr_ptr(mbuf);
	*psn = htons(sn);

	xio_mbuf_inc(mbuf, sizeof(uint16_t));

	/* and set ack serial number */
	psn = (uint16_t *)xio_mbuf_get_curr_ptr(mbuf);
	*psn = htons(ack_sn);

	xio_mbuf_inc(mbuf, sizeof(uint16_t));

	/* and set credits */
	psn = (uint16_t *)xio_mbuf_get_curr_ptr(mbuf);
	*psn = htons(credits);

	/* pop to the original place */
	xio_mbuf_pop(mbuf);

	return 0;
}

static inline uint16_t tx_window_sz(struct xio_rdma_transport *rdma_hndl)
{
	return rdma_hndl->max_sn - rdma_hndl->sn;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_xmit							     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_xmit(struct xio_rdma_transport *rdma_hndl)
{
	struct xio_task		*task = NULL, *task1, *task2;
	struct xio_rdma_task	*rdma_task = NULL;
	struct xio_rdma_task	*prev_rdma_task = NULL;
	struct xio_work_req	*first_wr = NULL;
	struct xio_work_req	*curr_wr = NULL;
	struct xio_work_req	*last_wr = NULL;
	struct xio_work_req	*prev_wr = &rdma_hndl->dummy_wr;
	uint16_t		tx_window;
	uint16_t		window = 0;
	uint16_t		retval;
	uint16_t		req_nr = 0;


	if (rdma_hndl->state != XIO_TRANSPORT_STATE_CONNECTED)
		return 0;

	tx_window = tx_window_sz(rdma_hndl);
#ifdef XIO_SRQ_ENABLE
	window = min(rdma_hndl->sqe_avail, tx_window);
#else
	/* save one credit for nop */
	if (rdma_hndl->peer_credits > 1) {
		window = min(rdma_hndl->peer_credits - 1, tx_window);
		window = min(window, rdma_hndl->sqe_avail);
	}
#endif
	/*
	TRACE_LOG("XMIT: tx_window:%d, peer_credits:%d, sqe_avail:%d\n",
		  tx_window,
		  rdma_hndl->peer_credits,
		  rdma_hndl->sqe_avail);
	*/
	if (window == 0) {
		xio_set_error(EAGAIN);
		return -1;
	}

	/* if "ready to send queue" is not empty */
	while (rdma_hndl->tx_ready_tasks_num) {
		task = list_first_entry(
				&rdma_hndl->tx_ready_list,
				struct xio_task,  tasks_list_entry);

		rdma_task = (struct xio_rdma_task *)task->dd_data;

		/* prefetch next buffer */
		if (rdma_hndl->tx_ready_tasks_num > 2) {
			task1 = list_first_entry_or_null(
					&task->tasks_list_entry,
					struct xio_task,  tasks_list_entry);
			if (task1) {
				xio_prefetch(task1->mbuf.buf.head);
				task2 = list_first_entry_or_null(
						&task1->tasks_list_entry,
						struct xio_task,
						tasks_list_entry);
				if (task2)
					xio_prefetch(task2->mbuf.buf.head);
			}
		}

		/* phantom task */
		if (rdma_task->phantom_idx) {
			if (req_nr >= window)
				break;
			curr_wr = &rdma_task->rdmad;

			prev_wr->send_wr.next = &curr_wr->send_wr;

			prev_rdma_task	= rdma_task;
			prev_wr		= curr_wr;
			req_nr++;
			rdma_hndl->tx_ready_tasks_num--;

			rdma_task->txd.send_wr.send_flags &= ~IBV_SEND_SIGNALED;

			list_move_tail(&task->tasks_list_entry,
				       &rdma_hndl->in_flight_list);
			continue;
		}
		if (rdma_task->out_ib_op == XIO_IB_RDMA_WRITE) {
			if (req_nr >= (window - 1))
				break;

			/* prepare it for rdma wr and concatenate the send
			 * wr to it */
			rdma_task->rdmad.send_wr.next = &rdma_task->txd.send_wr;
			rdma_task->txd.send_wr.send_flags |= IBV_SEND_SIGNALED;

			curr_wr = &rdma_task->rdmad;
			last_wr = &rdma_task->txd;

			req_nr++;
		} else if (rdma_task->out_ib_op == XIO_IB_RDMA_WRITE_DIRECT ||
			   rdma_task->out_ib_op == XIO_IB_RDMA_READ_DIRECT) {
			if (req_nr >= window)
				break;
			rdma_task->rdmad.send_wr.send_flags |=
							IBV_SEND_SIGNALED;
			curr_wr = &rdma_task->rdmad;
			last_wr = curr_wr;
		} else {
			if (req_nr >= window)
				break;
			curr_wr = &rdma_task->txd;
			last_wr = curr_wr;
		}
		if (rdma_task->out_ib_op != XIO_IB_RDMA_WRITE_DIRECT &&
		    rdma_task->out_ib_op != XIO_IB_RDMA_READ_DIRECT) {
			xio_rdma_write_sn(task, rdma_hndl->sn,
					  rdma_hndl->ack_sn,
					  rdma_hndl->credits);
			rdma_task->sn = rdma_hndl->sn;
			rdma_hndl->sn++;
			rdma_hndl->sim_peer_credits += rdma_hndl->credits;
			rdma_hndl->credits = 0;
			rdma_hndl->peer_credits--;
		}
		if (IS_REQUEST(task->tlv_type) ||
		    task->tlv_type == XIO_MSG_TYPE_RDMA)
			rdma_hndl->reqs_in_flight_nr++;
		else if (IS_RESPONSE(task->tlv_type))
			rdma_hndl->rsps_in_flight_nr++;
		else
			ERROR_LOG("Unexpected tlv_type %u, rdma_hndl:%p\n",
				  task->tlv_type, rdma_hndl);

		prev_wr->send_wr.next = &curr_wr->send_wr;
		prev_wr = last_wr;

		prev_rdma_task = rdma_task;
		req_nr++;
		rdma_hndl->tx_ready_tasks_num--;
		list_move_tail(&task->tasks_list_entry,
			       &rdma_hndl->in_flight_list);
	}
	if (req_nr) {
		first_wr = container_of(rdma_hndl->dummy_wr.send_wr.next,
					struct xio_work_req, send_wr);
		prev_rdma_task->txd.send_wr.next = NULL;
		if (tx_window_sz(rdma_hndl) < 1 ||
		    rdma_hndl->sqe_avail < req_nr + 1)
			prev_rdma_task->txd.send_wr.send_flags |=
				IBV_SEND_SIGNALED;
		retval = xio_post_send(rdma_hndl, first_wr, req_nr);
		if (unlikely(retval != 0)) {
			ERROR_LOG("xio_post_send failed\n");
			return -1;
		}
	}

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_xmit_rdma_rd							     */
/*---------------------------------------------------------------------------*/
static int xio_xmit_rdma_rd_(struct xio_rdma_transport *rdma_hndl,
			     struct list_head *rdma_rd_list,
			     struct list_head *rdma_rd_in_flight_list,
			     int *rdma_rd_in_flight,
			     int *kick_rdma_rd)
{
	struct xio_task		*task = NULL;
	struct xio_rdma_task	*rdma_task = NULL;
	struct xio_work_req	*first_wr = NULL;
	struct xio_work_req	*prev_wr = &rdma_hndl->dummy_wr;
	struct xio_work_req	*curr_wr = NULL;
	int num_reqs = 0;
	int err;

	if (rdma_hndl->state != XIO_TRANSPORT_STATE_CONNECTED)
		return 0;

	if (list_empty(rdma_rd_list) ||
	    rdma_hndl->sqe_avail == 0)
		goto exit;

	do {
		task = list_first_entry(
				rdma_rd_list,
				struct xio_task,  tasks_list_entry);
		list_move_tail(&task->tasks_list_entry,
			       rdma_rd_in_flight_list);
		rdma_task = (struct xio_rdma_task *)task->dd_data;

		/* pending "sends" that were delayed for rdma read completion
		 *  are moved to wait in the in_flight list
		 *   because of the need to keep order
		 */
		if (rdma_task->out_ib_op == XIO_IB_RECV) {
			(*rdma_rd_in_flight)++;
			continue;
		}

		/* prepare it for rdma read */
		curr_wr = &rdma_task->rdmad;
		prev_wr->send_wr.next = &curr_wr->send_wr;
		prev_wr = &rdma_task->rdmad;

		num_reqs++;
	} while (!list_empty(rdma_rd_list) &&
		 rdma_hndl->sqe_avail > num_reqs);

	if (num_reqs) {
		first_wr = container_of(rdma_hndl->dummy_wr.send_wr.next,
					struct xio_work_req, send_wr);
		prev_wr->send_wr.next = NULL;
		(*rdma_rd_in_flight) += num_reqs;
		/* submit the chain of rdma-rd requests, start from the first */
		err = xio_post_send(rdma_hndl, first_wr, num_reqs);
		if (unlikely(err))
			ERROR_LOG("xio_post_send failed\n");

		/* ToDo: error handling */
	}

exit:
	*kick_rdma_rd = !list_empty(rdma_rd_list);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_xmit_rdma_rd_req							     */
/*---------------------------------------------------------------------------*/
static inline int xio_xmit_rdma_rd_req(struct xio_rdma_transport *rdma_hndl)
{
	return xio_xmit_rdma_rd_(rdma_hndl,
				 &rdma_hndl->rdma_rd_req_list,
				 &rdma_hndl->rdma_rd_req_in_flight_list,
				 &rdma_hndl->rdma_rd_req_in_flight,
				 &rdma_hndl->kick_rdma_rd_req);
}

/*---------------------------------------------------------------------------*/
/* xio_xmit_rdma_rd_rsp							     */
/*---------------------------------------------------------------------------*/
static inline int xio_xmit_rdma_rd_rsp(struct xio_rdma_transport *rdma_hndl)
{
	return xio_xmit_rdma_rd_(rdma_hndl,
				  &rdma_hndl->rdma_rd_rsp_list,
				  &rdma_hndl->rdma_rd_rsp_in_flight_list,
				  &rdma_hndl->rdma_rd_rsp_in_flight,
				  &rdma_hndl->kick_rdma_rd_rsp);
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_rearm_rq							     */
/*---------------------------------------------------------------------------*/
int xio_rdma_rearm_rq(struct xio_rdma_transport *rdma_hndl)
{
	struct xio_task		*first_task = NULL;
	struct xio_task		*task = NULL;
	struct xio_task		*prev_task = NULL;
	struct xio_rdma_task	*rdma_task = NULL;
	struct xio_rdma_task	*prev_rdma_task = NULL;
	int			num_to_post;
	int			i;

#ifdef XIO_SRQ_ENABLE
	num_to_post = SRQ_DEPTH - rdma_hndl->tcq->srq->rqe_avail;
#else
	num_to_post = rdma_hndl->rq_depth + EXTRA_RQE - rdma_hndl->rqe_avail;
#endif
	for (i = 0; i < num_to_post; i++) {
		/* get ready to receive message */
		task = xio_rdma_primary_task_alloc(rdma_hndl);
		if (task == 0) {
			if (rdma_hndl->primary_pool_cls.pool)
				ERROR_LOG("primary tasks pool is empty\n");
			return -1;
		}
		/* initialize the rxd */
		rdma_task = (struct xio_rdma_task *)task->dd_data;
		if (!first_task)
			first_task = task;
		else
			prev_rdma_task->rxd.recv_wr.next =
						&rdma_task->rxd.recv_wr;

		prev_task = task;
		prev_rdma_task = rdma_task;
		rdma_task->out_ib_op = XIO_IB_RECV;
#ifdef XIO_SRQ_ENABLE
		list_add_tail(&task->tasks_list_entry,
				&rdma_hndl->tcq->srq->rx_list);
#else
		list_add_tail(&task->tasks_list_entry, &rdma_hndl->rx_list);
#endif
	}
	if (prev_task) {
		prev_rdma_task->rxd.recv_wr.next = NULL;
		xio_post_recv(rdma_hndl, first_task, num_to_post);
	}

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_rx_error_handler						     */
/*---------------------------------------------------------------------------*/
static inline int xio_rdma_rx_error_handler(
				struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task)
{
	/* remove the task from rx list */
	xio_tasks_pool_put(task);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_tx_error_handler						     */
/*---------------------------------------------------------------------------*/
static inline int xio_rdma_tx_error_handler(
				struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task)
{
	/* remove the task from in-flight list */
	xio_tasks_pool_put(task);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_rd_error_handler						     */
/*---------------------------------------------------------------------------*/
static inline int xio_rdma_rd_error_handler(
				struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task)
{
	/* remove the task from rdma rd in-flight list */
	xio_tasks_pool_put(task);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_wr_error_handler						     */
/*---------------------------------------------------------------------------*/
static inline int xio_rdma_wr_error_handler(
				struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);

	if (rdma_task->out_ib_op == XIO_IB_RDMA_WRITE_DIRECT)
		return 0;

	/* wait for the concatenated "send" */
	rdma_task->out_ib_op = XIO_IB_SEND;

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_handle_task_error						     */
/*---------------------------------------------------------------------------*/
static void xio_handle_task_error(struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	XIO_TO_RDMA_HNDL(task, rdma_hndl);

	switch (rdma_task->out_ib_op) {
	case XIO_IB_RECV:
		/* this should be the Flush, no task has been created yet */
		xio_rdma_rx_error_handler(rdma_hndl, task);
		break;
	case XIO_IB_SEND:
		/* the task should be completed now */
		xio_rdma_tx_error_handler(rdma_hndl, task);
		break;
	case XIO_IB_RDMA_READ:
	case XIO_IB_RDMA_READ_DIRECT:
		xio_rdma_rd_error_handler(rdma_hndl, task);
		break;
	case XIO_IB_RDMA_WRITE:
	case XIO_IB_RDMA_WRITE_DIRECT:
		xio_rdma_wr_error_handler(rdma_hndl, task);
		break;
	default:
		ERROR_LOG("unknown out_ib_op: rdma_hndl:%p, task:%p, type:0x%x, " \
			  "magic:0x%x, out_ib_op:0x%x\n",
			  rdma_hndl, task, task->tlv_type,
			  task->magic, rdma_task->out_ib_op);
		break;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_handle_wc_error                                                       */
/*---------------------------------------------------------------------------*/
static void xio_handle_wc_error(struct ibv_wc *wc, struct xio_srq *srq)
{
	struct xio_task *task = (struct xio_task *)ptr_from_int64(wc->wr_id);
	struct xio_rdma_task		*rdma_task = NULL;
	struct xio_rdma_transport       *rdma_hndl = NULL;
	int				retval;
	struct xio_key_int32		key;

	/* complete in case all flush errors were consumed */
	if (task && task->dd_data == ptr_from_int64(XIO_BEACON_WRID)) {
		rdma_hndl = container_of(task,
					 struct xio_rdma_transport,
					 beacon_task);

		rdma_hndl->beacon_sent = 0;
		TRACE_LOG("beacon rdma_hndl:%p\n", rdma_hndl);
		xio_set_timewait_timer(rdma_hndl);
		kref_put(&rdma_hndl->base.kref, xio_rdma_close_cb);
		return;
	}
	if (task && task->dd_data) {
		rdma_task = (struct xio_rdma_task *)task->dd_data;
		if (srq) {
			key.id = wc->qp_num;
			HT_LOOKUP(&srq->ht_rdma_hndl, &key, rdma_hndl,
					rdma_hndl_htbl);
		} else {
			rdma_hndl = (struct xio_rdma_transport *)task->context;
		}
	}
	if (wc->status == IBV_WC_WR_FLUSH_ERR) {
		TRACE_LOG("rdma_hndl:%p, rdma_task:%p, task:%p, " \
			  "wr_id:0x%llx, " \
			  "err:%s, vendor_err:0x%x\n",
			   rdma_hndl, rdma_task, task,
			   wc->wr_id,
			   ibv_wc_status_str(wc->status),
			   wc->vendor_err);
	} else {
		if (rdma_hndl)
			ERROR_LOG("[%s] - state:%d, rdma_hndl:%p, " \
				  "rdma_task:%p, task:%p, wr_id:0x%lx, " \
				  "err:%s, vendor_err:0x%x, " \
				  "byte_len:%d, opcode:0x%x\n",
				  rdma_hndl->base.is_client ?
				  "client" : "server",
				  rdma_hndl->state,
				  rdma_hndl, rdma_task, task,
				  wc->wr_id,
				  ibv_wc_status_str(wc->status),
				  wc->vendor_err,
				  wc->byte_len,
				  wc->opcode);
		else
			ERROR_LOG("[unknown] - state:-1, rdma_hndl:%p, " \
				  "rdma_task:%p, task:%p, wr_id:0x%lx, " \
				  "err:%s, vendor_err:0x%x, " \
				  "byte_len:%d, opcode:0x%x\n",
				  rdma_hndl, rdma_task, task,
				  wc->wr_id,
				  ibv_wc_status_str(wc->status),
				  wc->vendor_err,
				  wc->byte_len,
				  wc->opcode);
		/*
		if (task->omsg)
			xio_msg_dump(task->omsg);
		*/
		ERROR_LOG("qp_num:0x%x, src_qp:0x%x, wc_flags:0x%x, " \
			  "pkey_index:%d, slid:%d, sl:0x%x, dlid_path_bits:0x%x\n",
			   wc->qp_num, wc->src_qp, wc->wc_flags, wc->pkey_index,
			   wc->slid, wc->sl, wc->dlid_path_bits);
	}
	if (!task) {
		ERROR_LOG("got error with null task\n");
		return;
	}
	if (!rdma_hndl) {
		ERROR_LOG("got task with null rdma_hndl. task:%p\n", task);
		goto cleanup;
	}
	if (task && rdma_task)
		xio_handle_task_error(task);

	/* temporary  */
	if (wc->status != IBV_WC_WR_FLUSH_ERR ||
	    rdma_hndl->state == XIO_TRANSPORT_STATE_CONNECTED) {
		if (!rdma_hndl->rdma_disconnect_called) {
			rdma_hndl->disconnect_nr = 0;
			rdma_hndl->ignore_timewait = 1;
			xio_ctx_del_delayed_work(
					rdma_hndl->base.ctx,
					&rdma_hndl->disconnect_timeout_work);
			xio_set_disconnect_timer(rdma_hndl);
			if (wc->status ==  IBV_WC_RETRY_EXC_ERR)
				rdma_hndl->trans_retry_counter_exceeded = 1;

			if (!rdma_hndl->trans_retry_counter_exceeded) {
				ERROR_LOG("cq error reported. calling " \
						"rdma_disconnect. rdma_hndl:%p, status:%d\n",
						rdma_hndl, wc->status);
				rdma_hndl->rdma_disconnect_called = 1;
				rdma_hndl->state = XIO_TRANSPORT_STATE_DISCONNECTED;
				retval = rdma_disconnect(rdma_hndl->cm_id);
				if (retval)
					ERROR_LOG("rdma_hndl:%p rdma_disconnect" \
							"failed, %m\n", rdma_hndl);
			}
		} else {
			ERROR_LOG("cq error reported. not calling " \
					"rdma_disconnect. rdma_hndl:%p\n",
					rdma_hndl);
		}
	}
cleanup:
	/* task ref add before ibv_post_send/recv */
	xio_tasks_pool_put(task);
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_idle_handler						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_idle_handler(struct xio_rdma_transport *rdma_hndl)
{
	if (rdma_hndl->state != XIO_TRANSPORT_STATE_CONNECTED ||
	    !rdma_hndl->primary_pool_cls.task_lookup)
		return 0;

	/* Does the local have resources to send message?  */
	if (!rdma_hndl->sqe_avail)
		return 0;

	/* Try to do some useful work, want to spend time before calling the
	 * pool, this increase the chance that more messages will arrive
	 * and request notify will not be necessary
	 */

	if (rdma_hndl->kick_rdma_rd_req)
		xio_xmit_rdma_rd_req(rdma_hndl);

	if (rdma_hndl->kick_rdma_rd_rsp)
		xio_xmit_rdma_rd_rsp(rdma_hndl);

	/* Does the local have resources to send message?
	 * xio_xmit_rdma_rd may consumed the sqe_avail
	 */
	if (!rdma_hndl->sqe_avail)
		return 0;

#ifndef XIO_SRQ_ENABLE
	/* Can the peer receive messages? */
	if (!rdma_hndl->peer_credits)
		return 0;
#endif

	/* If we have real messages to send there is no need for
	 * a special NOP message as credits are piggybacked
	 */
	if (rdma_hndl->tx_ready_tasks_num &&
	    rdma_hndl->peer_credits > 1) {
		xio_rdma_xmit(rdma_hndl);
		return 0;
	}

#ifndef XIO_SRQ_ENABLE
	/* Does the peer have already maximum credits? */
	if (rdma_hndl->sim_peer_credits >= MAX_RECV_WR)
		return 0;

	/* Does the local have any credits to send? */
	if (!rdma_hndl->credits)
		return 0;

	TRACE_LOG("peer_credits:%d, credits:%d sim_peer_credits:%d\n",
		  rdma_hndl->peer_credits, rdma_hndl->credits,
		  rdma_hndl->sim_peer_credits);

	xio_rdma_send_nop(rdma_hndl);
#endif
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_rx_handler							     */
/*---------------------------------------------------------------------------*/
static XIO_F_ALWAYS_INLINE int xio_rdma_rx_handler(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task)
{
	struct list_head	*task_prev;
	struct xio_task		*task1, *task2;
	int			must_send = 0;
	int			retval;

	/* prefetch next buffer */
	if (likely(task->tasks_list_entry.next !=
		   task->tasks_list_entry.prev)) {
		task1 = list_entry(task->tasks_list_entry.next,
				   struct xio_task,  tasks_list_entry);
		task_prev = task->tasks_list_entry.prev;
		xio_prefetch(task1->mbuf.buf.head);
	} else {
		task1 = NULL;
		task_prev = NULL;
	}

	/* rearm the receive queue  */
	/*
	if ((rdma_hndl->state == XIO_TRANSPORT_STATE_CONNECTED) &&
	    (rdma_hndl->rqe_avail <= rdma_hndl->rq_depth + 1))
		xio_rdma_rearm_rq(rdma_hndl);
	*/

	retval = xio_mbuf_read_first_tlv(&task->mbuf);

	task->tlv_type = xio_mbuf_tlv_type(&task->mbuf);
	list_move_tail(&task->tasks_list_entry, &rdma_hndl->io_list);
#ifdef XIO_SRQ_ENABLE
	rdma_hndl->tcq->srq->rqe_avail--;
#else
	rdma_hndl->rqe_avail--;
	rdma_hndl->sim_peer_credits--;
#endif
	/* call recv completion  */
	switch (task->tlv_type) {
	case XIO_CREDIT_NOP:
		xio_rdma_on_recv_nop(rdma_hndl, task);
#ifdef XIO_SRQ_ENABLE
		if (rdma_hndl->tcq->srq->rqe_avail <= SRQ_DEPTH + 1)
#else
		if (rdma_hndl->rqe_avail <= rdma_hndl->rq_depth + 1)
#endif
			xio_rdma_rearm_rq(rdma_hndl);
		must_send = 1;
		break;
	case XIO_RDMA_READ_ACK:
		xio_rdma_on_recv_rdma_read_ack(rdma_hndl, task);
#ifdef XIO_SRQ_ENABLE
		if (rdma_hndl->tcq->srq->rqe_avail <= SRQ_DEPTH + 1)
#else
		if (rdma_hndl->rqe_avail <= rdma_hndl->rq_depth + 1)
#endif
			xio_rdma_rearm_rq(rdma_hndl);
		must_send = 1;
		break;
	case XIO_NEXUS_SETUP_REQ:
	case XIO_NEXUS_SETUP_RSP:
		xio_rdma_on_setup_msg(rdma_hndl, task);
		break;
	default:
		/* rearm the receive queue  */
#ifdef XIO_SRQ_ENABLE
		if (rdma_hndl->tcq->srq->rqe_avail <= SRQ_DEPTH + 1)
#else
		if (rdma_hndl->rqe_avail <= rdma_hndl->rq_depth + 1)
#endif
			xio_rdma_rearm_rq(rdma_hndl);
		if (IS_REQUEST(task->tlv_type))
			xio_rdma_on_recv_req(rdma_hndl, task);
		else if (IS_RESPONSE(task->tlv_type))
			xio_rdma_on_recv_rsp(rdma_hndl, task);
		else
			ERROR_LOG("unknown message type:0x%x\n",
				  task->tlv_type);
		break;
	}
	/*
	if (rdma_hndl->state != XIO_TRANSPORT_STATE_CONNECTED)
		return retval;
	*/

	/* transmit ready packets */
	if (!must_send && rdma_hndl->tx_ready_tasks_num)
		must_send = (tx_window_sz(rdma_hndl) >= SEND_THRESHOLD);

	/* resource are now available and rdma rd  requests are pending kick
	 * them
	 */
	if (rdma_hndl->kick_rdma_rd_req)
		xio_xmit_rdma_rd_req(rdma_hndl);

	if (rdma_hndl->kick_rdma_rd_rsp)
		xio_xmit_rdma_rd_rsp(rdma_hndl);

	if (must_send)
		xio_rdma_xmit(rdma_hndl);

	/* prefetch next buffer */
	if  (task1) {
		if (task1->tasks_list_entry.next != task_prev) {
			task2 = list_entry(task1->tasks_list_entry.next,
					   struct xio_task,  tasks_list_entry);
			xio_prefetch(task2);
		}
	}
	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_tx_comp_handler						     */
/*---------------------------------------------------------------------------*/
static XIO_F_ALWAYS_INLINE int xio_rdma_tx_comp_handler(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task)
{
	struct xio_task		*ptask, *next_ptask;
	struct xio_rdma_task	*rdma_task;
	int			found = 0;
	int			removed = 0;

	/* If we got a completion, it means all the previous tasks should've
	   been sent by now - due to ordering */
	list_for_each_entry_safe(ptask, next_ptask, &rdma_hndl->in_flight_list,
				 tasks_list_entry) {
		list_move_tail(&ptask->tasks_list_entry,
			       &rdma_hndl->tx_comp_list);
		removed++;
		rdma_task = (struct xio_rdma_task *)ptask->dd_data;

		rdma_hndl->sqe_avail++;

		/* phantom task */
		if (rdma_task->phantom_idx) {
			xio_tasks_pool_put(ptask);
			continue;
		}

		/* rdma wr utilizes two wqe but appears only once in the
		 * in flight list
		 */
		if (rdma_task->out_ib_op == XIO_IB_RDMA_WRITE)
			rdma_hndl->sqe_avail++;

		if (IS_RDMA_RD_ACK(ptask->tlv_type)) {
			rdma_hndl->rsps_in_flight_nr--;
			xio_tasks_pool_put(ptask);
		} else if (IS_REQUEST(ptask->tlv_type)) {
			rdma_hndl->max_sn++;
			rdma_hndl->reqs_in_flight_nr--;
			xio_rdma_on_req_send_comp(rdma_hndl, ptask);
			xio_tasks_pool_put(ptask);
		} else if (IS_RESPONSE(ptask->tlv_type)) {
			rdma_hndl->max_sn++;
			rdma_hndl->rsps_in_flight_nr--;
			xio_rdma_on_rsp_send_comp(rdma_hndl, ptask);
		} else if (IS_NOP(ptask->tlv_type)) {
			rdma_hndl->rsps_in_flight_nr--;
			xio_tasks_pool_put(ptask);
		} else if (ptask->tlv_type == XIO_MSG_TYPE_RDMA) {
			if (rdma_task->out_ib_op == XIO_IB_RDMA_WRITE_DIRECT) {
				rdma_hndl->reqs_in_flight_nr--;
				xio_rdma_on_direct_rdma_comp(
						rdma_hndl, ptask,
						XIO_WC_OP_RDMA_WRITE);
				xio_tasks_pool_put(ptask);
			}
		} else {
			ERROR_LOG("unexpected task %p tlv %u type:0x%x id:%d " \
				  "magic:0x%x\n",
				  ptask, ptask->tlv_type, rdma_task->out_ib_op,
				  ptask->ltid, ptask->magic);
			continue;
		}
		if (ptask == task) {
			found  = 1;
			break;
		}
	}
	/* resource are now available and rdma rd  requests are pending kick
	 * them
	 */
	if (rdma_hndl->kick_rdma_rd_req)
		xio_xmit_rdma_rd_req(rdma_hndl);

	if (rdma_hndl->kick_rdma_rd_rsp)
		xio_xmit_rdma_rd_rsp(rdma_hndl);

	if (rdma_hndl->tx_ready_tasks_num)
		xio_rdma_xmit(rdma_hndl);

	if (!found && removed)
		ERROR_LOG("not found but removed %d type:0x%x\n",
			  removed, task->tlv_type);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_rd_req_comp_handler						     */
/*---------------------------------------------------------------------------*/
static void xio_direct_rdma_rd_comp_handler(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);

	rdma_hndl->sqe_avail++;

	if (rdma_task->phantom_idx == 0) {
		rdma_hndl->reqs_in_flight_nr--;
		xio_rdma_on_direct_rdma_comp(rdma_hndl, task,
					     XIO_WC_OP_RDMA_READ);
	} else {
		xio_tasks_pool_put(task);
		xio_xmit_rdma_rd_req(rdma_hndl);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_rd_req_comp_handler						     */
/*---------------------------------------------------------------------------*/
static XIO_F_ALWAYS_INLINE void xio_rdma_rd_req_comp_handler(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	union xio_transport_event_data	event_data;

	rdma_hndl->rdma_rd_req_in_flight--;
	rdma_hndl->sqe_avail++;

	if (rdma_task->phantom_idx == 0) {
		list_move_tail(&task->tasks_list_entry, &rdma_hndl->io_list);
		xio_xmit_rdma_rd_req(rdma_hndl);

		/* fill notification event */
		event_data.msg.op		= XIO_WC_OP_RECV;
		event_data.msg.task		= task;

		if (task->status)
			xio_free_rdma_task_mem(&rdma_hndl->base, task);

		xio_transport_notify_observer(&rdma_hndl->base,
					      XIO_TRANSPORT_EVENT_NEW_MESSAGE,
					      &event_data);

		while (rdma_hndl->rdma_rd_req_in_flight) {
			task = list_first_entry(
					&rdma_hndl->rdma_rd_req_in_flight_list,
					struct xio_task,  tasks_list_entry);

			rdma_task = (struct xio_rdma_task *)task->dd_data;

			if (rdma_task->out_ib_op != XIO_IB_RECV)
				break;

			/* tasks that arrived in Send/Receive while pending
			 * "RDMA READ" tasks were in flight was fenced.
			 */
			rdma_hndl->rdma_rd_req_in_flight--;
			list_move_tail(&task->tasks_list_entry,
				       &rdma_hndl->io_list);
			event_data.msg.op	= XIO_WC_OP_RECV;
			event_data.msg.task	= task;

			if (task->status)
				xio_free_rdma_task_mem(&rdma_hndl->base, task);

			xio_transport_notify_observer(
					&rdma_hndl->base,
					XIO_TRANSPORT_EVENT_NEW_MESSAGE,
					&event_data);
		}
	} else {
		xio_tasks_pool_put(task);
		xio_xmit_rdma_rd_req(rdma_hndl);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_rd_rsp_comp_handler						     */
/*---------------------------------------------------------------------------*/
static XIO_F_ALWAYS_INLINE void xio_rdma_rd_rsp_comp_handler(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	union xio_transport_event_data	event_data;

	rdma_hndl->rdma_rd_rsp_in_flight--;
	rdma_hndl->sqe_avail++;

	if (rdma_task->phantom_idx == 0) {
		list_move_tail(&task->tasks_list_entry, &rdma_hndl->io_list);

		/* notify the peer that it can free resources */
		xio_rdma_send_rdma_read_ack(rdma_hndl, task->rtid);

		xio_xmit_rdma_rd_rsp(rdma_hndl);

		/* copy from task->in to sender_task->in */
		xio_rdma_post_recv_rsp(task);

		/* fill notification event */
		event_data.msg.op		= XIO_WC_OP_RECV;
		event_data.msg.task		= task;

		if (task->status)
			xio_free_rdma_task_mem(&rdma_hndl->base, task);

		xio_transport_notify_observer(&rdma_hndl->base,
					      XIO_TRANSPORT_EVENT_NEW_MESSAGE,
					      &event_data);

		while (rdma_hndl->rdma_rd_rsp_in_flight) {
			task = list_first_entry(
					&rdma_hndl->rdma_rd_rsp_in_flight_list,
					struct xio_task,  tasks_list_entry);

			rdma_task = (struct xio_rdma_task *)task->dd_data;

			if (rdma_task->out_ib_op != XIO_IB_RECV)
				break;

			/* tasks that arrived in Send/Receive while pending
			 * "RDMA READ" tasks were in flight was fenced.
			 */
			rdma_hndl->rdma_rd_rsp_in_flight--;
			list_move_tail(&task->tasks_list_entry,
				       &rdma_hndl->io_list);
			event_data.msg.op	= XIO_WC_OP_RECV;
			event_data.msg.task	= task;

			if (task->status)
				xio_free_rdma_task_mem(&rdma_hndl->base, task);

			xio_transport_notify_observer(
					&rdma_hndl->base,
					XIO_TRANSPORT_EVENT_NEW_MESSAGE,
					&event_data);
		}
	} else {
		xio_tasks_pool_put(task);
		xio_xmit_rdma_rd_rsp(rdma_hndl);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_match_request_to_response					     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_match_request_to_response(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task, uint32_t qp_num)
{
	struct xio_rdma_rsp_hdr	*tmp_rsp_hdr;
	struct xio_rdma_rsp_hdr	rsp_hdr;
	struct xio_task *sender_task;
	struct xio_rdma_transport *sender_rdma_hndl;


	memset(&rsp_hdr, 0, sizeof(rsp_hdr));
	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_rsp_hdr = (struct xio_rdma_rsp_hdr *)
				xio_mbuf_get_curr_ptr(&task->mbuf);
	UNPACK_LVAL(tmp_rsp_hdr, &rsp_hdr, rtid);
	UNPACK_SVAL(tmp_rsp_hdr, &rsp_hdr, sn);
	UNPACK_SVAL(tmp_rsp_hdr, &rsp_hdr, credits);

	/* find the sender task */
	sender_task =
		xio_rdma_primary_task_lookup(rdma_hndl, rsp_hdr.rtid);

	if (!sender_task || !sender_task->context) {
		ERROR_LOG("sender_task:%p", "sender_task_context:%p, rdma_hndl:%p\n",
			   sender_task, sender_task->context, rdma_hndl);
		goto cleanup;
	}
	sender_rdma_hndl = (struct xio_rdma_transport *)sender_task->context;
	if (!sender_rdma_hndl || !sender_rdma_hndl->qp) {
		ERROR_LOG("sender_rdma_hndl qp does not exist. " \
			  "sender_rdma_hndl%p, rdma_hndl:%p\n",
			  sender_rdma_hndl, rdma_hndl);
		goto cleanup;
	}
	if (qp_num != sender_rdma_hndl->qp->qp_num) {
		ERROR_LOG("qp mismatch qp_num:%d, sender_task_qp:%d, rdma_hndl:%p\n",
			  qp_num, sender_rdma_hndl->qp->qp_num, rdma_hndl);
		goto cleanup;
	}
	if (task == sender_task ||  task->context != sender_task->context) {
		ERROR_LOG("sender_task:%p, task:%p, task_context:%p, " \
			  "sender_task_context:%p, rdma_hndl:%p\n",
			  sender_task, task,
			  task->context, sender_task->context,
			  rdma_hndl);
		goto cleanup;
	}
	if (rdma_hndl->exp_sn != rsp_hdr.sn) {
		ERROR_LOG("ERROR: expected sn:%d, arrived sn:%d, rdma_hndl:%p\n",
		          rdma_hndl->exp_sn, rsp_hdr.sn, rdma_hndl);
		goto cleanup;
	}
	if (!IS_REQUEST(sender_task->tlv_type) ||
			sender_task->tlv_type == 0xbeef ||
			sender_task->tlv_type == 0xdead) {
		ERROR_LOG("invalid sender task tlv_type. sender_task:%p, \
			   sender_task->tlv_type:%p, rdma_hndl:%p\n",
			   sender_task, sender_task->tlv_type, rdma_hndl);
		goto cleanup;
	}
	task->sender_task = sender_task;
	return 0;

cleanup:
	ERROR_LOG("%s failed. rdma_hndl:%p, task:%p\n", __func__,
		  rdma_hndl, task);
	if (rdma_hndl->exp_sn == rsp_hdr.sn) {
		rdma_hndl->exp_sn++;
		rdma_hndl->ack_sn = rsp_hdr.sn;
		rdma_hndl->peer_credits += rsp_hdr.credits;
	}
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_test_request						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_test_request(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task)
{
	struct xio_rdma_req_hdr	*tmp_req_hdr;
	struct xio_rdma_req_hdr	req_hdr;


	memset(&req_hdr, 0, sizeof(req_hdr));
	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_req_hdr = (struct xio_rdma_req_hdr *)
				xio_mbuf_get_curr_ptr(&task->mbuf);
	UNPACK_SVAL(tmp_req_hdr, &req_hdr, sn);
	UNPACK_SVAL(tmp_req_hdr, &req_hdr, credits);

	if (rdma_hndl->exp_sn != req_hdr.sn) {
		ERROR_LOG("ERROR: expected sn:%d, arrived sn:%d, rdma_hndl:%p\n",
		          rdma_hndl->exp_sn, req_hdr.sn, rdma_hndl);
		goto cleanup;
	}
	return 0;

cleanup:
	ERROR_LOG("%s failed. rdma_hndl:%p, task:%p\n", __func__,
		  rdma_hndl, task);
	if (rdma_hndl->exp_sn == req_hdr.sn) {
		rdma_hndl->exp_sn++;
		rdma_hndl->ack_sn = req_hdr.sn;
		rdma_hndl->peer_credits += req_hdr.credits;
	}
	return -1;
}


/*---------------------------------------------------------------------------*/
/* xio_rdma_test_request_and_response					     */
/*---------------------------------------------------------------------------*/
int xio_rdma_test_request_and_response(struct xio_rdma_transport *rdma_hndl,
				       struct xio_task *task, int opcode,
				       uint32_t qp_num)
{
	int retval;
	uint64_t tlv_type;

	if (opcode != IBV_WC_RECV)
		return 0;

	xio_mbuf_reset(&task->mbuf);
	retval = xio_mbuf_read_first_tlv(&task->mbuf);
	if (retval)
		goto cleanup;

	tlv_type = xio_mbuf_tlv_type(&task->mbuf);
	switch (tlv_type) {
		case XIO_RDMA_READ_ACK:
		case XIO_NEXUS_SETUP_REQ:
		case XIO_NEXUS_SETUP_RSP:
			return 0;
		break;
	default:
		if (IS_RESPONSE(tlv_type))
			retval = xio_rdma_match_request_to_response(rdma_hndl, task, qp_num);
		else if (IS_REQUEST(tlv_type))
			retval = xio_rdma_test_request(rdma_hndl, task);
		else
			return 0;
		break;
	}
	if (retval)
		goto cleanup;

	task->tlv_type = tlv_type;
	return 0;

cleanup:
	ERROR_LOG("%s failed. rdma_hndl:%p\n", __func__, rdma_hndl);
	if (rdma_hndl->state == XIO_TRANSPORT_STATE_CONNECTED &&
	    !rdma_hndl->rdma_disconnect_called) {
		rdma_hndl->disconnect_nr = 0;
		rdma_hndl->ignore_timewait = 1;
		xio_ctx_del_delayed_work(
				rdma_hndl->base.ctx,
				&rdma_hndl->disconnect_timeout_work);
		xio_set_disconnect_timer(rdma_hndl);
		rdma_hndl->rdma_disconnect_called = 1;
		rdma_hndl->state = XIO_TRANSPORT_STATE_DISCONNECTED;
		ERROR_LOG("%s calling rdma_disconnect. rdma_hndl:%p\n",
			  __func__, rdma_hndl);
		retval = rdma_disconnect(rdma_hndl->cm_id);
		if (retval)
			ERROR_LOG("rdma_hndl:%p rdma_disconnect" \
					"failed, %m\n", rdma_hndl);
	}
	xio_tasks_pool_put(task);
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_handle_wc							     */
/*---------------------------------------------------------------------------*/
static XIO_F_ALWAYS_INLINE void xio_handle_wc(struct ibv_wc *wc,
					      int last_in_rxq, struct xio_srq *srq)
{
	struct xio_task *task = (struct xio_task *)ptr_from_int64(wc->wr_id);
	int		opcode = wc->opcode;
	struct xio_key_int32  key;
	struct xio_rdma_transport *rdma_hndl;
	int retval;

	if (srq) {
		key.id = wc->qp_num;
		HT_LOOKUP(&srq->ht_rdma_hndl, &key, rdma_hndl, rdma_hndl_htbl);
	} else {
		rdma_hndl = (struct xio_rdma_transport *)task->context;
	}
	if (!rdma_hndl) {
		ERROR_LOG("got task with null rdma_hndl. task:%p\n", task);
		goto cleanup;
	}
	if (unlikely(rdma_hndl->state == XIO_TRANSPORT_STATE_DISCONNECTED ||
		     rdma_hndl->state == XIO_TRANSPORT_STATE_RECONNECT ||
		     rdma_hndl->state == XIO_TRANSPORT_STATE_CLOSED ||
		     rdma_hndl->state == XIO_TRANSPORT_STATE_ERROR ||
		     rdma_hndl->state == XIO_TRANSPORT_STATE_DESTROYED)) {
		ERROR_LOG("receive message while transport is not connected. task is flushed. " \
			  "task:%p, opcode:%s, rdma_hndl:%p, state:%s\n",
			  task, ibv_wc_opcode_str(wc->opcode), rdma_hndl,
			  xio_transport_state_str(rdma_hndl->state));
		if (opcode != IBV_WC_SEND)
			xio_tasks_pool_put(task);
		goto cleanup;
	}
	/*
	TRACE_LOG("received opcode :%s [%x]\n",
		  ibv_wc_opcode_str(wc->opcode), wc->opcode);
	*/

	switch (opcode) {
	case IBV_WC_RECV:
		retval = xio_rdma_test_request_and_response(rdma_hndl,
							    task, opcode, wc->qp_num);
		if (retval) {
			ERROR_LOG("task tests failed rdma_hndl:%p, " \
				  "peer_rdma_hndl:%p, task:%p\n",
				  rdma_hndl, rdma_hndl->peer_rdma_hndl, task);
			return;
		}
		task->last_in_rxq = last_in_rxq;
		xio_rdma_rx_handler(rdma_hndl, task);
		break;
	case IBV_WC_SEND:
	case IBV_WC_RDMA_WRITE:
		if (opcode == IBV_WC_SEND ||
		    (opcode == IBV_WC_RDMA_WRITE &&
		     task->tlv_type == XIO_MSG_TYPE_RDMA))
			xio_rdma_tx_comp_handler(rdma_hndl, task);
		break;
	case IBV_WC_RDMA_READ:
		task->last_in_rxq = last_in_rxq;
		if (IS_REQUEST(task->tlv_type))
			xio_rdma_rd_req_comp_handler(rdma_hndl, task);
		else if (IS_RESPONSE(task->tlv_type))
			xio_rdma_rd_rsp_comp_handler(rdma_hndl, task);
		else if (task->tlv_type == XIO_MSG_TYPE_RDMA)
			xio_direct_rdma_rd_comp_handler(rdma_hndl, task);
		else
			ERROR_LOG("Unexpected tlv_type %u, rdma_hndl:%p\n",
				  task->tlv_type, rdma_hndl);
		break;
	default:
		ERROR_LOG("unknown opcode :%s [%x]\n",
			  ibv_wc_opcode_str(wc->opcode), wc->opcode);
		break;
	}

cleanup:
	/* task ref add before ibv_post_send/recv */
	xio_tasks_pool_put(task);
}

/*
 * Could read as many entries as possible without blocking, but
 * that just fills up a list of tasks.  Instead pop out of here
 * so that tx progress, like issuing rdma reads and writes, can
 * happen periodically.
 */
static int xio_poll_cq(struct xio_cq *tcq, int max_wc, int timeout_us)
{
	int		err = 0;
	int		stop = 0, tlv_type;
	int		wclen = max_wc, i, numwc  = 0;
	int		timeouts_num = 0;
	int		polled = 0, last_in_rxq = -1;
	cycles_t	timeout;
	cycles_t	start_time = 0;
	struct ibv_wc	*wc;
	struct xio_task *task;
	struct xio_rdma_task    *rdma_task;

	for (;;) {
		if (wclen > tcq->wc_array_len)
			wclen = tcq->wc_array_len;

		if (xio_context_is_loop_stopping(tcq->ctx) && polled) {
				err = 0; /* same as in budget */
				stop = 1;
				break;
		}
		err = ibv_poll_cq(tcq->cq, wclen, tcq->wc_array);
		polled = 1;
		if (err == 0) { /* no completions retrieved */
			if (timeout_us == 0)
				break;
			/* wait timeout before going out */
			if (timeouts_num == 0) {
				start_time = get_cycles();
			} else {
				/*calculate it again, need to spend time */
				timeout = timeout_us * g_mhz;
				if (timeout_us > 0 &&
				    (get_cycles() - start_time) > timeout)
					break;
			}
			if (xio_context_is_loop_stopping(tcq->ctx)) {
				err = 0; /* same as in budget */
				stop = 1;
				break;
			}

			timeouts_num++;
			continue;
		}

		if (unlikely(err < 0)) {
			ERROR_LOG("ibv_poll_cq failed\n");
			break;
		}
		timeouts_num = 0;

		wc = &tcq->wc_array[err - 1];
		for (i = err - 1; i >= 0; i--) {
			if (wc->status == IBV_WC_SUCCESS &&
				(wc->opcode == IBV_WC_RECV || wc->opcode == IBV_WC_RDMA_READ)) {
				task = (struct xio_task *)
					ptr_from_int64(wc->wr_id);
				rdma_task = (struct xio_rdma_task *)task->dd_data;
				if (!rdma_task->phantom_idx) {
					tlv_type = xio_mbuf_read_type(&task->mbuf);
					if (IS_APPLICATION_MSG(tlv_type)) {
						last_in_rxq = i;
						break;
					}
				}
			}
			wc--;
		}
		wc = &tcq->wc_array[0];
		for (i = 0; i < err; i++) {
			if (likely(wc->status == IBV_WC_SUCCESS))
				xio_handle_wc(wc, (i == last_in_rxq), tcq->srq);
			else
				xio_handle_wc_error(wc, tcq->srq);
			wc++;
		}
		numwc += err;
		if (numwc == max_wc) {
			err = 1;
			break;
		}
		wclen = max_wc - numwc;
	}

	return stop ? -1 : err;
}

/*---------------------------------------------------------------------------*/
/* xio_rearm_completions						     */
/*---------------------------------------------------------------------------*/
static void xio_rearm_completions(struct xio_cq *tcq)
{
	int err;

	err = ibv_req_notify_cq(tcq->cq, 0);
	if (unlikely(err)) {
		ERROR_LOG("ibv_req_notify_cq failed. (errno=%d %m)\n",
			  errno);
	}

	memset(&tcq->consume_cq_event, 0,
	       sizeof(tcq->consume_cq_event));
	tcq->consume_cq_event.handler	= xio_sched_consume_cq;
	tcq->consume_cq_event.data	= tcq;

	xio_context_add_event(tcq->ctx, &tcq->consume_cq_event);

	tcq->num_delayed_arm = 0;
}

/*---------------------------------------------------------------------------*/
/* xio_poll_cq_armable							     */
/*---------------------------------------------------------------------------*/
static void xio_poll_cq_armable(struct xio_cq *tcq)
{
	int err = -1;

	if (++tcq->num_poll_cq < NUM_POLL_CQ)
		err = xio_poll_cq(tcq, MAX_POLL_WC, tcq->ctx->polling_timeout);
	if (unlikely(err < 0)) {
		xio_rearm_completions(tcq);
		xio_rdma_idle_handler(tcq->rdma_hndl);
		return;
	}

	if (err == 0 && (++tcq->num_delayed_arm == MAX_NUM_DELAYED_ARM)) {
		/* no more completions on cq, give up and arm the interrupts */
		xio_rearm_completions(tcq);
	} else {
		memset(&tcq->poll_cq_event, 0, sizeof(tcq->poll_cq_event));
		tcq->poll_cq_event.handler = xio_sched_poll_cq;
		tcq->poll_cq_event.data	= tcq;

		xio_context_add_event(tcq->ctx, &tcq->poll_cq_event);
	}
}

/* xio_sched_consume_cq() is scheduled to consume completion events that
   could arrive after the cq had been seen empty, but just before
   the interrupts were re-armed.
   Intended to consume those remaining completions only, the function
   does not re-arm interrupts, but polls the cq until it's empty.
   As we always limit the number of completions polled at a time, we may
   need to schedule this functions few times.
   It may happen that during this process new completions occur, and
   we get an interrupt about that. Some of the "new" completions may be
   processed by the self-scheduling xio_sched_consume_cq(), which is
   a good thing, because we don't need to wait for the interrupt event.
   When the interrupt notification arrives, its handler will remove the
   scheduled event, and call xio_poll_cq_armable(), so that the polling
   cycle resumes normally.
*/
static void xio_sched_consume_cq(void *data)
{
	struct xio_cq *tcq = (struct xio_cq *)data;
	int err;

	if (++tcq->num_poll_cq >= NUM_POLL_CQ)
		return;

	err = xio_poll_cq(tcq, MAX_POLL_WC, tcq->ctx->polling_timeout);
	if (err > 0) {
		memset(&tcq->consume_cq_event, 0,
		       sizeof(tcq->consume_cq_event));
		tcq->consume_cq_event.handler = xio_sched_consume_cq;
		tcq->consume_cq_event.data    = tcq;

		xio_context_add_event(tcq->ctx, &tcq->consume_cq_event);
	}
}

/* Scheduled to poll cq after a completion event has been
   received and acknowledged, if no more completions are found
   the interrupts are re-armed */
static void xio_sched_poll_cq(void *data)
{
	struct xio_cq			*tcq = (struct xio_cq *)data;

	xio_poll_cq_armable(tcq);
	xio_rdma_idle_handler(tcq->rdma_hndl);
}

/*
 * Called from main event loop when a CQ notification is available.
 */
void xio_cq_event_handler(int fd  __attribute__ ((unused)),
			  int events __attribute__ ((unused)),
			  void *data)
{
	void				*cq_context;
	struct ibv_cq			*cq;
	struct xio_cq			*tcq = (struct xio_cq *)data;
	int				err;

	err = ibv_get_cq_event(tcq->channel, &cq, &cq_context);
	if (unlikely(err != 0)) {
		/* Just print the log message, if that was a serious problem,
		   it will express itself elsewhere */
		ERROR_LOG("failed to retrieve CQ event, cq:%p\n", cq);
		return;
	}
	tcq->cq_events_that_need_ack++;
	tcq->num_poll_cq = 0;
	/* if a poll was previously scheduled, remove it,
	   as it will be scheduled when necessary */
	xio_context_disable_event(&tcq->poll_cq_event);
	xio_context_disable_event(&tcq->consume_cq_event);

	xio_poll_cq_armable(tcq);

	/* accumulate number of cq events that need to
	 * be acked, and periodically ack them
	 */
	if (tcq->cq_events_that_need_ack == MAX_ACKED_CQE/*UINT_MAX*/) {
		ibv_ack_cq_events(tcq->cq, MAX_ACKED_CQE/*UINT_MAX*/);
		tcq->cq_events_that_need_ack = 0;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_poll_completions						     */
/*---------------------------------------------------------------------------*/
void xio_rdma_poll_completions(struct xio_cq *tcq, int timeout_us)
{
	void				*cq_context;
	struct ibv_cq			*cq;
	int				err;
	int				cq_rearmed = 0;

	err = ibv_get_cq_event(tcq->channel, &cq, &cq_context);
	if (!err) {
		tcq->cq_events_that_need_ack++;
		cq_rearmed = 1;
	} else if (errno != EAGAIN) {
		/* Just print the log message, if that was a serious problem,
		   it will express itself elsewhere */
		ERROR_LOG("failed to retrieve CQ event, cq:%p\n", cq);
		return;
	}
	/* if a poll was previously scheduled, remove it,
	   as it will be scheduled when necessary */
	xio_context_disable_event(&tcq->poll_cq_event);
	xio_context_disable_event(&tcq->consume_cq_event);

	xio_poll_cq(tcq, MAX_POLL_WC, timeout_us);
	/* TODO rearm interrupts optimization */
	if (cq_rearmed == 1) {
		err = ibv_req_notify_cq(tcq->cq, 0);
		if (unlikely(err)) {
			ERROR_LOG("ibv_req_notify_cq failed. (errno=%d %m)\n",
				  errno);
		}
	}
	xio_rdma_idle_handler(tcq->rdma_hndl);

	/* accumulate number of cq events that need to
	 * be acked, and periodically ack them
	 */
	if (tcq->cq_events_that_need_ack == MAX_ACKED_CQE/*UINT_MAX*/) {
		ibv_ack_cq_events(tcq->cq, MAX_ACKED_CQE/*UINT_MAX*/);
		tcq->cq_events_that_need_ack = 0;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_write_req_header						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_write_req_header(struct xio_rdma_transport *rdma_hndl,
				     struct xio_task *task,
				     struct xio_rdma_req_hdr *req_hdr)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_rdma_req_hdr		*tmp_req_hdr;
	struct xio_sge			*tmp_sge;
	struct xio_sge			sge;
	struct ibv_mr			*mr;
	struct xio_sg_table_ops		*sgtbl_ops;
	void				*sgtbl;
	void				*sg;
	size_t				hdr_len;
	uint32_t			i;

	sgtbl		= xio_sg_table_get(&task->omsg->in);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->in.sgl_type);

	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_req_hdr = (struct xio_rdma_req_hdr *)
				xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	tmp_req_hdr->version  = req_hdr->version;
	tmp_req_hdr->flags    = req_hdr->flags;
	PACK_SVAL(req_hdr, tmp_req_hdr, req_hdr_len);
	/* sn		shall be coded later */
	/* ack_sn	shall be coded later */
	/* credits	shall be coded later */
	tmp_req_hdr->sn = 0;
	tmp_req_hdr->ack_sn = ~0;
	tmp_req_hdr->credits = 0;
	PACK_LVAL(req_hdr, tmp_req_hdr, ltid);
	tmp_req_hdr->in_ib_op	   = req_hdr->in_ib_op;
	tmp_req_hdr->out_ib_op	   = req_hdr->out_ib_op;
	PACK_SVAL(req_hdr, tmp_req_hdr, in_num_sge);
	PACK_SVAL(req_hdr, tmp_req_hdr, out_num_sge);
	PACK_SVAL(req_hdr, tmp_req_hdr, ulp_hdr_len);
	PACK_SVAL(req_hdr, tmp_req_hdr, ulp_pad_len);
	/*remain_data_len is not used		*/
	PACK_LLVAL(req_hdr, tmp_req_hdr, ulp_imm_len);

	tmp_sge = (struct xio_sge *)((uint8_t *)tmp_req_hdr +
			   sizeof(struct xio_rdma_req_hdr));

	/* IN: requester expect small input written via send */
	sg = sge_first(sgtbl_ops, sgtbl);
	if (req_hdr->in_ib_op == XIO_IB_SEND) {
		for (i = 0;  i < req_hdr->in_num_sge; i++) {
			sge.addr = 0;
			sge.length = sge_length(sgtbl_ops, sg);
			sge.stag = 0;
			PACK_LLVAL(&sge, tmp_sge, addr);
			PACK_LVAL(&sge, tmp_sge, length);
			PACK_LVAL(&sge, tmp_sge, stag);
			tmp_sge++;
			sg = sge_next(sgtbl_ops, sgtbl, sg);
		}
	}
	/* IN: requester expect big input written rdma write */
	if (req_hdr->in_ib_op == XIO_IB_RDMA_WRITE) {
		for (i = 0;  i < req_hdr->in_num_sge; i++) {
			sge.addr = uint64_from_ptr(
					rdma_task->read_reg_mem[i].addr);
			sge.length  = rdma_task->read_reg_mem[i].length;
			if (rdma_task->read_reg_mem[i].mr) {
				mr = xio_rdma_mr_lookup(
						rdma_task->read_reg_mem[i].mr,
						rdma_hndl->tcq->dev);
				if (!mr)
					goto cleanup;

				sge.stag	= mr->rkey;
			} else {
				sge.stag	= 0;
			}
			PACK_LLVAL(&sge, tmp_sge, addr);
			PACK_LVAL(&sge, tmp_sge, length);
			PACK_LVAL(&sge, tmp_sge, stag);
			tmp_sge++;
		}
	}
	/* OUT: requester want to write data via rdma read */
	if (req_hdr->out_ib_op == XIO_IB_RDMA_READ) {
		for (i = 0;  i < req_hdr->out_num_sge; i++) {
			sge.addr = uint64_from_ptr(
					rdma_task->write_reg_mem[i].addr);
			sge.length  = rdma_task->write_reg_mem[i].length;
			if (rdma_task->write_reg_mem[i].mr) {
				mr = xio_rdma_mr_lookup(
						rdma_task->write_reg_mem[i].mr,
						rdma_hndl->tcq->dev);
				if (!mr)
					goto cleanup;

				sge.stag	= mr->rkey;
			} else {
				sge.stag	= 0;
			}
			PACK_LLVAL(&sge, tmp_sge, addr);
			PACK_LVAL(&sge, tmp_sge, length);
			PACK_LVAL(&sge, tmp_sge, stag);
			tmp_sge++;
		}
	}
	if (req_hdr->out_ib_op == XIO_IB_SEND) {
		for (i = 0;  i < req_hdr->out_num_sge; i++) {
			sge.addr = 0;
			sge.length = sge_length(sgtbl_ops, sg);
			sge.stag = 0;
			PACK_LLVAL(&sge, tmp_sge, addr);
			PACK_LVAL(&sge, tmp_sge, length);
			PACK_LVAL(&sge, tmp_sge, stag);
			tmp_sge++;
			sg = sge_next(sgtbl_ops, sgtbl, sg);
		}
	}
	hdr_len	= sizeof(struct xio_rdma_req_hdr);
	hdr_len += sizeof(struct xio_sge) * (req_hdr->in_num_sge +
					     req_hdr->out_num_sge);
#ifdef EYAL_TODO
	print_hex_dump_bytes("post_send: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.curr,
			     hdr_len + 16);
#endif
	xio_mbuf_inc(&task->mbuf, hdr_len);

	return 0;

cleanup:
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_read_req_header						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_read_req_header(struct xio_rdma_transport *rdma_hndl,
				    struct xio_task *task,
				    struct xio_rdma_req_hdr *req_hdr)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_rdma_req_hdr		*tmp_req_hdr;
	struct xio_sge			*tmp_sge;
	int				i;
	size_t				hdr_len;

	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_req_hdr = (struct xio_rdma_req_hdr *)
				xio_mbuf_get_curr_ptr(&task->mbuf);

	req_hdr->version  = tmp_req_hdr->version;
	req_hdr->flags    = tmp_req_hdr->flags;
	UNPACK_SVAL(tmp_req_hdr, req_hdr, req_hdr_len);

	if (unlikely(req_hdr->req_hdr_len != sizeof(struct xio_rdma_req_hdr))) {
		ERROR_LOG(
		"header length's read failed. arrived:%d  expected:%zd, rdma_hndl:%p\n",
		req_hdr->req_hdr_len, sizeof(struct xio_rdma_req_hdr), rdma_hndl);
		return -1;
	}
	UNPACK_SVAL(tmp_req_hdr, req_hdr, sn);
	UNPACK_SVAL(tmp_req_hdr, req_hdr, credits);
	UNPACK_LVAL(tmp_req_hdr, req_hdr, ltid);
	req_hdr->in_ib_op = tmp_req_hdr->in_ib_op;
	req_hdr->out_ib_op = tmp_req_hdr->out_ib_op;

	UNPACK_SVAL(tmp_req_hdr, req_hdr, in_num_sge);
	UNPACK_SVAL(tmp_req_hdr, req_hdr, out_num_sge);
	UNPACK_SVAL(tmp_req_hdr, req_hdr, ulp_hdr_len);
	UNPACK_SVAL(tmp_req_hdr, req_hdr, ulp_pad_len);

	/* remain_data_len not in use */
	UNPACK_LLVAL(tmp_req_hdr, req_hdr, ulp_imm_len);

	tmp_sge = (struct xio_sge *)((uint8_t *)tmp_req_hdr +
			   sizeof(struct xio_rdma_req_hdr));

	rdma_task->sn = req_hdr->sn;

	/* params for SEND/RDMA WRITE */
	for (i = 0;  i < req_hdr->in_num_sge; i++) {
		UNPACK_LLVAL(tmp_sge, &rdma_task->req_in_sge[i], addr);
		UNPACK_LVAL(tmp_sge, &rdma_task->req_in_sge[i], length);
		UNPACK_LVAL(tmp_sge, &rdma_task->req_in_sge[i], stag);
		tmp_sge++;
	}
	rdma_task->req_in_num_sge	= i;

	/* params for SEND/RDMA_READ */
	for (i = 0;  i < req_hdr->out_num_sge; i++) {
		UNPACK_LLVAL(tmp_sge, &rdma_task->req_out_sge[i], addr);
		UNPACK_LVAL(tmp_sge, &rdma_task->req_out_sge[i], length);
		UNPACK_LVAL(tmp_sge, &rdma_task->req_out_sge[i], stag);
		tmp_sge++;
	}
	rdma_task->req_out_num_sge	= i;

	hdr_len	= sizeof(struct xio_rdma_req_hdr);
	hdr_len += sizeof(struct xio_sge) * (req_hdr->in_num_sge +
					     req_hdr->out_num_sge);

	xio_mbuf_inc(&task->mbuf, hdr_len);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_write_rsp_header						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_write_rsp_header(struct xio_rdma_transport *rdma_hndl,
				     struct xio_task *task,
				     struct xio_rdma_rsp_hdr *rsp_hdr)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_sge			sge;
	struct xio_rdma_rsp_hdr		*tmp_rsp_hdr;
	struct xio_sge			*tmp_sge;
	struct ibv_mr			*mr;
	size_t				hdr_len;
	uint32_t			*wr_len;
	int				i;

	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_rsp_hdr = (struct xio_rdma_rsp_hdr *)
				xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	tmp_rsp_hdr->version  = rsp_hdr->version;
	tmp_rsp_hdr->flags    = rsp_hdr->flags;
	PACK_SVAL(rsp_hdr, tmp_rsp_hdr, rsp_hdr_len);
	/* sn		shall be coded later */
	/* ack_sn	shall be coded later */
	/* credits	shall be coded later */
	tmp_rsp_hdr->sn = 0;
	tmp_rsp_hdr->ack_sn = ~0;
	tmp_rsp_hdr->credits = 0;
	PACK_LVAL(rsp_hdr, tmp_rsp_hdr, rtid);
	tmp_rsp_hdr->out_ib_op = rsp_hdr->out_ib_op;
	PACK_LVAL(rsp_hdr, tmp_rsp_hdr, status);
	PACK_SVAL(rsp_hdr, tmp_rsp_hdr, out_num_sge);
	PACK_LVAL(rsp_hdr, tmp_rsp_hdr, ltid);
	PACK_SVAL(rsp_hdr, tmp_rsp_hdr, ulp_hdr_len);
	PACK_SVAL(rsp_hdr, tmp_rsp_hdr, ulp_pad_len);
	/* remain_data_len not in use */
	PACK_LLVAL(rsp_hdr, tmp_rsp_hdr, ulp_imm_len);

	hdr_len	= sizeof(struct xio_rdma_rsp_hdr);

	/* OUT: responder want to write data via rdma write */
	if (rsp_hdr->out_ib_op == XIO_IB_RDMA_WRITE) {
		wr_len = (uint32_t *)((uint8_t *)tmp_rsp_hdr +
				sizeof(struct xio_rdma_rsp_hdr));

		/* params for RDMA WRITE */
		for (i = 0;  i < rsp_hdr->out_num_sge; i++) {
			*wr_len = htonl(rdma_task->rsp_out_sge[i].length);
			wr_len++;
		}
		hdr_len += sizeof(uint32_t) * rsp_hdr->out_num_sge;
	}
	if (rsp_hdr->out_ib_op == XIO_IB_RDMA_READ) {
		tmp_sge = (struct xio_sge *)((uint8_t *)tmp_rsp_hdr +
				sizeof(struct xio_rdma_rsp_hdr));

		/* OUT: responder want to write data via rdma read */
		for (i = 0;  i < rsp_hdr->out_num_sge; i++) {
			sge.addr = uint64_from_ptr(
					rdma_task->write_reg_mem[i].addr);
			sge.length  = rdma_task->write_reg_mem[i].length;
			if (rdma_task->write_reg_mem[i].mr) {
				mr = xio_rdma_mr_lookup(
						rdma_task->write_reg_mem[i].mr,
						rdma_hndl->tcq->dev);
				if (!mr)
					goto cleanup;

				sge.stag	= mr->rkey;
			} else {
				sge.stag	= 0;
			}
			PACK_LLVAL(&sge, tmp_sge, addr);
			PACK_LVAL(&sge, tmp_sge, length);
			PACK_LVAL(&sge, tmp_sge, stag);
			tmp_sge++;
		}
		hdr_len += sizeof(struct xio_sge) * rsp_hdr->out_num_sge;
	}

	xio_mbuf_inc(&task->mbuf, hdr_len);

#ifdef EYAL_TODO
	print_hex_dump_bytes("post_send: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.tlv.head, 64);
#endif
	return 0;

cleanup:
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_read_rsp_header						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_read_rsp_header(struct xio_rdma_transport *rdma_hndl,
				    struct xio_task *task,
				    struct xio_rdma_rsp_hdr *rsp_hdr)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_rdma_rsp_hdr		*tmp_rsp_hdr;
	struct xio_sge			*tmp_sge;
	size_t				hdr_len;
	uint32_t			*wr_len;
	int				i;

	memset(rsp_hdr, 0, sizeof(*rsp_hdr));
	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_rsp_hdr = (struct xio_rdma_rsp_hdr *)
				xio_mbuf_get_curr_ptr(&task->mbuf);

	rsp_hdr->version  = tmp_rsp_hdr->version;
	rsp_hdr->flags    = tmp_rsp_hdr->flags;
	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, rsp_hdr_len);

	if (unlikely(rsp_hdr->rsp_hdr_len != sizeof(struct xio_rdma_rsp_hdr))) {
		ERROR_LOG(
		"header length's read failed. arrived:%d expected:%zd, rdma_hndl:%p\n",
		  rsp_hdr->rsp_hdr_len, sizeof(struct xio_rdma_rsp_hdr), rdma_hndl);
		return -1;
	}

	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, sn);
	/* ack_sn not used */
	tmp_rsp_hdr->ack_sn = ~0;
	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, credits);
	UNPACK_LVAL(tmp_rsp_hdr, rsp_hdr, rtid);
	rsp_hdr->out_ib_op = tmp_rsp_hdr->out_ib_op;
	UNPACK_LVAL(tmp_rsp_hdr, rsp_hdr, status);
	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, out_num_sge);
	UNPACK_LVAL(tmp_rsp_hdr, rsp_hdr, ltid);
	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, ulp_hdr_len);
	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, ulp_pad_len);
	/* remain_data_len not in use */
	UNPACK_LLVAL(tmp_rsp_hdr, rsp_hdr, ulp_imm_len);

	hdr_len	= sizeof(struct xio_rdma_rsp_hdr);
	if (rsp_hdr->out_ib_op == XIO_IB_RDMA_WRITE) {
		wr_len = (uint32_t  *)((uint8_t *)tmp_rsp_hdr +
				sizeof(struct xio_rdma_rsp_hdr));

		/* params for RDMA WRITE */
		for (i = 0;  i < rsp_hdr->out_num_sge; i++) {
			rdma_task->rsp_out_sge[i].length = ntohl(*wr_len);
			wr_len++;
		}
		rdma_task->rsp_out_num_sge = rsp_hdr->out_num_sge;

		hdr_len += sizeof(uint32_t) * rsp_hdr->out_num_sge;
	}
	if (rsp_hdr->out_ib_op == XIO_IB_RDMA_READ) {
		tmp_sge = (struct xio_sge *)((uint8_t *)tmp_rsp_hdr +
					     sizeof(struct xio_rdma_rsp_hdr));

		/* params for RDMA_READ */
		for (i = 0;  i < rsp_hdr->out_num_sge; i++) {
			UNPACK_LLVAL(tmp_sge, &rdma_task->req_out_sge[i],
				     addr);
			UNPACK_LVAL(tmp_sge, &rdma_task->req_out_sge[i],
				    length);
			UNPACK_LVAL(tmp_sge, &rdma_task->req_out_sge[i],
				    stag);
			tmp_sge++;
		}
		rdma_task->req_out_num_sge	= i;
		hdr_len += sizeof(struct xio_sge) * rsp_hdr->out_num_sge;
	}

	xio_mbuf_inc(&task->mbuf, hdr_len);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_prep_req_header						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_prep_req_header(struct xio_rdma_transport *rdma_hndl,
				    struct xio_task *task,
				    uint16_t ulp_hdr_len,
				    uint16_t ulp_pad_len,
				    uint64_t ulp_imm_len,
				    uint32_t status)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_rdma_req_hdr	req_hdr;

	if (unlikely(!IS_REQUEST(task->tlv_type))) {
		ERROR_LOG("unknown message type\n");
		return -1;
	}

	/* write the headers */

	/* fill request header */
	req_hdr.version		= XIO_REQ_HEADER_VERSION;
	req_hdr.req_hdr_len	= sizeof(req_hdr);
	req_hdr.ltid		= task->ltid;
	req_hdr.in_ib_op	= rdma_task->in_ib_op;
	req_hdr.out_ib_op	= rdma_task->out_ib_op;
	req_hdr.flags		= task->omsg_flags;

	xio_clear_internal_flags(&req_hdr.flags);
	if (test_bits(XIO_MSG_FLAG_PEER_WRITE_RSP, &task->omsg_flags))
		set_bits(XIO_MSG_FLAG_PEER_WRITE_RSP, &req_hdr.flags);
	else if (test_bits(XIO_MSG_FLAG_LAST_IN_BATCH, &task->omsg_flags))
		set_bits(XIO_MSG_FLAG_LAST_IN_BATCH, &req_hdr.flags);

	req_hdr.ulp_hdr_len	= ulp_hdr_len;
	req_hdr.ulp_pad_len	= ulp_pad_len;
	req_hdr.ulp_imm_len	= ulp_imm_len;
	req_hdr.in_num_sge	= rdma_task->read_num_reg_mem;
	req_hdr.out_num_sge	= rdma_task->write_num_reg_mem;

	if (xio_rdma_write_req_header(rdma_hndl, task, &req_hdr) != 0)
		goto cleanup;

	/* write the payload header */
	if (ulp_hdr_len) {
		if (xio_mbuf_write_array(
		    &task->mbuf,
		    task->omsg->out.header.iov_base,
		    task->omsg->out.header.iov_len) != 0)
			goto cleanup;
	}

	/* write the pad between header and data */
	if (ulp_pad_len)
		xio_mbuf_inc(&task->mbuf, ulp_pad_len);

	return 0;

cleanup:
	xio_set_error(XIO_E_MSG_SIZE);
	ERROR_LOG("xio_rdma_write_req_header failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_prep_rsp_header						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_prep_rsp_header(struct xio_rdma_transport *rdma_hndl,
				    struct xio_task *task,
				    uint16_t ulp_hdr_len,
				    uint16_t ulp_pad_len,
				    uint64_t ulp_imm_len,
				    uint32_t status)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_rdma_rsp_hdr	rsp_hdr = {0};

	if (unlikely(!IS_RESPONSE(task->tlv_type))) {
		ERROR_LOG("unknown message type\n");
		return -1;
	}

	/* fill response header */
	rsp_hdr.version		= XIO_RSP_HEADER_VERSION;
	rsp_hdr.rsp_hdr_len	= sizeof(rsp_hdr);
	rsp_hdr.rtid		= task->rtid;
	rsp_hdr.ltid		= task->ltid;
	rsp_hdr.out_ib_op	= rdma_task->out_ib_op;
	rsp_hdr.flags		= task->omsg_flags;
	if (rdma_task->out_ib_op == XIO_IB_RDMA_READ)
		rsp_hdr.out_num_sge	= rdma_task->write_num_reg_mem;
	else
		rsp_hdr.out_num_sge	= rdma_task->rsp_out_num_sge;

	rsp_hdr.ulp_hdr_len	= ulp_hdr_len;
	rsp_hdr.ulp_pad_len	= ulp_pad_len;
	rsp_hdr.ulp_imm_len	= ulp_imm_len;
	rsp_hdr.status		= status;

	xio_clear_internal_flags(&rsp_hdr.flags);

	if (xio_rdma_write_rsp_header(rdma_hndl, task, &rsp_hdr) != 0)
		goto cleanup;

	/* write the payload header */
	if (ulp_hdr_len) {
		if (xio_mbuf_write_array(
		    &task->mbuf,
		    task->omsg->out.header.iov_base,
		    task->omsg->out.header.iov_len) != 0)
			goto cleanup;
	}

	/* write the pad between header and data */
	if (ulp_pad_len)
		xio_mbuf_inc(&task->mbuf, ulp_pad_len);

	return 0;

cleanup:
	xio_set_error(XIO_E_MSG_SIZE);
	ERROR_LOG("xio_rdma_write_rsp_header failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_write_send_data						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_write_send_data(struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	XIO_TO_RDMA_HNDL(task, rdma_hndl);
	struct xio_mr		*xmr;
	struct xio_device	*dev = rdma_hndl->tcq->dev;
	struct ibv_mr		*mr;
	struct ibv_sge		*sge;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;
	size_t			i;

	sgtbl		= xio_sg_table_get(&task->omsg->out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->out.sgl_type);

	/* user provided mr */
	sg = sge_first(sgtbl_ops, sgtbl);
	if (sge_mr(sgtbl_ops, sg)) {
		sge = &rdma_task->txd.sge[1];
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			xmr = (struct xio_mr *)sge_mr(sgtbl_ops, sg);
			if (unlikely(!xmr)) {
				ERROR_LOG("failed to find mr on iov\n");
				goto cleanup;
			}

			/* get the corresponding key of the
			 * outgoing adapter */
			mr = xio_rdma_mr_lookup(xmr, dev);
			if (unlikely(!mr)) {
				ERROR_LOG("failed to find memory " \
						"handle\n");
				goto cleanup;
			}
			/* copy the iovec */
			/* send it on registered memory */
			sge->addr    = uint64_from_ptr(sge_addr(sgtbl_ops, sg));
			sge->length  = (uint32_t)sge_length(sgtbl_ops, sg);
			sge->lkey    = mr->lkey;
			sge++;
		}
		rdma_task->txd.send_wr.num_sge =
			tbl_nents(sgtbl_ops, sgtbl) + 1;
	} else {
		/* copy to internal buffer */
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			/* copy the data into internal buffer */
			if (xio_mbuf_write_array(
				&task->mbuf,
				sge_addr(sgtbl_ops, sg),
				sge_length(sgtbl_ops, sg)) != 0)
				goto cleanup;
		}
		rdma_task->txd.send_wr.num_sge = 1;
	}

	return 0;

cleanup:
	xio_set_error(XIO_E_MSG_SIZE);
	ERROR_LOG("xio_rdma_send_msg failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_prep_rsp_out_data						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_prep_rsp_out_data(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_rdma_rsp_hdr	rsp_hdr;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;
	struct xio_reg_mem	*write_reg_mem;
	size_t			retval, nents;
	uint64_t		xio_hdr_len;
	uint64_t		ulp_imm_len;
	uint16_t		ulp_hdr_len;
	uint16_t		ulp_pad_len = 0;
	uint32_t		i;
	int			enforce_write_rsp;

	sgtbl		= xio_sg_table_get(&task->omsg->out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->out.sgl_type);
	nents		= tbl_nents(sgtbl_ops, sgtbl);

	/* calculate headers */
	ulp_hdr_len = task->omsg->out.header.iov_len;
	ulp_imm_len = tbl_length(sgtbl_ops, sgtbl);

	xio_hdr_len = xio_mbuf_get_curr_offset(&task->mbuf);
	xio_hdr_len += sizeof(rsp_hdr);
	xio_hdr_len += rdma_task->req_in_num_sge * sizeof(struct xio_sge);
	enforce_write_rsp = (nents && (task->imsg_flags & XIO_MSG_FLAG_PEER_WRITE_RSP));

	/* force check on application messages only */
	if (ulp_hdr_len > rdma_hndl->peer_max_header &&
	    IS_APPLICATION_MSG(task->tlv_type)) {
		ERROR_LOG("hdr_len=%d is bigger than peer_max_header=%d\n",
				ulp_hdr_len, rdma_hndl->peer_max_header);
		goto cleanup;
	}

	/*
	if (rdma_hndl->max_inline_buf_sz < xio_hdr_len + ulp_hdr_len) {
		ERROR_LOG("header size %lu exceeds max header %lu\n",
			  ulp_hdr_len,
			  rdma_hndl->max_inline_buf_sz - xio_hdr_len);
		xio_set_error(XIO_E_MSG_SIZE);
		goto cleanup;
	}
	*/
	/* initialize the txd */
	rdma_task->txd.send_wr.num_sge = 1;

	if (g_options.inline_xio_data_align && ulp_imm_len) {
		uint16_t hdr_len = xio_hdr_len + ulp_hdr_len;

		ulp_pad_len = ALIGN(hdr_len, g_options.inline_xio_data_align) -
			      hdr_len;
	}

	/* Small data is outgoing via SEND unless the requester explicitly
	 * insisted on RDMA operation and provided resources.
	 * One sge is reserved for the header
	 */
	if (!enforce_write_rsp &&
	    ((ulp_imm_len == 0) || ((xio_hdr_len + ulp_hdr_len +
				     ulp_pad_len + ulp_imm_len) <=
				     (uint64_t)rdma_hndl->max_inline_buf_sz))) {
		rdma_task->out_ib_op = XIO_IB_SEND;
		/* write xio header to the buffer */
		retval = xio_rdma_prep_rsp_header(
				rdma_hndl, task,
				ulp_hdr_len, ulp_pad_len, ulp_imm_len,
				task->status);
		if (retval)
			goto cleanup;

		/* if there is data, set it to buffer or directly to the sge */
		if (ulp_imm_len) {
			retval = xio_rdma_write_send_data(task);
			if (retval)
				goto cleanup;
		} else {
			/* no data at all */
			tbl_set_nents(sgtbl_ops, sgtbl, 0);
		}
	} else {
		if (rdma_task->req_in_sge[0].addr &&
		    rdma_task->req_in_sge[0].length &&
		    rdma_task->req_in_sge[0].stag) {
			/* the data is sent via RDMA_WRITE */
			rdma_task->out_ib_op = XIO_IB_RDMA_WRITE;

			/* prepare rdma write */
			retval = xio_sched_rdma_wr_req(rdma_hndl, task);
			if (unlikely(retval)) {
				ERROR_LOG("Failed to write header\n");
				goto cleanup1;
			}

			/* and the header is sent via SEND */
			/* write xio header to the buffer */
			retval = xio_rdma_prep_rsp_header(
					rdma_hndl, task,
					ulp_hdr_len, 0, ulp_imm_len,
					XIO_E_SUCCESS);
			if (unlikely(retval)) {
				ERROR_LOG("xio_rdma_prep_rsp_header failed\n");
				goto cleanup1;
			}
		} else {
#if 0
			DEBUG_LOG("partial completion of request due " \
				  "to missing, response buffer\n");

			rdma_task->out_ib_op = XIO_IB_SEND;

			/* the client did not provide buffer for response */
			retval = xio_rdma_prep_rsp_header(
					rdma_hndl, task,
					ulp_hdr_len, 0, 0,
					XIO_E_RSP_BUF_SIZE_MISMATCH);

			tbl_set_nents(sgtbl_ops, sgtbl, 0);
#else
			/* the data is outgoing via SEND but the peer will do
			 * RDMA_READ */
			rdma_task->out_ib_op = XIO_IB_RDMA_READ;
			/* user provided mr */
			sg = sge_first(sgtbl_ops, sgtbl);
			if (sge_mr(sgtbl_ops, sg)) {
				write_reg_mem = rdma_task->write_reg_mem;
				for_each_sge(sgtbl, sgtbl_ops, sg, i) {
					write_reg_mem->addr =
						sge_addr(sgtbl_ops, sg);
					write_reg_mem->priv = NULL;
					write_reg_mem->mr = (struct xio_mr *)
							sge_mr(sgtbl_ops, sg);
					write_reg_mem->length =
						sge_length(sgtbl_ops, sg);
					write_reg_mem++;
				}
			} else {
				if (!rdma_hndl->rdma_mempool) {
					xio_set_error(XIO_E_NO_BUFS);
					ERROR_LOG("message /read/write " \
						  "failed - library's " \
						  "memory pool disabled\n");
					goto cleanup1;
				}

				/* user did not provide mr -
				 * take buffers from pool and do copy */
				write_reg_mem = rdma_task->write_reg_mem;
				for_each_sge(sgtbl, sgtbl_ops, sg, i) {
					retval = xio_mempool_alloc(
						rdma_hndl->rdma_mempool,
						sge_length(sgtbl_ops, sg),
						write_reg_mem);
					if (unlikely(retval)) {
						rdma_task->write_num_reg_mem
									= i;
						xio_set_error(ENOMEM);
						ERROR_LOG("mempool is empty" \
							  "for %zd bytes\n",
							  sge_length(sgtbl_ops,
								     sg));
						goto cleanup1;
					}

					write_reg_mem->length =
						sge_length(sgtbl_ops, sg);

					/* copy the data to the buffer */
					memcpy(write_reg_mem->addr,
					       sge_addr(sgtbl_ops, sg),
					       sge_length(sgtbl_ops, sg));
					write_reg_mem++;
				}
			}
			rdma_task->write_num_reg_mem =
				tbl_nents(sgtbl_ops, sgtbl);

			/* write xio header to the buffer */
			retval = xio_rdma_prep_rsp_header(
					rdma_hndl, task,
					ulp_hdr_len, 0, 0, XIO_E_SUCCESS);

			if (unlikely(retval)) {
				ERROR_LOG("Failed to write header\n");
				goto cleanup1;
			}

#endif
		}
	}

	return 0;
#if 1
cleanup1:
	for (i = 0; i < rdma_task->write_num_reg_mem; i++)
		xio_mempool_free(&rdma_task->write_reg_mem[i]);

	rdma_task->write_num_reg_mem = 0;

	ERROR_LOG("xio_rdma_send_msg failed\n");
	return -1;
#endif

cleanup:
	xio_set_error(XIO_E_MSG_SIZE);
	ERROR_LOG("xio_rdma_send_msg failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_prep_req_out_data						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_prep_req_out_data(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_vmsg		*vmsg = &task->omsg->out;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;
	uint64_t		xio_hdr_len;
	uint64_t		xio_max_hdr_len;
	uint64_t		ulp_imm_len;
	size_t			retval;
	uint16_t		ulp_hdr_len;
	uint16_t		ulp_pad_len = 0;
	unsigned int		i;
	int			nents;
	int			tx_by_sr;

	sgtbl		= xio_sg_table_get(&task->omsg->out);

	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->out.sgl_type);
	nents		= tbl_nents(sgtbl_ops, sgtbl);

	/* calculate headers */
	ulp_hdr_len	= vmsg->header.iov_len;
	ulp_imm_len	= tbl_length(sgtbl_ops, sgtbl);

	xio_hdr_len = xio_mbuf_get_curr_offset(&task->mbuf);
	xio_hdr_len += sizeof(struct xio_rdma_req_hdr);
	xio_hdr_len += sizeof(struct xio_sge) * rdma_task->req_in_num_sge;
	xio_max_hdr_len = xio_hdr_len + sizeof(struct xio_sge) * nents;

	if (g_options.inline_xio_data_align && ulp_imm_len) {
		uint16_t hdr_len = xio_hdr_len + ulp_hdr_len;

		ulp_pad_len = ALIGN(hdr_len, g_options.inline_xio_data_align) -
			      hdr_len;
	}

	/* initialize the txd */
	rdma_task->txd.send_wr.num_sge = 1;

	if (test_bits(XIO_MSG_FLAG_PEER_READ_REQ, &task->omsg_flags) && nents)
		tx_by_sr = 0;
	else
		/* test for using send/receive or rdma_read */
		tx_by_sr = (nents <= (rdma_hndl->max_sge - 1) &&
			    ((ulp_hdr_len + ulp_pad_len +
			      ulp_imm_len + xio_max_hdr_len) <=
			     rdma_hndl->max_inline_buf_sz) &&
			    (((int)(ulp_imm_len) <=
			      g_options.max_inline_xio_data) ||
			     ulp_imm_len == 0));

	/* The data is outgoing via SEND
	 * One sge is reserved for the header
	 */
	if (tx_by_sr) {
		rdma_task->out_ib_op = XIO_IB_SEND;
		/* user has small request - no rdma operation expected */
		rdma_task->write_num_reg_mem = 0;

		/* write xio header to the buffer */
		retval = xio_rdma_prep_req_header(
				rdma_hndl, task,
				ulp_hdr_len, ulp_pad_len, ulp_imm_len,
				XIO_E_SUCCESS);
		if (unlikely(retval))
			return -1;

		/* if there is data, set it to buffer or directly to the sge */
		if (ulp_imm_len) {
			retval = xio_rdma_write_send_data(task);
			if (unlikely(retval))
				return -1;
		}
	} else {
		/* the data is outgoing via SEND but the peer will do
		 * RDMA_READ */
		rdma_task->out_ib_op = XIO_IB_RDMA_READ;
		/* user provided mr */
		sg = sge_first(sgtbl_ops, sgtbl);
		if (sge_mr(sgtbl_ops, sg)) {
			for_each_sge(sgtbl, sgtbl_ops, sg, i) {
				rdma_task->write_reg_mem[i].addr =
					sge_addr(sgtbl_ops, sg);
				rdma_task->write_reg_mem[i].ctx = rdma_hndl->base.ctx;
				rdma_task->write_reg_mem[i].priv = NULL;
				rdma_task->write_reg_mem[i].mr =
					(struct xio_mr *)sge_mr(sgtbl_ops, sg);
				rdma_task->write_reg_mem[i].length =
					sge_length(sgtbl_ops, sg);
			}
		} else {
			if (!rdma_hndl->rdma_mempool) {
				xio_set_error(XIO_E_NO_BUFS);
				ERROR_LOG(
					"message /read/write failed - " \
					"library's memory pool disabled\n");
				goto cleanup;
			}

			/* user did not provide mr - take buffers from pool
			 * and do copy */
			for_each_sge(sgtbl, sgtbl_ops, sg, i) {
				retval = xio_mempool_alloc(
						rdma_hndl->rdma_mempool,
						sge_length(sgtbl_ops, sg),
						&rdma_task->write_reg_mem[i]);
				if (unlikely(retval)) {
					rdma_task->write_num_reg_mem = i;
					xio_set_error(ENOMEM);
					ERROR_LOG(
					"mempool is empty for %zd bytes\n",
					sge_length(sgtbl_ops, sg));
					goto cleanup;
				}

				rdma_task->write_reg_mem[i].length =
					sge_length(sgtbl_ops, sg);

				/* copy the data to the buffer */
				memcpy(rdma_task->write_reg_mem[i].addr,
				       sge_addr(sgtbl_ops, sg),
				       sge_length(sgtbl_ops, sg));
			}
		}
		rdma_task->write_num_reg_mem = tbl_nents(sgtbl_ops, sgtbl);

		/* write xio header to the buffer */
		retval = xio_rdma_prep_req_header(
				rdma_hndl, task,
				ulp_hdr_len, 0, 0, XIO_E_SUCCESS);

		if (unlikely(retval)) {
			ERROR_LOG("Failed to write header\n");
			goto cleanup;
		}
	}

	return 0;

cleanup:
	for (i = 0; i < rdma_task->write_num_reg_mem; i++)
		xio_mempool_free(&rdma_task->write_reg_mem[i]);

	rdma_task->write_num_reg_mem = 0;

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_prep_req_in_data						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_prep_req_in_data(
		struct xio_rdma_transport *rdma_hndl,
		struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	size_t				hdr_len;
	size_t				pad_len = 0;
	size_t				xio_hdr_len;
	size_t				data_len;
	struct xio_vmsg			*vmsg = &task->omsg->in;
	unsigned int			i;
	int				retval;
	struct xio_sg_table_ops		*sgtbl_ops;
	void				*sgtbl;
	void				*sg;
	int				enforce_write_rsp;
	int				nents;

	sgtbl		= xio_sg_table_get(&task->omsg->in);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->in.sgl_type);
	nents		= tbl_nents(sgtbl_ops, sgtbl);

	if (nents == 0) {
		rdma_task->in_ib_op = XIO_IB_SEND;
		rdma_task->read_num_reg_mem = 0;
		return 0;
	}

	data_len = tbl_length(sgtbl_ops, sgtbl);
	hdr_len  = vmsg->header.iov_len;
	if (hdr_len > rdma_hndl->peer_max_header) {
		ERROR_LOG("hdr_len=%d is bigger than peer_max_header=%d\n",
				hdr_len, rdma_hndl->peer_max_header);
		return -1;
	} else if (!hdr_len) {
		hdr_len = rdma_hndl->peer_max_header;
	}

	/* before working on the out - current place after the session header */
	xio_hdr_len = xio_mbuf_get_curr_offset(&task->mbuf);
	xio_hdr_len += sizeof(struct xio_rdma_rsp_hdr);
	xio_hdr_len += sizeof(struct xio_sge) * nents;

	if (g_options.inline_xio_data_align && data_len) {
		uint16_t hdr_len_ex = xio_hdr_len + hdr_len;

		pad_len = ALIGN(hdr_len_ex, g_options.inline_xio_data_align) -
				hdr_len_ex;
	}

	/* requester may insist on RDMA for small buffers to eliminate copy
	 * from receive buffers to user buffers
	 */
	enforce_write_rsp = task->omsg_flags & XIO_MSG_FLAG_PEER_WRITE_RSP;
	if (!enforce_write_rsp &&
	    data_len + pad_len + hdr_len + xio_hdr_len <= rdma_hndl->max_inline_buf_sz) {
		/* user has small response - no rdma operation expected */
		rdma_task->in_ib_op = XIO_IB_SEND;
		rdma_task->read_num_reg_mem = (data_len) ? nents : 0;
	} else  {
		/* user provided buffers with length for RDMA WRITE */
		/* user provided mr */
		rdma_task->in_ib_op = XIO_IB_RDMA_WRITE;
		sg = sge_first(sgtbl_ops, sgtbl);
		if (sge_mr(sgtbl_ops, sg)) {
			for_each_sge(sgtbl, sgtbl_ops, sg, i) {
				rdma_task->read_reg_mem[i].addr =
					sge_addr(sgtbl_ops, sg);
				rdma_task->read_reg_mem[i].priv = NULL;
				rdma_task->read_reg_mem[i].ctx = rdma_hndl->base.ctx;
				rdma_task->read_reg_mem[i].mr =
					(struct xio_mr *)sge_mr(sgtbl_ops, sg);
				rdma_task->read_reg_mem[i].length =
					sge_length(sgtbl_ops, sg);
			}
		} else  {
			if (!rdma_hndl->rdma_mempool) {
				xio_set_error(XIO_E_NO_BUFS);
				ERROR_LOG(
					"message /read/write failed - " \
					"library's memory pool disabled\n");
				goto cleanup;
			}

			/* user did not provide mr */
			for_each_sge(sgtbl, sgtbl_ops, sg, i) {
				retval = xio_mempool_alloc(
						rdma_hndl->rdma_mempool,
						sge_length(sgtbl_ops, sg),
						&rdma_task->read_reg_mem[i]);

				if (unlikely(retval)) {
					rdma_task->read_num_reg_mem = i;
					xio_set_error(ENOMEM);
					ERROR_LOG(
					"mempool is empty for %zd bytes\n",
					sge_length(sgtbl_ops, sg));
					goto cleanup;
				}
				rdma_task->read_reg_mem[i].length =
					sge_length(sgtbl_ops, sg);
			}
		}
		rdma_task->read_num_reg_mem = nents;
	}
	/*
	if (rdma_task->read_num_reg_mem > rdma_hndl->peer_max_out_iovsz) {
		ERROR_LOG("request in iovlen %d is bigger then peer " \
			  "max out iovlen %d\n",
			  rdma_task->read_num_reg_mem,
			  rdma_hndl->peer_max_out_iovsz);
		goto cleanup;
	}
	*/

	return 0;

cleanup:
	xio_free_rdma_rd_mem(rdma_hndl, task);
	xio_set_error(EMSGSIZE);

	return -1;
}

/*---------------------------------------------------------------------------*/
/* verify_req_send_limits						     */
/*---------------------------------------------------------------------------*/
static int verify_req_send_limits(const struct xio_rdma_transport *rdma_hndl)
{
	if (rdma_hndl->reqs_in_flight_nr + rdma_hndl->rsps_in_flight_nr >=
	    rdma_hndl->max_tx_ready_tasks_num - 1) {
		DEBUG_LOG("over limits reqs_in_flight_nr=%u, "\
			  "rsps_in_flight_nr=%u, max_tx_ready_tasks_num=%u, rdma_hndl=%p\n",
			  rdma_hndl->reqs_in_flight_nr,
			  rdma_hndl->rsps_in_flight_nr,
			  rdma_hndl->max_tx_ready_tasks_num,
			  rdma_hndl);
		xio_set_error(EAGAIN);
		return -1;
	}

	if (rdma_hndl->reqs_in_flight_nr >=
			rdma_hndl->max_tx_ready_tasks_num - 2) {
		DEBUG_LOG("over limits reqs_in_flight_nr=%u, " \
			  "max_tx_ready_tasks_num=%u, rdma_hndl=%p\n",
			  rdma_hndl->reqs_in_flight_nr,
			  rdma_hndl->max_tx_ready_tasks_num, rdma_hndl);
		xio_set_error(EAGAIN);
		return -1;
	}
	/* tx ready is full - refuse request */
	if (rdma_hndl->tx_ready_tasks_num >=
			rdma_hndl->max_tx_ready_tasks_num) {
		DEBUG_LOG("over limits tx_ready_tasks_num=%u, " \
			  "max_tx_ready_tasks_num=%u, " \
			  "reqs_in_flight_nr=%u, " \
			  "rsps_in_flight_nr=%u, rdma_hndl=%p\n",
			  rdma_hndl->tx_ready_tasks_num,
			  rdma_hndl->max_tx_ready_tasks_num,
			  rdma_hndl->reqs_in_flight_nr,
			  rdma_hndl->rsps_in_flight_nr,
			  rdma_hndl);
		xio_set_error(EAGAIN);
		return -1;
	}
	return 0;
}

/*---------------------------------------------------------------------------*/
/* verify_rsp_send_limits						     */
/*---------------------------------------------------------------------------*/
static int verify_rsp_send_limits(const struct xio_rdma_transport *rdma_hndl)
{
	if (rdma_hndl->reqs_in_flight_nr + rdma_hndl->rsps_in_flight_nr >=
	    rdma_hndl->max_tx_ready_tasks_num - 1) {
		DEBUG_LOG("over limits reqs_in_flight_nr=%u, "\
			  "rsps_in_flight_nr=%u, max_tx_ready_tasks_num=%u, " \
			  "rdma_hndl=%p\n",
			  rdma_hndl->reqs_in_flight_nr,
			  rdma_hndl->rsps_in_flight_nr,
			  rdma_hndl->max_tx_ready_tasks_num,
			  rdma_hndl);
		xio_set_error(EAGAIN);
		return -1;
	}

	if (rdma_hndl->rsps_in_flight_nr >=
			rdma_hndl->max_tx_ready_tasks_num - 2) {
		DEBUG_LOG("over limits rsps_in_flight_nr=%u, " \
			  "max_tx_ready_tasks_num=%u, rdma_hndl=%p\n",
			  rdma_hndl->rsps_in_flight_nr,
			  rdma_hndl->max_tx_ready_tasks_num,
			  rdma_hndl);
		xio_set_error(EAGAIN);
		return -1;
	}
	/* tx ready is full - refuse request */
	if (rdma_hndl->tx_ready_tasks_num >=
			rdma_hndl->max_tx_ready_tasks_num) {
		DEBUG_LOG("over limits tx_ready_tasks_num=%u, " \
			  "max_tx_ready_tasks_num=%u, " \
			  "reqs_in_flight_nr=%u, " \
			  "rsps_in_flight_nr=%u, rdma_hndl=%p\n",
			  rdma_hndl->tx_ready_tasks_num,
			  rdma_hndl->max_tx_ready_tasks_num,
			  rdma_hndl->reqs_in_flight_nr,
			  rdma_hndl->rsps_in_flight_nr,
			  rdma_hndl);
		xio_set_error(EAGAIN);
		return -1;
	}
	return 0;
}

/*---------------------------------------------------------------------------*/
/* kick_send_and_read							     */
/*---------------------------------------------------------------------------*/
static int kick_send_and_read(struct xio_rdma_transport *rdma_hndl,
			      struct xio_task *task,
			      int must_send)
{
	int retval = 0;

	/* transmit only if available */
	if (test_bits(XIO_MSG_FLAG_LAST_IN_BATCH, &task->omsg->flags) ||
	    task->is_control) {
		must_send = 1;
	} else {
		if (tx_window_sz(rdma_hndl) >= SEND_THRESHOLD)
			must_send = 1;
	}


	/* resource are now available and rdma rd  requests are pending kick
	 * them
	 */
	if (rdma_hndl->kick_rdma_rd_req) {
		retval = xio_xmit_rdma_rd_req(rdma_hndl);
		if (retval) {
			retval = xio_errno();
			if (retval != EAGAIN) {
				ERROR_LOG("xio_xmit_rdma_rd_req failed. %s\n",
					  xio_strerror(retval));
				return -1;
			}
			retval = 0;
		}
	}
	if (rdma_hndl->kick_rdma_rd_rsp) {
		retval = xio_xmit_rdma_rd_rsp(rdma_hndl);
		if (retval) {
			retval = xio_errno();
			if (retval != EAGAIN) {
				ERROR_LOG("xio_xmit_rdma_rd_rsp failed. %s\n",
					  xio_strerror(retval));
				return -1;
			}
			retval = 0;
		}
	}
	if (must_send) {
		retval = xio_rdma_xmit(rdma_hndl);
		if (retval) {
			retval = xio_errno();
			if (retval != EAGAIN) {
				ERROR_LOG("xio_xmit_rdma failed. %s\n",
					  xio_strerror(retval));
				return -1;
			}
			retval = 0;
		}
	}
	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_send_req							     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_send_req(struct xio_rdma_transport *rdma_hndl,
			     struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_mbuf		*mbuf = &task->mbuf;
	struct xio_work_req	*txd;
	struct ibv_sge		*sge;
	uint64_t		payload;
	size_t			retval;
	size_t			sge_len;
	int			i;
	int			must_send = 0;

	if (unlikely(verify_req_send_limits(rdma_hndl))) {
		xio_rdma_xmit(rdma_hndl);
		return -1;
	}

	/* prepare buffer for RDMA response  */
	retval = xio_rdma_prep_req_in_data(rdma_hndl, task);
	if (unlikely(retval != 0)) {
		ERROR_LOG("rdma_prep_req_in_data failed\n");
		return -1;
	}
	/* prepare the out message  */
	retval = xio_rdma_prep_req_out_data(rdma_hndl, task);
	if (unlikely(retval != 0)) {
		ERROR_LOG("rdma_prep_req_out_data failed\n");
		return -1;
	}

	payload = xio_mbuf_tlv_payload_len(mbuf);

	/* add tlv */
	if (xio_mbuf_write_tlv(mbuf, task->tlv_type, payload) != 0) {
		ERROR_LOG("write tlv failed\n");
		xio_set_error(EOVERFLOW);
		return -1;
	}

	txd = &rdma_task->txd;
	sge = &txd->sge[0];

	/* set the length */
	sge->length = xio_mbuf_get_curr_offset(mbuf);
	sge_len = sge->length;

	/* validate header */
	if (unlikely(XIO_TLV_LEN + payload != sge_len)) {
		ERROR_LOG("header validation failed\n");
		return -1;
	}
	xio_task_addref(task);

	/* check for inline */
	txd->send_wr.send_flags = 0;

	sge++;
	for (i = 1; i < txd->send_wr.num_sge; i++) {
		sge_len += sge->length;
		sge++;
	}

	if (sge_len < (size_t)rdma_hndl->max_inline_data)
		txd->send_wr.send_flags |= IBV_SEND_INLINE;

	if (IS_FIN(task->tlv_type)) {
		txd->send_wr.send_flags |= IBV_SEND_FENCE;
		must_send = 1;
	}

	if (unlikely(++rdma_hndl->req_sig_cnt >= HARD_CQ_MOD ||
		     task->is_control ||
		     task->omsg->flags & XIO_MSG_FLAG_IMM_SEND_COMP)) {
		/* avoid race between send completion and response arrival */
		txd->send_wr.send_flags |= IBV_SEND_SIGNALED;
		rdma_hndl->req_sig_cnt = 0;
	}

	rdma_task->out_ib_op = XIO_IB_SEND;

	if (IS_KEEPALIVE(task->tlv_type))
		list_move(&task->tasks_list_entry, &rdma_hndl->tx_ready_list);
	else
		list_move_tail(&task->tasks_list_entry, &rdma_hndl->tx_ready_list);

	rdma_hndl->tx_ready_tasks_num++;

	return kick_send_and_read(rdma_hndl, task, must_send);
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_send_rsp							     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_send_rsp(struct xio_rdma_transport *rdma_hndl,
			     struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_mbuf		*mbuf = &task->mbuf;
	struct xio_work_req	*txd;
	struct ibv_sge		*sge;
	size_t			retval;
	size_t			sge_len;
	uint64_t		payload;
	int			i;
	int			must_send = 0;

	if (unlikely(verify_rsp_send_limits(rdma_hndl))) {
		xio_rdma_xmit(rdma_hndl);
		return -1;
	}

	if (task->on_hold) {
		/* dynamically initialize header */
		struct xio_tasks_slab *slab =
				(struct xio_tasks_slab *)task->sender_task->slab;
		struct xio_rdma_tasks_slab *rdma_slab =
				(struct xio_rdma_tasks_slab *)slab->dd_data;
		struct ibv_mr *data_mr = xio_rdma_mr_lookup(rdma_slab->data_mr,
				                                    rdma_hndl->tcq->dev);
		rdma_task->txd.sge[0].addr =
				uint64_from_ptr(xio_mbuf_buf_head(&task->mbuf));
		rdma_task->txd.sge[0].lkey = data_mr->lkey;
	}

	/* prepare the out message  */
	retval = xio_rdma_prep_rsp_out_data(rdma_hndl, task);
	if (unlikely(retval != 0)) {
		ERROR_LOG("rdma_prep_rsp_out_data failed\n");
		goto cleanup;
	}

	payload = xio_mbuf_tlv_payload_len(mbuf);

	/* add tlv */
	if (xio_mbuf_write_tlv(mbuf, task->tlv_type, payload) != 0)
		goto cleanup;

	txd = &rdma_task->txd;
	sge = &txd->sge[0];

	/* set the length */
	sge->length = xio_mbuf_get_curr_offset(mbuf);
	sge_len = sge->length;

	/* validate header */
	if (unlikely(XIO_TLV_LEN + payload != sge_len)) {
		ERROR_LOG("header validation failed\n");
		goto cleanup;
	}

	txd->send_wr.send_flags = 0;
	if (++rdma_hndl->rsp_sig_cnt >= SOFT_CQ_MOD || task->is_control ||
	    task->omsg->flags & XIO_MSG_FLAG_IMM_SEND_COMP) {
		rdma_task->txd.send_wr.send_flags |= IBV_SEND_SIGNALED;
		rdma_hndl->rsp_sig_cnt = 0;
	}

	/* check for inline */
	if (rdma_task->out_ib_op == XIO_IB_SEND ||
	    rdma_task->out_ib_op == XIO_IB_RDMA_READ) {
		sge++;
		for (i = 1; i < txd->send_wr.num_sge; i++) {
			sge_len += sge->length;
			sge++;
		}

		if (sge_len < (size_t)rdma_hndl->max_inline_data)
			txd->send_wr.send_flags |= IBV_SEND_INLINE;

		if (IS_KEEPALIVE(task->tlv_type))
			list_move(&task->tasks_list_entry,
				  &rdma_hndl->tx_ready_list);
		else
			list_move_tail(&task->tasks_list_entry,
				       &rdma_hndl->tx_ready_list);

		rdma_hndl->tx_ready_tasks_num++;
	}

	if (IS_FIN(task->tlv_type)) {
		rdma_task->txd.send_wr.send_flags |= IBV_SEND_FENCE;
		must_send = 1;
	}
	if (rdma_task->out_ib_op == XIO_IB_RDMA_READ)
		xio_task_addref(task);

	return kick_send_and_read(rdma_hndl, task, must_send);
cleanup:
	xio_set_error(XIO_E_MSG_SIZE);
	ERROR_LOG("xio_rdma_send_msg failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_on_rsp_send_comp						     */
/*---------------------------------------------------------------------------*/
int xio_rdma_on_rsp_send_comp(struct xio_rdma_transport *rdma_hndl,
			      struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	union xio_transport_event_data event_data;

	if (task->on_hold) {
		/* dynamically initialize header */
		struct xio_tasks_slab *slab =
				(struct xio_tasks_slab *)task->slab;
		struct xio_rdma_tasks_slab *rdma_slab =
				(struct xio_rdma_tasks_slab *)slab->dd_data;
		struct ibv_mr *data_mr = xio_rdma_mr_lookup(rdma_slab->data_mr,
				                            rdma_hndl->tcq->dev);
		rdma_task->txd.sge[0].addr =
				uint64_from_ptr(xio_mbuf_buf_head(&task->sender_task->mbuf));
		rdma_task->txd.sge[0].lkey = data_mr->lkey;
	}
	if (IS_KEEPALIVE(task->tlv_type)) {
		if (task->ka_probes)
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, rdma_hndl:%p\n",
					__func__, task->tlv_type, task->session, task->connection,
					rdma_hndl);
	} else {
		if (!IS_NOP(task->tlv_type) && !IS_APPLICATION_MSG(task->tlv_type))
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, rdma_hndl:%p\n",
					__func__, task->tlv_type, task->session, task->connection,
					rdma_hndl);
	}

	if (rdma_task->out_ib_op == XIO_IB_RDMA_READ) {
		xio_tasks_pool_put(task);
		return 0;
	}

	event_data.msg.op	= XIO_WC_OP_SEND;
	event_data.msg.task	= task;

	xio_transport_notify_observer(&rdma_hndl->base,
				      XIO_TRANSPORT_EVENT_SEND_COMPLETION,
				      &event_data);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_on_req_send_comp						     */
/*---------------------------------------------------------------------------*/
int xio_rdma_on_req_send_comp(struct xio_rdma_transport *rdma_hndl,
			      struct xio_task *task)
{
	union xio_transport_event_data event_data;

	if (IS_KEEPALIVE(task->tlv_type)) {
		if (task->ka_probes)
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, rdma_hndl:%p\n",
					__func__, task->tlv_type, task->session, task->connection,
					rdma_hndl);
	} else {
		if (!IS_NOP(task->tlv_type) && !IS_APPLICATION_MSG(task->tlv_type))
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, rdma_hndl:%p\n",
					__func__, task->tlv_type, task->session, task->connection,
					rdma_hndl);
	}

	event_data.msg.op	= XIO_WC_OP_SEND;
	event_data.msg.task	= task;

	xio_transport_notify_observer(&rdma_hndl->base,
				      XIO_TRANSPORT_EVENT_SEND_COMPLETION,
				      &event_data);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_on_direct_rdma_comp						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_on_direct_rdma_comp(struct xio_rdma_transport *rdma_hndl,
					struct xio_task *task,
					enum xio_wc_op op)
{
	union xio_transport_event_data event_data;

	event_data.msg.op = op;
	event_data.msg.task = task;
	xio_transport_notify_observer(
		&rdma_hndl->base,
		XIO_TRANSPORT_EVENT_DIRECT_RDMA_COMPLETION,
		&event_data);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_post_recv_rsp						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_post_recv_rsp(struct xio_task *task)
{
	struct xio_msg		*imsg;
	struct xio_msg		*omsg;
	struct xio_sg_table_ops	*isgtbl_ops;
	void			*isgtbl;
	struct xio_sg_table_ops	*osgtbl_ops;
	void			*osgtbl;

	omsg		= task->sender_task->omsg;
	imsg		= &task->imsg;
	isgtbl		= xio_sg_table_get(&imsg->in);
	isgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(imsg->in.sgl_type);
	osgtbl		= xio_sg_table_get(&omsg->in);
	osgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(omsg->in.sgl_type);

	/* use provided only length - set user
	 * pointers */
	tbl_clone(osgtbl_ops, osgtbl, isgtbl_ops, isgtbl);

	/* also set bits */
	if (test_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF, &imsg->hints))
		set_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF, &omsg->hints);
	else
		clr_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF, &omsg->hints);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_on_recv_rsp							     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_on_recv_rsp(struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	XIO_TO_RDMA_TASK(task, rdma_sender_task);
	struct xio_task		*sender_task;
	union xio_transport_event_data event_data;
	struct xio_rdma_rsp_hdr	rsp_hdr;
	struct xio_sg_table_ops	*isgtbl_ops;
	struct xio_sg_table_ops	*osgtbl_ops;
	struct xio_msg		*imsg;
	struct xio_msg		*omsg;
	void			*ulp_hdr;
	void			*isgtbl;
	void			*osgtbl;
	void			*sg;
	unsigned int		i;
	int			retval = 0;
	int			header_err = 1; /* temporary do not activate */

	/* read the response header */
	retval = xio_rdma_read_rsp_header(rdma_hndl, task, &rsp_hdr);
	if (retval != 0) {
		xio_set_error(XIO_E_MSG_INVALID);
		header_err = 1;
		goto cleanup;
	}
	/* find the sender task */
	if (task->sender_task == NULL) {
		sender_task =
			xio_rdma_primary_task_lookup(rdma_hndl,
						     rsp_hdr.rtid);
		if (sender_task == NULL) {
			ERROR_LOG("sender task not found!!!!!. Releasing incoming response. rdma_hndl:%p\n", rdma_hndl);
			xio_tasks_pool_put(task);
			return 0;
		}
		task->sender_task = sender_task;
	} else {
		sender_task = task->sender_task;
	}
	/* update receive + send window */
	if (rdma_hndl->exp_sn == rsp_hdr.sn) {
		rdma_hndl->exp_sn++;
		rdma_hndl->ack_sn = rsp_hdr.sn;
		rdma_hndl->peer_credits += rsp_hdr.credits;
	} else {
		ERROR_LOG("ERROR: expected sn:%d, arrived sn:%d, rdma_hndl:%p\n",
			  rdma_hndl->exp_sn, rsp_hdr.sn, rdma_hndl);
	}
	if (!xio_transport_is_task_routable(sender_task)) {
		ERROR_LOG("invalid sender task. Releasing incoming response. rdma_hndl:%p\n", rdma_hndl);
		xio_tasks_pool_put(task);
		return 0;
	}
	if (!sender_task->omsg) {
		ERROR_LOG("null sender_task->omsg. Releasing incoming response. rdma_hndl:%p\n",
			  sender_task->tlv_type,
			  rdma_hndl);
		xio_tasks_pool_put(task);
		return 0;
	}
	if (IS_KEEPALIVE(task->tlv_type)) {
		if (sender_task->ka_probes)
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, rdma_hndl:%p\n",
					__func__, task->tlv_type, sender_task->session, sender_task->connection,
					rdma_hndl);
	} else {
		if (!IS_NOP(task->tlv_type) && !IS_APPLICATION_MSG(task->tlv_type))
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, rdma_hndl:%p\n",
					__func__, task->tlv_type, sender_task->session, sender_task->connection,
					rdma_hndl);
	}

	/* read the sn */
	rdma_task->sn = rsp_hdr.sn;
	task->sender_task = sender_task;
	rdma_sender_task = (struct xio_rdma_task *)sender_task->dd_data;
	task->rtid	 = rsp_hdr.ltid;

	/* mark the sender task as arrived */
	sender_task->state = XIO_TASK_STATE_RESPONSE_RECV;

	omsg		= sender_task->omsg;
	imsg		= &task->imsg;
	isgtbl		= xio_sg_table_get(&imsg->in);
	isgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(imsg->in.sgl_type);
	osgtbl		= xio_sg_table_get(&omsg->in);
	osgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(omsg->in.sgl_type);

	clr_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF, &imsg->hints);

	ulp_hdr = xio_mbuf_get_curr_ptr(&task->mbuf);
	/* msg from received message */
	if (rsp_hdr.ulp_hdr_len) {
		imsg->in.header.iov_base	= ulp_hdr;
		imsg->in.header.iov_len		= rsp_hdr.ulp_hdr_len;
	} else {
		imsg->in.header.iov_base	= NULL;
		imsg->in.header.iov_len		= 0;
	}
	task->status = rsp_hdr.status;

	/* handle the headers */
	if (omsg->in.header.iov_base) {
		/* copy header to user buffers */
		size_t hdr_len = 0;

		if (imsg->in.header.iov_len > omsg->in.header.iov_len)  {
			hdr_len = omsg->in.header.iov_len;
			task->status = XIO_E_MSG_SIZE;
		} else {
			hdr_len = imsg->in.header.iov_len;
		}
		if (hdr_len && imsg->in.header.iov_base)
			memcpy(omsg->in.header.iov_base,
			       imsg->in.header.iov_base,
			       hdr_len);
		else
			*((char *)omsg->in.header.iov_base) = 0;

		omsg->in.header.iov_len = hdr_len;
	} else {
		/* no copy - just pointers */
		memclonev(&omsg->in.header, 1, &imsg->in.header, 1);
	}

	switch (rsp_hdr.out_ib_op) {
	case XIO_IB_SEND:
			/* if data arrived, set the pointers */
		if (rsp_hdr.ulp_imm_len) {
			tbl_set_nents(isgtbl_ops, isgtbl, 1);
			sg = sge_first(isgtbl_ops, isgtbl);
			sge_set_addr(isgtbl_ops, sg,
				     (ulp_hdr + imsg->in.header.iov_len +
				     rsp_hdr.ulp_pad_len));
			sge_set_length(isgtbl_ops, sg,
				       rsp_hdr.ulp_imm_len);
		} else {
			tbl_set_nents(isgtbl_ops, isgtbl, 0);
		}
		if (tbl_nents(osgtbl_ops, osgtbl)) {
			/* deep copy */
			if (tbl_nents(isgtbl_ops, isgtbl)) {
				size_t idata_len  =
					tbl_length(isgtbl_ops, isgtbl);
				size_t odata_len  =
					tbl_length(osgtbl_ops, osgtbl);

				if (idata_len > odata_len) {
					task->status = XIO_E_MSG_SIZE;
					goto msg_comp;
				}
				sg = sge_first(osgtbl_ops, osgtbl);
				if (sge_addr(osgtbl_ops, sg))  {
					/* user provided buffer so do copy */
					tbl_copy(osgtbl_ops, osgtbl,
						 isgtbl_ops, isgtbl);
				} else {
					/* use provided only length - set user
					 * pointers */
					tbl_clone(osgtbl_ops, osgtbl,
						  isgtbl_ops, isgtbl);
				}
			} else {
				tbl_set_nents(osgtbl_ops, osgtbl,
					      tbl_nents(isgtbl_ops, isgtbl));
			}
		} else {
			tbl_clone(osgtbl_ops, osgtbl,
				  isgtbl_ops, isgtbl);
		}
		break;
	case XIO_IB_RDMA_WRITE:
		if (rdma_task->rsp_out_num_sge >
		    rdma_sender_task->read_num_reg_mem) {
			ERROR_LOG("local in data_iovec is too small %d < %d\n",
				  rdma_sender_task->read_num_reg_mem,
				  rdma_task->rsp_out_num_sge);
			task->status = XIO_E_MSG_SIZE;
			goto msg_comp;
		}

		tbl_set_nents(isgtbl_ops, isgtbl,
			      rdma_task->rsp_out_num_sge);

		sg = sge_first(isgtbl_ops, isgtbl);
		for (i = 0; i < rdma_task->rsp_out_num_sge; i++) {
			sge_set_addr(isgtbl_ops, sg,
				     ptr_from_int64(
				       rdma_sender_task->read_reg_mem[i].addr));
			sge_set_length(isgtbl_ops, sg,
				       rdma_task->rsp_out_sge[i].length);
			sg = sge_next(isgtbl_ops, isgtbl, sg);
		}

		if (tbl_nents(osgtbl_ops, osgtbl)) {
			/* user provided mr */
			sg = sge_first(osgtbl_ops, osgtbl);
			if (sge_mr(osgtbl_ops, sg))  {
				void *isg;
				/* data was copied directly to user buffer */
				/* need to update the buffer length */
				for_each_sge(isgtbl, isgtbl_ops, isg, i) {
					sge_set_length(
						osgtbl_ops, sg,
						sge_length(isgtbl_ops,
							   isg));

					sg = sge_next(osgtbl_ops,
						      osgtbl, sg);
				}
				tbl_set_nents(osgtbl_ops, osgtbl,
					      tbl_nents(isgtbl_ops, isgtbl));
			} else  {
				/* user provided buffer but not mr */
				/* deep copy */
				if (sge_addr(osgtbl_ops, sg))  {
					tbl_copy(osgtbl_ops, osgtbl,
						 isgtbl_ops, isgtbl);
					/* put buffers back to pool */
					for (
					i = 0;
					i < rdma_sender_task->read_num_reg_mem;
					i++) {
						xio_mempool_free(
					    &rdma_sender_task->read_reg_mem[i]);
					rdma_sender_task->read_reg_mem[i].ctx =
						rdma_hndl->base.ctx;
					rdma_sender_task->read_reg_mem[i].priv =
						NULL;
					}
					rdma_sender_task->read_num_reg_mem = 0;
				} else {
					/* use provided only length - set user
					 * pointers */
					tbl_clone(osgtbl_ops, osgtbl,
						  isgtbl_ops, isgtbl);
				}
			}
		} else {
			ERROR_LOG("empty out message\n");
		}
		break;
	case XIO_IB_RDMA_READ:
		/* schedule request for RDMA READ. in case of error
		 * don't schedule the rdma read operation */
		/*TRACE_LOG("scheduling rdma read\n");*/
		retval = xio_sched_rdma_rd(rdma_hndl, task);
		if (retval == 0)
			return 0;
		task->status = xio_errno();
		break;
	default:
		ERROR_LOG("%s unexpected op 0x%x\n", __func__,
			  rsp_hdr.out_ib_op);
		task->status = EINVAL;
		break;
	}

msg_comp:
	/* must delay the send due to pending rdma read responses
	 * if not user will get out of order messages - need fence
	 */
	if (!list_empty(&rdma_hndl->rdma_rd_rsp_list)) {
		list_move_tail(&task->tasks_list_entry,
			       &rdma_hndl->rdma_rd_rsp_list);
		rdma_hndl->kick_rdma_rd_rsp = 1;
		return 0;
	}
	if (rdma_hndl->rdma_rd_rsp_in_flight) {
		rdma_hndl->rdma_rd_rsp_in_flight++;
		list_move_tail(&task->tasks_list_entry,
			       &rdma_hndl->rdma_rd_rsp_in_flight_list);
		return 0;
	}

	/* fill notification event */
	event_data.msg.op	= XIO_WC_OP_RECV;
	event_data.msg.task	= task;

	if (task->status)
		xio_free_rdma_task_mem(&rdma_hndl->base, task);

	/* notify the upper layer of received message */
	xio_transport_notify_observer(&rdma_hndl->base,
				      XIO_TRANSPORT_EVENT_NEW_MESSAGE,
				      &event_data);
	return 0;

cleanup:
	retval = xio_errno();
	ERROR_LOG("xio_rdma_on_recv_rsp failed. (errno=%d %s)\n",
		  retval, xio_strerror(retval));
	if (header_err)
		xio_transport_notify_observer_error(&rdma_hndl->base, retval);
	else
		xio_transport_notify_message_error(&rdma_hndl->base, task,
				XIO_MSG_DIRECTION_IN, (enum xio_status)retval);

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_prep_rdma_op							     */
/*---------------------------------------------------------------------------*/
static int xio_prep_rdma_op(
		struct xio_task *task,
		struct xio_rdma_transport *rdma_hndl,
		enum xio_ib_op_code  xio_out_ib_op,
		enum ibv_wr_opcode   opcode,
		struct xio_sge *lsg_list, size_t lsize, size_t *out_lsize,
		struct xio_sge *rsg_list, size_t rsize, size_t *out_rsize,
		uint32_t op_size,
		int	max_sge,
		int	signaled,
		struct list_head *target_list,
		int	tasks_number)
{
	struct xio_task		*tmp_task;
	struct xio_rdma_task	*tmp_rdma_task;
	struct xio_work_req	*rdmad;
	struct xio_task		*ptask, *next_ptask;
	uint64_t		laddr  = lsg_list[0].addr;
	uint64_t		raddr  = rsg_list[0].addr;
	uint64_t		raddr_base  = raddr;
	uint32_t		llen	= lsg_list[0].length;
	uint32_t		rlen	= rsg_list[0].length;
	uint32_t		lkey	= lsg_list[0].stag;
	uint32_t		rkey	= rsg_list[0].stag;
	unsigned int		l = 0, r = 0, k = 0;
	uint32_t		tot_len = 0;
	uint32_t		int_len = 0;
	uint32_t		rint_len = 0;
	int			task_idx;

	LIST_HEAD(tmp_list);

	if (lsize < 1 || rsize < 1) {
		ERROR_LOG("iovec size < 1 lsize:%zd, rsize:%zd\n",
			  lsize, rsize);
		return -1;
	}

	task_idx = tasks_number - 1;

	if (task_idx == 0) {
		tmp_task = task;
	} else {
		/* take new task */
		tmp_task = xio_tasks_pool_get(rdma_hndl->phantom_tasks_pool,
					      rdma_hndl);
		if (unlikely(!tmp_task)) {
			ERROR_LOG("phantom tasks pool is empty\n");
			return -1;
		}
	}
	tmp_rdma_task =
		(struct xio_rdma_task *)tmp_task->dd_data;
	rdmad = &tmp_rdma_task->rdmad;

	while (1) {
		if (rlen < llen) {
			rdmad->send_wr.num_sge		= k + 1;
			rdmad->send_wr.wr_id		=
					uint64_from_ptr(tmp_task);
			rdmad->send_wr.next		= NULL;
			rdmad->send_wr.opcode		= opcode;
			rdmad->send_wr.send_flags	=
					(signaled ? IBV_SEND_SIGNALED : 0);
			rdmad->send_wr.wr.rdma.remote_addr = raddr_base;
			rdmad->send_wr.wr.rdma.rkey	   = rkey;

			rdmad->sge[k].addr		= laddr;
			rdmad->sge[k].length		= rlen;
			rdmad->sge[k].lkey		= lkey;
			k				= 0;

			tot_len				+= rlen;
			int_len				+= rlen;
			rint_len			= 0;
			tmp_rdma_task->out_ib_op	= xio_out_ib_op;
			tmp_rdma_task->phantom_idx	= task_idx;

			/* close the task */
			list_move_tail(&tmp_task->tasks_list_entry, &tmp_list);
			/* advance the remote index */
			r++;
			if (r == rsize) {
				lsg_list[l].length = int_len;
				int_len = 0;
				l++;
				break;
			}
			task_idx--;
			/* Is this the last task */
			if (task_idx) {
				/* take new task */
				tmp_task = xio_tasks_pool_get(
						rdma_hndl->phantom_tasks_pool,
						rdma_hndl);
				if (unlikely(!tmp_task)) {
					ERROR_LOG(
					      "phantom tasks pool is empty\n");
					goto cleanup;
				}
			} else {
				tmp_task = task;
			}

			tmp_rdma_task =
				(struct xio_rdma_task *)tmp_task->dd_data;
			rdmad = &tmp_rdma_task->rdmad;

			llen	-= rlen;
			laddr	+= rlen;
			raddr	= rsg_list[r].addr;
			rlen	= rsg_list[r].length;
			rkey	= rsg_list[r].stag;
			raddr_base  = raddr;
		} else if (llen < rlen) {
			rdmad->sge[k].addr	= laddr;
			rdmad->sge[k].length	= llen;
			rdmad->sge[k].lkey	= lkey;
			tot_len			+= llen;
			int_len			+= llen;
			rint_len		+= llen;

			lsg_list[l].length = int_len;
			int_len = 0;
			/* advance the local index */
			l++;
			k++;
			if (l == lsize || k == (unsigned int)max_sge - 1) {
				rdmad->send_wr.num_sge		= k;
				rdmad->send_wr.wr_id		=
						uint64_from_ptr(tmp_task);
				rdmad->send_wr.next		= NULL;
				rdmad->send_wr.opcode		= opcode;
				rdmad->send_wr.send_flags	=
					   (signaled ? IBV_SEND_SIGNALED : 0);
				rdmad->send_wr.wr.rdma.remote_addr = raddr_base;
				rdmad->send_wr.wr.rdma.rkey	= rkey;
				tmp_rdma_task->out_ib_op	= xio_out_ib_op;
				tmp_rdma_task->phantom_idx	= task_idx;
				/* close the task */
				list_move_tail(&tmp_task->tasks_list_entry,
					       &tmp_list);

				if (l == lsize) {
					rsg_list[r].length = rint_len;
					rint_len = 0;
					r++;
					break;
				}

				/* if we are here then k == max_sge - 1 */

				task_idx--;
				/* Is this the last task */
				if (task_idx) {
					/* take new task */
					tmp_task = xio_tasks_pool_get(
						rdma_hndl->phantom_tasks_pool,
						rdma_hndl);
					if (unlikely(!tmp_task)) {
						ERROR_LOG(
						      "phantom tasks pool is empty\n");
						goto cleanup;
					}
				} else {
					tmp_task = task;
				}

				tmp_rdma_task =
					(struct xio_rdma_task *)
							tmp_task->dd_data;
				rdmad = &tmp_rdma_task->rdmad;
				k = 0;
			}
			rlen	-= llen;
			raddr	+= llen;

			laddr	= lsg_list[l].addr;
			llen	= lsg_list[l].length;
			lkey	= lsg_list[l].stag;
		} else {
			rdmad->send_wr.num_sge		= k + 1;
			rdmad->send_wr.wr_id		=
						uint64_from_ptr(tmp_task);
			rdmad->send_wr.next		= NULL;
			rdmad->send_wr.opcode		= opcode;
			rdmad->send_wr.send_flags	=
					(signaled ? IBV_SEND_SIGNALED : 0);
			rdmad->send_wr.wr.rdma.remote_addr = raddr_base;
			rdmad->send_wr.wr.rdma.rkey	   = rkey;

			rdmad->sge[k].addr		= laddr;
			rdmad->sge[k].length		= llen;
			rdmad->sge[k].lkey		= lkey;
			k				= 0;

			tot_len			       += llen;
			int_len			       += llen;
			rint_len		       += llen;
			tmp_rdma_task->out_ib_op	= xio_out_ib_op;
			tmp_rdma_task->phantom_idx	= task_idx;

			/* close the task */
			list_move_tail(&tmp_task->tasks_list_entry,
				       &tmp_list);

			lsg_list[l].length = int_len;
			int_len = 0;
			rsg_list[r].length = rint_len;
			rint_len = 0;
			/* advance the remote and local indices */
			r++;
			l++;
			if ((l == lsize) || (r == rsize))
				break;

			task_idx--;
			/* Is this the last task */
			if (task_idx) {
				/* take new task */
				tmp_task =
					xio_tasks_pool_get(
						rdma_hndl->phantom_tasks_pool,
						rdma_hndl);
				if (unlikely(!tmp_task)) {
					ERROR_LOG(
					       "phantom tasks pool is empty\n");
					goto cleanup;
				}
			} else {
				tmp_task = task;
			}

			tmp_rdma_task =
				(struct xio_rdma_task *)tmp_task->dd_data;
			rdmad = &tmp_rdma_task->rdmad;

			laddr	= lsg_list[l].addr;
			llen	= lsg_list[l].length;
			lkey	= lsg_list[l].stag;

			raddr	= rsg_list[r].addr;
			rlen	= rsg_list[r].length;
			rkey	= rsg_list[r].stag;
			raddr_base  = raddr;
		}
	}
	*out_lsize = l;
	*out_rsize = r;

	if (tot_len < op_size) {
		ERROR_LOG("iovec exhausted\n");
		goto cleanup;
	}

	list_splice_tail(&tmp_list, target_list);

	return 0;
cleanup:

	/* list does not contain the original task */
	list_for_each_entry_safe(ptask, next_ptask, &tmp_list,
				 tasks_list_entry) {
		/* the tmp tasks are returned back to pool */
		xio_tasks_pool_put(ptask);
	}

	return -1;
}

/*---------------------------------------------------------------------------*/
/* init_lsg_list							     */
/*---------------------------------------------------------------------------*/
static void init_lsg_list(struct xio_rdma_transport *rdma_hndl,
			  struct xio_task *task,
			  struct xio_sge *lsg_list,
			  size_t *lsg_list_len,
			  size_t *llen)
{
	struct xio_sg_table_ops	*sgtbl_ops = (struct xio_sg_table_ops *)
			xio_sg_table_ops_get(task->omsg->out.sgl_type);
	void		*sgtbl = (struct xio_sg_table_ops *)
				xio_sg_table_get(&task->omsg->out);
	struct ibv_mr	*mr;
	void		*sg;
	unsigned int	i;

	*lsg_list_len = tbl_nents(sgtbl_ops, sgtbl);
	*llen = 0;

	for_each_sge(sgtbl, sgtbl_ops, sg, i) {
		lsg_list[i].addr = uint64_from_ptr(sge_addr(sgtbl_ops, sg));
		lsg_list[i].length = sge_length(sgtbl_ops, sg);
		mr = xio_rdma_mr_lookup((struct xio_mr *)sge_mr(sgtbl_ops, sg),
					rdma_hndl->tcq->dev);
		lsg_list[i].stag = mr->lkey;
		*llen += lsg_list[i].length;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_perform_direct_rdma						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_perform_direct_rdma(struct xio_rdma_transport *rdma_hndl,
					struct xio_task *task)
{
	enum xio_ib_op_code	out_ib_op;
	enum ibv_wr_opcode	wr_opcode;
	struct xio_sge		lsg_list[XIO_MAX_IOV];
	size_t			lsg_list_len;
	size_t			llen;
	int			retval = 0;
	size_t			lsg_out_list_len = 0;
	size_t			rsg_out_list_len = 0;
	int			tasks_used = 0;

	if (unlikely(verify_req_send_limits(rdma_hndl))) {
		xio_rdma_xmit(rdma_hndl);
		return -1;
	}

	init_lsg_list(rdma_hndl, task, lsg_list, &lsg_list_len, &llen);
	if (unlikely(task->omsg->rdma.length < llen)) {
		ERROR_LOG("peer provided too small iovec\n");
		task->status = XIO_E_REM_USER_BUF_OVERFLOW;
		return -1;
	}

	retval = xio_validate_rdma_op(
		lsg_list, lsg_list_len,
		task->omsg->rdma.rsg_list,
		task->omsg->rdma.nents,
		llen,
		rdma_hndl->max_sge,
		&tasks_used);
	if (unlikely(retval)) {
		ERROR_LOG("failed to validate input scatter lists\n");
		task->status = XIO_E_MSG_INVALID;
		return -1;
	}
	out_ib_op = task->omsg->rdma.is_read ? XIO_IB_RDMA_READ_DIRECT :
		    XIO_IB_RDMA_WRITE_DIRECT;
	wr_opcode = task->omsg->rdma.is_read ? IBV_WR_RDMA_READ :
		    IBV_WR_RDMA_WRITE;

	retval = xio_prep_rdma_op(task, rdma_hndl,
				  out_ib_op,
				  wr_opcode,
				  lsg_list, lsg_list_len, &lsg_out_list_len,
				  task->omsg->rdma.rsg_list,
				  task->omsg->rdma.nents,
				  &rsg_out_list_len,
				  llen,
				  rdma_hndl->max_sge,
				  0,
				  &rdma_hndl->tx_ready_list, tasks_used);
	if (unlikely(retval)) {
		ERROR_LOG("failed to allocate tasks\n");
		task->status = XIO_E_NO_BUFS;
		return -1;
	}
	rdma_hndl->tx_ready_tasks_num += tasks_used;

	return kick_send_and_read(rdma_hndl, task, 0 /* must_send  */);
}

/*---------------------------------------------------------------------------*/
/* xio_set_msg_in_data_iovec						     */
/*---------------------------------------------------------------------------*/
static inline void xio_set_msg_in_data_iovec(struct xio_task *task,
					     struct xio_sge *lsg_list,
					     size_t lsize)
{
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;
	unsigned int		i;

	sgtbl		= (struct xio_sg_table_ops *)
				xio_sg_table_get(&task->imsg.in);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->imsg.in.sgl_type);
	sg		= sge_first(sgtbl_ops, sgtbl);

	for (i = 0; i < lsize; i++) {
		sge_set_length(sgtbl_ops, sg, lsg_list[i].length);
		sg = sge_next(sgtbl_ops, sgtbl, sg);
	}
	tbl_set_nents(sgtbl_ops, sgtbl, lsize);
}

/*---------------------------------------------------------------------------*/
/* xio_free_rdma_rd_mem							     */
/*---------------------------------------------------------------------------*/
void xio_free_rdma_rd_mem(struct xio_rdma_transport *rdma_hndl,
			  struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;

	unsigned int		i;

	if (task->is_assigned) {
		if (task->unassign_data_in_buf)
			task->unassign_data_in_buf(&task->imsg,
						   task->unassign_user_context);
		task->is_assigned = 0;
		task->unassign_data_in_buf = NULL;
		task->unassign_user_context = NULL;
		clr_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF, &task->imsg.hints);
		sgtbl = (struct xio_sg_table_ops *)xio_sg_table_get(&task->imsg.in);
		sgtbl_ops = (struct xio_sg_table_ops *)
			xio_sg_table_ops_get(task->imsg.in.sgl_type);
		tbl_set_nents(sgtbl_ops, sgtbl, 0);
	} else {
		for (i = 0; i < rdma_task->read_num_reg_mem; i++) {
			xio_mempool_free(&rdma_task->read_reg_mem[i]);
			rdma_task->read_reg_mem[i].priv = NULL;
		}
		rdma_task->read_num_reg_mem = 0;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_free_rdma_task_mem 						     */
/*---------------------------------------------------------------------------*/
int xio_free_rdma_task_mem(struct xio_transport_base *trans_hndl,
			   struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);
	unsigned int	i;

	/* recycle RDMA  buffers back to pool */
	xio_free_rdma_rd_mem((struct xio_rdma_transport *)trans_hndl, task);

	/* put buffers back to pool */
	if (rdma_task->write_num_reg_mem) {
		for (i = 0; i < rdma_task->write_num_reg_mem; i++) {
			if (rdma_task->write_reg_mem[i].priv) {
				xio_mempool_free(&rdma_task->write_reg_mem[i]);
				rdma_task->write_reg_mem[i].priv = NULL;
			}
		}
		rdma_task->write_num_reg_mem	= 0;
	}
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_tasks_list_free_rdma_rd_tasks_mem				     */
/*---------------------------------------------------------------------------*/
static inline int xio_rdma_tasks_list_free_rdma_rd_mem(
		struct xio_rdma_transport *rdma_hndl, struct list_head *list)
{
	struct xio_task *ptask;

	list_for_each_entry(ptask, list, tasks_list_entry) {
		xio_free_rdma_rd_mem(rdma_hndl, ptask);
	}
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_free_all_rdma_rd_tasks_mem					     */
/*---------------------------------------------------------------------------*/
int xio_rdma_free_all_rdma_rd_tasks_mem(struct xio_rdma_transport *rdma_hndl)
{

	if (!list_empty(&rdma_hndl->rdma_rd_req_in_flight_list)) {
		xio_rdma_tasks_list_free_rdma_rd_mem(rdma_hndl,
				&rdma_hndl->rdma_rd_req_in_flight_list);
	}
	if (!list_empty(&rdma_hndl->rdma_rd_req_list)) {
		xio_rdma_tasks_list_free_rdma_rd_mem(rdma_hndl,
				&rdma_hndl->rdma_rd_req_list);
	}
	if (!list_empty(&rdma_hndl->rdma_rd_rsp_in_flight_list)) {
		xio_rdma_tasks_list_free_rdma_rd_mem(rdma_hndl,
				&rdma_hndl->rdma_rd_rsp_in_flight_list);
	}
	if (!list_empty(&rdma_hndl->rdma_rd_rsp_list)) {
		xio_rdma_tasks_list_free_rdma_rd_mem(rdma_hndl,
				&rdma_hndl->rdma_rd_rsp_list);
	}

	return 0;
}


/*---------------------------------------------------------------------------*/
/* xio_sched_rdma_rd							     */
/*---------------------------------------------------------------------------*/
static int xio_sched_rdma_rd(struct xio_rdma_transport *rdma_hndl,
			     struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);

	unsigned int		i;
	int			retval;
	size_t			llen = 0, rlen = 0;
	int			tasks_used = 0;
	struct xio_sge		lsg_list[XIO_MAX_IOV];
	size_t			lsg_list_len;
	size_t			lsg_out_list_len = 0;
	size_t			rsg_out_list_len = 0;
	struct ibv_mr		*mr;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;
	struct list_head	*rdma_rd_list;

	/* peer got request for rdma read */

	/* need for buffer to do rdma read. there are two options:	   */
	/* option 1: user provides call back that fills application memory */
	/* option 2: use internal buffer pool				   */

	/* hint the upper layer of sizes */
	sgtbl = (struct xio_sg_table_ops *)xio_sg_table_get(&task->imsg.in);
	sgtbl_ops = (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->imsg.in.sgl_type);
	tbl_set_nents(sgtbl_ops, sgtbl, rdma_task->req_out_num_sge);
	for_each_sge(sgtbl, sgtbl_ops, sg, i) {
		sge_set_addr(sgtbl_ops, sg, NULL);
		sge_set_length(sgtbl_ops, sg,
			       rdma_task->req_out_sge[i].length);
		rlen += rdma_task->req_out_sge[i].length;
		rdma_task->read_reg_mem[i].ctx = rdma_hndl->base.ctx;
		rdma_task->read_reg_mem[i].priv = NULL;
	}

	sgtbl		= xio_sg_table_get(&task->imsg.out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->imsg.out.sgl_type);
	if (rdma_task->req_in_num_sge) {
		tbl_set_nents(sgtbl_ops, sgtbl, rdma_task->req_in_num_sge);
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			sge_set_addr(sgtbl_ops, sg, NULL);
			sge_set_length(sgtbl_ops, sg,
				       rdma_task->req_in_sge[i].length);
			sge_set_mr(sgtbl_ops, sg, NULL);
			rdma_task->write_reg_mem[i].ctx = rdma_hndl->base.ctx;
			rdma_task->write_reg_mem[i].priv = NULL;
		}
	} else {
		tbl_set_nents(sgtbl_ops, sgtbl, 0);
	}
	sgtbl		= xio_sg_table_get(&task->imsg.in);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->imsg.in.sgl_type);

	/* use both to get info from above */
	task->is_assigned = 0;
	task->status = 0;
	xio_transport_assign_in_buf(&rdma_hndl->base, task);
	if (task->status) {
		WARN_LOG("assign_in_buf: error:%d. rdma read is ignored\n",
			  task->status);
		return -1;
	}
	if (task->is_assigned) {
		/* if user does not have buffers ignore */
		if (tbl_nents(sgtbl_ops, sgtbl) == 0) {
			WARN_LOG("application has not provided buffers\n");
			WARN_LOG("rdma read is ignored\n");
			task->status = XIO_E_NO_USER_BUFS;
			return -1;
		}
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			if (!sge_mr(sgtbl_ops, sg)) {
				ERROR_LOG("application has not provided mr\n");
				ERROR_LOG("rdma read is ignored\n");
				task->status = XIO_E_NO_USER_MR;
				return -1;
			}
			if (!sge_addr(sgtbl_ops, sg)) {
				ERROR_LOG("application has provided " \
					  "null address\n");
				ERROR_LOG("rdma read is ignored\n");
				task->status = XIO_E_NO_USER_BUFS;
				return -1;
			}
			llen += sge_length(sgtbl_ops, sg);
		}
		if (rlen  > llen) {
			ERROR_LOG("application provided too small iovec\n");
			ERROR_LOG("remote peer want to write %zd bytes while " \
				  "local peer provided buffer size %zd bytes\n",
				  rlen, llen);
			ERROR_LOG("rdma read is ignored\n");
			task->status = XIO_E_USER_BUF_OVERFLOW;
			return -1;
		}
		set_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF, &task->imsg.hints);
	} else {
		if (!rdma_hndl->rdma_mempool) {
				ERROR_LOG(
					"message /read/write failed - " \
					"library's memory pool disabled\n");
				task->status = XIO_E_NO_BUFS;
				goto cleanup;
		}

		tbl_set_nents(sgtbl_ops, sgtbl, rdma_task->req_out_num_sge);
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			retval = xio_mempool_alloc(
					rdma_hndl->rdma_mempool,
					rdma_task->req_out_sge[i].length,
					&rdma_task->read_reg_mem[i]);

			if (unlikely(retval)) {
				rdma_task->read_num_reg_mem = i;
				ERROR_LOG("mempool is empty for %zd bytes\n",
					  rdma_task->req_out_sge[i].length);

				task->status = ENOMEM;
				goto cleanup;
			}
			sge_set_addr(sgtbl_ops, sg,
				     rdma_task->read_reg_mem[i].addr);
			sge_set_length(sgtbl_ops, sg,
				       rdma_task->read_reg_mem[i].length);
			sge_set_mr(sgtbl_ops, sg,
				   rdma_task->read_reg_mem[i].mr);

			llen += rdma_task->read_reg_mem[i].length;
		}
		rdma_task->read_num_reg_mem = rdma_task->req_out_num_sge;
	}
	for_each_sge(sgtbl, sgtbl_ops, sg, i) {
		lsg_list[i].addr =
			uint64_from_ptr(sge_addr(sgtbl_ops, sg));
		lsg_list[i].length =
			uint64_from_ptr(sge_length(sgtbl_ops, sg));

		mr = xio_rdma_mr_lookup((struct xio_mr *)sge_mr(sgtbl_ops, sg),
					rdma_hndl->tcq->dev);
		lsg_list[i].stag = mr->rkey;
	}
	lsg_list_len = tbl_nents(sgtbl_ops, sgtbl);

	if (!task->sender_task)
		rdma_rd_list		= &rdma_hndl->rdma_rd_req_list;
	else
		rdma_rd_list		= &rdma_hndl->rdma_rd_rsp_list;

	retval = xio_validate_rdma_op(
			lsg_list, lsg_list_len,
			rdma_task->req_out_sge,
			rdma_task->req_out_num_sge,
			min(rlen, llen),
			rdma_hndl->max_sge,
			&tasks_used);
	if (retval) {
		ERROR_LOG("failed to validate input iovecs\n");
		ERROR_LOG("rdma read is ignored\n");
		task->status = XIO_E_MSG_INVALID;
		goto cleanup;
	}

	retval = xio_prep_rdma_op(task, rdma_hndl,
				  XIO_IB_RDMA_READ,
				  IBV_WR_RDMA_READ,
				  lsg_list,
				  lsg_list_len, &lsg_out_list_len,
				  rdma_task->req_out_sge,
				  rdma_task->req_out_num_sge,
				  &rsg_out_list_len,
				  min(rlen, llen),
				  rdma_hndl->max_sge,
				  1,
				  rdma_rd_list, tasks_used);
	if (retval) {
		ERROR_LOG("failed to allocate tasks\n");
		ERROR_LOG("rdma read is ignored\n");
		task->status = XIO_E_WRITE_FAILED;
		goto cleanup;
	}

	/* prepare the in side of the message */
	xio_set_msg_in_data_iovec(task, lsg_list, lsg_out_list_len);

	if (!task->sender_task)
		xio_xmit_rdma_rd_req(rdma_hndl);
	else
		xio_xmit_rdma_rd_rsp(rdma_hndl);

	return 0;
cleanup:
	xio_set_error(task->status);
	xio_free_rdma_rd_mem(rdma_hndl, task);

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_set_rsp_out_sge							     */
/*---------------------------------------------------------------------------*/
static inline void xio_set_rsp_out_sge(struct xio_task *task,
				       struct xio_sge *rsg_list,
				       size_t rsize)
{
	XIO_TO_RDMA_TASK(task, rdma_task);

	unsigned int	i;

	for (i = 0; i < rsize; i++)
		rdma_task->rsp_out_sge[i].length = rsg_list[i].length;

	rdma_task->rsp_out_num_sge = rsize;
}

/*---------------------------------------------------------------------------*/
/* xio_sched_rdma_wr_req						     */
/*---------------------------------------------------------------------------*/
static int xio_sched_rdma_wr_req(struct xio_rdma_transport *rdma_hndl,
				 struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);

	struct xio_sg_table_ops	*sgtbl_ops;
	struct xio_sge		lsg_list[XIO_MAX_IOV];
	struct ibv_mr		*mr;
	void			*sgtbl;
	void			*sg;
	size_t			lsg_list_len;
	size_t			lsg_out_list_len = 0;
	size_t			rsg_out_list_len = 0;
	size_t			rlen = 0, llen = 0;
	int			tasks_used = 0;
	unsigned int		i;
	int			retval = 0;
	int			enforce_write_rsp;

	sgtbl		= xio_sg_table_get(&task->omsg->out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->out.sgl_type);
	sg		= sge_first(sgtbl_ops, sgtbl);
	enforce_write_rsp = !!(task->imsg_flags & XIO_MSG_FLAG_PEER_WRITE_RSP);

	/* user did not provided mr */
	if (!sge_mr(sgtbl_ops, sg)) {
		if (!rdma_hndl->rdma_mempool) {
			xio_set_error(XIO_E_NO_BUFS);
			ERROR_LOG(
					"message /read/write failed - " \
					"library's memory pool disabled\n");
			goto cleanup;
		}
		/* user did not provide mr - take buffers from pool
		 * and do copy */
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			retval = xio_mempool_alloc(
					rdma_hndl->rdma_mempool,
					sge_length(sgtbl_ops, sg),
					&rdma_task->write_reg_mem[i]);
			if (unlikely(retval)) {
				rdma_task->write_num_reg_mem = i;
				xio_set_error(ENOMEM);
				ERROR_LOG("mempool is empty for %zd bytes\n",
					  sge_length(sgtbl_ops, sg));
				goto cleanup;
			}
			lsg_list[i].addr	= uint64_from_ptr(
					rdma_task->write_reg_mem[i].addr);
			lsg_list[i].length	= sge_length(sgtbl_ops, sg);
			mr = xio_rdma_mr_lookup(rdma_task->write_reg_mem[i].mr,
						rdma_hndl->tcq->dev);
			lsg_list[i].stag	= mr->lkey;

			llen		+= lsg_list[i].length;

			/* copy the data to the buffer */
			memcpy(rdma_task->write_reg_mem[i].addr,
			       sge_addr(sgtbl_ops, sg),
			       sge_length(sgtbl_ops, sg));
		}
		rdma_task->write_num_reg_mem = tbl_nents(sgtbl_ops, sgtbl);
	} else {
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			lsg_list[i].addr	= uint64_from_ptr(
						sge_addr(sgtbl_ops, sg));
			lsg_list[i].length	= sge_length(sgtbl_ops, sg);
			mr = xio_rdma_mr_lookup((struct xio_mr *)
							sge_mr(sgtbl_ops, sg),
						rdma_hndl->tcq->dev);
			lsg_list[i].stag	= mr->lkey;

			llen += lsg_list[i].length;
		}
	}
	lsg_list_len = tbl_nents(sgtbl_ops, sgtbl);

	if (enforce_write_rsp) {
		if (rdma_task->req_in_num_sge < lsg_list_len) {
			ERROR_LOG("peer provided too small iovec rlen:%d, llen:%d\n",
					rdma_task->req_in_num_sge, lsg_list_len);
			ERROR_LOG("rdma write is ignored\n");
			task->status = XIO_E_REM_USER_BUF_OVERFLOW;
			goto cleanup;
		}
		/* override remote length so all items will be arranged in
		 * one by one map */
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			if (rdma_task->req_in_sge[i].length >= sge_length(sgtbl_ops, sg)) {
				rdma_task->req_in_sge[i].length = sge_length(sgtbl_ops, sg);
				rlen += rdma_task->req_in_sge[i].length;
			} else {
				ERROR_LOG("peer provided too small iovec [%d] - " \
						"peer_len:%zd > local_len:%zd\n", i,
						rdma_task->req_in_sge[i].length,
						sge_length(sgtbl_ops, sg));
				ERROR_LOG("tcp write is ignored\n");
				task->status = EINVAL;
				goto cleanup;
			}
		}
	} else {
		for (i = 0;  i < rdma_task->req_in_num_sge; i++)
			rlen += rdma_task->req_in_sge[i].length;
	}
	if (rlen  < llen) {
		ERROR_LOG("peer provided too small iovec\n");
		ERROR_LOG("rdma write is ignored\n");
		task->status = XIO_E_REM_USER_BUF_OVERFLOW;
		goto cleanup;
	}
	retval = xio_validate_rdma_op(
			lsg_list, lsg_list_len,
			rdma_task->req_in_sge,
			rdma_task->req_in_num_sge,
			min(rlen, llen),
			rdma_hndl->max_sge,
			&tasks_used);
	if (retval) {
		ERROR_LOG("failed to invalidate input iovecs\n");
		ERROR_LOG("rdma write is ignored\n");
		task->status = XIO_E_MSG_INVALID;
		goto cleanup;
	}

	retval = xio_prep_rdma_op(task, rdma_hndl,
				  XIO_IB_RDMA_WRITE,
				  IBV_WR_RDMA_WRITE,
				  lsg_list, lsg_list_len, &lsg_out_list_len,
				  rdma_task->req_in_sge,
				  rdma_task->req_in_num_sge,
				  &rsg_out_list_len,
				  min(rlen, llen),
				  rdma_hndl->max_sge,
				  0,
				  &rdma_hndl->tx_ready_list, tasks_used);
	if (retval) {
		ERROR_LOG("failed to allocate tasks\n");
		ERROR_LOG("rdma write is ignored\n");
		task->status = XIO_E_READ_FAILED;
		goto cleanup;
	}
	/* prepare response to peer */
	xio_set_rsp_out_sge(task, rdma_task->req_in_sge, rsg_out_list_len);

	/* xio_prep_rdma_op used splice to transfer "tasks_used"  to
	 * tx_ready_list
	 */
	rdma_hndl->tx_ready_tasks_num += tasks_used;
	return 0;
cleanup:
	for (i = 0; i < rdma_task->write_num_reg_mem; i++)
		xio_mempool_free(&rdma_task->write_reg_mem[i]);

	rdma_task->write_num_reg_mem = 0;
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_on_recv_req							     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_on_recv_req(struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);

	union xio_transport_event_data event_data;
	struct xio_rdma_req_hdr	req_hdr;
	struct xio_sg_table_ops	*sgtbl_ops;
	struct xio_msg		*imsg;
	void			*ulp_hdr;
	void			*sgtbl;
	void			*sg;
	unsigned int		i;
	int			retval = 0;
	int			header_err = 1; /* temporary do not activate */

	/* read header */
	retval = xio_rdma_read_req_header(rdma_hndl, task, &req_hdr);
	if (retval != 0) {
		xio_set_error(XIO_E_MSG_INVALID);
		header_err = 1;
		goto cleanup;
	}
	if (rdma_hndl->exp_sn == req_hdr.sn) {
		rdma_hndl->exp_sn++;
		rdma_hndl->ack_sn = req_hdr.sn;
		rdma_hndl->peer_credits += req_hdr.credits;
	} else {
		ERROR_LOG("ERROR: expected sn:%d, " \
			  "arrived sn:%d out_ib_op:%u in_num_sge:%u " \
			  "out_num_sge:%u, rdma_hndl:%p\n",
			  rdma_hndl->exp_sn, req_hdr.sn,
			  req_hdr.out_ib_op, req_hdr.in_num_sge,
			  req_hdr.out_num_sge, rdma_hndl);
	}
	/* save originator identifier */
	task->imsg_flags	= req_hdr.flags;
	task->rtid		= req_hdr.ltid;

	imsg		= &task->imsg;
	sgtbl		= xio_sg_table_get(&imsg->out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(imsg->out.sgl_type);

	ulp_hdr = xio_mbuf_get_curr_ptr(&task->mbuf);

	imsg->type = (enum xio_msg_type)task->tlv_type;
	imsg->in.header.iov_len	= req_hdr.ulp_hdr_len;
	clr_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF, &imsg->hints);

	if (req_hdr.ulp_hdr_len)
		imsg->in.header.iov_base	= ulp_hdr;
	else
		imsg->in.header.iov_base	= NULL;

	/* hint upper layer about expected response */
	if (rdma_task->req_in_num_sge) {
		tbl_set_nents(sgtbl_ops, sgtbl, rdma_task->req_in_num_sge);
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			sge_set_addr(sgtbl_ops, sg, NULL);
			sge_set_length(sgtbl_ops, sg,
				       rdma_task->req_in_sge[i].length);
				       sge_set_mr(sgtbl_ops, sg, NULL);
		}
	} else {
		tbl_set_nents(sgtbl_ops, sgtbl, 0);
	}

	switch (req_hdr.out_ib_op) {
	case XIO_IB_SEND:
		sgtbl		= xio_sg_table_get(&imsg->in);
		sgtbl_ops	= (struct xio_sg_table_ops *)
					xio_sg_table_ops_get(imsg->in.sgl_type);
		if (req_hdr.ulp_imm_len) {
			/* incoming data via SEND */
			/* if data arrived, set the pointers */
			tbl_set_nents(sgtbl_ops, sgtbl, 1);
			sg = sge_first(sgtbl_ops, sgtbl);
			sge_set_addr(sgtbl_ops, sg,
				     (ulp_hdr + imsg->in.header.iov_len +
				     req_hdr.ulp_pad_len));
			sge_set_length(sgtbl_ops, sg, req_hdr.ulp_imm_len);
		} else {
			/* no data at all */
			tbl_set_nents(sgtbl_ops, sgtbl, 0);
		}
		break;
	case XIO_IB_RDMA_READ:
		/* schedule request for RDMA READ. in case of error
		 * don't schedule the rdma read operation */
		/*TRACE_LOG("scheduling rdma read\n");*/
		retval = xio_sched_rdma_rd(rdma_hndl, task);
		if (retval == 0)
			return 0;
		task->status = xio_errno();
		break;
	default:
		ERROR_LOG("unexpected out_ib_op, rdma_hndl:%p\n", rdma_hndl);
		xio_set_error(XIO_E_MSG_INVALID);
		task->status = XIO_E_MSG_INVALID;
		break;
	}

	/* must delay the send due to pending rdma read requests
	 * if not user will get out of order messages - need fence
	 */
	if (!IS_KEEPALIVE(task->tlv_type)) {
		if (!list_empty(&rdma_hndl->rdma_rd_req_list)) {
			list_move_tail(&task->tasks_list_entry,
					&rdma_hndl->rdma_rd_req_list);
			rdma_hndl->kick_rdma_rd_req = 1;
			return 0;
		}
		if (rdma_hndl->rdma_rd_req_in_flight) {
			rdma_hndl->rdma_rd_req_in_flight++;
			list_move_tail(&task->tasks_list_entry,
					&rdma_hndl->rdma_rd_req_in_flight_list);
			return 0;
		}
	}
	/* fill notification event */
	event_data.msg.op	= XIO_WC_OP_RECV;
	event_data.msg.task	= task;

	if (task->status)
		xio_free_rdma_task_mem(&rdma_hndl->base, task);

	xio_transport_notify_observer(&rdma_hndl->base,
				      XIO_TRANSPORT_EVENT_NEW_MESSAGE,
				      &event_data);

	return 0;

cleanup:
	retval = xio_errno();
	ERROR_LOG("xio_rdma_on_recv_req failed. (errno=%d %s)\n", retval,
		  xio_strerror(retval));
	if (header_err)
		xio_transport_notify_observer_error(&rdma_hndl->base, retval);
	else
		xio_transport_notify_message_error(&rdma_hndl->base, task,
				XIO_MSG_DIRECTION_IN, (enum xio_status)retval);

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_write_setup_msg						     */
/*---------------------------------------------------------------------------*/
static void xio_rdma_write_setup_msg(struct xio_rdma_transport *rdma_hndl,
				     struct xio_task *task,
				     struct xio_rdma_setup_msg *msg)
{
	struct xio_rdma_setup_msg *tmp_msg;
	struct xio_rkey_tbl_pack *ptbl;
	struct xio_rkey_tbl *tbl;
	int i;

	/* set the mbuf after tlv header */
	xio_mbuf_set_val_start(&task->mbuf);

	/* jump after connection setup header */
	if (rdma_hndl->base.is_client)
		xio_mbuf_inc(&task->mbuf,
			     sizeof(struct xio_nexus_setup_req));
	else
		xio_mbuf_inc(&task->mbuf,
			     sizeof(struct xio_nexus_setup_rsp));

	tmp_msg = (struct xio_rdma_setup_msg *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	PACK_LLVAL(msg, tmp_msg, buffer_sz);
	PACK_SVAL(msg, tmp_msg, sq_depth);
	PACK_SVAL(msg, tmp_msg, rq_depth);
	PACK_SVAL(msg, tmp_msg, credits);
	PACK_LVAL(msg, tmp_msg, max_in_iovsz);
	PACK_LVAL(msg, tmp_msg, max_out_iovsz);
	PACK_SVAL(msg, tmp_msg, rkey_tbl_size);
	PACK_LVAL(msg, tmp_msg, max_header_len);
	PACK_LLVAL(msg, tmp_msg, my_handle);
	PACK_LVAL(msg, tmp_msg, my_pid);
	PACK_LVAL(msg, tmp_msg, my_tid);
	
#ifdef EYAL_TODO
	print_hex_dump_bytes("post_send: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.tlv.head,
			     64);
#endif
	xio_mbuf_inc(&task->mbuf, sizeof(struct xio_rdma_setup_msg));

	if (!msg->rkey_tbl_size)
		return;

	tbl = rdma_hndl->rkey_tbl;
	ptbl = (struct xio_rkey_tbl_pack *)xio_mbuf_get_curr_ptr(&task->mbuf);
	for (i = 0; i < rdma_hndl->rkey_tbl_size; i++) {
		PACK_LVAL(tbl, ptbl, old_rkey);
		PACK_LVAL(tbl, ptbl, new_rkey);
		tbl++;
		xio_mbuf_inc(&task->mbuf, sizeof(struct xio_rkey_tbl_pack));
		ptbl = (struct xio_rkey_tbl_pack *)
				xio_mbuf_get_curr_ptr(&task->mbuf);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_read_setup_msg						     */
/*---------------------------------------------------------------------------*/
static void xio_rdma_read_setup_msg(struct xio_rdma_transport *rdma_hndl,
				    struct xio_task *task,
				    struct xio_rdma_setup_msg *msg)
{
	struct xio_rdma_setup_msg *tmp_msg;
	struct xio_rkey_tbl_pack *ptbl;
	struct xio_rkey_tbl *tbl;
	int i;

	/* set the mbuf after tlv header */
	xio_mbuf_set_val_start(&task->mbuf);

	/* jump after connection setup header */
	if (rdma_hndl->base.is_client)
		xio_mbuf_inc(&task->mbuf,
			     sizeof(struct xio_nexus_setup_rsp));
	else
		xio_mbuf_inc(&task->mbuf,
			     sizeof(struct xio_nexus_setup_req));

	tmp_msg = (struct xio_rdma_setup_msg *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	UNPACK_LLVAL(tmp_msg, msg, buffer_sz);
	UNPACK_SVAL(tmp_msg, msg, sq_depth);
	UNPACK_SVAL(tmp_msg, msg, rq_depth);
	UNPACK_SVAL(tmp_msg, msg, credits);
	UNPACK_LVAL(tmp_msg, msg, max_in_iovsz);
	UNPACK_LVAL(tmp_msg, msg, max_out_iovsz);
	UNPACK_SVAL(tmp_msg, msg, rkey_tbl_size);
	UNPACK_LVAL(tmp_msg, msg, max_header_len);
	UNPACK_LLVAL(tmp_msg, msg, my_handle);
	UNPACK_LVAL(tmp_msg, msg, my_pid);
	UNPACK_LVAL(tmp_msg, msg, my_tid);

#ifdef EYAL_TODO
	print_hex_dump_bytes("post_send: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.curr,
			     64);
#endif
	xio_mbuf_inc(&task->mbuf, sizeof(struct xio_rdma_setup_msg));
	rdma_hndl->peer_rdma_hndl = ptr_from_int64(msg->my_handle);
	rdma_hndl->peer_pid = (pid_t)msg->my_pid;
	rdma_hndl->peer_tid = (pid_t)msg->my_tid;

	if (!msg->rkey_tbl_size)
		return;

	rdma_hndl->peer_rkey_tbl = (struct xio_rkey_tbl *)
				       xio_context_ucalloc(rdma_hndl->base.ctx,
						       msg->rkey_tbl_size, sizeof(*tbl));
	if (unlikely(!rdma_hndl->peer_rkey_tbl)) {
		ERROR_LOG("calloc failed. (errno=%m)\n");
		xio_strerror(ENOMEM);
		msg->rkey_tbl_size = -1;
		return;
	}

	tbl = rdma_hndl->peer_rkey_tbl;
	ptbl = (struct xio_rkey_tbl_pack *)xio_mbuf_get_curr_ptr(&task->mbuf);
	for (i = 0; i < msg->rkey_tbl_size; i++) {
		UNPACK_LVAL(ptbl, tbl, old_rkey);
		UNPACK_LVAL(ptbl, tbl, new_rkey);
		tbl++;
		xio_mbuf_inc(&task->mbuf, sizeof(struct xio_rkey_tbl_pack));
		ptbl = (struct xio_rkey_tbl_pack *)
				xio_mbuf_get_curr_ptr(&task->mbuf);
	}
	rdma_hndl->peer_rkey_tbl_size = msg->rkey_tbl_size;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_send_setup_req						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_send_setup_req(struct xio_rdma_transport *rdma_hndl,
				   struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);

	struct xio_rdma_setup_msg  req;
	uint16_t payload;

	req.buffer_sz		= xio_rdma_get_inline_buffer_size();
	req.sq_depth		= rdma_hndl->sq_depth;
	req.rq_depth		= rdma_hndl->rq_depth;
	req.credits		= 0;
	req.max_in_iovsz	= rdma_options.max_in_iovsz;
	req.max_out_iovsz	= rdma_options.max_out_iovsz;
	req.rkey_tbl_size	= rdma_hndl->rkey_tbl_size;
	req.max_header_len	= g_options.max_inline_xio_hdr;
	req.my_handle		= uint64_from_ptr(rdma_hndl);
	req.my_pid		= getpid();
	req.my_tid		= syscall(SYS_gettid);

	xio_rdma_write_setup_msg(rdma_hndl, task, &req);

	payload = xio_mbuf_tlv_payload_len(&task->mbuf);

	/* add tlv */
	if (xio_mbuf_write_tlv(&task->mbuf, task->tlv_type, payload) != 0)
		return  -1;

	DEBUG_LOG("%s - rdma_hndl:%p\n", __func__, rdma_hndl);

	/* set the length */
	rdma_task->txd.sge[0].length	= xio_mbuf_data_length(&task->mbuf);

	rdma_task->txd.send_wr.send_flags = IBV_SEND_SIGNALED;
	if (rdma_task->txd.sge[0].length < (size_t)rdma_hndl->max_inline_data)
		rdma_task->txd.send_wr.send_flags |= IBV_SEND_INLINE;

	rdma_task->txd.send_wr.next	= NULL;
	rdma_task->out_ib_op		= XIO_IB_SEND;
	rdma_task->txd.send_wr.num_sge	= 1;

	xio_task_addref(task);
	rdma_hndl->reqs_in_flight_nr++;
	list_move_tail(&task->tasks_list_entry, &rdma_hndl->in_flight_list);

	rdma_hndl->peer_credits--;
	xio_post_send(rdma_hndl, &rdma_task->txd, 1);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_send_setup_rsp						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_send_setup_rsp(struct xio_rdma_transport *rdma_hndl,
				   struct xio_task *task)
{
	XIO_TO_RDMA_TASK(task, rdma_task);

	uint16_t payload;

	rdma_hndl->sim_peer_credits += rdma_hndl->credits;

	rdma_hndl->setup_rsp.credits		= rdma_hndl->credits;
	rdma_hndl->setup_rsp.max_in_iovsz	= rdma_options.max_in_iovsz;
	rdma_hndl->setup_rsp.max_out_iovsz	= rdma_options.max_out_iovsz;
	rdma_hndl->setup_rsp.buffer_sz		= rdma_hndl->membuf_sz;
	rdma_hndl->setup_rsp.max_header_len	= g_options.max_inline_xio_hdr;
	rdma_hndl->setup_rsp.my_handle		= uint64_from_ptr(rdma_hndl);
	rdma_hndl->setup_rsp.my_pid		= getpid();
	rdma_hndl->setup_rsp.my_tid		= syscall(SYS_gettid);


	xio_rdma_write_setup_msg(rdma_hndl, task, &rdma_hndl->setup_rsp);
	rdma_hndl->credits = 0;

	payload = xio_mbuf_tlv_payload_len(&task->mbuf);

	/* add tlv */
	if (xio_mbuf_write_tlv(&task->mbuf, task->tlv_type, payload) != 0)
		return  -1;

	DEBUG_LOG("%s - rdma_hndl:%p\n", __func__, rdma_hndl);

	/* set the length */
	rdma_task->txd.sge[0].length = xio_mbuf_data_length(&task->mbuf);
	rdma_task->txd.send_wr.send_flags = IBV_SEND_SIGNALED;
	if (rdma_task->txd.sge[0].length < (size_t)rdma_hndl->max_inline_data)
		rdma_task->txd.send_wr.send_flags |= IBV_SEND_INLINE;
	rdma_task->txd.send_wr.next	= NULL;
	rdma_task->out_ib_op		= XIO_IB_SEND;
	rdma_task->txd.send_wr.num_sge	= 1;

	rdma_hndl->rsps_in_flight_nr++;
	list_move(&task->tasks_list_entry, &rdma_hndl->in_flight_list);

	rdma_hndl->peer_credits--;
	xio_post_send(rdma_hndl, &rdma_task->txd, 1);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_on_setup_msg						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_on_setup_msg(struct xio_rdma_transport *rdma_hndl,
				 struct xio_task *task)
{
	union xio_transport_event_data event_data;
	struct xio_rdma_setup_msg *rsp  = &rdma_hndl->setup_rsp;
	uint64_t local_buf_size;

	if (rdma_hndl->base.is_client) {
		struct xio_task *sender_task = NULL;

		if (!list_empty(&rdma_hndl->in_flight_list))
			sender_task = list_first_entry(
					&rdma_hndl->in_flight_list,
					struct xio_task,  tasks_list_entry);
		else if (!list_empty(&rdma_hndl->tx_comp_list))
			sender_task = list_first_entry(
					&rdma_hndl->tx_comp_list,
					struct xio_task,  tasks_list_entry);
		else
			ERROR_LOG("could not find sender task\n");

		if (sender_task->tlv_type != XIO_NEXUS_SETUP_REQ)
			ERROR_LOG("%s - failed to find sender task. rdma_hndl:%p\n",
				  __func__, rdma_hndl);

		task->sender_task = sender_task;
		xio_rdma_read_setup_msg(rdma_hndl, task, rsp);
		/* get the initial credits */
		rdma_hndl->peer_credits += rsp->credits;
	} else {
		struct xio_rdma_setup_msg req;

		xio_rdma_read_setup_msg(rdma_hndl, task, &req);

		/* current implementation is symmetric */
		local_buf_size  = xio_rdma_get_inline_buffer_size();
		rsp->buffer_sz  = min(req.buffer_sz, local_buf_size);
		rsp->sq_depth	= max(req.sq_depth, rdma_hndl->rq_depth);
		rsp->rq_depth	= max(req.rq_depth, rdma_hndl->sq_depth);
		rsp->max_in_iovsz	= req.max_in_iovsz;
		rsp->max_out_iovsz	= req.max_out_iovsz;
		rsp->max_header_len	= req.max_header_len;
	}

	/* save the values */
	rdma_hndl->rq_depth		= rsp->rq_depth;
	rdma_hndl->sq_depth		= rsp->sq_depth;
	rdma_hndl->membuf_sz		= rsp->buffer_sz;
	rdma_hndl->max_inline_buf_sz	= rsp->buffer_sz;
	rdma_hndl->peer_max_in_iovsz	= rsp->max_in_iovsz;
	rdma_hndl->peer_max_out_iovsz	= rsp->max_out_iovsz;
	rdma_hndl->peer_max_header	= rsp->max_header_len;

	/* initialize send window */
	rdma_hndl->sn = 0;
	rdma_hndl->ack_sn = ~0;
	rdma_hndl->credits = 0;
	rdma_hndl->max_sn = rdma_hndl->sq_depth;

	/* initialize receive window */
	rdma_hndl->exp_sn = 0;
	rdma_hndl->max_exp_sn = 0;

	rdma_hndl->max_tx_ready_tasks_num = rdma_hndl->sq_depth;

	/* fill notification event */
	event_data.msg.op	= XIO_WC_OP_RECV;
	event_data.msg.task	= task;

	DEBUG_LOG("%s - rdma_hndl:%p, peer_rdma_hndl:%p, peer_pid:%d, peer_tid:%d\n",
		  __func__,
		  rdma_hndl, rdma_hndl->peer_rdma_hndl,
		  rdma_hndl->peer_pid, rdma_hndl->peer_tid);

	xio_transport_notify_observer(&rdma_hndl->base,
				      XIO_TRANSPORT_EVENT_NEW_MESSAGE,
				      &event_data);
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_write_rdma_read_ack_hdr						     */
/*---------------------------------------------------------------------------*/
static void xio_write_rdma_read_ack_hdr(struct xio_rdma_transport *rdma_hndl,
					struct xio_task *task,
					struct xio_rdma_read_ack_hdr *rra)
{
	struct xio_rdma_read_ack_hdr *tmp_rra;

	xio_mbuf_reset(&task->mbuf);

	/* set start of the tlv */
	if (xio_mbuf_tlv_start(&task->mbuf) != 0)
		return;

	/* set the mbuf after tlv header */
	xio_mbuf_set_val_start(&task->mbuf);

	/* get the pointer */
	tmp_rra = (struct xio_rdma_read_ack_hdr *)
					xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	PACK_SVAL(rra, tmp_rra, hdr_len);
	PACK_LVAL(rra, tmp_rra, rtid);

	xio_mbuf_inc(&task->mbuf, sizeof(*rra));
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_send_rdma_read_ack						     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_send_rdma_read_ack(struct xio_rdma_transport *rdma_hndl,
				       int rtid)
{
	uint64_t			payload;
	struct xio_task			*task;
	struct xio_rdma_task		*rdma_task;
	struct xio_rdma_read_ack_hdr	rra = {
		.hdr_len	= sizeof(rra),
		.rtid		= rtid,
	};

	task = xio_rdma_primary_task_alloc(rdma_hndl);
	if (!task) {
		ERROR_LOG("primary tasks pool is empty\n");
		return -1;
	}
	task->omsg = NULL;

	task->tlv_type	= XIO_RDMA_READ_ACK;
	rdma_task	= (struct xio_rdma_task *)task->dd_data;

	/* write the message */
	xio_write_rdma_read_ack_hdr(rdma_hndl, task, &rra);

	payload = xio_mbuf_tlv_payload_len(&task->mbuf);

	/* add tlv */
	if (xio_mbuf_write_tlv(&task->mbuf, task->tlv_type, payload) != 0)
		return  -1;

	/* set the length */
	rdma_task->txd.sge[0].length	= xio_mbuf_data_length(&task->mbuf);
	rdma_task->txd.send_wr.send_flags = 0;
	if (rdma_task->txd.sge[0].length < (size_t)rdma_hndl->max_inline_data)
		rdma_task->txd.send_wr.send_flags |= IBV_SEND_INLINE;

	rdma_task->txd.send_wr.next	= NULL;
	rdma_task->out_ib_op		= XIO_IB_SEND;
	rdma_task->txd.send_wr.num_sge	= 1;

	rdma_hndl->rsps_in_flight_nr++;
	list_add_tail(&task->tasks_list_entry, &rdma_hndl->in_flight_list);

	rdma_hndl->peer_credits--;
	xio_post_send(rdma_hndl, &rdma_task->txd, 1);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_read_rdma_read_ack_hdr						     */
/*---------------------------------------------------------------------------*/
static void xio_read_rdma_read_ack_hdr(struct xio_rdma_transport *rdma_hndl,
				       struct xio_task *task,
				       struct xio_rdma_read_ack_hdr *rra)
{
	struct xio_rdma_read_ack_hdr *tmp_rra;

	/* goto to the first tlv and set the mbuf after tlv header */
	xio_mbuf_set_val_start(&task->mbuf);

	/* get the pointer */
	tmp_rra = (struct xio_rdma_read_ack_hdr *)
					xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	UNPACK_SVAL(tmp_rra, rra, hdr_len);
	UNPACK_LVAL(tmp_rra, rra, rtid);

	xio_mbuf_inc(&task->mbuf, sizeof(*rra));
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_on_recv_rdma_read_ack					     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_on_recv_rdma_read_ack(struct xio_rdma_transport *rdma_hndl,
					  struct xio_task *task)
{
	struct xio_rdma_read_ack_hdr	rra;
	union xio_transport_event_data	event_data;
	struct xio_task			*req_task;

	xio_read_rdma_read_ack_hdr(rdma_hndl, task, &rra);

	/* the rx task is returned back to pool */
	xio_tasks_pool_put(task);

	/* find the sender task */
	req_task = xio_rdma_primary_task_lookup(rdma_hndl, rra.rtid);

	event_data.msg.op	= XIO_WC_OP_SEND;
	event_data.msg.task	= req_task;

	xio_transport_notify_observer(&rdma_hndl->base,
				      XIO_TRANSPORT_EVENT_SEND_COMPLETION,
				      &event_data);

	return 0;
}

#ifndef XIO_SRQ_ENABLE
/*---------------------------------------------------------------------------*/
/* xio_rdma_write_nop							     */
/*---------------------------------------------------------------------------*/
static void xio_rdma_write_nop(struct xio_rdma_transport *rdma_hndl,
			       struct xio_task *task, struct xio_nop_hdr *nop)
{
	struct  xio_nop_hdr *tmp_nop;

	xio_mbuf_reset(&task->mbuf);

	/* set start of the tlv */
	if (xio_mbuf_tlv_start(&task->mbuf) != 0)
		return;

	/* set the mbuf after tlv header */
	xio_mbuf_set_val_start(&task->mbuf);

	/* get the pointer */
	tmp_nop = (struct xio_nop_hdr *)xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	PACK_SVAL(nop, tmp_nop, hdr_len);
	PACK_SVAL(nop, tmp_nop, sn);
	PACK_SVAL(nop, tmp_nop, ack_sn);
	PACK_SVAL(nop, tmp_nop, credits);
	tmp_nop->opcode = nop->opcode;
	tmp_nop->flags = nop->flags;

#ifdef EYAL_TODO
	print_hex_dump_bytes("write_nop: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.tlv.head,
			     64);
#endif
	xio_mbuf_inc(&task->mbuf, sizeof(*nop));
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_send_nop							     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_send_nop(struct xio_rdma_transport *rdma_hndl)
{
	uint64_t		payload;
	struct xio_task		*task;
	struct xio_rdma_task	*rdma_task;
	struct  xio_nop_hdr	nop = {
		.hdr_len	= sizeof(nop),
		.sn		= rdma_hndl->sn,
		.ack_sn		= rdma_hndl->ack_sn,
		.credits	= rdma_hndl->credits,
		.opcode		= 0,
		.flags		= 0
	};

	TRACE_LOG("SEND_NOP. rdma_hndl:%p\n", rdma_hndl);

	task = xio_rdma_primary_task_alloc(rdma_hndl);
	if (!task) {
		ERROR_LOG("primary tasks pool is empty\n");
		return -1;
	}
	task->omsg = NULL;

	task->tlv_type	= XIO_CREDIT_NOP;
	rdma_task	= (struct xio_rdma_task *)task->dd_data;

	/* write the message */
	xio_rdma_write_nop(rdma_hndl, task, &nop);
	rdma_hndl->sim_peer_credits += rdma_hndl->credits;
	rdma_hndl->credits = 0;

	payload = xio_mbuf_tlv_payload_len(&task->mbuf);

	/* add tlv */
	if (xio_mbuf_write_tlv(&task->mbuf, task->tlv_type, payload) != 0)
		return  -1;

	/* set the length */
	rdma_task->txd.sge[0].length	= xio_mbuf_data_length(&task->mbuf);
	rdma_task->txd.send_wr.send_flags =
		IBV_SEND_SIGNALED | IBV_SEND_FENCE;
	if (rdma_task->txd.sge[0].length < (size_t)rdma_hndl->max_inline_data)
		rdma_task->txd.send_wr.send_flags |= IBV_SEND_INLINE;

	rdma_task->txd.send_wr.next	= NULL;
	rdma_task->out_ib_op		= XIO_IB_SEND;
	rdma_task->txd.send_wr.num_sge	= 1;

	rdma_hndl->rsps_in_flight_nr++;
	list_add_tail(&task->tasks_list_entry, &rdma_hndl->in_flight_list);

	rdma_hndl->peer_credits--;
	xio_post_send(rdma_hndl, &rdma_task->txd, 1);

	return 0;
}
#endif
/*---------------------------------------------------------------------------*/
/* xio_rdma_read_nop							     */
/*---------------------------------------------------------------------------*/
static void xio_rdma_read_nop(struct xio_rdma_transport *rdma_hndl,
			      struct xio_task *task, struct xio_nop_hdr *nop)
{
	struct  xio_nop_hdr *tmp_nop;

	/* goto to the first tlv and set the mbuf after tlv header */
	xio_mbuf_set_val_start(&task->mbuf);

	/* get the pointer */
	tmp_nop = (struct xio_nop_hdr *)xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	UNPACK_SVAL(tmp_nop, nop, hdr_len);
	UNPACK_SVAL(tmp_nop, nop, sn);
	UNPACK_SVAL(tmp_nop, nop, ack_sn);
	UNPACK_SVAL(tmp_nop, nop, credits);
	nop->opcode = tmp_nop->opcode;
	nop->flags = tmp_nop->flags;

#ifdef EYAL_TODO
	print_hex_dump_bytes("post_send: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.tlv.head,
			     64);
#endif
	xio_mbuf_inc(&task->mbuf, sizeof(*nop));
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_on_recv_nop							     */
/*---------------------------------------------------------------------------*/
static int xio_rdma_on_recv_nop(struct xio_rdma_transport *rdma_hndl,
				struct xio_task *task)
{
	struct xio_nop_hdr	nop;

	TRACE_LOG("RECV_NOP. rdma_hndl:%p\n", rdma_hndl);
	xio_rdma_read_nop(rdma_hndl, task, &nop);

	if (rdma_hndl->exp_sn == nop.sn)
		rdma_hndl->peer_credits += nop.credits;
	else
		ERROR_LOG("ERROR: expected sn:%d, arrived sn:%d, rdma_hndl:%p\n",
			  rdma_hndl->exp_sn, nop.sn, rdma_hndl);

	/* the rx task is returned back to pool */
	xio_tasks_pool_put(task);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_send							     */
/*---------------------------------------------------------------------------*/
int xio_rdma_send(struct xio_transport_base *transport,
		  struct xio_task *task)
{
	void	*rdma_hndl = transport;
	int	retval = -1;

	switch (task->tlv_type) {
	case XIO_NEXUS_SETUP_REQ:
		retval = xio_rdma_send_setup_req(
			(struct xio_rdma_transport *)rdma_hndl, task);
		break;
	case XIO_NEXUS_SETUP_RSP:
		retval = xio_rdma_send_setup_rsp(
			(struct xio_rdma_transport *)rdma_hndl, task);
		break;
	case XIO_MSG_TYPE_RDMA:
		retval = xio_rdma_perform_direct_rdma(
			(struct xio_rdma_transport *)rdma_hndl, task);
		break;
	default:
		if (IS_REQUEST(task->tlv_type))
			retval = xio_rdma_send_req(
				(struct xio_rdma_transport *)rdma_hndl, task);
		else if (IS_RESPONSE(task->tlv_type))
			retval = xio_rdma_send_rsp(
				(struct xio_rdma_transport *)rdma_hndl, task);
		else
			ERROR_LOG("unknown message type:0x%x\n",
				  task->tlv_type);
		break;
	}

	return retval;
}


