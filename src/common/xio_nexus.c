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
#include <sys/hashtable.h>
#include <xio_os.h>
#include "libxio.h"
#include "xio_log.h"
#include "xio_common.h"
#include "xio_protocol.h"
#include "xio_hash.h"
#include "xio_observer.h"
#include "xio_ev_data.h"
#include "xio_objpool.h"
#include "xio_workqueue.h"
#include "xio_protocol.h"
#include "xio_mbuf.h"
#include "xio_task.h"
#include "xio_transport.h"
#include "xio_context.h"
#include "xio_nexus_cache.h"
#include "xio_server.h"
#include "xio_session.h"
#include "xio_nexus.h"
#include "xio_msg_list.h"
#include "xio_connection.h"
#include <xio_env_adv.h>

/*---------------------------------------------------------------------------*/
/* private structures							     */
/*---------------------------------------------------------------------------*/
struct xio_observers_htbl_node {
	struct xio_observer	*observer;
	uint32_t		id;
	uint32_t		pad;
	struct list_head	observers_htbl_node;

};

struct xio_event_params {
	struct xio_ev_data			event;
	struct xio_nexus			*nexus;
	union xio_transport_event_data		event_data;
	struct list_head			events_list_node;
};

struct xio_nexus_observer_work {
	struct xio_observer_event	observer_event;
	xio_work_handle_t		observer_work;
};

static int xio_msecs[] = {60000, 30000, 15000, 0};

#define XIO_SERVER_GRACE_PERIOD 1000
#define XIO_SERVER_TIMEOUT (60000 + 30000 + 15000 + XIO_SERVER_GRACE_PERIOD)

/*---------------------------------------------------------------------------*/
/* forward declarations							     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_primary_pool_create(struct xio_nexus *nexus);
static int xio_nexus_primary_pool_recreate(struct xio_nexus *nexus);
static int xio_nexus_on_transport_event(void *observer, void *sender,
					int event, void *event_data);
static void xio_nexus_on_transport_closed(struct xio_nexus *nexus,
					  union xio_transport_event_data
					  *event_data);
static int xio_nexus_flush_all_tasks(struct xio_nexus *nexus);
static int xio_nexus_destroy(struct xio_nexus *nexus);
static int xio_nexus_xmit(struct xio_nexus *nexus);
static void xio_nexus_trans_release_handler(void *nexus_);
static void xio_nexus_trans_error_handler(void *ev_params_);
static void xio_nexus_error_handler(void *ev_params_);
static void xio_nexus_disconnect_handler(void *nexus_);

static void  xio_nexus_free_work_params(void *_nexus)
{
	struct xio_nexus *nexus = (struct xio_nexus *) _nexus;

	kfree(nexus->connect_work_params);
	nexus->connect_work_params = NULL;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_server_reconnect		                                     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_server_reconnect(struct xio_nexus *nexus);

/*---------------------------------------------------------------------------*/
/* xio_nexus_client_reconnect						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_client_reconnect(struct xio_nexus *nexus);

/*---------------------------------------------------------------------------*/
/* xio_nexus_client_reconnect_timeout					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_client_reconnect_failed(void *data);

static void xio_nexus_cancel_dwork(struct xio_nexus *nexus)
{
	xio_ctx_del_delayed_work(nexus->ctx,
				 &nexus->close_time_hndl);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_init_observers_htbl					     */
/*---------------------------------------------------------------------------*/
static inline void xio_nexus_init_observers_htbl(struct xio_nexus *nexus)
{
	INIT_LIST_HEAD(&nexus->observers_htbl);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_free_observers_htbl					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_free_observers_htbl(struct xio_nexus *nexus)
{
	struct xio_observers_htbl_node	*node, *next_node;

	list_for_each_entry_safe(node, next_node,
				 &nexus->observers_htbl,
				 observers_htbl_node) {
		list_del(&node->observers_htbl_node);
		xio_context_kfree(nexus->ctx, node);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_hash_observer						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_hash_observer(struct xio_nexus *nexus,
				   struct xio_observer *observer,
				   uint32_t id)
{
	struct xio_observers_htbl_node	*node;

	node = (struct xio_observers_htbl_node *)
			xio_context_kcalloc(nexus->ctx, 1, sizeof(*node), GFP_KERNEL);
	if (!node) {
		xio_set_error(ENOMEM);
		ERROR_LOG("xio_context_kcalloc failed. %m\n");
		return -1;
	}
	node->observer	= observer;
	node->id	= id;

	list_add_tail(&node->observers_htbl_node,
		      &nexus->observers_htbl);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_delete_observer						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_delete_observer(struct xio_nexus *nexus,
				     struct xio_observer *observer)
{
	struct xio_observers_htbl_node	*node, *next_node;

	list_for_each_entry_safe(node, next_node,
				 &nexus->observers_htbl,
				 observers_htbl_node) {
		if (node->observer == observer) {
			list_del(&node->observers_htbl_node);
			xio_context_kfree(nexus->ctx, node);
			return 0;
		}
	}

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_observer_lookup						     */
/*---------------------------------------------------------------------------*/
struct xio_observer *xio_nexus_observer_lookup(struct xio_nexus *nexus,
					       uint32_t id)
{
	struct xio_observers_htbl_node	*node, *next_node;

	list_for_each_entry_safe(node, next_node,
			    &nexus->observers_htbl,
			    observers_htbl_node) {
		if (node->id == id)
			return node->observer;
	}

	return NULL;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_reg_observer						     */
/*---------------------------------------------------------------------------*/
void xio_nexus_reg_observer(struct xio_nexus *nexus,
			    struct xio_observer *observer,
			    uint32_t oid)
{
	spin_lock(&nexus->nexus_obs_lock);
	xio_observable_reg_observer(&nexus->observable, observer);
	xio_nexus_hash_observer(nexus, observer, oid);
	spin_unlock(&nexus->nexus_obs_lock);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_unreg_observer						     */
/*---------------------------------------------------------------------------*/
void xio_nexus_unreg_observer(struct xio_nexus *nexus,
			      struct xio_observer *observer)
{
	spin_lock(&nexus->nexus_obs_lock);
	xio_nexus_delete_observer(nexus, observer);
	xio_observable_unreg_observer(&nexus->observable, observer);
	spin_unlock(&nexus->nexus_obs_lock);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_get_primary_task						     */
/*---------------------------------------------------------------------------*/
struct xio_task *xio_nexus_get_primary_task(struct xio_nexus *nexus)
{
	struct xio_task *task;

	if (unlikely(!nexus || !nexus->transport_hndl))
		return NULL;

	task = xio_tasks_pool_get(
			nexus->primary_tasks_pool, nexus->transport_hndl);

	if (unlikely(!task))
		return  NULL;

	task->nexus = nexus;

	return task;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_notify_server		                                     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_notify_server(struct xio_nexus *nexus, int event,
				    void *event_data)
{
	if (nexus->server)
		xio_observable_notify_observer(&nexus->observable,
					       &nexus->server->observer,
					       event, event_data);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_write_setup_req						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_write_setup_req(struct xio_task *task,
				     struct xio_nexus_setup_req *req)
{
	struct xio_nexus_setup_req *tmp_req;

	 /* reset the whole mbuf before building a message */
	 xio_mbuf_reset(&task->mbuf);

	/* set start of the tlv */
	if (xio_mbuf_tlv_start(&task->mbuf) != 0)
		return -1;

	/* set the mbuf after tlv header */
	xio_mbuf_set_val_start(&task->mbuf);

	tmp_req = (struct xio_nexus_setup_req *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* fill request */
	PACK_SVAL(req, tmp_req, version);
	PACK_SVAL(req, tmp_req, flags);
	PACK_LVAL(req, tmp_req, cid);

	xio_mbuf_inc(&task->mbuf, sizeof(struct xio_nexus_setup_req));

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_read_setup_req						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_read_setup_req(struct xio_task *task,
				    struct xio_nexus_setup_req *req)
{
	struct xio_nexus_setup_req *tmp_req;

	 /* reset the whole mbuf before building a message */
	 xio_mbuf_reset(&task->mbuf);

	/* set start of the tlv */
	if (xio_mbuf_tlv_start(&task->mbuf) != 0)
		return -1;

	tmp_req = (struct xio_nexus_setup_req *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* fill request */
	UNPACK_SVAL(tmp_req, req, version);
	UNPACK_SVAL(tmp_req, req, flags);
	UNPACK_LVAL(tmp_req, req, cid);

	xio_mbuf_inc(&task->mbuf, sizeof(struct xio_nexus_setup_req));

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_write_setup_rsp						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_write_setup_rsp(struct xio_task *task,
				     struct xio_nexus_setup_rsp *rsp)
{
	struct xio_nexus_setup_rsp *tmp_rsp;

	/* set start of the tlv */
	if (xio_mbuf_tlv_start(&task->mbuf) != 0)
		return -1;

	tmp_rsp = (struct xio_nexus_setup_rsp *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* fill request */
	PACK_LVAL(rsp, tmp_rsp, cid);
	PACK_LVAL(rsp, tmp_rsp, status);
	PACK_SVAL(rsp, tmp_rsp, version);
	PACK_SVAL(rsp, tmp_rsp, flags);

	xio_mbuf_inc(&task->mbuf, sizeof(struct xio_nexus_setup_rsp));

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_read_setup_rsp						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_read_setup_rsp(struct xio_task *task,
				    struct xio_nexus_setup_rsp *rsp)
{
	struct xio_nexus_setup_rsp *tmp_rsp;

	/* set start of the tlv */
	if (xio_mbuf_tlv_start(&task->mbuf) != 0)
		return -1;

	tmp_rsp = (struct xio_nexus_setup_rsp *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* fill request */
	UNPACK_LVAL(tmp_rsp, rsp, cid);
	UNPACK_LVAL(tmp_rsp, rsp, status);
	UNPACK_SVAL(tmp_rsp, rsp, version);
	UNPACK_SVAL(tmp_rsp, rsp, flags);

	xio_mbuf_inc(&task->mbuf, sizeof(struct xio_nexus_setup_rsp));

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_send_setup_req						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_send_setup_req(struct xio_nexus *nexus)
{
	struct xio_task	*task;
	struct xio_nexus_setup_req req = {0};
	struct xio_transport_base *trans_hndl;
	int    retval = 0;
	struct xio_tasks_pool *pool;

	DEBUG_LOG("send setup request nexus:%p\n", nexus);

	if (!nexus->transport->send) {
		ERROR_LOG("transport does not implement \"send\"\n");
		xio_set_error(ENOSYS);
		return -1;
	}
	/* when reconnecting before the dup2 send is done via new handle */
	if (nexus->state == XIO_NEXUS_STATE_RECONNECT) {
		req.flags = XIO_RECONNECT;
		req.cid = nexus->server_cid;
		trans_hndl = nexus->new_transport_hndl;
	} else {
		req.flags = 0;
		req.cid = 0;
		trans_hndl = nexus->transport_hndl;
	}

	if (nexus->srq_enabled)
		pool = nexus->primary_tasks_pool;
	else
		pool = nexus->initial_tasks_pool;
	task =  xio_tasks_pool_get(pool, trans_hndl);
	if (!task) {
		ERROR_LOG("%s task pool is empty\n", pool->params.pool_name);
		return -1;
	}
	task->nexus = nexus;
	task->tlv_type = XIO_NEXUS_SETUP_REQ;
	task->omsg = NULL;

	req.version = XIO_VERSION;

	retval = xio_nexus_write_setup_req(task, &req);
	if (retval)
		goto cleanup;

	/* always add it to the top */
	list_add(&task->tasks_list_entry, &nexus->tx_queue);

	if (!trans_hndl) {
		ERROR_LOG("null transport handle state=%d\n", nexus->state);
		xio_tasks_pool_put(task);
		return -1;
	}
	DEBUG_LOG("%s: nexus:%p, %s_hndl:%p\n", __func__,
		  nexus, xio_proto_str(trans_hndl->proto), trans_hndl);
	retval = nexus->transport->send(trans_hndl, task);
	if (retval != 0) {
		ERROR_LOG("send setup request failed\n");
		xio_tasks_pool_put(task);
		return -1;
	}

	return 0;

cleanup:
	xio_tasks_pool_put(task);
	xio_set_error(XIO_E_MSG_INVALID);
	ERROR_LOG("receiving setup request failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_swap							     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_swap(struct xio_nexus *old, struct xio_nexus *_new)
{
	struct xio_transport		*transport;
	struct xio_tasks_pool		*initial_tasks_pool;

	if (old->transport != _new->transport) {
		ERROR_LOG("can't swap not the same transport\n");
		return -1;
	}

	transport = old->transport;

	if (!transport->dup2) {
		ERROR_LOG("transport doesn't support dup2\n");
		return -ENOSYS;
	}

	/* SWAP observers */
	/* disconnect observers */
	xio_observable_unreg_observer(
			&_new->transport_hndl->observable,
			&_new->trans_observer);

	xio_observable_unreg_observer(
			&old->transport_hndl->observable,
			&old->trans_observer);

	/* reconnect observers (swapped) */
	xio_observable_reg_observer(
			&_new->transport_hndl->observable,
			&old->trans_observer);

	xio_observable_reg_observer(
			&old->transport_hndl->observable,
			&_new->trans_observer);

	/* Swap the initial pool as the setup request arrived on the a task
	 * from the initial pool and should be answered using the same task
	 */
	initial_tasks_pool = old->initial_tasks_pool;
	old->initial_tasks_pool = _new->initial_tasks_pool;
	_new->initial_tasks_pool = initial_tasks_pool;

	xio_tasks_pool_remap(old->primary_tasks_pool, _new->transport_hndl);
	/* make old_nexus->transport_hndl copy of new_nexus->transport_hndl
	 * old_nexus->trasport_hndl will be closed, note that observers were
	 * swapped
	 */
	if (transport->dup2(_new->transport_hndl, &old->transport_hndl)) {
		ERROR_LOG("dup2 transport failed\n");
		return -1;
	}

	/*
	 * Unregister the new_nexus (it was temporary) from the context.
	 */
	xio_context_unreg_observer(_new->ctx, &_new->ctx_observer);

	/* silently destroy new_nexus (it was temporary) but do not close
	 * its transport handler since it was copied from _new to old,
	 * _new->transport_hndl is now used as old_nexus->transport_hndl.
	 *
	 * if the failure is on the client side, destroy the temporary new_nexus.
	 * if the failure is on the server side, the temporary new_nexus will be
	 * destroyed after the transport closes (by calling xio_nexus_on_transport_closed
	 * after a XIO_TRANSPORT_EVENT_CLOSED occurs on the server side.
	 */
	_new->transport_hndl = NULL;
	if (old->transport_hndl->is_client) xio_nexus_destroy(_new);

	/* TODO what about messages held by the application */

	/* be ready to receive messages */
	if (xio_nexus_primary_pool_recreate(old)) {
		ERROR_LOG("recreate primary pool failed\n");
		return -1;
	}

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_force_close_timeout					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_on_force_close_timeout(int actual_timeout_ms, void *data)
{
	struct xio_nexus *nexus = (struct xio_nexus *)data;
	xio_nexus_force_close(nexus);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_schedule_force_close					     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_schedule_force_close(struct xio_nexus *nexus)
{
	int retval = xio_ctx_add_delayed_work(
			nexus->ctx,
			1, nexus,
			xio_nexus_on_force_close_timeout,
			&nexus->close_time_hndl);

	if (retval)
		ERROR_LOG("%s failed. nexus:%p, retval:%d\n",
			  __func__, nexus, retval);
	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_recv_setup_req						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_recv_setup_req(struct xio_nexus *new_nexus,
				       struct xio_task *task)
{
	struct xio_nexus_setup_req req;
	struct xio_nexus_setup_rsp rsp;
	struct xio_nexus *nexus;
	uint32_t status = 0;
	uint32_t cid;
	int      retval = 0;
	uint16_t flags = 0;

	if (new_nexus->state == XIO_NEXUS_STATE_CLOSED) {
		ERROR_LOG("got a request for a closing nexus %p\n", new_nexus);
	}

	DEBUG_LOG("receiving setup request nexus:%p\n", new_nexus);
	retval = xio_nexus_read_setup_req(task, &req);
	if (retval != 0)
		goto cleanup;

	/* verify version */
	if (req.version != XIO_VERSION) {
		ERROR_LOG("client invalid version.cver:0x%x, sver::0x%x\n",
			  req.version, XIO_VERSION);
		xio_nexus_schedule_force_close(new_nexus);
		xio_set_error(XIO_E_INVALID_VERSION);
		return -1;
	}

	/* by default nexus is the new nexus */
	nexus = new_nexus;
	if (req.flags & XIO_RECONNECT) {
		struct xio_nexus *dis_nexus;
		/* Server side reconnect strategy, use new transport with the
		 * old nexus
		 */
		cid = req.cid;
		flags = XIO_RECONNECT;
		dis_nexus = xio_nexus_cache_lookup(cid);
		if (dis_nexus && dis_nexus != new_nexus) {
			/* stop timer */
			xio_nexus_cancel_dwork(dis_nexus);

			retval = xio_nexus_swap(dis_nexus, new_nexus);
			if (retval != 0) {
				ERROR_LOG("swap nexus failed\n");
				return -1;
			}
			/* retransmission will start after setup response is
			 * transmitted - xio_nexus_on_send_setup_rsp_comp
			 */
			nexus = dis_nexus;
		} else {
			flags = XIO_CID;
			status = XIO_E_UNSUCCESSFUL;
		}
		goto send_response;
	}

	cid = nexus->cid;
	/* time to prepare the primary pool if srq is disabled. In case
	 * srq was enabled, it was created in order to send the nexus setup */
	if (!nexus->srq_enabled) {
		retval = xio_nexus_primary_pool_create(nexus);
		if (retval != 0) {
			ERROR_LOG("create primary pool failed\n");
			status = ENOMEM;
			goto send_response;
		}
	}

send_response:
	/* reset mbuf */
	xio_mbuf_reset(&task->mbuf);

	/* write response */
	task->tlv_type	= XIO_NEXUS_SETUP_RSP;
	task->omsg	= NULL;
	task->nexus	= nexus;

	rsp.cid		= cid;
	rsp.status	= status;
	rsp.version	= XIO_VERSION;
	rsp.flags	= flags;

	retval = xio_nexus_write_setup_rsp(task, &rsp);
	if (retval != 0)
		goto cleanup;

	/* send it */
	DEBUG_LOG("%s: nexus:%p, trans_hndl:%p\n", __func__,
		  nexus, nexus->transport_hndl);
	list_move(&task->tasks_list_entry, &nexus->tx_queue);
	retval = nexus->transport->send(nexus->transport_hndl, task);
	if (retval != 0) {
		ERROR_LOG("send setup response failed\n");
		return -1;
	}

	return 0;
cleanup:
	xio_set_error(XIO_E_MSG_INVALID);
	ERROR_LOG("receiving setup request failed\n");

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_prep_new_transport						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_prep_new_transport(struct xio_nexus *nexus)
{
	int retval;

	/* ignore close event on transport_hndl (part of dup2) */
	xio_observable_unreg_observer(
			&nexus->transport_hndl->observable,
			&nexus->trans_observer);

	/* nexus is an observer of the new transport (see open API)
	 * no need to register
	 */
	xio_tasks_pool_remap(nexus->primary_tasks_pool,
			     nexus->new_transport_hndl);
	/* make nexus->transport_hndl copy of nexus->new_transport_hndl
	 * old nexus->trasport_hndl will be closed
	 */
	if (nexus->transport->dup2(nexus->new_transport_hndl,
				   &nexus->transport_hndl)) {
		ERROR_LOG("dup2 transport failed\n");
		return -1;
	}

	/* TODO: what about messages held by the application */
	/* be ready to receive messages */
	retval = xio_nexus_primary_pool_recreate(nexus);
	if (retval != 0) {
		ERROR_LOG("recreate primary pool failed\n");
		return -1;
	}

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_recv_setup_rsp						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_recv_setup_rsp(struct xio_nexus *nexus,
				       struct xio_task *task)
{
	struct xio_nexus_setup_rsp	rsp;
	int				retval;

	DEBUG_LOG("receiving setup response. nexus:%p\n", nexus);
	retval = xio_nexus_read_setup_rsp(task, &rsp);
	if (retval != 0)
		goto cleanup;

	if (rsp.status) {
		xio_set_error(rsp.status);
		ERROR_LOG("remote peer reported status %d - [%s]\n",
			  rsp.status, xio_strerror(rsp.status));
		if (rsp.flags & XIO_CID) {
			/* reconnection is impossible since remote
			 * CID was not found on server side
			 */
			/* Stop timer */
			xio_nexus_cancel_dwork(nexus);
			if (nexus->state == XIO_NEXUS_STATE_RECONNECT) {
				retval = xio_nexus_prep_new_transport(nexus);
				if (retval != 0) {
					ERROR_LOG(
					      "prep new transport failed\n");
					return -1;
				}
			}

			/* Kill nexus */
			nexus->state = XIO_NEXUS_STATE_DISCONNECTED;
			DEBUG_LOG("nexus state changed to disconnected nexus:%p\n",nexus);
			xio_observable_notify_all_observers(
					&nexus->observable,
					XIO_NEXUS_EVENT_DISCONNECTED,
					NULL);
		} else {
			union xio_nexus_event_data nexus_event_data;

			nexus_event_data.error.reason =  XIO_E_CONNECT_ERROR;
			xio_observable_notify_all_observers(
					&nexus->observable,
					XIO_NEXUS_EVENT_ERROR,
					&nexus_event_data);
		}
		xio_tasks_pool_put(task->sender_task);
		task->sender_task = NULL;
		xio_tasks_pool_put(task);

		return 0;
	}
	if (rsp.version != XIO_VERSION) {
		ERROR_LOG("server invalid version.cver:0x%x, sver::0x%x\n",
			  XIO_VERSION, rsp.version);
		xio_tasks_pool_put(task->sender_task);
		task->sender_task = NULL;
		xio_tasks_pool_put(task);
		{
			union xio_nexus_event_data nexus_event_data;

			nexus_event_data.error.reason =  XIO_E_INVALID_VERSION;
			xio_observable_notify_all_observers(
					&nexus->observable,
					XIO_NEXUS_EVENT_ERROR,
					&nexus_event_data);
		}
		xio_nexus_schedule_force_close(nexus);
		return 0;
	}
	DEBUG_LOG("%s: nexus:%p, trans_hndl:%p\n", __func__,
		  nexus, nexus->transport_hndl);
	/* recycle the tasks */
	xio_tasks_pool_put(task->sender_task);
	task->sender_task = NULL;
	xio_tasks_pool_put(task);

	if (nexus->state != XIO_NEXUS_STATE_RECONNECT) {
		if (!nexus->srq_enabled) {
			/* create the primary */
			retval = xio_nexus_primary_pool_create(nexus);
			if (retval != 0) {
				ERROR_LOG("create primary pool failed\n");
				return -1;
			}
		}
		nexus->state = XIO_NEXUS_STATE_CONNECTED;

		xio_observable_notify_all_observers(&nexus->observable,
						    XIO_NEXUS_EVENT_ESTABLISHED,
						    NULL);
		/* remember server cid for reconnect */
		nexus->server_cid = rsp.cid;
	} else {
		/* Stop reconnect timer */
		xio_nexus_cancel_dwork(nexus);

		retval = xio_nexus_prep_new_transport(nexus);
		if (retval != 0) {
			ERROR_LOG("prep new transport failed\n");
			return -1;
		}
		nexus->state = XIO_NEXUS_STATE_CONNECTED;

		/* Tell session to re-initiate transmission */
		xio_observable_notify_all_observers(&nexus->observable,
						    XIO_NEXUS_EVENT_RECONNECTED,
						    NULL);
	}

	return 0;
cleanup:
	xio_set_error(XIO_E_MSG_INVALID);
	ERROR_LOG("receiving setup request failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_send_setup_rsp_comp					     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_send_setup_rsp_comp(struct xio_nexus *nexus,
					    struct xio_task *task)
{
	enum xio_nexus_event nexus_event;

	if (nexus->state == XIO_NEXUS_STATE_RECONNECT)
		/* Tell session to re-initiate transmission */
		nexus_event = XIO_NEXUS_EVENT_RECONNECTED;
	else
		nexus_event = XIO_NEXUS_EVENT_ESTABLISHED;

	/* Set new state */
	nexus->state = XIO_NEXUS_STATE_CONNECTED;
	xio_observable_notify_all_observers(&nexus->observable,
					    nexus_event,
					    NULL);

	/* recycle the task */
	xio_tasks_pool_put(task);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_recv_session_setup_req					     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_recv_session_setup_req(struct xio_nexus *nexus,
					       struct xio_task *task)
{
	union xio_nexus_event_data nexus_event_data;

	task->nexus = nexus;
	nexus_event_data.msg.task = task;
	nexus_event_data.msg.op = XIO_WC_OP_RECV;

	/* add reference count to opened nexus that new
	 * session is join in */
	if (!nexus->is_first_req)
		xio_nexus_addref(nexus);
	else
		nexus->is_first_req = 0;

	/* always route "hello" to server */
	xio_nexus_notify_server(
			nexus,
			XIO_NEXUS_EVENT_NEW_MESSAGE,
			&nexus_event_data);
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_recv_req						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_recv_req(struct xio_nexus *nexus,
				 struct xio_task *task)
{
	union xio_nexus_event_data nexus_event_data;

	task->nexus = nexus;
	nexus_event_data.msg.task = task;
	nexus_event_data.msg.op = XIO_WC_OP_RECV;

	/* route the message to any of observer */
	xio_observable_notify_any_observer(
			&nexus->observable,
			XIO_NEXUS_EVENT_NEW_MESSAGE,
			&nexus_event_data);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_recv_rsp						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_recv_rsp(struct xio_nexus *nexus,
				 struct xio_task *task)
{
	union xio_nexus_event_data nexus_event_data;

	task->nexus = nexus;

	nexus_event_data.msg.task = task;
	nexus_event_data.msg.op = XIO_WC_OP_RECV;
	if (likely(task->sender_task)) {
		if (unlikely(!task->sender_task->connection || !task->sender_task->nexus)) {
			DEBUG_LOG("connection disconnected from nexus. task dismissed\n");
			goto task_cleanup;
		}
		if (unlikely( task->sender_task->nexus != nexus)) {
			DEBUG_LOG("spurious event\n");
			goto task_cleanup;
		}
		if (unlikely(task->sender_task->connection->nexus != nexus)) {
			DEBUG_LOG("connection disconnected from nexus. task dismissed\n");
			goto task_cleanup;
		}
		/* route the response to the sender session */
		xio_observable_notify_observer(
				&nexus->observable,
				&task->sender_task->session->observer,
				XIO_NEXUS_EVENT_NEW_MESSAGE,
				&nexus_event_data);
	} else {
		/* route the message to any of observer */
		xio_observable_notify_any_observer(
			&nexus->observable,
			XIO_NEXUS_EVENT_NEW_MESSAGE,
			&nexus_event_data);
	}

	return 0;

task_cleanup:
	if (task->sender_task && !task->on_hold) {
		xio_tasks_pool_put(task->sender_task);
		task->sender_task = NULL;
	}
	xio_tasks_pool_put(task);
	return 0;

}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_send_msg_comp						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_send_msg_comp(struct xio_nexus *nexus,
				      struct xio_task *task)
{
	union xio_nexus_event_data nexus_event_data;

	nexus_event_data.msg.task	= task;
	nexus_event_data.msg.op		= XIO_WC_OP_SEND;

	if (task && task->session)
		xio_observable_notify_observer(
				&nexus->observable,
				&task->session->observer,
				XIO_NEXUS_EVENT_SEND_COMPLETION,
				&nexus_event_data);
	else
		ERROR_LOG("spurious event. nexus:%p, task:%p\n", nexus, task);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_initial_pool_create					     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_initial_pool_create(struct xio_nexus *nexus)
{
	struct xio_tasks_pool_ops	*pool_ops;
	struct xio_transport_base	*transport_hndl;
	struct xio_tasks_pool_cls	pool_cls;
	struct xio_context		*ctx;
	enum xio_proto			proto;
	int				retval;

	if (nexus->state == XIO_NEXUS_STATE_RECONNECT)
		transport_hndl = nexus->new_transport_hndl;
	else
		transport_hndl = nexus->transport_hndl;

	proto		= transport_hndl->proto;
	ctx		= nexus->ctx;

	retval = xio_ctx_pool_create(ctx, proto,
				     XIO_CONTEXT_POOL_CLASS_INITIAL);
	if (retval) {
		ERROR_LOG("Failed to create initial pool. nexus:%p\n", nexus);
		return -1;
	}

	/* set pool helpers to the transport */
	if (nexus->transport->set_pools_cls) {
		pool_cls.pool		= NULL;
		pool_cls.task_get	= (struct xio_task *(*)(void *, void *))
						xio_tasks_pool_get;
		pool_cls.task_lookup	= (struct xio_task * (*)(void *, int))
						xio_tasks_pool_lookup;
		pool_cls.task_put	= (void (*)(struct xio_task *))
						xio_tasks_pool_put;

		nexus->transport->set_pools_cls(transport_hndl,
						&pool_cls, NULL);
	}
	pool_ops = ctx->initial_pool_ops[proto];

	if (pool_ops->pool_post_create)
		pool_ops->pool_post_create(
				transport_hndl,
				ctx->initial_tasks_pool[proto],
				ctx->initial_tasks_pool[proto]->dd_data);

	nexus->initial_tasks_pool = ctx->initial_tasks_pool[proto];

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_initial_pool_create					     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_primary_pool_create(struct xio_nexus *nexus)
{
	struct xio_tasks_pool_ops	*pool_ops;
	struct xio_transport_base	*transport_hndl;
	struct xio_tasks_pool_cls	pool_cls;
	struct xio_context		*ctx;
	enum xio_proto			proto;
	int				retval;
	struct xio_task			*task;

	transport_hndl  = nexus->transport_hndl;
	proto		= transport_hndl->proto;
	ctx		= nexus->ctx;

	retval = xio_ctx_pool_create(ctx, proto,
				     XIO_CONTEXT_POOL_CLASS_PRIMARY);
	if (retval) {
		ERROR_LOG("Failed to create primary pool. nexus:%p\n", nexus);
		return -1;
	}

	/* set pool helpers to the transport */
	if (nexus->transport->set_pools_cls) {
		pool_cls.pool		= NULL;
		pool_cls.task_get	= (struct xio_task *(*)(void *, void *))
						xio_tasks_pool_get;
		pool_cls.task_lookup	= (struct xio_task * (*)(void *, int))
						xio_tasks_pool_lookup;
		pool_cls.task_put	= (void (*)(struct xio_task *))
						xio_tasks_pool_put;
		nexus->transport->set_pools_cls(transport_hndl,
						NULL, &pool_cls);
	}
	pool_ops = ctx->primary_pool_ops[proto];

	if (pool_ops->pool_post_create)
		pool_ops->pool_post_create(
				transport_hndl,
				ctx->primary_tasks_pool[proto],
				ctx->primary_tasks_pool[proto]->dd_data);

	nexus->primary_tasks_pool = ctx->primary_tasks_pool[proto];

	/* set pool context as the nexus's transport handler */
	nexus->primary_tasks_pool->params.pool_hooks.context = nexus->transport_hndl;

	list_for_each_entry(task, &nexus->primary_tasks_pool->stack, tasks_list_entry) {
		xio_task_reinit(nexus->transport_hndl, task);
	}

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_primary_pool_recreate					     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_primary_pool_recreate(struct xio_nexus *nexus)
{
	struct xio_tasks_pool_cls	pool_cls;
	struct xio_tasks_pool_ops	*pool_ops;
	struct xio_context		*ctx;
	enum xio_proto			proto;

	proto		= nexus->transport_hndl->proto;
	ctx		= nexus->ctx;
	pool_ops	= ctx->primary_pool_ops[proto];

	if (!pool_ops || !nexus->primary_tasks_pool)
		return -1;

	/* set pool helpers to the transport */
	if (nexus->transport->set_pools_cls) {
		pool_cls.pool		= NULL;
		pool_cls.task_get	= (struct xio_task *(*)(void *, void *))
						xio_tasks_pool_get;
		pool_cls.task_lookup	= (struct xio_task * (*)(void *, int))
						xio_tasks_pool_lookup;
		pool_cls.task_put	= xio_tasks_pool_put;

		nexus->transport->set_pools_cls(nexus->transport_hndl,
					       NULL,
					       &pool_cls);
	}
	/* Equivalent to old xio_rdma_primary_pool_run,
	 * will call xio_rdma_rearm_rq
	 */
	if (pool_ops->pool_post_create)
		pool_ops->pool_post_create(
				nexus->transport_hndl,
				nexus->primary_tasks_pool,
				nexus->primary_tasks_pool->dd_data);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_release_cb							     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_release_cb(int actual_timeout_ms, void *data)
{
	struct xio_nexus *nexus = (struct xio_nexus *)data;

	if (nexus->released || !xio_observable_is_empty(&nexus->observable))
		return;
	nexus->released = 1;

	if (nexus->transport_hndl)
		DEBUG_LOG("physical nexus close. nexus:%p %s_hndl:%p\n",
			  nexus, xio_proto_str(nexus->transport_hndl->proto),
			  nexus->transport_hndl);
	else
		DEBUG_LOG("physical nexus close. nexus:%p trans_hndl:NULL\n",
			  nexus);

	if (!nexus->is_listener)
		xio_nexus_cache_remove(nexus->cid);

	if (nexus->state != XIO_NEXUS_STATE_DISCONNECTED) {
		nexus->state = XIO_NEXUS_STATE_CLOSED;
		DEBUG_LOG("nexus state changed to closed. nexus:%p\n", nexus);
	}
	xio_ctx_del_delayed_work(nexus->ctx,
				 &nexus->close_time_hndl);

	/* now it is zero */
	if (nexus->transport_hndl &&
	    nexus->transport && nexus->transport->close)
		nexus->transport->close(nexus->transport_hndl);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_release							     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_release(void *data)
{
	struct xio_nexus *nexus = (struct xio_nexus *)data;

	if (nexus->released || !xio_observable_is_empty(&nexus->observable))
		return;

	xio_ctx_del_delayed_work(nexus->ctx,
				 &nexus->close_time_hndl);

	xio_nexus_release_cb(0, data);
}

/*---------------------------------------------------------------------------*/
/* xio_on_context_close							     */
/*---------------------------------------------------------------------------*/
static void xio_on_context_close(struct xio_nexus *nexus,
				 struct xio_context *ctx)
{
	DEBUG_LOG("xio_on_context_close. nexus:%p, ctx:%p\n", nexus, ctx);

	/* remove the nexus from table */
	xio_nexus_cache_remove(nexus->cid);

	xio_ctx_del_delayed_work(ctx, &nexus->close_time_hndl);

	/* shut down the context and its dependent without waiting */
	if (nexus->transport->context_shutdown)
		nexus->transport->context_shutdown(nexus->transport_hndl, ctx);

	/* at that stage the nexus may no longer exist */
}

/*---------------------------------------------------------------------------*/
/* xio_on_context_event							     */
/*---------------------------------------------------------------------------*/
static int xio_on_context_event(void *observer, void *sender, int event,
				void *event_data)
{
	DEBUG_LOG("xio_on_context_event\n");
	if (event == XIO_CONTEXT_EVENT_CLOSE) {
		DEBUG_LOG("context: [close] ctx:%p\n", sender);
		xio_on_context_close((struct xio_nexus *)observer,
				     (struct xio_context *)sender);
	}

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_on_server_close							     */
/*---------------------------------------------------------------------------*/
static void xio_on_server_close(struct xio_nexus *nexus,
				struct xio_server *server)
{
	DEBUG_LOG("xio_on_server_close. nexus:%p, server:%p\n", nexus, server);
	if (nexus->server) {
		xio_server_unreg_observer(nexus->server,
					  &nexus->srv_observer);
		nexus->server = NULL;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_on_server_event							     */
/*---------------------------------------------------------------------------*/
static int xio_on_server_event(void *observer, void *sender, int event,
			       void *event_data)
{
	DEBUG_LOG("xio_on_server_event\n");
	if (event == XIO_SERVER_EVENT_CLOSE) {
		DEBUG_LOG("server: [close] server:%p\n", sender);
		xio_on_server_close((struct xio_nexus *)observer,
				    (struct xio_server *)sender);
	}

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_create							     */
/*---------------------------------------------------------------------------*/
struct xio_nexus *xio_nexus_create(struct xio_nexus *parent_nexus,
				   struct xio_transport_base *transport_hndl)
{
	struct xio_nexus		*nexus;
	int			retval;

	if (parent_nexus->transport_hndl->is_client)
		return NULL;

	/* allocate nexus */
	nexus = (struct xio_nexus *)
			xio_context_kcalloc(transport_hndl->ctx,
					1, sizeof(struct xio_nexus), GFP_KERNEL);
	if (!nexus) {
		xio_set_error(ENOMEM);
		ERROR_LOG("xio_context_kcalloc failed. %m\n");
		return NULL;
	}

	XIO_OBSERVER_INIT(&nexus->trans_observer, nexus,
			  xio_nexus_on_transport_event);

	/* start listen to server events */
	XIO_OBSERVER_INIT(&nexus->srv_observer, nexus,
			  xio_on_server_event);

	spin_lock_init(&nexus->nexus_obs_lock);

	XIO_OBSERVABLE_INIT(&nexus->observable, nexus);

	xio_nexus_init_observers_htbl(nexus);

	/* start listen to context events */
	XIO_OBSERVER_INIT(&nexus->ctx_observer, nexus,
			  xio_on_context_event);

	INIT_LIST_HEAD(&nexus->tx_queue);

	xio_context_reg_observer(transport_hndl->ctx, &nexus->ctx_observer);

	/* add the nexus to temporary list */
	nexus->transport_hndl		= transport_hndl;
	nexus->transport		= parent_nexus->transport;
	nexus->server			= parent_nexus->server;
	nexus->srq_enabled		= parent_nexus->srq_enabled;
	kref_init(&nexus->kref);
	nexus->state			= XIO_NEXUS_STATE_OPEN;
	nexus->is_first_req		= 1;
	nexus->ctx			= transport_hndl->ctx;
	mutex_init(&nexus->lock_connect);

	xio_nexus_cache_add(nexus, &nexus->cid);

	/* add  the new nexus as observer to server */
	if (nexus->server)
		xio_server_reg_observer(nexus->server,
					&nexus->srv_observer);

	/* add  the new nexus as observer to transport */
	xio_transport_reg_observer(nexus->transport_hndl,
				   &nexus->trans_observer);

	if (nexus->transport->get_pools_setup_ops) {
		struct xio_context *ctx  = nexus->ctx;
		enum xio_proto proto = nexus->transport_hndl->proto;

		if (!ctx->primary_pool_ops[proto] ||
		    !ctx->initial_pool_ops[proto])
			nexus->transport->get_pools_setup_ops(
					nexus->transport_hndl,
					&ctx->initial_pool_ops[proto],
					&ctx->primary_pool_ops[proto]);
	} else {
		ERROR_LOG("transport does not implement \"add_observer\"\n");
		goto cleanup;
	}
	if (nexus->srq_enabled)
		retval = xio_nexus_primary_pool_create(nexus);
	else
		retval = xio_nexus_initial_pool_create(nexus);

	if (retval != 0) {
		ERROR_LOG("failed to setup pool\n");
		goto cleanup;
	}
	nexus->disconnect_event.handler		= xio_nexus_disconnect_handler;
	nexus->disconnect_event.data		= nexus;

	nexus->trans_release_event.handler	= xio_nexus_trans_release_handler;
	nexus->trans_release_event.data		= nexus;

	INIT_LIST_HEAD(&nexus->events_list);

	DEBUG_LOG("nexus: [new] ptr:%p, transport_hndl:%p\n", nexus,
		  nexus->transport_hndl);

	return nexus;

cleanup:
	xio_nexus_destroy(nexus);
	return NULL;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_message_error						     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_on_message_error(struct xio_nexus *nexus,
				       union xio_transport_event_data
				       *event_data)
{
	union xio_nexus_event_data	nexus_event_data;

	nexus_event_data.msg_error.reason =  event_data->msg_error.reason;
	nexus_event_data.msg_error.direction = event_data->msg_error.direction;
	nexus_event_data.msg_error.task	=  event_data->msg_error.task;

	xio_observable_notify_any_observer(&nexus->observable,
					   XIO_NEXUS_EVENT_MESSAGE_ERROR,
					   &nexus_event_data);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_new_transport						     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_on_new_transport(struct xio_nexus *nexus,
				       union xio_transport_event_data
				       *event_data)
{
	union xio_nexus_event_data	nexus_event_data;
	struct xio_nexus			*child_nexus;

	child_nexus = xio_nexus_create(
			nexus,
			event_data->new_connection.child_trans_hndl);

	DEBUG_LOG("%s: nexus:%p, trans_hndl:%p\n", __func__,
		  child_nexus, event_data->new_connection.child_trans_hndl);
	nexus_event_data.new_nexus.child_nexus = child_nexus;
	if (!child_nexus) {
		ERROR_LOG("failed to create child nexus\n");
		goto exit;
	}

	/* notify of new child to server */
	xio_nexus_notify_server(
			nexus,
			XIO_NEXUS_EVENT_NEW_CONNECTION,
			&nexus_event_data);

	return;
exit:
	xio_nexus_notify_server(
			nexus,
			XIO_NEXUS_EVENT_ERROR,
			&nexus_event_data);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_transport_closed					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_on_transport_closed(struct xio_nexus *nexus,
					  union xio_transport_event_data
					  *event_data)
{
	/* remove the nexus from table */
	xio_nexus_cache_remove(nexus->cid);

	xio_ctx_del_delayed_work(nexus->ctx,
				  &nexus->close_time_hndl);

	if (xio_observable_is_empty(&nexus->observable))
		xio_nexus_destroy(nexus);
	else {
		nexus->defered_close = 1;
		xio_transport_unreg_observer(nexus->transport_hndl,
					     &nexus->trans_observer);
		xio_context_unreg_observer(nexus->ctx,
					   &nexus->ctx_observer);
		nexus->transport_hndl = NULL;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_transport_error		                                     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_on_transport_error(struct xio_nexus *nexus,
					 union xio_transport_event_data
					 *event_data)
{
	union xio_nexus_event_data nexus_event_data;

	nexus_event_data.error.reason =  event_data->error.reason;

	xio_nexus_state_set(nexus, XIO_NEXUS_STATE_ERROR);
	xio_nexus_flush_all_tasks(nexus);
	if (!xio_observable_is_empty(&nexus->observable)) {
		xio_observable_notify_all_observers(&nexus->observable,
				XIO_NEXUS_EVENT_ERROR,
				&nexus_event_data);
	} else {
		xio_context_add_event(nexus->ctx,
				&nexus->trans_release_event);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_transport_established					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_on_transport_established(struct xio_nexus *nexus,
					       union xio_transport_event_data
					       *event_data)
{
	int retval;

	if (!nexus->transport_hndl->is_client)
		return;

	if (nexus->srq_enabled)
		retval = xio_nexus_primary_pool_create(nexus);
	else
		retval = xio_nexus_initial_pool_create(nexus);

	if (retval)
		ERROR_LOG("creation of task pool failed\n");

	xio_nexus_send_setup_req(nexus);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_trans_release_handler					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_trans_release_handler(void *nexus_)
{
	struct xio_nexus *nexus = (struct xio_nexus *)nexus_;

	xio_nexus_release(nexus);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_disconnect_handler						     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_disconnect_handler(void *nexus_)
{
	struct xio_nexus *nexus = (struct xio_nexus *)nexus_;

	DEBUG_LOG("%s - nexus:%p\n", __func__, nexus);
	xio_nexus_flush_all_tasks(nexus);
	if (!xio_observable_is_empty(&nexus->observable)) {
		xio_observable_notify_all_observers(
				&nexus->observable,
				XIO_NEXUS_EVENT_DISCONNECTED,
				NULL);
	} else {
		xio_context_add_event(nexus->ctx,
				      &nexus->trans_release_event);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_trans_error_handler					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_trans_error_handler(void *ev_params_)
{
	struct xio_event_params *ev_params =
				(struct xio_event_params *)ev_params_;
	struct xio_context *ctx;

	if (!ev_params || !ev_params->nexus)
		return;

	ctx = ev_params->nexus->ctx;

	list_del(&ev_params->events_list_node);

	xio_context_disable_event(&ev_params->event);

	if (ev_params->nexus->state == XIO_NEXUS_STATE_RECONNECT)
		xio_nexus_client_reconnect_failed(ev_params->nexus);
	else
		xio_nexus_on_transport_error(ev_params->nexus,
					     &ev_params->event_data);

	xio_context_kfree(ctx, ev_params);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_error_handler						     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_error_handler(void *ev_params_)
{
	struct xio_event_params *ev_params =
				(struct xio_event_params *)ev_params_;
	struct xio_context *ctx;
	union xio_nexus_event_data nexus_event_data;

	if (!ev_params || !ev_params->nexus)
		return;

	list_del(&ev_params->events_list_node);
	ctx = ev_params->nexus->ctx;

	xio_context_disable_event(&ev_params->event);

	nexus_event_data.msg_error.reason = ev_params->event_data.msg_error.reason;
	nexus_event_data.msg_error.direction = ev_params->event_data.msg_error.direction;
	nexus_event_data.msg_error.task = ev_params->event_data.msg_error.task;

	xio_observable_notify_any_observer(
			&ev_params->nexus->observable,
			XIO_NEXUS_EVENT_MESSAGE_ERROR,
			&nexus_event_data);

	xio_context_kfree(ctx, ev_params);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_transport_disconnecting				             */
/*---------------------------------------------------------------------------*/
static void xio_nexus_on_transport_disconnecting(struct xio_nexus *nexus,
						union xio_transport_event_data
						*event_data)
{
	/* remove the nexus from table */
	xio_nexus_cache_remove(nexus->cid);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_transport_disconnected				             */
/*---------------------------------------------------------------------------*/
static void xio_nexus_on_transport_disconnected(struct xio_nexus *nexus,
						union xio_transport_event_data
						*event_data)
{
	int ret;

	/* cancel old timers */
	xio_ctx_del_delayed_work(nexus->ctx,
				 &nexus->close_time_hndl);
	/* Try to reconnect */
	if (g_options.reconnect) {
		if (nexus->transport_hndl->is_client)
			ret = xio_nexus_client_reconnect(nexus);
		else
			ret = xio_nexus_server_reconnect(nexus);

		if (!ret) {
			DEBUG_LOG("reconnect attempt nexus:%p\n", nexus);
			return;
		}
		ERROR_LOG("can't reconnect nexus:%p\n", nexus);
	}

	/* Can't reconnect */
	nexus->state = XIO_NEXUS_STATE_DISCONNECTED;
	DEBUG_LOG("%s - nexus:%p\n", __func__, nexus);

	if (!nexus->is_listener)
		xio_nexus_cache_remove(nexus->cid);

	xio_context_add_event(nexus->ctx,
			      &nexus->disconnect_event);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_new_message				                     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_new_message(struct xio_nexus *nexus,
				    union xio_transport_event_data *event_data)
{
	int	retval = -1;
	struct xio_task	*task = event_data->msg.task;

	task->nexus = nexus;
	switch (task->tlv_type) {
	case XIO_NEXUS_SETUP_RSP:
		retval = xio_nexus_on_recv_setup_rsp(nexus, task);
		break;
	case XIO_NEXUS_SETUP_REQ:
		retval = xio_nexus_on_recv_setup_req(nexus, task);
		break;
	case XIO_CONNECTION_HELLO_REQ:
	case XIO_SESSION_SETUP_REQ:
		retval = xio_nexus_on_recv_session_setup_req(nexus, task);
		break;
	default:
		if (IS_REQUEST(task->tlv_type))
			retval = xio_nexus_on_recv_req(nexus, task);
		else if (IS_RESPONSE(task->tlv_type))
			retval = xio_nexus_on_recv_rsp(nexus, task);
		else
			ERROR_LOG("unexpected message type %u\n",
				  task->tlv_type);
		break;
	}

	if (retval != 0) {
		ERROR_LOG("failed to handle message. " \
			  "nexus:%p tlv_type:0x%x op:%d\n",
			  nexus, task->tlv_type, event_data->msg.op);
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_send_completion				                     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_send_completion(struct xio_nexus *nexus,
					union xio_transport_event_data
					*event_data)
{
	int	retval = -1;
	struct xio_task	*task = event_data->msg.task;

	switch (task->tlv_type) {
	case XIO_NEXUS_SETUP_RSP:
		retval = xio_nexus_on_send_setup_rsp_comp(nexus, task);
		break;
	case XIO_NEXUS_SETUP_REQ:
		retval = 0;
		break;
	default:
		retval = xio_nexus_on_send_msg_comp(nexus, task);
		break;
	}

	if (retval != 0) {
		ERROR_LOG("failed to handle message. " \
			  "nexus:%p tlv_type:%d op:%d\n",
			  nexus, task->tlv_type, event_data->msg.op);
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_direct_rdma_completion					     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_direct_rdma_completion(
	struct xio_nexus *nexus,
	union xio_transport_event_data *event_data)
{
	struct xio_task	*task = event_data->msg.task;
	union xio_nexus_event_data nexus_event_data;

	nexus_event_data.msg.task = task;
	nexus_event_data.msg.op = event_data->msg.op;

	xio_observable_notify_observer(
			&nexus->observable,
			&task->session->observer,
			XIO_NEXUS_EVENT_DIRECT_RDMA_COMPLETION,
			&nexus_event_data);
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_assign_in_buf						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_assign_in_buf(struct xio_nexus *nexus,
				      union xio_transport_event_data
				      *event_data)
{
	int				retval = 0;
	struct xio_task			*task = event_data->msg.task;
	union xio_nexus_event_data	nexus_event_data = {};

	nexus_event_data.assign_in_buf.task = event_data->msg.task;
	task->nexus = nexus;

	xio_observable_notify_any_observer(
			&nexus->observable,
			XIO_NEXUS_EVENT_ASSIGN_IN_BUF,
			&nexus_event_data);

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_on_transport_event		                                     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_on_transport_event(void *observer, void *sender,
					int event, void *event_data)
{
	struct xio_nexus		*nexus = (struct xio_nexus *)observer;
	struct xio_event_params		*ev_params;
	int				 tx = 1;
	union xio_transport_event_data *ev_data =
			(union xio_transport_event_data *)event_data;

	switch (event) {
	case XIO_TRANSPORT_EVENT_NEW_MESSAGE:
/*
		DEBUG_LOG("nexus: [notification] - new message. " \
			 "nexus:%p, transport:%p\n", observer, sender);
*/
		xio_nexus_on_new_message(nexus, ev_data);
		break;
	case XIO_TRANSPORT_EVENT_SEND_COMPLETION:
/*
		DEBUG_LOG("nexus: [notification] - send completion. " \
			 "nexus:%p, transport:%p\n", observer, sender);
*/
		xio_nexus_on_send_completion(nexus, ev_data);
		break;
	case XIO_TRANSPORT_EVENT_DIRECT_RDMA_COMPLETION:
		xio_nexus_on_direct_rdma_completion(nexus, ev_data);
		break;
	case XIO_TRANSPORT_EVENT_ASSIGN_IN_BUF:
		xio_nexus_on_assign_in_buf(nexus, ev_data);
		break;
	case XIO_TRANSPORT_EVENT_MESSAGE_ERROR:
		DEBUG_LOG("nexus: [notification] - message error. " \
			 "nexus:%p, transport:%p\n", observer, sender);
		xio_nexus_on_message_error(nexus, ev_data);
		break;
	case XIO_TRANSPORT_EVENT_NEW_CONNECTION:
		DEBUG_LOG("nexus: [notification] - new transport. " \
			 "nexus:%p, transport:%p\n", observer, sender);
		xio_nexus_on_new_transport(nexus, ev_data);
		break;
	case XIO_TRANSPORT_EVENT_ESTABLISHED:
		DEBUG_LOG("nexus: [notification] - transport established. " \
			 "nexus:%p, transport:%p\n", observer, sender);
		xio_nexus_on_transport_established(nexus, ev_data);
		break;
	case XIO_TRANSPORT_EVENT_DISCONNECTING:
		DEBUG_LOG("nexus: [notification] - transport disconnecting. "  \
			 "nexus:%p, transport:%p\n", observer, sender);
		xio_nexus_on_transport_disconnecting(nexus, ev_data);
		break;
	case XIO_TRANSPORT_EVENT_DISCONNECTED:
		DEBUG_LOG("nexus: [notification] - transport disconnected. "  \
			 "nexus:%p, transport:%p\n", observer, sender);
		xio_nexus_on_transport_disconnected(nexus, ev_data);
		tx = 0;
		break;
	case XIO_TRANSPORT_EVENT_CLOSED:
		DEBUG_LOG("nexus: [notification] - transport closed. "  \
			 "nexus:%p, transport:%p\n", observer, sender);
		xio_nexus_on_transport_closed(nexus, ev_data);
		// tx = 0;
		return 0;
	case XIO_TRANSPORT_EVENT_REFUSED:
		DEBUG_LOG("nexus: [notification] - transport refused. " \
			 "nexus:%p, transport:%p\n", observer, sender);
		if (nexus->state == XIO_NEXUS_STATE_RECONNECT) {
			xio_nexus_client_reconnect_failed(nexus);
		} else {
			nexus->state = XIO_NEXUS_STATE_DISCONNECTED;
			DEBUG_LOG("nexus state changed to disconnected. nexus:%p\n", nexus);
			xio_nexus_flush_all_tasks(nexus);
			xio_observable_notify_all_observers(
					&nexus->observable,
					XIO_NEXUS_EVENT_REFUSED,
					&event_data);
		}
		tx = 0;
		break;
	case XIO_TRANSPORT_EVENT_ERROR:
		DEBUG_LOG("nexus: [notification] - transport error. " \
			 "nexus:%p, transport:%p\n", observer, sender);
		ev_params = (struct xio_event_params *)
				xio_context_kcalloc(nexus->ctx, 1,
					sizeof(*ev_params), GFP_KERNEL);
		if (!ev_params) {
			ERROR_LOG("failed to allocate memory\n");
			return -1;
		}
		ev_params->nexus = nexus;
		memcpy(&ev_params->event_data, ev_data, sizeof(*ev_data));
		ev_params->event.handler = xio_nexus_trans_error_handler;
		ev_params->event.data = ev_params;
		list_add(&ev_params->events_list_node, &nexus->events_list);

		if (!nexus->is_listener)
			xio_nexus_cache_remove(nexus->cid);

		xio_context_add_event(nexus->ctx,
				      &ev_params->event);

		tx = 0;
		break;
	}

	if (tx && !list_empty(&nexus->tx_queue))
		xio_nexus_xmit(nexus);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_destroy		                                             */
/*---------------------------------------------------------------------------*/
static int xio_nexus_destroy(struct xio_nexus *nexus)
{
	struct xio_event_params *ev_data, *next_ev_data;

	DEBUG_LOG("nexus:%p - close complete\n", nexus);

	xio_context_disable_event(&nexus->trans_release_event);
	xio_context_disable_event(&nexus->disconnect_event);

	list_for_each_entry_safe(ev_data, next_ev_data,
				 &nexus->events_list,
				 events_list_node) {
		ev_data->nexus = NULL;
		list_del(&ev_data->events_list_node);
		xio_context_disable_event(&ev_data->event);
		xio_context_kfree(nexus->ctx, ev_data);
	}
	if (nexus->server)
		xio_server_unreg_observer(nexus->server,
					  &nexus->srv_observer);

	if (nexus->transport_hndl)
		xio_transport_unreg_observer(nexus->transport_hndl,
					     &nexus->trans_observer);

	spin_lock(&nexus->nexus_obs_lock);
	xio_nexus_free_observers_htbl(nexus);
	xio_observable_unreg_all_observers(&nexus->observable);
	spin_unlock(&nexus->nexus_obs_lock);

	xio_ctx_del_delayed_work(
			nexus->ctx,
			&nexus->close_time_hndl);

	xio_nexus_flush_all_tasks(nexus);

	xio_nexus_cache_remove(nexus->cid);

	xio_context_unreg_observer(nexus->ctx, &nexus->ctx_observer);

	xio_context_kfree(nexus->ctx, nexus->portal_uri);
	nexus->portal_uri = NULL;

	xio_context_kfree(nexus->ctx, nexus->out_if_addr);
	nexus->out_if_addr = NULL;

	XIO_OBSERVER_DESTROY(&nexus->trans_observer);

	XIO_OBSERVABLE_DESTROY(&nexus->observable);

	XIO_OBSERVER_DESTROY(&nexus->ctx_observer);
	XIO_OBSERVER_DESTROY(&nexus->srv_observer);
	mutex_destroy(&nexus->lock_connect);

	if (nexus->connect_work_params) {
		xio_ctx_del_work(
			nexus->ctx,
			&nexus->connect_work_params->observer_work);
		xio_nexus_free_work_params(nexus);
		nexus->connect_work_params = NULL;
	}
	xio_context_kfree(nexus->ctx, nexus);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_open		                                             */
/*---------------------------------------------------------------------------*/
struct xio_nexus *xio_nexus_open(struct xio_context *ctx,
				 const char *portal_uri,
				 struct xio_observer  *observer, uint32_t oid,
				 uint32_t attr_mask,
				 struct xio_nexus_init_attr *init_attr)

{
	struct xio_transport		*transport;
	struct xio_nexus		*nexus = NULL;
	char				proto[8];
	struct xio_transport_init_attr	*ptrans_init_attr = NULL;
	struct xio_nexus_query_params	query;

	/* look for opened nexus */
	query.ctx = ctx;
	query.portal_uri = portal_uri;
	query.tos = 0;
	query.tos_enabled = 0;
	if (attr_mask && init_attr) {
		if (test_bits(XIO_NEXUS_ATTR_TOS, &attr_mask)) {
			query.tos = init_attr->tos;
			query.tos_enabled = 1;
		}
	}
	/* find nexus in cache */
	nexus = xio_nexus_cache_find(&query);
	if (nexus && nexus->transport_hndl &&
	    nexus->state == XIO_NEXUS_STATE_CONNECTED) {
		DEBUG_LOG("nexus was found in cache. using it. nexus:%p\n", nexus);
		xio_ctx_del_delayed_work(nexus->ctx,
				         &nexus->close_time_hndl);
		if (observer) {
			spin_lock(&nexus->nexus_obs_lock);
			xio_observable_reg_observer(&nexus->observable,
						    observer);
			xio_nexus_hash_observer(nexus, observer, oid);
			spin_unlock(&nexus->nexus_obs_lock);
		}
		return nexus;
	} else {
		nexus = NULL;
	}

	/* extract portal from uri */
	if (xio_uri_get_proto(portal_uri, proto, sizeof(proto)) != 0) {
		xio_set_error(XIO_E_ADDR_ERROR);
		ERROR_LOG("parsing uri failed. uri: %s\n", portal_uri);
		return NULL;
	}
	/* get the transport's proto */
	transport = xio_get_transport(proto);
	if (!transport) {
		ERROR_LOG("failed to load %s transport layer.\n", proto);
		ERROR_LOG("validate that your system support %s " \
			  "and the accelio's %s module is loaded\n",
			  proto, proto);
		xio_set_error(ENOPROTOOPT);
		return NULL;
	}

	if (!transport->open) {
		ERROR_LOG("transport %s does not implement \"open\"\n",
			  proto);
		xio_set_error(ENOSYS);
		return NULL;
	}
	/* allocate nexus */
	nexus = (struct xio_nexus *)
			xio_context_kcalloc(ctx,
					1, sizeof(struct xio_nexus), GFP_KERNEL);
	if (!nexus) {
		xio_set_error(ENOMEM);
		ERROR_LOG("xio_context_kcalloc failed. %m\n");
		return NULL;
	}
	XIO_OBSERVER_INIT(&nexus->trans_observer, nexus,
			  xio_nexus_on_transport_event);
	XIO_OBSERVABLE_INIT(&nexus->observable, nexus);
	INIT_LIST_HEAD(&nexus->tx_queue);
	mutex_init(&nexus->lock_connect);

	INIT_LIST_HEAD(&nexus->events_list);

	xio_nexus_init_observers_htbl(nexus);

	if (observer) {
		spin_lock(&nexus->nexus_obs_lock);
		xio_observable_reg_observer(&nexus->observable, observer);
		xio_nexus_hash_observer(nexus, observer, oid);
		spin_unlock(&nexus->nexus_obs_lock);
	}

	/* start listen to server events */
	XIO_OBSERVER_INIT(&nexus->srv_observer, nexus,
			  xio_on_server_event);

	/* start listen to context events */
	XIO_OBSERVER_INIT(&nexus->ctx_observer, nexus,
			  xio_on_context_event);

	xio_context_reg_observer(ctx, &nexus->ctx_observer);

	if (attr_mask && init_attr) {
		if (test_bits(XIO_NEXUS_ATTR_TOS, &attr_mask)) {
			set_bits(XIO_TRANSPORT_ATTR_TOS,
				 &nexus->trans_attr_mask);
			nexus->trans_attr.tos = init_attr->tos;
			ptrans_init_attr = &nexus->trans_attr;
		}
	}

	nexus->ctx = ctx;
	nexus->transport_hndl = transport->open(
					transport, ctx,
					&nexus->trans_observer,
					nexus->trans_attr_mask,
					ptrans_init_attr);
	if (!nexus->transport_hndl) {
		ERROR_LOG("transport open failed\n");
		goto cleanup;
	}
	nexus->transport = transport;
	kref_init(&nexus->kref);
	nexus->state = XIO_NEXUS_STATE_OPEN;

#ifdef XIO_SRQ_ENABLE
	if (nexus->transport_hndl->proto == XIO_PROTO_RDMA)
		nexus->srq_enabled = 1;
	else
		nexus->srq_enabled = 0;
#else
	nexus->srq_enabled = 0;
#endif

	if (nexus->transport->get_pools_setup_ops) {
		struct xio_context *ctx  = nexus->ctx;
		enum xio_proto proto = nexus->transport_hndl->proto;

		if (!ctx->primary_pool_ops[proto] ||
		    !ctx->initial_pool_ops[proto])
			nexus->transport->get_pools_setup_ops(
					nexus->transport_hndl,
					&ctx->initial_pool_ops[proto],
					&ctx->primary_pool_ops[proto]);
	} else {
		ERROR_LOG("transport does not implement \"add_observer\"\n");
		goto cleanup;
	}
	nexus->disconnect_event.handler		= xio_nexus_disconnect_handler;
	nexus->disconnect_event.data		= nexus;

	nexus->trans_release_event.handler	= xio_nexus_trans_release_handler;
	nexus->trans_release_event.data		= nexus;

	xio_nexus_cache_add(nexus, &nexus->cid);

	DEBUG_LOG("nexus: [new] nexus:%p, transport_hndl:%p\n", nexus,
		  nexus->transport_hndl);

	return nexus;
cleanup:
	xio_nexus_destroy(nexus);

	return NULL;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_reconnect		                                             */
/* client side reconnection						     */
/*---------------------------------------------------------------------------*/
int xio_nexus_reconnect(struct xio_nexus *nexus)
{
	struct xio_transport *transport;
	struct xio_context *ctx;
	int retval;

	if (nexus->state != XIO_NEXUS_STATE_RECONNECT) {
		xio_set_error(XIO_E_STATE);
		ERROR_LOG("reconnect not permitted in current state(%d)\n",
			  nexus->state);
		return -1;
	}

	transport = nexus->transport;
	ctx = nexus->ctx;

	nexus->new_transport_hndl = transport->open(nexus->transport, ctx,
						   &nexus->trans_observer,
						   nexus->trans_attr_mask,
						   &nexus->trans_attr);

	if (!nexus->new_transport_hndl) {
		ERROR_LOG("transport open failed\n");
		return -1;
	}

	retval = transport->connect(nexus->new_transport_hndl,
				    nexus->portal_uri,
				    nexus->out_if_addr);

	if (retval != 0) {
		/* ignore close notification */
		xio_observable_unreg_observer(
				&nexus->new_transport_hndl->observable,
				&nexus->trans_observer);

		transport->close(nexus->new_transport_hndl);
		nexus->new_transport_hndl = NULL;
		ERROR_LOG("transport reconnect failed\n");
		return -1;
	}

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_notify_observer_work                                            */
/*---------------------------------------------------------------------------*/
static void xio_nexus_notify_observer_work(int actual_timeout_ms, void *_work_params)
{
	struct xio_nexus *nexus =
                (struct xio_nexus *) _work_params;

	if (!nexus || !nexus->connect_work_params)
		return;

	DEBUG_LOG("%s: nexus:%p, state:%d\n", __func__,
		  nexus, nexus->state);

	if (nexus->state == XIO_NEXUS_STATE_CONNECTED)
		xio_observable_notify_observer(
				nexus->connect_work_params->observer_event.observable,
				nexus->connect_work_params->observer_event.observer,
				nexus->connect_work_params->observer_event.event,
				nexus->connect_work_params->observer_event.event_data);
	xio_ctx_set_work_destructor(nexus->ctx,
	                            nexus,
				    xio_nexus_free_work_params,
				    &nexus->connect_work_params->observer_work);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_connect                                                         */
/*---------------------------------------------------------------------------*/
int xio_nexus_connect(struct xio_nexus *nexus, const char *portal_uri,
		      struct xio_observer *observer, const char *out_if)
{
	int retval;

	if (!nexus->transport->connect) {
		ERROR_LOG("transport does not implement \"connect\"\n");
		xio_set_error(ENOSYS);
		return -1;
	}
	DEBUG_LOG("%s: nexus:%p, %s_hndl:%p, portal:%s, state:%d\n", __func__,
			nexus, xio_proto_str(nexus->transport_hndl->proto),
			nexus->transport_hndl, portal_uri, nexus->state);
	mutex_lock(&nexus->lock_connect);
	switch (nexus->state) {
	case XIO_NEXUS_STATE_OPEN:
		/* for reconnect */
		nexus->portal_uri = xio_context_kstrdup(nexus->ctx, portal_uri, GFP_KERNEL);
		if (!nexus->portal_uri) {
			ERROR_LOG("memory alloc failed\n");
			xio_set_error(ENOMEM);
			goto cleanup1;
		}
		if (out_if) {
			nexus->out_if_addr  = xio_context_kstrdup(nexus->ctx, out_if, GFP_KERNEL);
			if (!nexus->out_if_addr) {
				ERROR_LOG("memory alloc failed\n");
				xio_set_error(ENOMEM);
				goto cleanup2;
			}
		}
		retval = nexus->transport->connect(nexus->transport_hndl,
						  portal_uri,
						  out_if);
		if (retval != 0)
			goto cleanup3;
		nexus->state = XIO_NEXUS_STATE_CONNECTING;
		break;
	case XIO_NEXUS_STATE_CONNECTED:
		/* moving the notification to the ctx the nexus is running on
		 * to avoid session_setup_request from being sent on another thread
		 */
		nexus->connect_work_params = (struct xio_nexus_observer_work *)
				xio_context_kmalloc(nexus->ctx,
						sizeof(*nexus->connect_work_params), GFP_KERNEL);
		if (unlikely(!nexus->connect_work_params)) {
			ERROR_LOG("failed to allocate memory\n");
			goto cleanup1;
		}
		memset(&nexus->connect_work_params->observer_work, 0,
		       sizeof(nexus->connect_work_params->observer_work));
		nexus->connect_work_params->observer_event.observer = observer;
		nexus->connect_work_params->observer_event.observable = &nexus->observable;
		nexus->connect_work_params->observer_event.event = XIO_NEXUS_EVENT_ESTABLISHED;
		nexus->connect_work_params->observer_event.event_data = NULL;
		xio_ctx_add_work(nexus->ctx,
                                 nexus,
                                 xio_nexus_notify_observer_work,
                                 &nexus->connect_work_params->observer_work);
		break;
	default:
		break;
	}
	mutex_unlock(&nexus->lock_connect);

	return 0;

cleanup3:
	xio_context_kfree(nexus->ctx, nexus->out_if_addr);
	nexus->out_if_addr = NULL;
cleanup2:
	xio_context_kfree(nexus->ctx, nexus->portal_uri);
	nexus->portal_uri = NULL;
cleanup1:
	ERROR_LOG("transport connect failed\n");
	mutex_unlock(&nexus->lock_connect);
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_listen			                                     */
/*---------------------------------------------------------------------------*/
int xio_nexus_listen(struct xio_nexus *nexus, const char *portal_uri,
		     uint16_t *src_port, int backlog)
{
	int retval;

	if (!nexus->transport->listen) {
		ERROR_LOG("transport does not implement \"listen\"\n");
		xio_set_error(ENOSYS);
		return -1;
	}
	if (nexus->state == XIO_NEXUS_STATE_OPEN) {
		/* do not hold the listener nexus in storage */
		xio_nexus_cache_remove(nexus->cid);
		retval = nexus->transport->listen(nexus->transport_hndl,
						 portal_uri, src_port,
						 backlog);
		if (retval != 0) {
			DEBUG_LOG("transport listen failed. uri:[%s]\n",
				  portal_uri);
			return -1;
		}
		nexus->state = XIO_NEXUS_STATE_LISTEN;
		nexus->is_listener = 1;
	}

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_accept			                                     */
/*---------------------------------------------------------------------------*/
int xio_nexus_accept(struct xio_nexus *nexus)
{
	int retval;

	if (!nexus->transport->accept) {
		ERROR_LOG("transport does not implement \"accept\"\n");
		xio_set_error(ENOSYS);
		return -1;
	}
	if (nexus->state == XIO_NEXUS_STATE_OPEN) {
		retval = nexus->transport->accept(nexus->transport_hndl);
		if (retval != 0) {
			ERROR_LOG("transport accept failed.\n");
			return -1;
		}
	}
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_reject			                                     */
/*---------------------------------------------------------------------------*/
int xio_nexus_reject(struct xio_nexus *nexus)
{
	int retval;

	if (!nexus->transport->reject) {
		ERROR_LOG("transport does not implement \"reject\"\n");
		xio_set_error(ENOSYS);
		return -1;
	}
	if (nexus->state == XIO_NEXUS_STATE_OPEN) {
		retval = nexus->transport->reject(nexus->transport_hndl);
		if (retval != 0) {
			ERROR_LOG("transport reject failed.\n");
			return -1;
		}
	}
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_delayed_close		                                     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_delayed_close(struct kref *kref)
{
	struct xio_nexus *nexus = container_of(kref,
					     struct xio_nexus,
					     kref);
	int		retval;

	DEBUG_LOG("%s. nexus:%p, state:%d\n", __func__,
		  nexus, nexus->state);

	switch (nexus->state) {
	case XIO_NEXUS_STATE_LISTEN:
		/* the listener nexus, called from xio_unbind */
	case XIO_NEXUS_STATE_OPEN:
	case XIO_NEXUS_STATE_ERROR:
	case XIO_NEXUS_STATE_DISCONNECTED:
		xio_nexus_release(nexus);
		break;
	default:
		/* only client shall cause disconnection */
		retval = xio_ctx_add_delayed_work(
				nexus->ctx,
				g_options.transport_close_timeout, nexus,
				xio_nexus_release_cb,
				&nexus->close_time_hndl);
		if (retval)
			ERROR_LOG("xio_nexus_delayed_close failed\n");
		break;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_close		                                             */
/*---------------------------------------------------------------------------*/
void xio_nexus_close(struct xio_nexus *nexus, struct xio_observer *observer)
{
	DEBUG_LOG("nexus: [putref] ptr:%p, refcnt:%d\n", nexus,
		  atomic_read(&nexus->kref.refcount));

	if (nexus->defered_close && xio_observable_is_empty(&nexus->observable)) {
		xio_nexus_destroy(nexus);
		return;
	}

	if (observer) {
		xio_nexus_notify_observer(
				nexus, observer,
				XIO_NEXUS_EVENT_CLOSED, NULL);
		xio_nexus_unreg_observer(nexus, observer);
	}
	kref_put(&nexus->kref, xio_nexus_delayed_close);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_disconnect		                                             */
/*---------------------------------------------------------------------------*/
void xio_nexus_disconnect(struct xio_nexus *nexus, struct xio_observer *observer)
{
	DEBUG_LOG("%s - nexus:%p,  state:%d\n",
		  __func__, nexus, nexus->state);
	nexus->state = XIO_NEXUS_STATE_DISCONNECTED;
	xio_nexus_close(nexus, observer);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_force_close						     */
/*---------------------------------------------------------------------------*/
void xio_nexus_force_close(struct xio_nexus *nexus)
{
	DEBUG_LOG("nexus: [putref] ptr:%p, refcnt:%d\n", nexus,
		  atomic_read(&nexus->kref.refcount));

	if (nexus->defered_close &&
	    xio_observable_is_empty(&nexus->observable)) {
		xio_nexus_destroy(nexus);
		return;
	}
	xio_observable_notify_all_observers(&nexus->observable,
			XIO_NEXUS_EVENT_CLOSED,
			NULL);

	/* unreg all observers */
	xio_observable_unreg_all_observers(&nexus->observable);
	/* remove all refcounts */
	kref_init(&nexus->kref);
	/* release */
	xio_nexus_release(nexus);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_flush_all_tasks						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_flush_all_tasks(struct xio_nexus *nexus)
{
	if (!list_empty(&nexus->tx_queue)) {
		DEBUG_LOG("tx_queue not empty! nexus:%p\n", nexus);
		xio_tasks_list_flush(&nexus->tx_queue);
	}

	return 0;
}

static struct xio_task *find_first_response_task(struct xio_nexus *nexus)
{
	struct xio_task *task;
	list_for_each_entry(task,&nexus->tx_queue, tasks_list_entry) {
		if (IS_RESPONSE(task->tlv_type))
			return task;
	}
	return NULL;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_xmit							     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_xmit(struct xio_nexus *nexus)
{
	int retval = 0;
	struct xio_task	*task;

	if (!nexus->transport) {
		ERROR_LOG("transport not initialized. nexus:%p\n", nexus);
		return -1;
	}
	if (!nexus->transport->send || !nexus->transport_hndl)
		return 0;

	if (nexus->state == XIO_NEXUS_STATE_DISCONNECTED)
		return 0;

	while (1) {
		if (list_empty(&nexus->tx_queue))
			break;

		task = list_first_entry(&nexus->tx_queue,
					struct xio_task,  tasks_list_entry);
		retval = nexus->transport->send(nexus->transport_hndl, task);
		if (retval != 0) {
			struct xio_event_params *ev_params;

			if (xio_errno() == EAGAIN) {
				if (IS_REQUEST(task->tlv_type)) {
					task = find_first_response_task(nexus);
					if (!task)
						return 0;

					retval = nexus->transport->send(
							nexus->transport_hndl, task);
					if (!retval || xio_errno() == EAGAIN)
						return 0;
				} else {
					return 0;
				}
			}

			ERROR_LOG("transport send failed err:%d\n",
				  xio_errno());
			ev_params = (struct xio_event_params *)
				xio_context_kcalloc(nexus->ctx, 1,
						sizeof(*ev_params), GFP_KERNEL);
			if (!ev_params) {
				ERROR_LOG("failed to allocate memory\n");
				return -1;
			}
			ev_params->nexus = nexus;
			ev_params->event_data.msg_error.reason =
						(enum xio_status)xio_errno();
			ev_params->event_data.msg_error.direction =
							XIO_MSG_DIRECTION_OUT;
			ev_params->event_data.msg_error.task = task;
			ev_params->event.handler = xio_nexus_error_handler;
			ev_params->event.data = ev_params;
			list_add(&ev_params->events_list_node, &nexus->events_list);

			xio_context_add_event(nexus->ctx,
				              &ev_params->event);

			/* special error for connection */
			xio_set_error(ENOMSG);
			retval = -ENOMSG;

			break;
		}
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_send							     */
/*---------------------------------------------------------------------------*/
int xio_nexus_send(struct xio_nexus *nexus, struct xio_task *task)
{
	int		retval;

	if (!nexus->transport) {
		ERROR_LOG("transport not initialized. nexus:%p\n", nexus);
		return -1;
	}
	if (!nexus->transport->send)
		return 0;

	/* push to end of the queue - prioritize ka */
	if (IS_KEEPALIVE(task->tlv_type))
		list_move(&task->tasks_list_entry, &nexus->tx_queue);
	else
		list_move_tail(&task->tasks_list_entry, &nexus->tx_queue);

	/* xmit it to the transport */
	retval = xio_nexus_xmit(nexus);

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_poll							     */
/*---------------------------------------------------------------------------*/
int xio_nexus_poll(struct xio_nexus *nexus, long min_nr, long nr,
		   struct timespec *timeout)
{
	int	retval = 0;

	if (unlikely(!nexus || !nexus->transport_hndl ||
		     !nexus->transport)) {
		xio_set_error(EINVAL);
		return -1;
	}
	if (nexus->transport->poll) {
		retval = nexus->transport->poll(nexus->transport_hndl,
					       min_nr, nr, timeout);
		if (retval < 0) {
			ERROR_LOG("transport poll failed\n");
			return -1;
		}
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_set_opt							     */
/*---------------------------------------------------------------------------*/
int xio_nexus_set_opt(struct xio_nexus *nexus, int optname, const void *optval,
		      int optlen)
{
	if (unlikely(!nexus || !nexus->transport_hndl ||
		     !nexus->transport)) {
		xio_set_error(EINVAL);
		return -1;
	}

	if (nexus->transport->set_opt)
		return nexus->transport->set_opt(nexus->transport_hndl,
				optname, optval, optlen);

	xio_set_error(XIO_E_NOT_SUPPORTED);
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_get_opt							     */
/*---------------------------------------------------------------------------*/
int xio_nexus_get_opt(struct xio_nexus *nexus, int optname, void *optval,
		      int *optlen)
{
	if (unlikely(!nexus || !nexus->transport_hndl ||
		     !nexus->transport)) {
		xio_set_error(EINVAL);
		return -1;
	}

	if (nexus->transport->get_opt)
		return nexus->transport->get_opt(nexus->transport_hndl,
				optname, optval, optlen);

	xio_set_error(XIO_E_NOT_SUPPORTED);
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_modify							     */
/*---------------------------------------------------------------------------*/
int xio_nexus_modify(struct xio_nexus *nexus,
		     struct xio_nexus_attr *attr, int attr_mask)
{
	int			   tattr_mask = 0;
	struct xio_transport_attr tattr;

	if (unlikely(!nexus || !nexus->transport_hndl ||
		     !nexus->transport)) {
		xio_set_error(EINVAL);
		return -1;
	}

	if (!nexus->transport->modify)
		goto not_supported;

	memset(&tattr, 0, sizeof(tattr));
	if (test_flag(XIO_NEXUS_ATTR_TOS, &attr_mask)) {
		tattr_mask |= XIO_TRANSPORT_ATTR_TOS;
		tattr.tos = attr->tos;
	}
	if (tattr_mask == 0)
		goto not_supported;

	return nexus->transport->modify(nexus->transport_hndl,
					&tattr, tattr_mask);
not_supported:
	xio_set_error(XIO_E_NOT_SUPPORTED);
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_query							     */
/*---------------------------------------------------------------------------*/
int xio_nexus_query(struct xio_nexus *nexus,
		    struct xio_nexus_attr *attr, int attr_mask)
{
	int			   tattr_mask = 0, retval;
	struct xio_transport_attr tattr;

	if (unlikely(!nexus || !nexus->transport_hndl ||
		     !nexus->transport)) {
		xio_set_error(EINVAL);
		return -1;
	}

	if (!nexus->transport->modify)
		goto not_supported;

	memset(&tattr, 0, sizeof(tattr));
	if (test_flag(XIO_NEXUS_ATTR_TOS, &attr_mask))
		tattr_mask |= XIO_TRANSPORT_ATTR_TOS;

	if (tattr_mask == 0)
		goto not_supported;

	retval = nexus->transport->query(nexus->transport_hndl,
					 &tattr, tattr_mask);
	if (retval)
		return -1;

	if (test_flag(XIO_NEXUS_ATTR_TOS, &attr_mask))
		attr->tos = tattr.tos;

not_supported:
	xio_set_error(XIO_E_NOT_SUPPORTED);
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_get_peer_addr						     */
/*---------------------------------------------------------------------------*/
int xio_nexus_get_peer_addr(struct xio_nexus *nexus,
			    struct sockaddr_storage *sa, socklen_t len)
{
	if (unlikely(!nexus || !nexus->transport_hndl)) {
		xio_set_error(EINVAL);
		memset(sa, 0, sizeof(*sa));
		return -1;
	}
	memcpy(sa, &nexus->transport_hndl->peer_addr, len);
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_get_local_addr						     */
/*---------------------------------------------------------------------------*/
int xio_nexus_get_local_addr(struct xio_nexus *nexus,
			     struct sockaddr_storage *sa, socklen_t len)
{
	if (unlikely(!nexus || !nexus->transport_hndl)) {
		xio_set_error(EINVAL);
		memset(sa, 0, sizeof(*sa));
		return -1;
	}
	memcpy(sa, &nexus->transport_hndl->local_addr, len);
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_server_reconnect_timeout					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_server_reconnect_timeout(int actual_timeout_ms, void *data)
{
	struct xio_nexus *nexus = (struct xio_nexus *)data;

	/* No reconnect within timeout */
	nexus->state = XIO_NEXUS_STATE_DISCONNECTED;
	DEBUG_LOG("nexus state changed to disconnected. nexus:%p\n", nexus);
	xio_nexus_flush_all_tasks(nexus);
	xio_observable_notify_all_observers(&nexus->observable,
					    XIO_NEXUS_EVENT_DISCONNECTED,
					    NULL);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_server_reconnect		                                     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_server_reconnect(struct xio_nexus *nexus)
{
	int		retval;

	if (nexus->state != XIO_NEXUS_STATE_CONNECTED)
		return -1;

	xio_nexus_state_set(nexus, XIO_NEXUS_STATE_RECONNECT);

	xio_observable_notify_all_observers(&nexus->observable,
						XIO_NEXUS_EVENT_RECONNECTING,
					    NULL);

	/* Just wait and see if some client tries to reconnect */
	retval = xio_ctx_add_delayed_work(nexus->ctx,
					  XIO_SERVER_TIMEOUT, nexus,
					  xio_nexus_server_reconnect_timeout,
					  &nexus->close_time_hndl);
	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_client_reconnect_timeout					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_client_reconnect_timeout(int actual_timeout_ms, void *data)
{
	struct xio_nexus *nexus = (struct xio_nexus *)data;
	int retval;

	/* Try to reconnect after the waiting period */
	retval = xio_nexus_reconnect(nexus);
	if (!retval) {
		DEBUG_LOG("reconnect succeed. nexus:%p\n", nexus);
		return;
	}

	if (nexus->reconnect_retries) {
		nexus->reconnect_retries--;
		xio_ctx_add_delayed_work(
				nexus->ctx,
				xio_msecs[nexus->reconnect_retries],
				nexus,
				xio_nexus_client_reconnect_timeout,
				&nexus->close_time_hndl);
	} else {
		/* retries number exceeded */
		nexus->state = XIO_NEXUS_STATE_DISCONNECTED;
		DEBUG_LOG("nexus state changed to disconnected. nexus:%p\n", nexus);
		xio_nexus_flush_all_tasks(nexus);
		xio_observable_notify_all_observers(
				&nexus->observable,
				XIO_NEXUS_EVENT_DISCONNECTED,
				NULL);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_client_reconnect_failed					     */
/*---------------------------------------------------------------------------*/
static void xio_nexus_client_reconnect_failed(void *data)
{
	struct xio_nexus *nexus = (struct xio_nexus *)data;
	int retval;

	retval = xio_nexus_prep_new_transport(nexus);
	if (retval != 0)
		ERROR_LOG("prep new transport failed\n");

	/* Failed to reconnect (connect was called) */
	if (nexus->reconnect_retries) {
		nexus->reconnect_retries--;
		retval = xio_ctx_add_delayed_work(
				nexus->ctx,
				xio_msecs[nexus->reconnect_retries],
				nexus,
				xio_nexus_client_reconnect_timeout,
				&nexus->close_time_hndl);
		if (retval)
			ERROR_LOG("adding delayed work failed\n");
	} else {
		/* retries number exceeded */
		nexus->state = XIO_NEXUS_STATE_DISCONNECTED;
		DEBUG_LOG("nexus state changed to disconnected. nexus:%p\n", nexus);
		xio_nexus_flush_all_tasks(nexus);
		xio_observable_notify_all_observers(
				&nexus->observable,
				XIO_NEXUS_EVENT_DISCONNECTED,
				NULL);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_client_reconnect						     */
/*---------------------------------------------------------------------------*/
static int xio_nexus_client_reconnect(struct xio_nexus *nexus)
{
	/* With client we do an exponential back-off first delay is 0 */
	int		retval;

	if (nexus->state != XIO_NEXUS_STATE_CONNECTED)
		return -1;

	if (!nexus->transport->dup2)
		return -1;

	if (nexus->state == XIO_NEXUS_STATE_RECONNECT)
		return 0;

	xio_nexus_state_set(nexus, XIO_NEXUS_STATE_RECONNECT);

	xio_observable_notify_all_observers(&nexus->observable,
					    XIO_NEXUS_EVENT_RECONNECTING,
					    NULL);

	/* All portal_uri and out_if were saved in the nexus
	 * observer is not used in this flow
	 */

	/* Three retries but vector start from 0 */
	nexus->reconnect_retries = 3;
	/* Try to reconnect immediately
	 * Note connect may succeed but we may get a reject */
	retval = xio_nexus_reconnect(nexus);
	if (!retval)
		return 0;

	nexus->reconnect_retries = 2;
	retval = xio_ctx_add_delayed_work(nexus->ctx,
					  xio_msecs[nexus->reconnect_retries],
					  nexus,
					  xio_nexus_client_reconnect_timeout,
					  &nexus->close_time_hndl);

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_update_task						     */
/*---------------------------------------------------------------------------*/
int xio_nexus_update_task(struct xio_nexus *nexus, struct xio_task *task)
{
	/* transport may not need to update tasks */
	if (!nexus->transport->update_task)
		return 0;

	if (nexus->transport->update_task(nexus->transport_hndl, task))
		return -1;

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_update_rkey						     */
/*---------------------------------------------------------------------------*/
int xio_nexus_update_rkey(struct xio_nexus *nexus,
			  uint32_t *rkey)
{
	if (!nexus->transport->update_rkey)
		return 0;

	if (nexus->transport->update_rkey(nexus->transport_hndl, rkey))
		return -1;
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_set_server							     */
/*---------------------------------------------------------------------------*/
void xio_nexus_set_server(struct xio_nexus *nexus, struct xio_server *server)
{
	nexus->server = server;
	if (server)
		xio_server_reg_observer(server, &nexus->srv_observer);
}

/*---------------------------------------------------------------------------*/
/* xio_nexus_dump_tasks_queues						     */
/*---------------------------------------------------------------------------*/
void xio_nexus_dump_tasks_queues(struct xio_nexus *nexus)
{

	if (!list_empty(&nexus->tx_queue)) {
		xio_dump_task_list("nexus", nexus,
				   &nexus->tx_queue,
				   "tx_queue");
	}
	if (nexus->transport->dump_tasks_queues) {
		nexus->transport->dump_tasks_queues(nexus->transport_hndl);
	}
}
