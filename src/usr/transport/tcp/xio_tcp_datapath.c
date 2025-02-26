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
#include "xio_observer.h"
#include "xio_protocol.h"
#include "xio_mbuf.h"
#include "xio_task.h"
#include "xio_sg_table.h"
#include "xio_transport.h"
#include "xio_usr_transport.h"
#include "xio_ev_data.h"
#include "xio_objpool.h"
#include "xio_workqueue.h"
#include "xio_context.h"
#include "xio_tcp_transport.h"
#include "xio_mem.h"

extern struct xio_tcp_options tcp_options;

/*---------------------------------------------------------------------------*/
/* xio_free_rdma_read_mem						     */
/*---------------------------------------------------------------------------*/
void xio_free_rdma_read_mem(struct xio_tcp_transport *tcp_hndl,
			  struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);

	unsigned int		i;

	if (task->is_assigned) {
		if (task->unassign_data_in_buf)
			task->unassign_data_in_buf(&task->imsg,
						   task->unassign_user_context);
		task->is_assigned = 0;
		task->unassign_data_in_buf = NULL;
		task->unassign_user_context = NULL;
		clr_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF, &task->imsg.hints);
	} else {
		for (i = 0; i < tcp_task->read_num_reg_mem; i++) {
			xio_mempool_free(&tcp_task->read_reg_mem[i]);
			tcp_task->read_reg_mem[i].priv = NULL;
		}
		tcp_task->read_num_reg_mem = 0;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_free_tcp_task_mem						     */
/*---------------------------------------------------------------------------*/
int xio_free_tcp_task_mem(
		struct xio_transport_base *trans_hndl,
		struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	XIO_TO_TCP_HNDL(task, tcp_hndl);
	unsigned int	i;

	/* recycle TCP  buffers back to pool */

	/* put buffers back to pool */
	xio_free_rdma_read_mem(tcp_hndl, task);

	for (i = 0; i < tcp_task->write_num_reg_mem; i++) {
		if (tcp_task->write_reg_mem[i].priv) {
			xio_mempool_free(&tcp_task->write_reg_mem[i]);
			tcp_task->write_reg_mem[i].priv = NULL;
		}
	}
	tcp_task->write_num_reg_mem = 0;

	return 0;
}


/*---------------------------------------------------------------------------*/
/* xio_tcp_send_work                                                         */
/*---------------------------------------------------------------------------*/
static int xio_tcp_send_work(int fd, void **buf, uint32_t *len, int block)
{
	int retval;

	while (*len) {
		retval = send(fd, (const char *)*buf, *len, MSG_NOSIGNAL);
		switch (retval) {
		case -1:
			if (xio_get_last_socket_error() != XIO_EAGAIN) {
				xio_set_error(xio_get_last_socket_error());
				ERROR_LOG("send failed. (errno=%d)\n",
					  xio_get_last_socket_error());
				/* ORK todo how to recover on remote side?*/
				return -1;
			} else if (!block) {
				xio_set_error(xio_get_last_socket_error());
				ERROR_LOG("nonblocking send failed. (errno=%d)\n",
						xio_get_last_socket_error());
				/* ORK todo set epollout event
				 * to trigger send again */
				/* ORK todo polling on sendmsg few more times
				 * before returning*/
				return -1;
			}
			break;
		case 0: /* Peer closed the connection */
			ERROR_LOG("send failed. peer closed connection\n");
			return -1;
		default: /* successful send */
			*len -= retval;
			inc_ptr(*buf, retval);
		break;
		}
	}
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_sendmsg_work                                                      */
/*---------------------------------------------------------------------------*/
static int xio_tcp_sendmsg_work(int fd,
				struct xio_tcp_work_req *xio_send,
				int block)
{
	int			retval = 0, tmp_bytes, sent_bytes = 0;
	int			eagain_count = TX_EAGAIN_RETRY;
	unsigned int		i;

	while (xio_send->tot_iov_byte_len) {
		retval = sendmsg(fd, &xio_send->msg, MSG_NOSIGNAL);
		switch (retval) {
		case -1:
			if (xio_get_last_socket_error() != XIO_EAGAIN) {
				xio_set_error(xio_get_last_socket_error());
				DEBUG_LOG("sendmsg failed. (errno=%d)\n",
					  xio_get_last_socket_error());
				return -1;
			} else if (!block && (eagain_count-- == 0)) {
				xio_set_error(xio_get_last_socket_error());
				return -1;
			}
		break;
		case 0: /* Peer closed the connection */
			ERROR_LOG("send failed. peer closed connection\n");
			return -1;
		default: /* successful send */
			sent_bytes += retval;
			xio_send->tot_iov_byte_len -= retval;

			if (xio_send->tot_iov_byte_len == 0) {
				xio_send->msg.msg_iovlen = 0;
				break;
			}

			tmp_bytes = 0;
			for (i = 0; i < xio_send->msg.msg_iovlen; i++) {
				if (xio_send->msg.msg_iov[i].iov_len +
						tmp_bytes < (size_t)retval) {
					tmp_bytes +=
					xio_send->msg.msg_iov[i].iov_len;
				} else {
					xio_send->msg.msg_iov[i].iov_len -=
							(retval - tmp_bytes);
					inc_ptr(
					      xio_send->msg.msg_iov[i].iov_base,
					      retval - tmp_bytes);
					xio_send->msg.msg_iov =
						&xio_send->msg.msg_iov[i];
					xio_send->msg.msg_iovlen -= i;
					break;
				}
			}

			eagain_count = TX_EAGAIN_RETRY;
			break;
		}
	}
	return sent_bytes;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_write_setup_msg						     */
/*---------------------------------------------------------------------------*/
static void xio_tcp_write_setup_msg(struct xio_tcp_transport *tcp_hndl,
				    struct xio_task *task,
				    struct xio_tcp_setup_msg *msg)
{
	struct xio_tcp_setup_msg	*tmp_msg;

	/* set the mbuf after tlv header */
	xio_mbuf_set_val_start(&task->mbuf);

	/* jump after connection setup header */
	if (tcp_hndl->base.is_client)
		xio_mbuf_inc(&task->mbuf,
			     sizeof(struct xio_nexus_setup_req));
	else
		xio_mbuf_inc(&task->mbuf,
			     sizeof(struct xio_nexus_setup_rsp));

	tmp_msg = (struct xio_tcp_setup_msg *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	PACK_LLVAL(msg, tmp_msg, buffer_sz);
	PACK_LVAL(msg, tmp_msg, max_in_iovsz);
	PACK_LVAL(msg, tmp_msg, max_out_iovsz);
	PACK_LVAL(msg, tmp_msg, max_header_len);
	PACK_LLVAL(msg, tmp_msg, my_handle);
	PACK_LVAL(msg, tmp_msg, my_pid);
	PACK_LVAL(msg, tmp_msg, my_tid);

#ifdef EYAL_TODO
	print_hex_dump_bytes("post_send: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.tlv.head,
			     64);
#endif
	xio_mbuf_inc(&task->mbuf, sizeof(struct xio_tcp_setup_msg));
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_read_setup_msg						     */
/*---------------------------------------------------------------------------*/
static void xio_tcp_read_setup_msg(struct xio_tcp_transport *tcp_hndl,
				   struct xio_task *task,
				   struct xio_tcp_setup_msg *msg)
{
	struct xio_tcp_setup_msg	*tmp_msg;

	/* set the mbuf after tlv header */
	xio_mbuf_set_val_start(&task->mbuf);

	/* jump after connection setup header */
	if (tcp_hndl->base.is_client)
		xio_mbuf_inc(&task->mbuf,
			     sizeof(struct xio_nexus_setup_rsp));
	else
		xio_mbuf_inc(&task->mbuf,
			     sizeof(struct xio_nexus_setup_req));

	tmp_msg = (struct xio_tcp_setup_msg *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	UNPACK_LLVAL(tmp_msg, msg, buffer_sz);
	UNPACK_LVAL(tmp_msg, msg, max_in_iovsz);
	UNPACK_LVAL(tmp_msg, msg, max_out_iovsz);
	UNPACK_LVAL(tmp_msg, msg, max_header_len);
	UNPACK_LLVAL(tmp_msg, msg, my_handle);
	UNPACK_LVAL(tmp_msg, msg, my_pid);
	UNPACK_LVAL(tmp_msg, msg, my_tid);

#ifdef EYAL_TODO
	print_hex_dump_bytes("post_send: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.curr,
			     64);
#endif
	tcp_hndl->peer_tcp_hndl = ptr_from_int64(msg->my_handle);
	tcp_hndl->peer_pid = (pid_t)msg->my_pid;
	tcp_hndl->peer_tid = (pid_t)msg->my_tid;

	xio_mbuf_inc(&task->mbuf, sizeof(struct xio_tcp_setup_msg));
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_send_setup_req						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_send_setup_req(struct xio_tcp_transport *tcp_hndl,
				  struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	uint16_t payload;
	struct xio_tcp_setup_msg  req;

	DEBUG_LOG("%s: tcp_hndl:%p\n", __func__, tcp_hndl);

	req.buffer_sz		= xio_tcp_get_inline_buffer_size();
	req.max_in_iovsz	= tcp_options.max_in_iovsz;
	req.max_out_iovsz	= tcp_options.max_out_iovsz;
	req.max_header_len      = g_options.max_inline_xio_hdr;
	req.my_handle		= uint64_from_ptr(tcp_hndl);
	req.my_pid		= getpid();
	req.my_tid		= syscall(SYS_gettid);

	xio_tcp_write_setup_msg(tcp_hndl, task, &req);

	payload = xio_mbuf_tlv_payload_len(&task->mbuf);

	/* add tlv */
	if (xio_mbuf_write_tlv(&task->mbuf, task->tlv_type, payload) != 0)
		return  -1;

	/* set the length */
	tcp_task->txd.msg_iov[0].iov_len = xio_mbuf_data_length(&task->mbuf);
	tcp_task->txd.msg_len		 = 1;
	tcp_task->txd.tot_iov_byte_len	 = tcp_task->txd.msg_iov[0].iov_len;
	tcp_task->txd.msg.msg_iov	 = tcp_task->txd.msg_iov;
	tcp_task->txd.msg.msg_iovlen	 = tcp_task->txd.msg_len;

	tcp_task->out_tcp_op		 = XIO_TCP_SEND;

	xio_task_addref(task);

	xio_tcp_sendmsg_work(tcp_hndl->sock.cfd, &tcp_task->txd, 1);

	list_move_tail(&task->tasks_list_entry, &tcp_hndl->in_flight_list);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_send_setup_rsp						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_send_setup_rsp(struct xio_tcp_transport *tcp_hndl,
				  struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	uint16_t payload;
	struct xio_tcp_setup_msg *rsp = &tcp_hndl->setup_rsp;

	DEBUG_LOG("%s: tcp_hndl:%p\n", __func__, tcp_hndl);

	rsp->max_in_iovsz	= tcp_options.max_in_iovsz;
	rsp->max_out_iovsz	= tcp_options.max_out_iovsz;
	rsp->buffer_sz          = tcp_hndl->membuf_sz;
	rsp->max_header_len     = g_options.max_inline_xio_hdr;
	rsp->my_handle		= uint64_from_ptr(tcp_hndl);
	rsp->my_pid		= getpid();
	rsp->my_tid		= syscall(SYS_gettid);


	xio_tcp_write_setup_msg(tcp_hndl, task, rsp);

	payload = xio_mbuf_tlv_payload_len(&task->mbuf);

	/* add tlv */
	if (xio_mbuf_write_tlv(&task->mbuf, task->tlv_type, payload) != 0)
		return  -1;

	/* set the length */
	tcp_task->txd.msg_iov[0].iov_len = xio_mbuf_data_length(&task->mbuf);
	tcp_task->txd.msg_len		 = 1;
	tcp_task->txd.tot_iov_byte_len	 = tcp_task->txd.msg_iov[0].iov_len;
	tcp_task->txd.msg.msg_iov	 = tcp_task->txd.msg_iov;
	tcp_task->txd.msg.msg_iovlen	 = tcp_task->txd.msg_len;

	tcp_task->out_tcp_op		 = XIO_TCP_SEND;

	xio_tcp_sendmsg_work(tcp_hndl->sock.cfd, &tcp_task->txd, 1);

	list_move(&task->tasks_list_entry, &tcp_hndl->in_flight_list);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_rdma_on_setup_msg						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_on_setup_msg(struct xio_tcp_transport *tcp_hndl,
				struct xio_task *task)
{
	union xio_transport_event_data event_data;
	struct xio_tcp_setup_msg *rsp  = &tcp_hndl->setup_rsp;
	uint64_t local_buf_size;

	if (tcp_hndl->base.is_client) {
		struct xio_task *sender_task = NULL;

		if (!list_empty(&tcp_hndl->in_flight_list))
			sender_task = list_first_entry(
					&tcp_hndl->in_flight_list,
					struct xio_task,  tasks_list_entry);
		else if (!list_empty(&tcp_hndl->tx_comp_list))
			sender_task = list_first_entry(
					&tcp_hndl->tx_comp_list,
					struct xio_task,  tasks_list_entry);
		else
			ERROR_LOG("could not find sender task\n");

		task->sender_task = sender_task;
		xio_tcp_read_setup_msg(tcp_hndl, task, rsp);
	} else {
		struct xio_tcp_setup_msg req;

		xio_tcp_read_setup_msg(tcp_hndl, task, &req);

		/* current implementation is symmetric */
		local_buf_size		= xio_tcp_get_inline_buffer_size();
		rsp->buffer_sz		= min(req.buffer_sz, local_buf_size);
		rsp->max_in_iovsz	= req.max_in_iovsz;
		rsp->max_out_iovsz	= req.max_out_iovsz;
		rsp->max_header_len     = req.max_header_len;
	}

	tcp_hndl->max_inline_buf_sz	= (size_t)rsp->buffer_sz;
	tcp_hndl->membuf_sz		= (size_t)rsp->buffer_sz;
	tcp_hndl->peer_max_in_iovsz	= rsp->max_in_iovsz;
	tcp_hndl->peer_max_out_iovsz	= rsp->max_out_iovsz;
	tcp_hndl->peer_max_header      = rsp->max_header_len;

	tcp_hndl->sn = 0;

	tcp_hndl->state = XIO_TRANSPORT_STATE_CONNECTED;

	/* fill notification event */
	event_data.msg.op	= XIO_WC_OP_RECV;
	event_data.msg.task	= task;

	list_move_tail(&task->tasks_list_entry, &tcp_hndl->io_list);

	DEBUG_LOG("%s - tcp_hndl:%p, peer_tcp_hndl:%p, peer_pid:%d, peer_tid:%d\n",
		  __func__,
		  tcp_hndl, tcp_hndl->peer_tcp_hndl,
		  tcp_hndl->peer_pid, tcp_hndl->peer_tid);

	if (task->status)
		xio_free_tcp_task_mem(&tcp_hndl->base, task);

	xio_transport_notify_observer(&tcp_hndl->base,
				      XIO_TRANSPORT_EVENT_NEW_MESSAGE,
				      &event_data);
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_send_connect_msg		                                     */
/*---------------------------------------------------------------------------*/
int xio_tcp_send_connect_msg(int fd, struct xio_tcp_connect_msg *msg)
{
	int retval;
	struct xio_tcp_connect_msg smsg;
	uint32_t size = sizeof(struct xio_tcp_connect_msg);
	void *buf = &smsg;

	memset(&smsg, 0, sizeof(smsg));
	smsg.version = msg->version;
	smsg.sock_type = (enum xio_tcp_sock_type)
				htonl((uint32_t)msg->sock_type);
	PACK_SVAL(msg, &smsg, second_port);
	PACK_SVAL(msg, &smsg, port);
	PACK_LVAL(msg, &smsg, unique_id);

	DEBUG_LOG("%s fd:%d, version:%d, message_size:%d\n",
		  __func__, fd, smsg.version, size);
	retval = xio_tcp_send_work(fd, &buf, &size, 1);
	if (retval < 0) {
		if (xio_get_last_socket_error() == XIO_EAGAIN) {
			/* ORK todo set event */
		} else {
			ERROR_LOG("send return with %d. (errno=%d %m)\n",
				  retval, xio_get_last_socket_error());
			return retval;
		}
	}
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_write_req_header						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_write_req_header(struct xio_tcp_transport *tcp_hndl,
				    struct xio_task *task,
				    struct xio_tcp_req_hdr *req_hdr)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	struct xio_tcp_req_hdr		*tmp_req_hdr;
	struct xio_sge			*tmp_sge;
	struct xio_sge			sge;
	size_t				hdr_len;
	uint32_t			i;
	struct xio_sg_table_ops		*sgtbl_ops;
	void				*sgtbl;
	void				*sg;

	sgtbl		= xio_sg_table_get(&task->omsg->in);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->in.sgl_type);

	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_req_hdr = (struct xio_tcp_req_hdr *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	tmp_req_hdr->version  = req_hdr->version;
	tmp_req_hdr->flags    = req_hdr->flags;
	PACK_SVAL(req_hdr, tmp_req_hdr, req_hdr_len);
	PACK_LVAL(req_hdr, tmp_req_hdr, ltid);
	tmp_req_hdr->in_tcp_op	   = req_hdr->in_tcp_op;
	tmp_req_hdr->out_tcp_op	   = req_hdr->out_tcp_op;

	PACK_SVAL(req_hdr, tmp_req_hdr, in_num_sge);
	PACK_SVAL(req_hdr, tmp_req_hdr, out_num_sge);
	PACK_SVAL(req_hdr, tmp_req_hdr, ulp_hdr_len);
	PACK_SVAL(req_hdr, tmp_req_hdr, ulp_pad_len);
	/*remain_data_len is not used		*/
	PACK_LLVAL(req_hdr, tmp_req_hdr, ulp_imm_len);

	tmp_sge = (struct xio_sge *)((uint8_t *)tmp_req_hdr +
			   sizeof(struct xio_tcp_req_hdr));

	/* IN: requester expect small input written via send */
	sg = sge_first(sgtbl_ops, sgtbl);
	if (req_hdr->in_tcp_op == XIO_TCP_SEND) {
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
	if (req_hdr->in_tcp_op == XIO_TCP_WRITE) {
		for (i = 0;  i < req_hdr->in_num_sge; i++) {
			sge.addr = uint64_from_ptr(tcp_task->read_reg_mem[i].addr);
			sge.length = tcp_task->read_reg_mem[i].length;
			sge.stag = 0;
			PACK_LLVAL(&sge, tmp_sge, addr);
			PACK_LVAL(&sge, tmp_sge, length);
			PACK_LVAL(&sge, tmp_sge, stag);
			tmp_sge++;
		}
	}
	/* OUT: requester want to write data via rdma read */
	if (req_hdr->out_tcp_op == XIO_TCP_READ) {
		for (i = 0;  i < req_hdr->out_num_sge; i++) {
			sge.addr = uint64_from_ptr(tcp_task->write_reg_mem[i].addr);
			sge.length = tcp_task->write_reg_mem[i].length;
			sge.stag = 0;
			PACK_LLVAL(&sge, tmp_sge, addr);
			PACK_LVAL(&sge, tmp_sge, length);
			PACK_LVAL(&sge, tmp_sge, stag);
			tmp_sge++;
		}
	}
	if (req_hdr->out_tcp_op == XIO_TCP_SEND) {
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

	hdr_len	= sizeof(struct xio_tcp_req_hdr);
	hdr_len += sizeof(struct xio_sge) * (req_hdr->in_num_sge +
					     req_hdr->out_num_sge);
#ifdef EYAL_TODO
	print_hex_dump_bytes("post_send: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.curr,
			     hdr_len + 16);
#endif
	xio_mbuf_inc(&task->mbuf, hdr_len);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_prep_req_header						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_prep_req_header(struct xio_tcp_transport *tcp_hndl,
				   struct xio_task *task,
				   uint16_t ulp_hdr_len,
				   uint16_t ulp_pad_len,
				   uint64_t ulp_imm_len,
				   uint32_t status)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	struct xio_tcp_req_hdr	req_hdr;

	if (unlikely(!IS_REQUEST(task->tlv_type))) {
		ERROR_LOG("unknown message type\n");
		return -1;
	}

	/* write the headers */

	/* fill request header */
	req_hdr.version		= XIO_TCP_REQ_HEADER_VERSION;
	req_hdr.req_hdr_len	= sizeof(req_hdr);
	req_hdr.ltid		= task->ltid;
	req_hdr.in_tcp_op	= tcp_task->in_tcp_op;
	req_hdr.out_tcp_op	= tcp_task->out_tcp_op;
	req_hdr.flags		= task->omsg_flags;

	xio_clear_internal_flags(&req_hdr.flags);
	if (test_bits(XIO_MSG_FLAG_PEER_WRITE_RSP, &task->omsg_flags))
		set_bits(XIO_MSG_FLAG_PEER_WRITE_RSP, &req_hdr.flags);
	else if (test_bits(XIO_MSG_FLAG_LAST_IN_BATCH, &task->omsg_flags))
		set_bits(XIO_MSG_FLAG_LAST_IN_BATCH, &req_hdr.flags);

	req_hdr.ulp_hdr_len	= ulp_hdr_len;
	req_hdr.ulp_pad_len	= ulp_pad_len;
	req_hdr.ulp_imm_len	= ulp_imm_len;
	req_hdr.in_num_sge	= tcp_task->read_num_reg_mem;
	req_hdr.out_num_sge	= tcp_task->write_num_reg_mem;

	if (xio_tcp_write_req_header(tcp_hndl, task, &req_hdr) != 0)
		goto cleanup;

	tcp_task->txd.ctl_msg_len = xio_mbuf_tlv_len(&task->mbuf);

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
/* xio_tcp_write_send_data						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_write_send_data(
		struct xio_tcp_transport *tcp_hndl,
		struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	size_t			i;
	size_t			byte_len = 0;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;

	sgtbl		= xio_sg_table_get(&task->omsg->out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->out.sgl_type);

	/* user provided mr */
	sg = sge_first(sgtbl_ops, sgtbl);
	if (sge_mr(sgtbl_ops, sg) || !tcp_options.enable_mr_check) {
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			tcp_task->txd.msg_iov[i + 1].iov_base =
						sge_addr(sgtbl_ops, sg);
			tcp_task->txd.msg_iov[i + 1].iov_len =
						sge_length(sgtbl_ops, sg);

			byte_len += sge_length(sgtbl_ops, sg);
		}
		tcp_task->txd.msg_len =
				tbl_nents(sgtbl_ops, sgtbl) + 1;
		tcp_task->txd.tot_iov_byte_len = byte_len;
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
		tcp_task->txd.msg_len = 1;
		tcp_task->txd.tot_iov_byte_len = 0;
	}

	return 0;

cleanup:
	xio_set_error(XIO_E_MSG_SIZE);
	ERROR_LOG("xio_tcp_send_msg failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_prep_req_out_data						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_prep_req_out_data(
		struct xio_tcp_transport *tcp_hndl,
		struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	struct xio_vmsg		*vmsg = &task->omsg->out;
	uint64_t		xio_hdr_len;
	uint64_t		xio_max_hdr_len;
	uint64_t		ulp_hdr_len;
	uint64_t		ulp_pad_len = 0;
	uint64_t		ulp_imm_len;
	size_t			retval;
	unsigned int		i;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;
	int			tx_by_sr;
	int			nents;

	sgtbl		= xio_sg_table_get(&task->omsg->out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->out.sgl_type);
	nents		= tbl_nents(sgtbl_ops, sgtbl);

	/* calculate headers */
	ulp_hdr_len	= vmsg->header.iov_len;
	ulp_imm_len	= tbl_length(sgtbl_ops, sgtbl);

	xio_hdr_len = xio_mbuf_get_curr_offset(&task->mbuf);
	xio_hdr_len += sizeof(struct xio_tcp_req_hdr);
	xio_hdr_len += sizeof(struct xio_sge) * tcp_task->read_num_reg_mem;
	xio_max_hdr_len = xio_hdr_len + sizeof(struct xio_sge) * nents;

	if (g_options.inline_xio_data_align && ulp_imm_len) {
		uint16_t hdr_len = xio_hdr_len + ulp_hdr_len;

		ulp_pad_len = ALIGN(hdr_len, g_options.inline_xio_data_align) -
			      hdr_len;
	}

	/*
	if (tcp_hndl->max_inline_buf_sz	 < (xio_hdr_len + ulp_hdr_len)) {
		ERROR_LOG("header size %lu exceeds max header %lu\n",
			  ulp_hdr_len, tcp_hndl->max_inline_buf_sz -
			  xio_hdr_len);
		xio_set_error(XIO_E_MSG_SIZE);
		return -1;
	}
	*/
	/* test for using send/receive or rdma_read */
	if (test_bits(XIO_MSG_FLAG_PEER_READ_REQ, &task->omsg_flags) && nents)
		tx_by_sr = 0;
	else
		tx_by_sr = (((ulp_hdr_len + ulp_pad_len +
			      ulp_imm_len + xio_max_hdr_len) <=
			     tcp_hndl->max_inline_buf_sz) &&
			     (((int)(ulp_imm_len) <=
			       g_options.max_inline_xio_data) ||
			      ulp_imm_len == 0));

	/* the data is outgoing via SEND */
	if (tx_by_sr) {
		tcp_task->out_tcp_op = XIO_TCP_SEND;
		/* user has small request - no rdma operation expected */
		tcp_task->write_num_reg_mem = 0;

		/* write xio header to the buffer */
		retval = xio_tcp_prep_req_header(
				tcp_hndl, task,
				(uint16_t)ulp_hdr_len,
				(uint16_t)ulp_pad_len, ulp_imm_len,
				XIO_E_SUCCESS);
		if (retval)
			return -1;

		/* if there is data, set it to buffer or directly to the sge */
		if (ulp_imm_len) {
			retval = xio_tcp_write_send_data(tcp_hndl, task);
			if (retval)
				return -1;
		} else {
			tcp_task->txd.tot_iov_byte_len = 0;
			tcp_task->txd.msg_len = 1;
		}
	} else {
		tcp_task->out_tcp_op = XIO_TCP_READ;
		sg = sge_first(sgtbl_ops, sgtbl);
		if (sge_mr(sgtbl_ops, sg) || !tcp_options.enable_mr_check) {
			for_each_sge(sgtbl, sgtbl_ops, sg, i) {
				tcp_task->write_reg_mem[i].addr =
					sge_addr(sgtbl_ops, sg);
				tcp_task->write_reg_mem[i].priv = NULL;
				tcp_task->write_reg_mem[i].ctx =
							tcp_hndl->base.ctx;
				tcp_task->write_reg_mem[i].mr =
					(struct xio_mr *)sge_mr(sgtbl_ops, sg);
				tcp_task->write_reg_mem[i].length =
					sge_length(sgtbl_ops, sg);
			}
		} else {
			if (!tcp_hndl->tcp_mempool) {
				xio_set_error(XIO_E_NO_BUFS);
				ERROR_LOG("message /read/write failed - " \
					  "library's memory pool disabled\n");
				goto cleanup;
			}

			/* user did not provide mr - take buffers from pool
			 * and do copy */
			for_each_sge(sgtbl, sgtbl_ops, sg, i) {
				retval = xio_mempool_alloc(
						tcp_hndl->tcp_mempool,
						sge_length(sgtbl_ops, sg),
						&tcp_task->write_reg_mem[i]);
				if (unlikely(retval)) {
					tcp_task->write_num_reg_mem = i;
					xio_set_error(ENOMEM);
					ERROR_LOG("mempool is empty " \
						  "for %zd bytes\n",
						  sge_length(sgtbl_ops, sg));
					goto cleanup;
				}

				tcp_task->write_reg_mem[i].length =
					sge_length(sgtbl_ops, sg);
				tcp_task->write_reg_mem[i].ctx =
							tcp_hndl->base.ctx;
				/* copy the data to the buffer */
				memcpy(tcp_task->write_reg_mem[i].addr,
				       sge_addr(sgtbl_ops, sg),
				       sge_length(sgtbl_ops, sg));
			}
		}
		tcp_task->write_num_reg_mem = tbl_nents(sgtbl_ops, sgtbl);

		if (ulp_imm_len) {
			tcp_task->txd.tot_iov_byte_len = 0;
			for (i = 0; i < tcp_task->write_num_reg_mem; i++)  {
				tcp_task->txd.msg_iov[i + 1].iov_base =
						tcp_task->write_reg_mem[i].addr;
				tcp_task->txd.msg_iov[i + 1].iov_len =
						tcp_task->write_reg_mem[i].length;
				tcp_task->txd.tot_iov_byte_len +=
						tcp_task->write_reg_mem[i].length;
			}
			tcp_task->txd.msg_len = tcp_task->write_num_reg_mem + 1;
		} else {
			tcp_task->txd.tot_iov_byte_len = 0;
			tcp_task->txd.msg_len = 1;
		}

		/* write xio header to the buffer */
		retval = xio_tcp_prep_req_header(
				tcp_hndl, task,
				(uint16_t)ulp_hdr_len, 0, 0, XIO_E_SUCCESS);

		if (retval) {
			ERROR_LOG("Failed to write header\n");
			goto cleanup;
		}
	}

	return 0;

cleanup:
	for (i = 0; i < tcp_task->write_num_reg_mem; i++)
		xio_mempool_free(&tcp_task->write_reg_mem[i]);

	tcp_task->write_num_reg_mem = 0;

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_on_rsp_send_comp						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_on_rsp_send_comp(struct xio_tcp_transport *tcp_hndl,
				    struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	union xio_transport_event_data event_data;

	if (task->on_hold) {
		/* dynamically initialize header */
		tcp_task->txd.msg_iov[0].iov_base = xio_mbuf_buf_head(&task->sender_task->mbuf);
		tcp_task->txd.ctl_msg = xio_mbuf_buf_head(&task->sender_task->mbuf);
	}

	if (IS_KEEPALIVE(task->tlv_type)) {
		if (task->ka_probes)
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, tcp_hndl:%p\n",
					__func__, task->tlv_type, task->session, task->connection,
					tcp_hndl);
	} else {
		if (!IS_NOP(task->tlv_type) && !IS_APPLICATION_MSG(task->tlv_type))
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, tcp_hndl:%p\n",
					__func__, task->tlv_type, task->session, task->connection,
					tcp_hndl);
	}
	event_data.msg.op	= XIO_WC_OP_SEND;
	event_data.msg.task	= task;

	xio_transport_notify_observer(&tcp_hndl->base,
				      XIO_TRANSPORT_EVENT_SEND_COMPLETION,
				      &event_data);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_on_req_send_comp						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_on_req_send_comp(struct xio_tcp_transport *tcp_hndl,
				    struct xio_task *task)
{
	union xio_transport_event_data event_data;

	if (IS_KEEPALIVE(task->tlv_type)) {
		if (task->ka_probes)
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, tcp_hndl:%p\n",
					__func__, task->tlv_type, task->session, task->connection,
					tcp_hndl);
	} else {
		if (!IS_NOP(task->tlv_type) && !IS_APPLICATION_MSG(task->tlv_type))
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, tcp_hndl:%p\n",
					__func__, task->tlv_type, task->session, task->connection,
					tcp_hndl);
	}

	event_data.msg.op	= XIO_WC_OP_SEND;
	event_data.msg.task	= task;

	xio_transport_notify_observer(&tcp_hndl->base,
				      XIO_TRANSPORT_EVENT_SEND_COMPLETION,
				      &event_data);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_tx_comp_handler						     */
/*---------------------------------------------------------------------------*/
static void xio_tcp_tx_completion_handler(int actual_timeout_ms, void *xio_task)
{
	struct xio_task		*ptask, *next_ptask;
	int			found = 0;
	int			removed = 0;
	struct xio_task		*task = (struct xio_task *)xio_task;

	XIO_TO_TCP_HNDL(task, tcp_hndl);

	if (unlikely(!tcp_hndl))
		return;

	list_for_each_entry_safe(ptask, next_ptask, &tcp_hndl->in_flight_list,
				 tasks_list_entry) {
		XIO_TO_TCP_TASK(ptask, tcp_task);

		list_move_tail(&ptask->tasks_list_entry,
			       &tcp_hndl->tx_comp_list);
		removed++;

		xio_ctx_del_work(tcp_hndl->base.ctx, &tcp_task->comp_work);
		if (IS_REQUEST(ptask->tlv_type)) {
			xio_tcp_on_req_send_comp(tcp_hndl, ptask);
			xio_tasks_pool_put(ptask);
		} else if (IS_RESPONSE(ptask->tlv_type)) {
			xio_tcp_on_rsp_send_comp(tcp_hndl, ptask);
		} else {
			ERROR_LOG("unexpected task %p id:%d magic:0x%lx\n",
				  ptask,
				  ptask->ltid, ptask->magic);
			continue;
		}
		if (ptask == task) {
			found  = 1;
			break;
		}
	}

	if (!found && removed)
		ERROR_LOG("not found but removed %d type:0x%x\n",
			  removed, task->tlv_type);

	tcp_hndl->tx_comp_cnt = 0;

	/* after work completion - report disconnect */
	if (tcp_hndl->state == XIO_TRANSPORT_STATE_DISCONNECTED) {
		xio_context_add_event(tcp_hndl->base.ctx,
				      &tcp_hndl->disconnect_event);
	} else {
		xio_context_disable_event(&tcp_hndl->disconnect_event);
		if (tcp_hndl->tx_ready_tasks_num)
			xio_tcp_xmit(tcp_hndl);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_disconnect_helper						     */
/*---------------------------------------------------------------------------*/
void xio_tcp_disconnect_helper(void *xio_tcp_hndl)
{
	struct xio_tcp_transport *tcp_hndl = (struct xio_tcp_transport *)
		xio_tcp_hndl;

	if (tcp_hndl->state >= XIO_TRANSPORT_STATE_DISCONNECTED)
		return;

	tcp_hndl->state = XIO_TRANSPORT_STATE_DISCONNECTED;

	/* flush all tasks in completion */
	if (!list_empty(&tcp_hndl->in_flight_list)) {
		struct xio_task *task = NULL;

		xio_context_disable_event(&tcp_hndl->disconnect_event);
		task = list_last_entry(&tcp_hndl->in_flight_list,
				struct xio_task,
				tasks_list_entry);
		if (task) {
			XIO_TO_TCP_TASK(task, tcp_task);

			xio_ctx_add_work(tcp_hndl->base.ctx, task,
					xio_tcp_tx_completion_handler,
					&tcp_task->comp_work);
		}
	} else {
		/* call disconnect if no message to flush otherwise defer */
		xio_context_add_event(tcp_hndl->base.ctx,
				&tcp_hndl->disconnect_event);
	}
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_write_sn							     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_write_sn(struct xio_task *task, uint16_t sn)
{
	uint16_t *psn;

	/* save the current place */
	xio_mbuf_push(&task->mbuf);
	/* goto the first transport header*/
	xio_mbuf_set_trans_hdr(&task->mbuf);

	/* jump over the first uint32_t */
	xio_mbuf_inc(&task->mbuf, sizeof(uint32_t));

	/* and set serial number */
	psn = (uint16_t *)xio_mbuf_get_curr_ptr(&task->mbuf);
	*psn = htons(sn);

	/* pop to the original place */
	xio_mbuf_pop(&task->mbuf);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_xmit								     */
/*---------------------------------------------------------------------------*/
int xio_tcp_xmit(struct xio_tcp_transport *tcp_hndl)
{
	struct xio_task		*task = NULL, *task_success = NULL,
				*next_task = NULL;
	struct xio_tcp_task	*tcp_task = NULL, *next_tcp_task = NULL;
	int			retval = 0, retval2 = 0;
	int			imm_comp = 0;
	int			batch_nr = TX_BATCH, batch_count = 0, tmp_count;
	unsigned int		i;
	unsigned int		iov_len;
	uint64_t		bytes_sent;

	if (tcp_hndl->tx_ready_tasks_num == 0 ||
	    tcp_hndl->tx_comp_cnt > COMPLETION_BATCH_MAX ||
	    tcp_hndl->state != XIO_TRANSPORT_STATE_CONNECTED) {
		xio_set_error(XIO_EAGAIN);
		return -1;
	}

	task = list_first_entry(&tcp_hndl->tx_ready_list, struct xio_task,
				tasks_list_entry);

	/* if "ready to send queue" is not empty */
	while (likely(tcp_hndl->tx_ready_tasks_num &&
		      (tcp_hndl->tx_comp_cnt < COMPLETION_BATCH_MAX))) {
		next_task = list_first_entry_or_null(&task->tasks_list_entry,
						     struct xio_task,
						     tasks_list_entry);
		next_tcp_task = next_task ?
			(struct xio_tcp_task *)next_task->dd_data : NULL;

		tcp_task = (struct xio_tcp_task *)task->dd_data;

		switch (tcp_task->txd.stage) {
		case XIO_TCP_TX_BEFORE:
			xio_tcp_write_sn(task, tcp_hndl->sn);
			tcp_task->sn = tcp_hndl->sn;
			tcp_hndl->sn++;
			tcp_task->txd.stage = XIO_TCP_TX_IN_SEND_CTL;
			/*fallthrough*/
		case XIO_TCP_TX_IN_SEND_CTL:
			/* for single socket, ctl_msg_len is zero */
			if (tcp_task->txd.ctl_msg_len == 0) {
				tcp_task->txd.stage = XIO_TCP_TX_IN_SEND_DATA;
				break;
			}

			tcp_hndl->tmp_work.msg_iov[batch_count].iov_base =
					tcp_task->txd.ctl_msg;
			tcp_hndl->tmp_work.msg_iov[batch_count].iov_len =
					tcp_task->txd.ctl_msg_len;
			++tcp_hndl->tmp_work.msg_len;
			tcp_hndl->tmp_work.tot_iov_byte_len +=
					tcp_task->txd.ctl_msg_len;

			++batch_count;
			if (batch_count != batch_nr &&
			    batch_count != tcp_hndl->tx_ready_tasks_num &&
			    next_task &&
			    next_tcp_task->txd.stage
			    <= XIO_TCP_TX_IN_SEND_CTL) {
				task = next_task;
				break;
			}

			tcp_hndl->tmp_work.msg.msg_iov =
					tcp_hndl->tmp_work.msg_iov;
			tcp_hndl->tmp_work.msg.msg_iovlen =
					tcp_hndl->tmp_work.msg_len;

			if (IS_KEEPALIVE(task->tlv_type)) {
				if (task->ka_probes)
					DEBUG_LOG("%s - ka on wire: tlv_type:0x%x, session:%p, connection:%p, tcp_hndl:%p\n",
							__func__, task->tlv_type, task->session, task->connection, tcp_hndl);
			} else {
				if (!IS_NOP(task->tlv_type) && !IS_APPLICATION_MSG(task->tlv_type))
					DEBUG_LOG("%s - control on wire: tlv_type:0x%x, session:%p, connection:%p, tcp_hndl:%p\n",
							__func__, task->tlv_type, task->session, task->connection, tcp_hndl);
			}

			retval = xio_tcp_sendmsg_work(tcp_hndl->sock.cfd,
						      &tcp_hndl->tmp_work, 0);

			task = list_first_entry(&tcp_hndl->tx_ready_list,
						struct xio_task,
						tasks_list_entry);
			iov_len = tcp_hndl->tmp_work.msg_len -
					tcp_hndl->tmp_work.msg.msg_iovlen;
			for (i = 0; i < iov_len; i++) {
				tcp_task = (struct xio_tcp_task *)task->dd_data;
				tcp_task->txd.stage = XIO_TCP_TX_IN_SEND_DATA;
				tcp_task->txd.ctl_msg_len = 0;
				task = list_first_entry_or_null(
						&task->tasks_list_entry,
						struct xio_task,
						tasks_list_entry);
			}
			if (tcp_hndl->tmp_work.msg.msg_iovlen) {
				tcp_task = (struct xio_tcp_task *)task->dd_data;
				tcp_task->txd.ctl_msg =
				tcp_hndl->tmp_work.msg.msg_iov[0].iov_base;
				tcp_task->txd.ctl_msg_len =
				tcp_hndl->tmp_work.msg.msg_iov[0].iov_len;
			}
			tcp_hndl->tmp_work.msg_len = 0;
			tcp_hndl->tmp_work.tot_iov_byte_len = 0;
			batch_count = 0;

			if (retval < 0) {
				if (xio_get_last_socket_error() ==
							XIO_ECONNRESET ||
					xio_get_last_socket_error() ==
							XIO_ECONNABORTED ||
					/*EPIPE is not rellevant for Windows*/
					xio_get_last_socket_error() == EPIPE) {
					DEBUG_LOG("tcp trans got reset ");
					DEBUG_LOG("tcp_hndl=%p\n", tcp_hndl);
					xio_tcp_disconnect_helper(tcp_hndl);
					return 0;
				}

				if (xio_get_last_socket_error() != XIO_EAGAIN)
					return -1;

				/* for eagain, add event for ready for write*/
				retval = xio_context_modify_ev_handler(
						tcp_hndl->base.ctx,
						tcp_hndl->sock.cfd,
						XIO_POLLIN | XIO_POLLRDHUP |
						XIO_POLLOUT);
				if (retval != 0)
					ERROR_LOG("modify events failed.\n");

				retval = -1;
				goto handle_completions;
			}

			task = list_first_entry(
				&tcp_hndl->tx_ready_list,
				struct xio_task,  tasks_list_entry);

			break;
		case XIO_TCP_TX_IN_SEND_DATA:

			for (i = 0; i < tcp_task->txd.msg.msg_iovlen; i++) {
				tcp_hndl->tmp_work.msg_iov
				[tcp_hndl->tmp_work.msg_len].iov_base =
					tcp_task->txd.msg.msg_iov[i].iov_base;
				tcp_hndl->tmp_work.msg_iov
				[tcp_hndl->tmp_work.msg_len].iov_len =
					tcp_task->txd.msg.msg_iov[i].iov_len;
				++tcp_hndl->tmp_work.msg_len;
			}
			tcp_hndl->tmp_work.tot_iov_byte_len +=
					tcp_task->txd.tot_iov_byte_len;

			++batch_count;
			if (batch_count != batch_nr &&
			    batch_count != tcp_hndl->tx_ready_tasks_num &&
			    next_task &&
			    (next_tcp_task->txd.stage ==
			    XIO_TCP_TX_IN_SEND_DATA) &&
			    (next_tcp_task->txd.msg.msg_iovlen +
			    tcp_hndl->tmp_work.msg_len) < IOV_MAX) {
				task = next_task;
				break;
			}

			tcp_hndl->tmp_work.msg.msg_iov =
					tcp_hndl->tmp_work.msg_iov;
			tcp_hndl->tmp_work.msg.msg_iovlen =
					tcp_hndl->tmp_work.msg_len;

			bytes_sent = tcp_hndl->tmp_work.tot_iov_byte_len;
			retval = xio_tcp_sendmsg_work(tcp_hndl->sock.dfd,
						      &tcp_hndl->tmp_work, 0);
			bytes_sent -= tcp_hndl->tmp_work.tot_iov_byte_len;

			task = list_first_entry(&tcp_hndl->tx_ready_list,
						struct xio_task,
						tasks_list_entry);
			iov_len = tcp_hndl->tmp_work.msg_len -
					tcp_hndl->tmp_work.msg.msg_iovlen;
			tmp_count = batch_count;
			while (tmp_count) {
				tcp_task = (struct xio_tcp_task *)task->dd_data;

				if (tcp_task->txd.msg.msg_iovlen > iov_len)
					break;

				iov_len -= tcp_task->txd.msg.msg_iovlen;
				bytes_sent -= tcp_task->txd.tot_iov_byte_len;

				tcp_hndl->tx_ready_tasks_num--;

				list_move_tail(&task->tasks_list_entry,
					       &tcp_hndl->in_flight_list);

				task_success = task;

				++tcp_hndl->tx_comp_cnt;

				imm_comp = imm_comp || task->is_control ||
					   (task->omsg &&
					    (task->omsg->flags &
						XIO_MSG_FLAG_IMM_SEND_COMP));

				--tmp_count;

				task = list_first_entry(
					&tcp_hndl->tx_ready_list,
					struct xio_task,  tasks_list_entry);
			}
			if (tcp_hndl->tmp_work.msg.msg_iovlen) {
				tcp_task = (struct xio_tcp_task *)task->dd_data;
				tcp_task->txd.msg.msg_iov =
				&tcp_task->txd.msg.msg_iov[iov_len];
				tcp_task->txd.msg.msg_iov[0].iov_base =
				tcp_hndl->tmp_work.msg.msg_iov[0].iov_base;
				tcp_task->txd.msg.msg_iov[0].iov_len =
				tcp_hndl->tmp_work.msg.msg_iov[0].iov_len;
				tcp_task->txd.msg.msg_iovlen -= iov_len;
				tcp_task->txd.tot_iov_byte_len -= bytes_sent;
			}

			tcp_hndl->tmp_work.msg_len = 0;
			tcp_hndl->tmp_work.tot_iov_byte_len = 0;
			batch_count = 0;

			if (retval < 0) {
				if (xio_get_last_socket_error() ==
							XIO_ECONNRESET ||
					xio_get_last_socket_error() ==
							XIO_ECONNABORTED ||
					/*EPIPE is not relevant for Windows*/
					xio_get_last_socket_error() == EPIPE) {
					DEBUG_LOG("tcp trans got reset ");
					DEBUG_LOG("tcp_hndl=%p\n", tcp_hndl);
					xio_tcp_disconnect_helper(tcp_hndl);
					return 0;
				}

				if (xio_get_last_socket_error() != XIO_EAGAIN)
					return -1;

				/* for eagain, add event for ready for write*/
				retval = xio_context_modify_ev_handler(
						tcp_hndl->base.ctx,
						tcp_hndl->sock.dfd,
						XIO_POLLIN | XIO_POLLRDHUP |
						XIO_POLLOUT);
				if (retval != 0)
					ERROR_LOG("modify events failed.\n");

				retval = -1;
				goto handle_completions;
			}

			task = list_first_entry(&tcp_hndl->tx_ready_list,
						struct xio_task,
						tasks_list_entry);

			break;
		default:
			ERROR_LOG("unknown TX stage %d. disconnecting tcp_hndl:%p\n", tcp_task->txd.stage, tcp_hndl);
			xio_tcp_disconnect_helper(tcp_hndl);
			return -1;
			break;
		}
	}

handle_completions:

	if (task_success &&
	    (tcp_hndl->tx_comp_cnt >= COMPLETION_BATCH_MAX ||
	     imm_comp)) {
		tcp_task = (struct xio_tcp_task *)task_success->dd_data;
		retval2 = xio_ctx_add_work(tcp_hndl->base.ctx,
					   task_success,
					   xio_tcp_tx_completion_handler,
					   &tcp_task->comp_work);
		if (retval2 != 0) {
			ERROR_LOG("xio_ctx_add_work failed.\n");
			return retval2;
		}
	}
	xio_context_disable_event(&tcp_hndl->flush_tx_event);

	return retval < 0 ? retval : 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_prep_req_in_data						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_prep_req_in_data(struct xio_tcp_transport *tcp_hndl,
				    struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	size_t				hdr_len;
	size_t				pad_len = 0;
	size_t				data_len;
	size_t				xio_hdr_len;
	struct xio_vmsg			*vmsg = &task->omsg->in;
	unsigned int			i;
	int				retval;
	struct xio_sg_table_ops		*sgtbl_ops;
	void				*sgtbl;
	void				*sg;
	int				nents;

	sgtbl		= xio_sg_table_get(&task->omsg->in);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->in.sgl_type);
	nents		= tbl_nents(sgtbl_ops, sgtbl);

	if (nents == 0) {
		tcp_task->in_tcp_op = XIO_TCP_SEND;
		tcp_task->read_num_reg_mem = 0;
		return 0;
	}

	data_len = tbl_length(sgtbl_ops, sgtbl);
	hdr_len	 = vmsg->header.iov_len;
	if (hdr_len > tcp_hndl->peer_max_header) {
		ERROR_LOG("hdr_len=%d is bigger than peer_max_header=%d\n",
				hdr_len, tcp_hndl->peer_max_header);
		return -1;
	} else if (!hdr_len) {
		hdr_len = tcp_hndl->peer_max_header;
	}

	/* before working on the out - current place after the session header */
	xio_hdr_len = xio_mbuf_get_curr_offset(&task->mbuf);
	xio_hdr_len += sizeof(struct xio_tcp_rsp_hdr);
	xio_hdr_len += sizeof(struct xio_sge) * nents;

	if (g_options.inline_xio_data_align && data_len) {
		uint16_t hdr_len_ex = xio_hdr_len + hdr_len;

		pad_len = ALIGN(hdr_len_ex, g_options.inline_xio_data_align) -
				hdr_len_ex;
	}

	/* requester may insist on RDMA for small buffers to eliminate copy
	 * from receive buffers to user buffers
	 */
	if (!(task->omsg_flags & XIO_MSG_FLAG_PEER_WRITE_RSP) &&
	    data_len + pad_len + hdr_len + xio_hdr_len <= tcp_hndl->max_inline_buf_sz) {
		/* user has small response - no rdma operation expected */
		tcp_task->in_tcp_op = XIO_TCP_SEND;
		tcp_task->read_num_reg_mem = (data_len) ? tbl_nents(sgtbl_ops, sgtbl) : 0;
	} else  {
		/* user provided buffers with length for RDMA WRITE */
		/* user provided mr */
		tcp_task->in_tcp_op = XIO_TCP_WRITE;
		sg = sge_first(sgtbl_ops, sgtbl);
		if (sge_addr(sgtbl_ops, sg) &&
		    (sge_mr(sgtbl_ops, sg) || !tcp_options.enable_mr_check)) {
			for_each_sge(sgtbl, sgtbl_ops, sg, i) {
				tcp_task->read_reg_mem[i].addr =
					sge_addr(sgtbl_ops, sg);
				tcp_task->read_reg_mem[i].priv = NULL;
				tcp_task->read_reg_mem[i].ctx = tcp_hndl->base.ctx;
				tcp_task->read_reg_mem[i].mr =
					(struct xio_mr *)sge_mr(sgtbl_ops, sg);
				tcp_task->read_reg_mem[i].length =
					sge_length(sgtbl_ops, sg);
			}
		} else {
			if (!tcp_hndl->tcp_mempool) {
				xio_set_error(XIO_E_NO_BUFS);
				ERROR_LOG("message /read/write failed - " \
					  "library's memory pool disabled\n");
				goto cleanup;
			}

			/* user did not provide mr */
			for_each_sge(sgtbl, sgtbl_ops, sg, i) {
				retval = xio_mempool_alloc(
						tcp_hndl->tcp_mempool,
						sge_length(sgtbl_ops, sg),
						&tcp_task->read_reg_mem[i]);

				if (unlikely(retval)) {
					tcp_task->read_num_reg_mem = i;
					xio_set_error(ENOMEM);
					ERROR_LOG(
					"mempool is empty for %zd bytes\n",
					sge_length(sgtbl_ops, sg));
					goto cleanup;
				}
				tcp_task->read_reg_mem[i].length =
					sge_length(sgtbl_ops, sg);
			}
		}
		tcp_task->read_num_reg_mem = nents;
	}
	if (tcp_task->read_num_reg_mem > tcp_hndl->peer_max_out_iovsz) {
		ERROR_LOG("request in iovlen %d is bigger " \
			  "than peer max out iovlen %d\n",
			  tcp_task->read_num_reg_mem,
			  tcp_hndl->peer_max_out_iovsz);
		goto cleanup;
	}

	return 0;

cleanup:
	xio_free_rdma_read_mem(tcp_hndl, task);
	xio_set_error(EMSGSIZE);

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_single_sock_set_txd						     */
/*---------------------------------------------------------------------------*/
size_t xio_tcp_single_sock_set_txd(struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	size_t			iov_len;
	size_t			tlv_len;

	tcp_task->txd.ctl_msg_len = 0;

	iov_len = xio_mbuf_get_curr_offset(&task->mbuf);
	tcp_task->txd.msg_iov[0].iov_len = iov_len;

	tlv_len = iov_len - XIO_TLV_LEN;
	if (tcp_task->out_tcp_op == XIO_TCP_SEND)
		tlv_len += (size_t)tcp_task->txd.tot_iov_byte_len;

	tcp_task->txd.tot_iov_byte_len += iov_len;

	tcp_task->txd.msg.msg_iov = tcp_task->txd.msg_iov;
	tcp_task->txd.msg.msg_iovlen = tcp_task->txd.msg_len;

	return tlv_len;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_dual_sock_set_txd						     */
/*---------------------------------------------------------------------------*/
size_t xio_tcp_dual_sock_set_txd(struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	size_t			iov_len;
	/* is this is an application message than send the user header
	 * with the XIO message to be used with assign_data_in_buf
	 */
	if (IS_APPLICATION_MSG(task->tlv_type)) {
		iov_len = xio_mbuf_get_curr_offset(&task->mbuf);
		tcp_task->txd.ctl_msg_len = iov_len;
		tcp_task->txd.msg_iov[0].iov_len = iov_len;
		/* header is sent with XIO management data*/
		--tcp_task->txd.msg_len;
		/* set the send location considering the user header */
		if (iov_len == 0)
			tcp_task->txd.msg.msg_iov = tcp_task->txd.msg_iov;
		else
			tcp_task->txd.msg.msg_iov = &tcp_task->txd.msg_iov[1];
	} else {
		iov_len = xio_mbuf_get_curr_offset(&task->mbuf)
					- tcp_task->txd.ctl_msg_len;
		tcp_task->txd.msg_iov[0].iov_len = iov_len;
		inc_ptr(tcp_task->txd.msg_iov[0].iov_base,
				tcp_task->txd.ctl_msg_len);

		tcp_task->txd.tot_iov_byte_len += iov_len;

		if (tcp_task->txd.msg_iov[0].iov_len == 0) {
			tcp_task->txd.msg.msg_iov = &tcp_task->txd.msg_iov[1];
			--tcp_task->txd.msg_len;
		} else {
			tcp_task->txd.msg.msg_iov = tcp_task->txd.msg_iov;
		}
	}
	tcp_task->txd.msg.msg_iovlen = tcp_task->txd.msg_len;
	return tcp_task->txd.ctl_msg_len - XIO_TLV_LEN;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_send_req							     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_send_req(struct xio_tcp_transport *tcp_hndl,
			    struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	size_t			retval;
	size_t			tlv_len;

	/* prepare buffer for response  */
	retval = xio_tcp_prep_req_in_data(tcp_hndl, task);
	if (unlikely(retval != 0)) {
		ERROR_LOG("tcp_prep_req_in_data failed\n");
		return -1;
	}

	/* prepare the out message  */
	retval = xio_tcp_prep_req_out_data(tcp_hndl, task);
	if (unlikely(retval != 0)) {
		ERROR_LOG("tcp_prep_req_out_data failed\n");
		return -1;
	}

	/* set the length */
	tlv_len = tcp_hndl->sock.ops->set_txd(task);

	/* add tlv */
	if (xio_mbuf_write_tlv(&task->mbuf, task->tlv_type, tlv_len) != 0) {
		ERROR_LOG("write tlv failed\n");
		xio_set_error(EOVERFLOW);
		return -1;
	}

	xio_task_addref(task);

	tcp_task->out_tcp_op = XIO_TCP_SEND;

	if (IS_KEEPALIVE(task->tlv_type))
		list_move(&task->tasks_list_entry, &tcp_hndl->tx_ready_list);
	else
		list_move_tail(&task->tasks_list_entry, &tcp_hndl->tx_ready_list);

	tcp_hndl->tx_ready_tasks_num++;

	retval = xio_tcp_xmit(tcp_hndl);
	if (retval) {
		if (xio_errno() != XIO_EAGAIN) {
			DEBUG_LOG("xio_tcp_xmit failed\n");
			return -1;
		}
		xio_context_add_event(tcp_hndl->base.ctx,
				      &tcp_hndl->flush_tx_event);
		retval = 0;
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_write_rsp_header						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_write_rsp_header(struct xio_tcp_transport *tcp_hndl,
				    struct xio_task *task,
				    struct xio_tcp_rsp_hdr *rsp_hdr)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	struct xio_tcp_rsp_hdr		*tmp_rsp_hdr;
	uint32_t			*wr_len;
	int				i;
	size_t				hdr_len;

	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_rsp_hdr = (struct xio_tcp_rsp_hdr *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	/* pack relevant values */
	tmp_rsp_hdr->version  = rsp_hdr->version;
	tmp_rsp_hdr->flags    = rsp_hdr->flags;
	PACK_SVAL(rsp_hdr, tmp_rsp_hdr, rsp_hdr_len);
	PACK_LVAL(rsp_hdr, tmp_rsp_hdr, ltid);
	PACK_LVAL(rsp_hdr, tmp_rsp_hdr, rtid);
	tmp_rsp_hdr->out_tcp_op = rsp_hdr->out_tcp_op;
	PACK_LVAL(rsp_hdr, tmp_rsp_hdr, status);
	PACK_SVAL(rsp_hdr, tmp_rsp_hdr, out_num_sge);
	PACK_SVAL(rsp_hdr, tmp_rsp_hdr, ulp_hdr_len);
	PACK_SVAL(rsp_hdr, tmp_rsp_hdr, ulp_pad_len);
	/* remain_data_len not in use */
	PACK_LLVAL(rsp_hdr, tmp_rsp_hdr, ulp_imm_len);

	if (rsp_hdr->out_num_sge) {
		wr_len = (uint32_t *)((uint8_t *)tmp_rsp_hdr +
				sizeof(struct xio_tcp_rsp_hdr));

		/* params for RDMA WRITE equivalent*/
		for (i = 0;  i < rsp_hdr->out_num_sge; i++) {
			*wr_len = htonl(tcp_task->rsp_out_sge[i].length);
			wr_len++;
		}
	}

	hdr_len	= sizeof(struct xio_tcp_rsp_hdr);
	hdr_len += sizeof(uint32_t) * rsp_hdr->out_num_sge;

	xio_mbuf_inc(&task->mbuf, hdr_len);

#ifdef EYAL_TODO
	print_hex_dump_bytes("post_send: ", DUMP_PREFIX_ADDRESS,
			     task->mbuf.tlv.head, 64);
#endif
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_prep_rsp_header						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_prep_rsp_header(struct xio_tcp_transport *tcp_hndl,
				   struct xio_task *task,
				   uint16_t ulp_hdr_len,
				   uint16_t ulp_pad_len,
				   uint64_t ulp_imm_len,
				   uint32_t status)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	struct xio_tcp_rsp_hdr	rsp_hdr = {};

	if (unlikely(!IS_RESPONSE(task->tlv_type))) {
		ERROR_LOG("unknown message type\n");
		return -1;
	}

	/* fill response header */
	rsp_hdr.version		= XIO_TCP_RSP_HEADER_VERSION;
	rsp_hdr.rsp_hdr_len	= sizeof(rsp_hdr);
	rsp_hdr.rtid            = task->rtid;
	rsp_hdr.ltid		= task->ltid;
	rsp_hdr.out_tcp_op	= tcp_task->out_tcp_op;
	rsp_hdr.flags		= task->omsg_flags;
	rsp_hdr.out_num_sge	= tcp_task->rsp_out_num_sge;
	rsp_hdr.ulp_hdr_len	= ulp_hdr_len;
	rsp_hdr.ulp_pad_len	= ulp_pad_len;
	rsp_hdr.ulp_imm_len	= ulp_imm_len;
	rsp_hdr.status		= status;

	xio_clear_internal_flags(&rsp_hdr.flags);

	if (xio_tcp_write_rsp_header(tcp_hndl, task, &rsp_hdr) != 0)
		goto cleanup;

	tcp_task->txd.ctl_msg_len = xio_mbuf_tlv_len(&task->mbuf);

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
	ERROR_LOG("xio_tcp_write_rsp_header failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_prep_rsp_wr_data						     */
/*---------------------------------------------------------------------------*/
int xio_tcp_prep_rsp_wr_data(struct xio_tcp_transport *tcp_hndl,
			     struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	unsigned int i, llen = 0, rlen = 0;
	int retval;
	int enforce_write_rsp;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;

	sgtbl		= xio_sg_table_get(&task->omsg->out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->out.sgl_type);
	enforce_write_rsp = !!(task->imsg_flags & XIO_MSG_FLAG_PEER_WRITE_RSP);

	/* user did not provided mr */
	sg = sge_first(sgtbl_ops, sgtbl);
	if (!sge_mr(sgtbl_ops, sg) &&
	    tcp_options.enable_mr_check) {
		if (!tcp_hndl->tcp_mempool) {
			xio_set_error(XIO_E_NO_BUFS);
			ERROR_LOG("message /read/write failed - " \
				  "library's memory pool disabled\n");
			goto cleanup;
		}
		/* user did not provide mr - take buffers from pool
		 * and do copy */
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			retval = xio_mempool_alloc(
					tcp_hndl->tcp_mempool,
					sge_length(sgtbl_ops, sg),
					&tcp_task->write_reg_mem[i]);
			if (unlikely(retval)) {
				tcp_task->write_num_reg_mem = i;
				xio_set_error(ENOMEM);
				ERROR_LOG("mempool is empty for %zd bytes\n",
					  sge_length(sgtbl_ops, sg));
				goto cleanup;
			}

			/* copy the data to the buffer */
			memcpy(tcp_task->write_reg_mem[i].addr,
			       sge_addr(sgtbl_ops, sg),
			       sge_length(sgtbl_ops, sg));

			tcp_task->txd.msg_iov[i + 1].iov_base =
					tcp_task->write_reg_mem[i].addr;
			tcp_task->txd.msg_iov[i + 1].iov_len =
					sge_length(sgtbl_ops, sg);
			llen += sge_length(sgtbl_ops, sg);
		}
	} else {
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			tcp_task->txd.msg_iov[i + 1].iov_base =
				sge_addr(sgtbl_ops, sg);
			tcp_task->txd.msg_iov[i + 1].iov_len =
					sge_length(sgtbl_ops, sg);
			llen += sge_length(sgtbl_ops, sg);
		}
	}

	tcp_task->txd.msg_len =
			tbl_nents(sgtbl_ops, sgtbl) + 1;
	tcp_task->txd.tot_iov_byte_len = llen;

	for (i = 0;  i < tcp_task->req_in_num_sge; i++)
		rlen += tcp_task->req_in_sge[i].length;

	if (rlen  < llen) {
		ERROR_LOG("peer provided too small iovec\n");
		ERROR_LOG("tcp write is ignored\n");
		task->status = EINVAL;
		goto cleanup;
	}

	i = 0;
	if (enforce_write_rsp) {
		if (tcp_task->req_in_num_sge < tbl_nents(sgtbl_ops, sgtbl)) {
			ERROR_LOG("peer provided too small iovec\n");
			ERROR_LOG("tcp write is ignored\n");
			task->status = EINVAL;
			goto cleanup;
		}
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			if (tcp_task->req_in_sge[i].length >= sge_length(sgtbl_ops, sg)) {
				tcp_task->rsp_out_sge[i].length = sge_length(sgtbl_ops, sg);
			} else {
				ERROR_LOG("peer provided too small iovec [%d] - " \
					  "peer_len:%d > local_len:%d\n", i,
					  tcp_task->req_in_sge[i].length,
					  sge_length(sgtbl_ops, sg));
				ERROR_LOG("tcp write is ignored\n");
				task->status = EINVAL;
				goto cleanup;
			}
		}
	} else {
		while (llen) {
			if (tcp_task->req_in_sge[i].length < llen) {
				tcp_task->rsp_out_sge[i].length =
					tcp_task->req_in_sge[i].length;
			} else {
				tcp_task->rsp_out_sge[i].length =
					llen;
			}
			llen -= tcp_task->rsp_out_sge[i].length;
			++i;
		}
	}
	tcp_task->rsp_out_num_sge = i;

	return 0;
cleanup:
	for (i = 0; i < tcp_task->write_num_reg_mem; i++)
		xio_mempool_free(&tcp_task->write_reg_mem[i]);

	tcp_task->write_num_reg_mem = 0;
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_send_rsp							     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_send_rsp(struct xio_tcp_transport *tcp_hndl,
			    struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	uint64_t		xio_hdr_len;
	uint64_t		ulp_hdr_len;
	uint64_t		ulp_pad_len = 0;
	uint64_t		ulp_imm_len;
	size_t			retval, nents;
	int			enforce_write_rsp;
	int			tlv_len = 0;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;

	if (task->on_hold) {
		/* dynamically initialize header */
		tcp_task->txd.msg_iov[0].iov_base = xio_mbuf_buf_head(&task->mbuf);
		tcp_task->txd.ctl_msg = xio_mbuf_buf_head(&task->mbuf);
	}

	sgtbl		= xio_sg_table_get(&task->omsg->out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->omsg->out.sgl_type);
	nents		= tbl_nents(sgtbl_ops, sgtbl);

	/* calculate headers */
	ulp_hdr_len	= task->omsg->out.header.iov_len;
	ulp_imm_len	= tbl_length(sgtbl_ops, sgtbl);
	xio_hdr_len = xio_mbuf_get_curr_offset(&task->mbuf);
	xio_hdr_len += sizeof(struct xio_tcp_rsp_hdr);
	xio_hdr_len += tcp_task->req_in_num_sge * sizeof(struct xio_sge);
	enforce_write_rsp = (nents && (task->imsg_flags & XIO_MSG_FLAG_PEER_WRITE_RSP));

	if (ulp_hdr_len > tcp_hndl->peer_max_header &&
	    IS_APPLICATION_MSG(task->tlv_type)) {
			ERROR_LOG("hdr_len=%d is bigger than peer_max_header=%d\n",
				ulp_hdr_len, tcp_hndl->peer_max_header);
		goto cleanup;
	}

	if (g_options.inline_xio_data_align && ulp_imm_len) {
		uint16_t hdr_len = xio_hdr_len + ulp_hdr_len;

		ulp_pad_len = ALIGN(hdr_len, g_options.inline_xio_data_align) -
			      hdr_len;
	}
	/*
	if (tcp_hndl->max_inline_buf_sz < xio_hdr_len + ulp_hdr_len) {
		ERROR_LOG("header size %lu exceeds max header %lu\n",
			  ulp_hdr_len,
			  tcp_hndl->max_inline_buf_sz - xio_hdr_len);
		xio_set_error(XIO_E_MSG_SIZE);
		goto cleanup;
	}
	*/

	/* Small data is outgoing via SEND unless the requester explicitly
	 * insisted on RDMA operation and provided resources.
	 */
	if (!enforce_write_rsp &&
	    ((ulp_imm_len == 0) || ((xio_hdr_len + ulp_hdr_len +
				     ulp_pad_len + ulp_imm_len)
				    <= tcp_hndl->max_inline_buf_sz))) {
		tcp_task->out_tcp_op = XIO_TCP_SEND;
		/* write xio header to the buffer */
		retval = xio_tcp_prep_rsp_header(
				tcp_hndl, task,
				(uint16_t)ulp_hdr_len,
				(uint16_t)ulp_pad_len, ulp_imm_len,
				task->status);
		if (retval)
			goto cleanup;

		/* if there is data, set it to buffer or directly to the sge */
		if (ulp_imm_len) {
			retval = xio_tcp_write_send_data(tcp_hndl, task);
			if (retval)
				goto cleanup;
		}
	} else {
		if (tcp_task->req_in_sge[0].addr &&
		    tcp_task->req_in_sge[0].length) {
			/* the data is sent via RDMA_WRITE equivalent*/
			tcp_task->out_tcp_op = XIO_TCP_WRITE;
			/* prepare rdma write equivalent */
			retval = xio_tcp_prep_rsp_wr_data(tcp_hndl, task);
			if (retval)
				goto cleanup;

			/* and the header is sent via SEND */
			/* write xio header to the buffer */
			retval = xio_tcp_prep_rsp_header(
					tcp_hndl, task,
					(uint16_t)ulp_hdr_len, 0, ulp_imm_len,
					XIO_E_SUCCESS);
			if (retval)
                                goto cleanup;
		} else {
			ERROR_LOG("partial completion of request due " \
					"to missing, response buffer\n");

			/* the client did not provide buffer for response */
			xio_tcp_prep_rsp_header(
					tcp_hndl, task,
					(uint16_t)ulp_hdr_len, 0, 0,
					XIO_E_PARTIAL_MSG);
			goto cleanup;
		}
	}

	if (ulp_imm_len == 0) {
		/* no data at all */
		tbl_set_nents(sgtbl_ops, sgtbl, 0);
		tcp_task->txd.tot_iov_byte_len = 0;
		tcp_task->txd.msg_len = 1;
	}

	/* set the length */
	tlv_len = tcp_hndl->sock.ops->set_txd(task);

	/* add tlv */
	if (xio_mbuf_write_tlv(&task->mbuf, task->tlv_type, tlv_len) != 0)
		goto cleanup;

	if (IS_KEEPALIVE(task->tlv_type))
		list_move(&task->tasks_list_entry, &tcp_hndl->tx_ready_list);
	else
		list_move_tail(&task->tasks_list_entry, &tcp_hndl->tx_ready_list);

	tcp_hndl->tx_ready_tasks_num++;

	retval = xio_tcp_xmit(tcp_hndl);
	if (retval) {
		/* no need xio_get_last_error here */
		retval = xio_errno();
		if (retval != XIO_EAGAIN) {
			ERROR_LOG("xio_xmit_tcp failed. %s\n",
					xio_strerror(retval));
			return -1;
		}
		xio_context_add_event(tcp_hndl->base.ctx,
				      &tcp_hndl->flush_tx_event);
		retval = 0;
	}

	return retval;

cleanup:
	xio_set_error(XIO_E_MSG_SIZE);
	ERROR_LOG("xio_tcp_send_msg failed\n");
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_read_req_header						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_read_req_header(struct xio_tcp_transport *tcp_hndl,
				   struct xio_task *task,
				   struct xio_tcp_req_hdr *req_hdr)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	struct xio_tcp_req_hdr		*tmp_req_hdr;
	struct xio_sge			*tmp_sge;
	int				i;
	size_t				hdr_len;

	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_req_hdr = (struct xio_tcp_req_hdr *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	req_hdr->version  = tmp_req_hdr->version;
	req_hdr->flags    = tmp_req_hdr->flags;
	UNPACK_SVAL(tmp_req_hdr, req_hdr, req_hdr_len);

	if (unlikely(req_hdr->req_hdr_len != sizeof(struct xio_tcp_req_hdr))) {
		ERROR_LOG(
		"header length's read failed. arrived:%d  expected:%zd\n",
		req_hdr->req_hdr_len, sizeof(struct xio_tcp_req_hdr));
		return -1;
	}

	UNPACK_SVAL(tmp_req_hdr, req_hdr, sn);
	UNPACK_LVAL(tmp_req_hdr, req_hdr, ltid);
	req_hdr->out_tcp_op = tmp_req_hdr->out_tcp_op;
	req_hdr->in_tcp_op = tmp_req_hdr->in_tcp_op;

	UNPACK_SVAL(tmp_req_hdr, req_hdr, in_num_sge);
	UNPACK_SVAL(tmp_req_hdr, req_hdr, out_num_sge);
	UNPACK_SVAL(tmp_req_hdr, req_hdr, ulp_hdr_len);
	UNPACK_SVAL(tmp_req_hdr, req_hdr, ulp_pad_len);

	/* remain_data_len not in use */
	UNPACK_LLVAL(tmp_req_hdr, req_hdr, ulp_imm_len);

	tmp_sge = (struct xio_sge *)((uint8_t *)tmp_req_hdr +
			sizeof(struct xio_tcp_req_hdr));

	tcp_task->sn = req_hdr->sn;

	/* params for SEND/RDMA_WRITE */
	for (i = 0;  i < req_hdr->in_num_sge; i++) {
		UNPACK_LLVAL(tmp_sge, &tcp_task->req_in_sge[i], addr);
		UNPACK_LVAL(tmp_sge, &tcp_task->req_in_sge[i], length);
		UNPACK_LVAL(tmp_sge, &tcp_task->req_in_sge[i], stag);
		tmp_sge++;
	}
	tcp_task->req_in_num_sge	= i;

	/* params for RDMA_READ */
	for (i = 0;  i < req_hdr->out_num_sge; i++) {
		UNPACK_LLVAL(tmp_sge, &tcp_task->req_out_sge[i], addr);
		UNPACK_LVAL(tmp_sge, &tcp_task->req_out_sge[i], length);
		UNPACK_LVAL(tmp_sge, &tcp_task->req_out_sge[i], stag);
		tmp_sge++;
	}
	tcp_task->req_out_num_sge	= i;

	hdr_len	= sizeof(struct xio_tcp_req_hdr);
	hdr_len += sizeof(struct xio_sge) * (req_hdr->in_num_sge +
					     req_hdr->out_num_sge);

	xio_mbuf_inc(&task->mbuf, hdr_len);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_read_rsp_header						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_read_rsp_header(struct xio_tcp_transport *tcp_hndl,
				   struct xio_task *task,
				   struct xio_tcp_rsp_hdr *rsp_hdr)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	struct xio_tcp_rsp_hdr		*tmp_rsp_hdr;
	uint32_t			*wr_len;
	int				i;
	size_t				hdr_len;

	memset(rsp_hdr, 0, sizeof(*rsp_hdr));
	/* point to transport header */
	xio_mbuf_set_trans_hdr(&task->mbuf);
	tmp_rsp_hdr = (struct xio_tcp_rsp_hdr *)
			xio_mbuf_get_curr_ptr(&task->mbuf);

	rsp_hdr->version  = tmp_rsp_hdr->version;
	rsp_hdr->flags    = tmp_rsp_hdr->flags;
	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, rsp_hdr_len);

	if (unlikely(rsp_hdr->rsp_hdr_len != sizeof(struct xio_tcp_rsp_hdr))) {
		ERROR_LOG(
		"header length's read failed. arrived:%d expected:%zd\n",
		  rsp_hdr->rsp_hdr_len, sizeof(struct xio_tcp_rsp_hdr));
		return -1;
	}

	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, sn);
	UNPACK_LVAL(tmp_rsp_hdr, rsp_hdr, rtid);
	UNPACK_LVAL(tmp_rsp_hdr, rsp_hdr, ltid);
	rsp_hdr->out_tcp_op = tmp_rsp_hdr->out_tcp_op;
	UNPACK_LVAL(tmp_rsp_hdr, rsp_hdr, status);
	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, out_num_sge);
	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, ulp_hdr_len);
	UNPACK_SVAL(tmp_rsp_hdr, rsp_hdr, ulp_pad_len);
	/* remain_data_len not in use */
	UNPACK_LLVAL(tmp_rsp_hdr, rsp_hdr, ulp_imm_len);

	if (rsp_hdr->out_num_sge) {
		wr_len = (uint32_t *)((uint8_t *)tmp_rsp_hdr +
				sizeof(struct xio_tcp_rsp_hdr));

		/* params for RDMA WRITE */
		for (i = 0;  i < rsp_hdr->out_num_sge; i++) {
			tcp_task->rsp_out_sge[i].length = ntohl(*wr_len);
			wr_len++;
		}
		tcp_task->rsp_out_num_sge = rsp_hdr->out_num_sge;
	}

	hdr_len	= sizeof(struct xio_tcp_rsp_hdr);
	hdr_len += sizeof(uint32_t) * rsp_hdr->out_num_sge;

	xio_mbuf_inc(&task->mbuf, hdr_len);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_recv_ctl_work						     */
/*---------------------------------------------------------------------------*/
int xio_tcp_recv_ctl_work(struct xio_tcp_transport *tcp_hndl, int fd,
			  struct xio_tcp_work_req *xio_recv, int block)
{
	int			retval;
	int			bytes_to_copy;

	if (xio_recv->tot_iov_byte_len == 0)
		return 1;

	if (xio_recv->msg.msg_iovlen > 1 ||
	    xio_recv->tot_iov_byte_len != xio_recv->msg.msg_iov[0].iov_len) {
		ERROR_LOG("expecting only 1 sized iovec\n");
		return 0;
	}

	while (xio_recv->tot_iov_byte_len) {
		while (tcp_hndl->tmp_rx_buf_len == 0) {
			retval = recv(fd, (char *)tcp_hndl->tmp_rx_buf,
				      TMP_RX_BUF_SIZE, 0);
			if (retval > 0) {
				tcp_hndl->tmp_rx_buf_len = retval;
				tcp_hndl->tmp_rx_buf_cur = tcp_hndl->tmp_rx_buf;
			} else if (retval == 0) {
				/*so errno is not EAGAIN*/
				xio_set_error(XIO_ECONNABORTED);
				DEBUG_LOG("tcp transport got EOF,tcp_hndl=%p\n",
					  tcp_hndl);
				return 0;
			} else {
				if (xio_get_last_socket_error() == XIO_EAGAIN) {
					if (!block) {
						xio_set_error(
						   xio_get_last_socket_error());
						return -1;
					}
				} else if (xio_get_last_socket_error() ==
						XIO_ECONNRESET ||
					   xio_get_last_socket_error() ==
						XIO_ECONNABORTED) {
					xio_set_error(
						   xio_get_last_socket_error());
					DEBUG_LOG("recv failed.(errno=%d)\n",
						  xio_get_last_socket_error());
					return 0;
				} else {
					xio_set_error(
						   xio_get_last_socket_error());
					ERROR_LOG("recv failed.(errno=%d)\n",
						  xio_get_last_socket_error());
					return -1;
				}
			}
		}
		bytes_to_copy = xio_recv->tot_iov_byte_len >
				tcp_hndl->tmp_rx_buf_len ?
				(int)tcp_hndl->tmp_rx_buf_len :
				(int)xio_recv->tot_iov_byte_len;
		memcpy(xio_recv->msg.msg_iov[0].iov_base,
		       tcp_hndl->tmp_rx_buf_cur, bytes_to_copy);
		inc_ptr(tcp_hndl->tmp_rx_buf_cur, bytes_to_copy);
		inc_ptr(xio_recv->msg.msg_iov[0].iov_base, bytes_to_copy);
		tcp_hndl->tmp_rx_buf_len -= bytes_to_copy;
		xio_recv->msg.msg_iov[0].iov_len -= bytes_to_copy;
		xio_recv->tot_iov_byte_len -= bytes_to_copy;
	}

	xio_recv->msg.msg_iovlen = 0;

	return 1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_recvmsg_work							     */
/*---------------------------------------------------------------------------*/
int xio_tcp_recvmsg_work(struct xio_tcp_transport *tcp_hndl, int fd,
			 struct xio_tcp_work_req *xio_recv, int block)
{
	unsigned int		i;
	int			retval;
	int			recv_bytes = 0, tmp_bytes;

	if (xio_recv->tot_iov_byte_len == 0)
		return 1;

	while (xio_recv->tot_iov_byte_len) {
		retval = recvmsg(fd, &xio_recv->msg, MSG_DONTWAIT);
		if (retval > 0) {
			if (xio_recv->tot_iov_byte_len >= (uint64_t)retval) {
				recv_bytes += retval;
				xio_recv->tot_iov_byte_len -= retval;

				if (xio_recv->tot_iov_byte_len == 0) {
					xio_recv->msg.msg_iovlen = 0;
					break;
				}
			} else {
				xio_set_error(EBADMSG);
				ERROR_LOG("unexpected message size. (errno=%d)" \
					  "tot_iov_byte_len:%lu, retval:%d\n",
					  EBADMSG,
					  xio_recv->tot_iov_byte_len, retval);
				return -1;
			}
			tmp_bytes = 0;
			for (i = 0; i < xio_recv->msg.msg_iovlen; i++) {
				if (xio_recv->msg.msg_iov[i].iov_len +
						tmp_bytes <= (size_t)retval) {
					tmp_bytes +=
					xio_recv->msg.msg_iov[i].iov_len;
				} else {
					xio_recv->msg.msg_iov[i].iov_len -=
							(retval - tmp_bytes);
					inc_ptr(
					  xio_recv->msg.msg_iov[i].iov_base,
					  retval - tmp_bytes);
					xio_recv->msg.msg_iov =
						&xio_recv->msg.msg_iov[i];
					xio_recv->msg.msg_iovlen -= i;
					break;
				}
			}
		} else if (retval == 0) {
			xio_set_error(ECONNABORTED); /*so errno is not EAGAIN*/
			DEBUG_LOG("tcp transport got EOF, tcp_hndl=%p\n",
				  tcp_hndl);
			return 0;
		} else {
			if (xio_get_last_socket_error() == XIO_EAGAIN) {
				if (!block) {
					xio_set_error(
						xio_get_last_socket_error());
					return -1;
				}
			} else if (xio_get_last_socket_error() ==
				   XIO_ECONNRESET ||
				   xio_get_last_socket_error() ==
				   XIO_ECONNABORTED) {
				xio_set_error(xio_get_last_socket_error());
				DEBUG_LOG("recvmsg failed. (errno=%d)\n",
					  xio_get_last_socket_error());
				return 0;
			}
			xio_set_error(xio_get_last_socket_error());
			ERROR_LOG("recvmsg failed. (errno=%d)\n",
				  xio_get_last_socket_error());
			return -1;
		}
	}

	return recv_bytes;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_single_sock_set_rxd						     */
/*---------------------------------------------------------------------------*/
void xio_tcp_single_sock_set_rxd(struct xio_task *task,
				 void *buf, uint32_t len)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	tcp_task->rxd.tot_iov_byte_len = 0;
	tcp_task->rxd.msg_len = 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_dual_sock_set_rxd						     */
/*---------------------------------------------------------------------------*/
void xio_tcp_dual_sock_set_rxd(struct xio_task *task,
			       void *buf, uint32_t len)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	if (IS_APPLICATION_MSG(task->tlv_type))
		inc_ptr(buf, task->imsg.in.header.iov_len);
	tcp_task->rxd.msg_iov[0].iov_base = buf;
	tcp_task->rxd.msg_iov[0].iov_len = len;
	tcp_task->rxd.tot_iov_byte_len = len;
	if (len) {
		tcp_task->rxd.msg_len = 1;
		tcp_task->rxd.msg.msg_iovlen = 1;
		tcp_task->rxd.msg.msg_iov = tcp_task->rxd.msg_iov;
	} else {
		tcp_task->rxd.msg_len = 0;
		tcp_task->rxd.msg.msg_iovlen = 0;
	}
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_rd_req_header						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_rd_req_header(struct xio_tcp_transport *tcp_hndl,
				 struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	unsigned int		i, vec_size = 0;
	int			retval;
	size_t			rlen = 0, llen = 0;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;

	/* responder side got request for rdma read */

	/* need for buffer to do rdma read. there are two options:	   */
	/* option 1: user provides call back that fills application memory */
	/* option 2: use internal buffer pool				   */

	/* hint the upper layer of sizes */
	sgtbl		= xio_sg_table_get(&task->imsg.in);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->imsg.in.sgl_type);
	tbl_set_nents(sgtbl_ops, sgtbl, tcp_task->req_out_num_sge);
	for_each_sge(sgtbl, sgtbl_ops, sg, i) {
		sge_set_addr(sgtbl_ops, sg, NULL);
		sge_set_length(sgtbl_ops, sg,
			       tcp_task->req_out_sge[i].length);
		rlen += tcp_task->req_out_sge[i].length;
		tcp_task->read_reg_mem[i].ctx = tcp_hndl->base.ctx;
		tcp_task->read_reg_mem[i].priv = NULL;
	}
	sgtbl		= xio_sg_table_get(&task->imsg.out);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->imsg.out.sgl_type);
	if (tcp_task->req_in_num_sge) {
		tbl_set_nents(sgtbl_ops, sgtbl, tcp_task->req_in_num_sge);
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			sge_set_addr(sgtbl_ops, sg, NULL);
			sge_set_length(sgtbl_ops, sg,
				       tcp_task->req_in_sge[i].length);
			tcp_task->write_reg_mem[i].ctx = tcp_hndl->base.ctx;
			tcp_task->write_reg_mem[i].priv = NULL;
		}
	} else {
		tbl_set_nents(sgtbl_ops, sgtbl, 0);
	}
	sgtbl		= xio_sg_table_get(&task->imsg.in);
	sgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(task->imsg.in.sgl_type);

	task->is_assigned = 0;
	task->status = 0;
	xio_transport_assign_in_buf(&tcp_hndl->base, task);
	if (task->status) {
		WARN_LOG("assign_in_buf: error:%d. rdma read is ignored\n",
			  task->status);
		return -1;
	}
	if (task->is_assigned) {
		/* if user does not have buffers ignore */
		if (tbl_nents(sgtbl_ops, sgtbl) == 0) {
			WARN_LOG("application has not provided buffers\n");
			WARN_LOG("tcp read is ignored\n");
			task->status = XIO_E_NO_USER_BUFS;
			return -1;
		}

		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			if (!sge_mr(sgtbl_ops, sg) && tcp_options.enable_mr_check) {
				ERROR_LOG("application has not provided mr\n");
				ERROR_LOG("tcp read is ignored\n");
				task->status = XIO_E_NO_USER_MR;
				return -1;
			}
			if (!sge_addr(sgtbl_ops, sg)) {
				ERROR_LOG("application has provided " \
					  "null address\n");
				ERROR_LOG("tcp read is ignored\n");
				task->status = XIO_E_NO_USER_BUFS;
				return -1;
			}
			llen += sge_length(sgtbl_ops, sg);
			vec_size++;
			if (llen > rlen) {
				sge_set_length(sgtbl_ops, sg, rlen -
					       (llen -
						sge_length(sgtbl_ops, sg)));
				tcp_task->req_out_sge[i].length =
						sge_length(sgtbl_ops, sg);
				break;
			}
			tcp_task->req_out_sge[i].length =
						sge_length(sgtbl_ops, sg);
		}
		if (rlen  > llen) {
			ERROR_LOG("application provided too small iovec\n");
			ERROR_LOG("remote peer want to write %zd bytes while" \
				  "local peer provided buffer size %zd bytes\n",
				  rlen, llen);
			ERROR_LOG("tcp read is ignored\n");
			task->status = XIO_E_USER_BUF_OVERFLOW;
			return -1;
		}

		tcp_task->req_out_num_sge = vec_size;
		tbl_set_nents(sgtbl_ops, sgtbl, vec_size);
		set_bits(XIO_MSG_HINT_ASSIGNED_DATA_IN_BUF, &task->imsg.hints);
	} else {
		if (!tcp_hndl->tcp_mempool) {
				ERROR_LOG("message /read/write failed - " \
					  "library's memory pool disabled\n");
				task->status = XIO_E_NO_BUFS;
				goto cleanup;
		}

		tbl_set_nents(sgtbl_ops, sgtbl, tcp_task->req_out_num_sge);
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			retval = xio_mempool_alloc(
					tcp_hndl->tcp_mempool,
					tcp_task->req_out_sge[i].length,
					&tcp_task->read_reg_mem[i]);

			if (retval) {
				tcp_task->read_num_reg_mem = i;
				ERROR_LOG("mempool is empty for %zd bytes\n",
					  tcp_task->req_out_sge[i].length);
				task->status = ENOMEM;
				goto cleanup;
			}
			sge_set_addr(sgtbl_ops, sg,
				     tcp_task->read_reg_mem[i].addr);
			sge_set_length(sgtbl_ops, sg,
				       tcp_task->read_reg_mem[i].length);
			sge_set_mr(sgtbl_ops, sg,
				   tcp_task->read_reg_mem[i].mr);
		}
		tcp_task->read_num_reg_mem = tcp_task->req_out_num_sge;
	}

	sg = sge_first(sgtbl_ops, sgtbl);
	for (i = 0;  i < tcp_task->req_out_num_sge; i++) {
		tcp_task->rxd.msg_iov[i + 1].iov_base =
			sge_addr(sgtbl_ops, sg);
		tcp_task->rxd.msg_iov[i + 1].iov_len =
			tcp_task->req_out_sge[i].length;
		sge_set_length(sgtbl_ops, sg,
			       tcp_task->req_out_sge[i].length);
		sg = sge_next(sgtbl_ops, sgtbl, sg);
	}
	tcp_task->rxd.msg_len += tcp_task->req_out_num_sge;

	/* prepare the in side of the message */
	tcp_task->rxd.tot_iov_byte_len += rlen;
	if (tcp_task->rxd.msg.msg_iovlen)
		tcp_task->rxd.msg.msg_iov = tcp_task->rxd.msg_iov;
	else
		tcp_task->rxd.msg.msg_iov = &tcp_task->rxd.msg_iov[1];
	tcp_task->rxd.msg.msg_iovlen = tcp_task->rxd.msg_len;

	return 0;
cleanup:
	xio_free_rdma_read_mem(tcp_hndl, task);
	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_on_recv_req_header						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_on_recv_req_header(struct xio_tcp_transport *tcp_hndl,
				      struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	int			retval = 0;
	struct xio_tcp_req_hdr	req_hdr;
	struct xio_msg		*imsg;
	void			*ulp_hdr;
	unsigned int		i;
	struct xio_sg_table_ops	*sgtbl_ops;
	void			*sgtbl;
	void			*sg;
	int			header_err = 1; /* temporary do not activate */

	/* read header */
	retval = xio_tcp_read_req_header(tcp_hndl, task, &req_hdr);
	if (retval != 0) {
		xio_set_error(XIO_E_MSG_INVALID);
		header_err = 1;
		goto cleanup;
	}

	/* save originator identifier */
	task->rtid		= req_hdr.ltid;
	task->imsg_flags	= req_hdr.flags;

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
	if (tcp_task->req_in_num_sge) {
		tbl_set_nents(sgtbl_ops, sgtbl, tcp_task->req_in_num_sge);
		for_each_sge(sgtbl, sgtbl_ops, sg, i) {
			sge_set_addr(sgtbl_ops, sg, NULL);
			sge_set_length(sgtbl_ops, sg,
				       tcp_task->req_in_sge[i].length);
				       sge_set_mr(sgtbl_ops, sg, NULL);
		}
	} else {
		tbl_set_nents(sgtbl_ops, sgtbl, 0);
	}

	tcp_task->out_tcp_op = (enum xio_tcp_op_code)req_hdr.out_tcp_op;
	tcp_task->in_tcp_op = (enum xio_tcp_op_code)req_hdr.in_tcp_op;

	switch (req_hdr.out_tcp_op) {
	case XIO_TCP_SEND:
		if (IS_APPLICATION_MSG(task->tlv_type))
			/* we already got the header with the XIO management */
			tcp_hndl->sock.ops->set_rxd(task, ulp_hdr,
					(uint32_t)req_hdr.ulp_imm_len);
		else
			tcp_hndl->sock.ops->set_rxd(task, ulp_hdr,
					req_hdr.ulp_hdr_len +
					req_hdr.ulp_pad_len +
					(uint32_t)req_hdr.ulp_imm_len);
		sgtbl		= xio_sg_table_get(&imsg->in);
		sgtbl_ops	= (struct xio_sg_table_ops *)
					xio_sg_table_ops_get(imsg->in.sgl_type);
		if (req_hdr.ulp_imm_len) {
			/* incoming data via SEND */
			/* if data arrived, set the pointers */
			tbl_set_nents(sgtbl_ops, sgtbl, 1);
			sg = sge_first(sgtbl_ops, sgtbl);

			sge_set_addr(sgtbl_ops, sg,
				     sum_to_ptr(ulp_hdr,
						imsg->in.header.iov_len +
							req_hdr.ulp_pad_len));
			sge_set_length(sgtbl_ops, sg,
				       (size_t)req_hdr.ulp_imm_len);
		} else {
			/* no data at all */
			tbl_set_nents(sgtbl_ops, sgtbl, 0);
		}
		break;
	case XIO_TCP_READ:
		/* we already got the header with the XIO management */
		tcp_hndl->sock.ops->set_rxd(task, ulp_hdr,
				(uint32_t)req_hdr.ulp_imm_len);
		/* handle RDMA READ equivalent. */
		retval = xio_tcp_rd_req_header(tcp_hndl, task);
		if (unlikely(retval))
			goto cleanup;
		break;
	default:
		ERROR_LOG("unexpected opcode\n");
		xio_set_error(XIO_E_MSG_INVALID);
		task->status = XIO_E_MSG_INVALID;
		break;
	}

	return 0;

cleanup:
	retval = xio_errno(); /* no need get_last_socket_error() */
	ERROR_LOG("xio_tcp_on_recv_req failed. (errno=%d %s)\n", retval,
		  xio_strerror(retval));
	if (header_err)
		xio_transport_notify_observer_error(&tcp_hndl->base, retval);
	else
		xio_transport_notify_message_error(&tcp_hndl->base, task,
				XIO_MSG_DIRECTION_IN, (enum xio_status)retval);

	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_on_recv_req_data						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_on_recv_req_data(struct xio_tcp_transport *tcp_hndl,
				    struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	union xio_transport_event_data event_data;

	switch (tcp_task->out_tcp_op) {
	case XIO_TCP_SEND:
		break;
	case XIO_TCP_READ:
		/* handle RDMA READ equivalent. */
		break;
	default:
		ERROR_LOG("unexpected opcode\n");
		break;
	}

	/* fill notification event */
	event_data.msg.op	= XIO_WC_OP_RECV;
	event_data.msg.task	= task;

	list_move_tail(&task->tasks_list_entry, &tcp_hndl->io_list);

	if (task->status)
		xio_free_tcp_task_mem(&tcp_hndl->base, task);

	xio_transport_notify_observer(&tcp_hndl->base,
				      XIO_TRANSPORT_EVENT_NEW_MESSAGE,
				      &event_data);

	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_on_recv_rsp_header						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_on_recv_rsp_header(struct xio_tcp_transport *tcp_hndl,
				      struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	int			retval = 0;
	struct xio_tcp_rsp_hdr	rsp_hdr;
	struct xio_msg		*imsg;
	void			*ulp_hdr;
	struct xio_tcp_task	*tcp_sender_task;
	unsigned int		i;
	struct xio_sg_table_ops	*isgtbl_ops;
	void			*isgtbl;
	void			*sg;
	int			header_err = 1; /* temporary do not activate */

	/* read the response header */
	retval = xio_tcp_read_rsp_header(tcp_hndl, task, &rsp_hdr);
	if (retval != 0) {
		xio_set_error(XIO_E_MSG_INVALID);
		header_err = 1;
		goto cleanup;
	}
	/* read the sn */
	tcp_task->sn = rsp_hdr.sn;

	/* find the sender task */
	task->sender_task =
		xio_tcp_primary_task_lookup(tcp_hndl, rsp_hdr.rtid);
	if (task->sender_task == NULL) {
		ERROR_LOG("sender task not found!!!!!. Releasing incoming response. tcp_hndl:%p\n", tcp_hndl);
		xio_tasks_pool_put(task);
		return 0;
	}
	if (IS_KEEPALIVE(task->tlv_type)) {
		if (task->sender_task->ka_probes)
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, tcp_hndl:%p\n",
					__func__, task->tlv_type, task->sender_task->session, task->sender_task->connection,
					tcp_hndl);
	} else {
		if (!IS_NOP(task->tlv_type) && !IS_APPLICATION_MSG(task->tlv_type))
			DEBUG_LOG("%s - tlv_type:0x%x, session:%p, connection:%p, tcp_hndl:%p\n",
					__func__, task->tlv_type, task->sender_task->session, task->sender_task->connection,
					tcp_hndl);
	}

	task->rtid       = rsp_hdr.ltid;

	tcp_sender_task = (struct xio_tcp_task *)task->sender_task->dd_data;

	/* mark the sender task as arrived */
	task->sender_task->state = XIO_TASK_STATE_RESPONSE_RECV;

	imsg		= &task->imsg;
	isgtbl		= xio_sg_table_get(&imsg->in);
	isgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(imsg->in.sgl_type);

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

	tcp_task->out_tcp_op = (enum xio_tcp_op_code)rsp_hdr.out_tcp_op;

	switch (rsp_hdr.out_tcp_op) {
	case XIO_TCP_SEND:
		if (IS_APPLICATION_MSG(task->tlv_type))
			/* we already got the header with the XIO management */
			tcp_hndl->sock.ops->set_rxd(task, ulp_hdr,
						(uint32_t)rsp_hdr.ulp_imm_len);
		else
			tcp_hndl->sock.ops->set_rxd(task, ulp_hdr,
					rsp_hdr.ulp_hdr_len + rsp_hdr.ulp_pad_len +
					(uint32_t)rsp_hdr.ulp_imm_len);
		/* if data arrived, set the pointers */
		if (rsp_hdr.ulp_imm_len) {
			tbl_set_nents(isgtbl_ops, isgtbl, 1);
			sg = sge_first(isgtbl_ops, isgtbl);
			sge_set_addr(isgtbl_ops, sg,
				     sum_to_ptr(ulp_hdr,
						imsg->in.header.iov_len +
							rsp_hdr.ulp_pad_len));
			sge_set_length(isgtbl_ops, sg,
				       (size_t)rsp_hdr.ulp_imm_len);
		} else {
			tbl_set_nents(isgtbl_ops, isgtbl, 0);
		}
		break;
	case XIO_TCP_WRITE:
		/* the data size is set later this is only for the header size */
		tcp_hndl->sock.ops->set_rxd(task->sender_task, ulp_hdr, 0);
		if (tcp_task->rsp_out_num_sge >
		    tcp_sender_task->read_num_reg_mem) {
			ERROR_LOG("local in data_iovec is too small %d < %d\n",
				  tcp_sender_task->read_num_reg_mem,
				  tcp_task->rsp_out_num_sge);
			goto partial_msg;
		}

		tbl_set_nents(isgtbl_ops, isgtbl,
			      tcp_task->rsp_out_num_sge);
		sg = sge_first(isgtbl_ops, isgtbl);
		for (i = 0; i < tcp_task->rsp_out_num_sge; i++) {
			sge_set_addr(isgtbl_ops, sg,
				     tcp_sender_task->read_reg_mem[i].addr);
			sge_set_length(isgtbl_ops, sg,
				       tcp_task->rsp_out_sge[i].length);
			tcp_sender_task->rxd.msg_iov[i + 1].iov_base =
					tcp_sender_task->read_reg_mem[i].addr;
			tcp_sender_task->rxd.msg_iov[i + 1].iov_len =
					tcp_task->rsp_out_sge[i].length;
			sg = sge_next(isgtbl_ops, isgtbl, sg);
		}

		tcp_sender_task->rxd.msg_len +=
				tcp_task->rsp_out_num_sge;
		tcp_sender_task->rxd.tot_iov_byte_len +=
				rsp_hdr.ulp_imm_len;
		if (tcp_sender_task->rxd.msg.msg_iovlen)
			tcp_sender_task->rxd.msg.msg_iov =
					tcp_sender_task->rxd.msg_iov;
		else
			tcp_sender_task->rxd.msg.msg_iov =
					&tcp_sender_task->rxd.msg_iov[1];
		tcp_sender_task->rxd.msg.msg_iovlen =
				tcp_sender_task->rxd.msg_len;
		break;
	default:
		ERROR_LOG("unexpected opcode %d\n", rsp_hdr.out_tcp_op);
		break;
	}

partial_msg:
	return 0;

cleanup:
	retval = xio_errno();
	ERROR_LOG("xio_tcp_on_recv_rsp failed. (errno=%d %s)\n",
		  retval, xio_strerror(retval));
	if (header_err)
		xio_transport_notify_observer_error(&tcp_hndl->base, retval);
	else
		xio_transport_notify_message_error(&tcp_hndl->base, task,
				XIO_MSG_DIRECTION_IN, (enum xio_status)retval);


	return -1;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_on_recv_rsp_data						     */
/*---------------------------------------------------------------------------*/
static int xio_tcp_on_recv_rsp_data(struct xio_tcp_transport *tcp_hndl,
				    struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	union xio_transport_event_data event_data;
	struct xio_msg		*imsg;
	struct xio_msg		*omsg;
	unsigned int		i;
	struct xio_tcp_task	*tcp_sender_task;
	struct xio_sg_table_ops	*isgtbl_ops;
	void			*isgtbl;
	struct xio_sg_table_ops	*osgtbl_ops;
	void			*osgtbl;
	void			*sg;

	omsg		= task->sender_task->omsg;
	imsg		= &task->imsg;
	isgtbl		= xio_sg_table_get(&imsg->in);
	isgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(imsg->in.sgl_type);
	osgtbl		= xio_sg_table_get(&omsg->in);
	osgtbl_ops	= (struct xio_sg_table_ops *)
				xio_sg_table_ops_get(omsg->in.sgl_type);

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
		if (hdr_len)
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

	switch (tcp_task->out_tcp_op) {
	case XIO_TCP_SEND:
		if (tbl_nents(osgtbl_ops, osgtbl)) {
			/* deep copy */
			if (tbl_nents(isgtbl_ops, isgtbl)) {
				size_t idata_len  =
					tbl_length(isgtbl_ops, isgtbl);
				size_t odata_len  =
					tbl_length(osgtbl_ops, osgtbl);
				if (idata_len > odata_len) {
					task->status = XIO_E_MSG_SIZE;
					goto partial_msg;
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
	case XIO_TCP_WRITE:
		/* user provided mr */
		sg = sge_first(osgtbl_ops, osgtbl);
		if (sge_addr(osgtbl_ops, sg) &&
		    (sge_mr(osgtbl_ops, sg) || !tcp_options.enable_mr_check))  {
			void *isg;
			/* data was copied directly to user buffer */
			/* need to update the buffer length */
			for_each_sge(isgtbl, isgtbl_ops, isg, i) {
				sge_set_length(osgtbl_ops, sg,
					       sge_length(isgtbl_ops, isg));
				sg = sge_next(osgtbl_ops, osgtbl, sg);
			}
			tbl_set_nents(osgtbl_ops, osgtbl,
				      tbl_nents(isgtbl_ops, isgtbl));
		} else  {
			/* user provided buffer but not mr */
			/* deep copy */
			if (sge_addr(osgtbl_ops, sg))  {
				tbl_copy(osgtbl_ops, osgtbl,
					 isgtbl_ops, isgtbl);
				tcp_sender_task = (struct xio_tcp_task *)
						task->sender_task->dd_data;
				/* put buffers back to pool */
				for (i = 0; i < tcp_sender_task->read_num_reg_mem;
						i++) {
					xio_mempool_free(
						&tcp_sender_task->read_reg_mem[i]);
					tcp_sender_task->read_reg_mem[i].priv =
						NULL;
				}
				tcp_sender_task->read_num_reg_mem = 0;
			} else {
				/* use provided only length - set user
				 * pointers */
				tbl_clone(osgtbl_ops, osgtbl,
					  isgtbl_ops, isgtbl);
			}
		}
		break;
	default:
		ERROR_LOG("unexpected opcode %d\n", tcp_task->out_tcp_op);
		break;
	}

partial_msg:

	/* fill notification event */
	event_data.msg.op	= XIO_WC_OP_RECV;
	event_data.msg.task	= task;

	list_move_tail(&task->tasks_list_entry, &tcp_hndl->io_list);

	if (task->status)
		xio_free_tcp_task_mem(&tcp_hndl->base, task);

	/* notify the upper layer of received message */
	xio_transport_notify_observer(&tcp_hndl->base,
				      XIO_TRANSPORT_EVENT_NEW_MESSAGE,
				      &event_data);
	return 0;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_send							     */
/*---------------------------------------------------------------------------*/
int xio_tcp_send(struct xio_transport_base *transport,
		 struct xio_task *task)
{
	struct xio_tcp_transport *tcp_hndl =
		(struct xio_tcp_transport *)transport;
	int	retval = -1;

	switch (task->tlv_type) {
	case XIO_NEXUS_SETUP_REQ:
		retval = xio_tcp_send_setup_req(tcp_hndl, task);
		/* bypass send completion and release to initial pool */
		xio_tasks_pool_put(task);
		break;
	case XIO_NEXUS_SETUP_RSP:
		/* bypass send completion and release to initial pool */
		retval = xio_tcp_send_setup_rsp(tcp_hndl, task);
		xio_tasks_pool_put(task);
		break;
	default:
		if (IS_REQUEST(task->tlv_type))
			retval = xio_tcp_send_req(tcp_hndl, task);
		else if (IS_RESPONSE(task->tlv_type))
			retval = xio_tcp_send_rsp(tcp_hndl, task);
		else
			ERROR_LOG("unknown message type:0x%x\n",
				  task->tlv_type);
		break;
	}

	return retval;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_get_data_rxd							     */
/*---------------------------------------------------------------------------*/
struct xio_tcp_work_req *xio_tcp_get_data_rxd(struct xio_task *task)
{
	XIO_TO_TCP_TASK(task, tcp_task);
	struct xio_tcp_task *tcp_sender_task;

	switch (tcp_task->out_tcp_op) {
	case XIO_TCP_SEND:
	case XIO_TCP_READ:
		return &tcp_task->rxd;
	case XIO_TCP_WRITE:
		tcp_sender_task = (struct xio_tcp_task *)
					task->sender_task->dd_data;
		return &tcp_sender_task->rxd;
	default:
		ERROR_LOG("unexpected opcode %d\n", tcp_task->out_tcp_op);
		break;
	}

	return NULL;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_rx_data_handler						     */
/*---------------------------------------------------------------------------*/
int xio_tcp_rx_data_handler(struct xio_tcp_transport *tcp_hndl, int batch_nr)
{
	int retval = 0, recvmsg_retval = 0;
	struct xio_tcp_task *tcp_task, *next_tcp_task;
	struct xio_task *task, *next_task/*, *task1 = NULL, *task2*/;
	unsigned int i, last_in_rxq;
	int batch_count = 0, tmp_count = 0, ret_count = 0;
	unsigned int iov_len;
	uint64_t bytes_recv;
	struct xio_tcp_work_req *rxd_work, *next_rxd_work;

	task = list_first_entry_or_null(&tcp_hndl->rx_list,
					struct xio_task,
					tasks_list_entry);

	while (task && batch_count < batch_nr) {
		tcp_task = (struct xio_tcp_task *)task->dd_data;

		if (tcp_task->rxd.stage != XIO_TCP_RX_IO_DATA)
			break;

		next_task = list_first_entry_or_null(
				&task->tasks_list_entry,
				struct xio_task,  tasks_list_entry);
		next_tcp_task = next_task ? (struct xio_tcp_task *)
						next_task->dd_data : NULL;
		next_rxd_work = (next_tcp_task &&
				 next_tcp_task->rxd.stage == XIO_TCP_RX_IO_DATA)
				 ? xio_tcp_get_data_rxd(next_task) : NULL;

		/* An Accelio application runs on Side A would crush,
		 * when it connects Side B by a port binded by an
		 * application (not accelio) run on Side B.
		 */
		rxd_work = xio_tcp_get_data_rxd(task);
		if (!rxd_work) {
			ERROR_LOG("rxd_work is NULL! Disconnect!\n");
			xio_tcp_disconnect_helper(tcp_hndl);
			return -1;
		}

		for (i = 0; i < rxd_work->msg.msg_iovlen; i++) {
			tcp_hndl->tmp_work.msg_iov
			[tcp_hndl->tmp_work.msg_len].iov_base =
				rxd_work->msg.msg_iov[i].iov_base;
			tcp_hndl->tmp_work.msg_iov
			[tcp_hndl->tmp_work.msg_len].iov_len =
				rxd_work->msg.msg_iov[i].iov_len;
			++tcp_hndl->tmp_work.msg_len;
		}
		tcp_hndl->tmp_work.tot_iov_byte_len +=
				rxd_work->tot_iov_byte_len;

		++batch_count;
		++tmp_count;

		if (batch_count != batch_nr && next_rxd_work &&
		    (next_rxd_work->msg.msg_iovlen + tcp_hndl->tmp_work.msg_len)
		    < IOV_MAX) {
			task = next_task;
			continue;
		}

		tcp_hndl->tmp_work.msg.msg_iov = tcp_hndl->tmp_work.msg_iov;
		tcp_hndl->tmp_work.msg.msg_iovlen = tcp_hndl->tmp_work.msg_len;

		bytes_recv = tcp_hndl->tmp_work.tot_iov_byte_len;
		recvmsg_retval = xio_tcp_recvmsg_work(tcp_hndl,
						      tcp_hndl->sock.dfd,
						      &tcp_hndl->tmp_work, 0);
		bytes_recv -= tcp_hndl->tmp_work.tot_iov_byte_len;

		task = list_first_entry(&tcp_hndl->rx_list,
					struct xio_task,  tasks_list_entry);
		iov_len = tcp_hndl->tmp_work.msg_len -
				tcp_hndl->tmp_work.msg.msg_iovlen;
		for (i = 0; i < (unsigned int)tmp_count; i++) {
			tcp_task = (struct xio_tcp_task *)task->dd_data;
			rxd_work = xio_tcp_get_data_rxd(task);

			if (rxd_work->msg.msg_iovlen > iov_len)
				break;

			iov_len -= rxd_work->msg.msg_iovlen;
			bytes_recv -= rxd_work->tot_iov_byte_len;

			task = list_first_entry(&task->tasks_list_entry,
						struct xio_task,
						tasks_list_entry);
		}
		if (tcp_hndl->tmp_work.msg.msg_iovlen) {
			tcp_task = (struct xio_tcp_task *)task->dd_data;
			rxd_work = xio_tcp_get_data_rxd(task);
			rxd_work->msg.msg_iov = &rxd_work->msg.msg_iov[iov_len];
			rxd_work->msg.msg_iov[0].iov_base =
				tcp_hndl->tmp_work.msg.msg_iov[0].iov_base;
			rxd_work->msg.msg_iov[0].iov_len =
				tcp_hndl->tmp_work.msg.msg_iov[0].iov_len;
			rxd_work->msg.msg_iovlen -= iov_len;
			rxd_work->tot_iov_byte_len -= bytes_recv;
		}

		tcp_hndl->tmp_work.msg_len = 0;
		tcp_hndl->tmp_work.tot_iov_byte_len = 0;

                /* look for the maximum last in rxq index */
                tmp_count = 0;
                last_in_rxq = 0;
                list_for_each_entry(task, &tcp_hndl->rx_list, tasks_list_entry) {
                        if (IS_APPLICATION_MSG(task->tlv_type))
                                last_in_rxq = (int)tmp_count;
                        if (++tmp_count == (int)i)
                                break;
                }
                tmp_count = 0;

		task = list_first_entry(&tcp_hndl->rx_list, struct xio_task,
					tasks_list_entry);
		while (i--) {
                        task->last_in_rxq = (ret_count == (int)last_in_rxq);
			++ret_count;
			tcp_task = (struct xio_tcp_task *)task->dd_data;
			if (IS_REQUEST(task->tlv_type)) {
				retval =
					xio_tcp_on_recv_req_data(tcp_hndl,
							task);
			} else if (IS_RESPONSE(task->tlv_type)) {
				if (!xio_transport_is_task_routable(task->sender_task)) {
					ERROR_LOG("invalid sender task. Releasing incoming response. tcp_hndl:%p\n", tcp_hndl);
					xio_tasks_pool_put(task);
					retval = 0;
				} else {
					retval =
						xio_tcp_on_recv_rsp_data(tcp_hndl,
								task);
				}
			} else {
				ERROR_LOG("unknown message type:0x%x\n",
						task->tlv_type);
			}
			if (retval < 0)
				return retval;

			task = list_first_entry(&tcp_hndl->rx_list,
						struct xio_task,
						tasks_list_entry);
		}

		if (recvmsg_retval == 0) {
			DEBUG_LOG("tcp transport got EOF, tcp_hndl=%p\n",
				  tcp_hndl);
			if (tcp_task->out_tcp_op == XIO_TCP_READ) { /*TODO needed?*/
				xio_free_rdma_read_mem(tcp_hndl, task);
			}
			xio_tcp_disconnect_helper(tcp_hndl);
			return -1;
		} else if (recvmsg_retval < 0) {
			if (xio_errno() != XIO_EAGAIN) {
				ERROR_LOG("recvmsg failed:%d. Disconnecting\n", xio_errno());
				xio_tcp_disconnect_helper(tcp_hndl);
			}
			break;
		}

		task = list_first_entry_or_null(&tcp_hndl->rx_list,
						struct xio_task,
						tasks_list_entry);
	}

	if (tcp_hndl->tx_ready_tasks_num) {
		retval = xio_tcp_xmit(tcp_hndl);
		if (retval < 0) {
			if (xio_errno() != XIO_EAGAIN) {
				ERROR_LOG("xio_tcp_xmit failed:%d. Disconnecting\n", xio_errno());
				xio_tcp_disconnect_helper(tcp_hndl);
				return -1;
			}
			return ret_count;
		}
	}

	return ret_count;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_rx_ctl_handler						     */
/*---------------------------------------------------------------------------*/
int xio_tcp_rx_ctl_handler(struct xio_tcp_transport *tcp_hndl, int batch_nr)
{
	int retval = 0;
	struct xio_tcp_task *tcp_task;
	struct xio_task *task, *task_next;
	int exit;
	int count;

	tcp_hndl->io_waiting_tasks = 0;

	task = list_first_entry_or_null(&tcp_hndl->rx_list,
					struct xio_task,
					tasks_list_entry);

	count = 0;
	exit = 0;
	while (task && (&task->tasks_list_entry != &tcp_hndl->rx_list) &&
	       (count < batch_nr) && !exit) {
		tcp_task = (struct xio_tcp_task *)task->dd_data;

		switch (tcp_task->rxd.stage) {
		case XIO_TCP_RX_START:
			/* ORK todo find a better place to rearm rx_list?*/
			if (tcp_hndl->state ==
					XIO_TRANSPORT_STATE_CONNECTED ||
			    tcp_hndl->state ==
					XIO_TRANSPORT_STATE_DISCONNECTED) {
				task_next =
					xio_tcp_primary_task_alloc(tcp_hndl);
				if (!task_next) {
					if (tcp_hndl->primary_pool_cls.pool)
						ERROR_LOG(
							"primary task pool is empty\n");
					exit = 1;
					continue;
				} else {
					list_add_tail(
						&task_next->tasks_list_entry,
						&tcp_hndl->rx_list);
				}
			}
			tcp_task->rxd.tot_iov_byte_len = sizeof(struct xio_tlv);
			tcp_task->rxd.msg.msg_iov = tcp_task->rxd.msg_iov;
			tcp_task->rxd.msg.msg_iovlen = 1;
			tcp_task->rxd.stage = XIO_TCP_RX_TLV;
			/*fallthrough*/
		case XIO_TCP_RX_TLV:
			retval = tcp_hndl->sock.ops->rx_ctl_work(
					tcp_hndl,
					tcp_hndl->sock.cfd,
					&tcp_task->rxd, 0);
			if (retval == 0) {
				DEBUG_LOG("tcp transport got EOF,tcp_hndl=%p\n",
					  tcp_hndl);
				if (count) {
					exit = 1;
					break;
				}
				xio_tcp_disconnect_helper(tcp_hndl);
				return -1;
			} else if (retval < 0) {
				if (xio_errno() != XIO_EAGAIN) {
					xio_tcp_disconnect_helper(tcp_hndl);
					return -1;
				}
				exit = 1;
				break;
			}
			retval = xio_mbuf_read_first_tlv(&task->mbuf);
			if (retval < 0) {
                                exit = 1;
                                break;
                        }
			tcp_task->rxd.msg.msg_iov[0].iov_base =
					tcp_task->rxd.msg_iov[1].iov_base;
			tcp_task->rxd.msg.msg_iov[0].iov_len =
					(size_t)task->mbuf.tlv.len;
			tcp_task->rxd.msg.msg_iovlen = 1;
			tcp_task->rxd.tot_iov_byte_len = task->mbuf.tlv.len;
			tcp_task->rxd.stage = XIO_TCP_RX_HEADER;
			/*fallthrough*/
		case XIO_TCP_RX_HEADER:
			retval = tcp_hndl->sock.ops->rx_ctl_work(
					tcp_hndl,
					tcp_hndl->sock.cfd,
					&tcp_task->rxd, 0);
			if (retval == 0) {
				DEBUG_LOG("tcp transport got EOF,tcp_hndl=%p\n",
					  tcp_hndl);
				if (count) {
					exit = 1;
					break;
				}
				xio_tcp_disconnect_helper(tcp_hndl);
				return -1;
			} else if (retval < 0) {
				if (xio_errno() != XIO_EAGAIN) {
					xio_tcp_disconnect_helper(tcp_hndl);
					return -1;
				}
				exit = 1;
				break;
			}
			task->tlv_type = xio_mbuf_tlv_type(&task->mbuf);
			/* call recv completion  */
			switch (task->tlv_type) {
			case XIO_NEXUS_SETUP_REQ:
			case XIO_NEXUS_SETUP_RSP:
				xio_tcp_on_setup_msg(tcp_hndl, task);
				return 1;
			default:
				if (IS_REQUEST(task->tlv_type))
					retval =
					xio_tcp_on_recv_req_header(tcp_hndl,
								   task);
				else if (IS_RESPONSE(task->tlv_type))
					retval =
					xio_tcp_on_recv_rsp_header(tcp_hndl,
								   task);
				else
					ERROR_LOG("unknown message type:0x%x\n",
						  task->tlv_type);
				if (unlikely(retval < 0)) {
					return retval;
				}
			}
			tcp_task->rxd.stage = XIO_TCP_RX_IO_DATA;
			/*fallthrough*/
		case XIO_TCP_RX_IO_DATA:
			++count;
			break;
		default:
			ERROR_LOG("unknown stage type:%d\n",
				  tcp_task->rxd.stage);
			break;
		}

		if (tcp_task->rxd.tot_iov_byte_len)
			tcp_hndl->io_waiting_tasks++;

		task = list_first_entry(&task->tasks_list_entry,
					struct xio_task,  tasks_list_entry);
	}

	if (count == 0)
		return 0;

	retval = tcp_hndl->sock.ops->rx_data_handler(tcp_hndl, batch_nr);
	if (unlikely(retval < 0))
		return retval;
	count = retval;

	if (tcp_hndl->tx_ready_tasks_num) {
		retval = xio_tcp_xmit(tcp_hndl);
		if (retval < 0) {
			if (xio_errno() != XIO_EAGAIN) {
				ERROR_LOG("xio_tcp_xmit failed\n");
				return -1;
			}
			return count;
		}
	}

	return count;
}

/*---------------------------------------------------------------------------*/
/* xio_tcp_poll								     */
/*---------------------------------------------------------------------------*/
int xio_tcp_poll(struct xio_transport_base *transport,
		 long min_nr, long max_nr,
		 struct timespec *ts_timeout)
{
	struct xio_tcp_transport	*tcp_hndl;
	int				nr_comp = 0, recv_counter;
	cycles_t			timeout = -1;
	cycles_t			start_time = get_cycles();

	if (min_nr > max_nr)
		return -1;

	if (ts_timeout)
		timeout = (cycles_t)(timespec_to_usecs(ts_timeout) * g_mhz);

	tcp_hndl = (struct xio_tcp_transport *)transport;

	if (tcp_hndl->state != XIO_TRANSPORT_STATE_CONNECTED) {
		ERROR_LOG("tcp transport is not connected, state=%d\n",
			  tcp_hndl->state);
		return -1;
	}

	while (1) {
		/* ORK todo blocking recv with timeout?*/
		recv_counter = tcp_hndl->sock.ops->rx_ctl_handler(tcp_hndl);
		if (recv_counter < 0 && xio_errno() != XIO_EAGAIN)
			break;

		nr_comp += recv_counter;
		max_nr -= recv_counter;
		if (nr_comp >= min_nr || max_nr <= 0)
			break;
		if ((get_cycles() - start_time) >= timeout)
			break;
	}

	return nr_comp;
}

