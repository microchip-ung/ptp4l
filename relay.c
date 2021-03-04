/**
 * @file relay.c
 * @note Copyright (C) 2018 Richard Cochran <richardcochran@gmail.com>
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation; either version 2 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License along
 * with this program; if not, write to the Free Software Foundation, Inc.,
 * 51 Franklin Street, Fifth Floor, Boston, MA 02110-1335 USA.
 */
#include <errno.h>

#include "port.h"
#include "port_private.h"
#include "print.h"
#include "rtnl.h"
#include "tc.h"

static int relay_forward(struct port *q, struct ptp_message *msg)
{
	uint16_t steps_removed;
	struct port *p;
	int cnt;

	if (msg_type(msg) == ANNOUNCE) {
		steps_removed = ntohs(msg->announce.stepsRemoved);
		if (steps_removed + 1 <= clock_max_steps_removed(q->clock))
			msg->announce.stepsRemoved = htons(1 + steps_removed);
	}

	for (p = clock_first_port(q->clock); p; p = LIST_NEXT(p, list)) {
		if (tc_blocked(q, p, msg)) {
			continue;
		}

		msg->header.sourcePortIdentity.clockIdentity = p->portIdentity.clockIdentity;
		msg->header.sourcePortIdentity.portNumber = htons(p->portIdentity.portNumber);
		cnt = transport_send(p->trp, &p->fda, TRANS_GENERAL, msg);
		if (cnt <= 0) {
			pr_err("tc failed to forward message on port %d",
			       portnum(p));
			port_dispatch(p, EV_FAULT_DETECTED, 0);
		}
	}
	return 0;
}

static void relay_complete_syfup(struct port *q, struct port *p,
				 struct ptp_message *msg, tmv_t residence)
{
	enum tc_match type = TC_MISMATCH;
	struct ptp_message *dup;
	struct ptp_message *fup;
	struct tc_txd *txd;
	Integer64 c1, c2;
	int cnt;

	TAILQ_FOREACH(txd, &p->tc_transmitted, list) {
		type = tc_match_syfup(portnum(q), msg, txd);
		switch (type) {
		case TC_MISMATCH:
			break;
		case TC_SYNC_FUP:
			fup = msg;
			residence = txd->residence;
			break;
		case TC_FUP_SYNC:
			fup = txd->msg;
			break;
		case TC_DELAY_REQRESP:
			pr_err("tc: unexpected match of delay request - sync!");
			return;
		}
		if (type != TC_MISMATCH) {
			break;
		}
	}

	if (type == TC_MISMATCH) {
		txd = tc_allocate();
		if (!txd) {
			port_dispatch(p, EV_FAULT_DETECTED, 0);
			return;
		}
		msg_get(msg);
		txd->msg = msg;
		txd->residence = residence;
		txd->ingress_port = portnum(q);
		TAILQ_INSERT_TAIL(&p->tc_transmitted, txd, list);
		return;
	}

	c1 = net2host64(fup->header.correction);
	c2 = c1 + tmv_to_TimeInterval(residence);
	c2 += q->asymmetry;
	fup->header.correction = host2net64(c2);

	dup = msg_allocate();
	if (!dup)
		return;

	memcpy(dup, fup, sizeof(*dup));
	dup->refcnt = 1;
	dup->header.sourcePortIdentity.clockIdentity = p->portIdentity.clockIdentity;
	dup->header.sourcePortIdentity.portNumber = htons(p->portIdentity.portNumber);
	if (p->follow_up_info)
		clock_follow_up_info_update(p->clock, dup);
	cnt = transport_send(p->trp, &p->fda, TRANS_GENERAL, dup);
	if (cnt <= 0) {
		pr_err("tc failed to forward follow up on port %d", portnum(p));
		port_dispatch(p, EV_FAULT_DETECTED, 0);
	}
	/* Restore original correction value for next egress port. */
	fup->header.correction = host2net64(c1);
	TAILQ_REMOVE(&p->tc_transmitted, txd, list);
	msg_put(txd->msg);
	msg_put(dup);
	tc_recycle(txd);
}

static void relay_complete(struct port *q, struct port *p,
			   struct ptp_message *msg, tmv_t residence)
{
	switch (msg_type(msg)) {
	case SYNC:
	case FOLLOW_UP:
		relay_complete_syfup(q, p, msg, residence);
		break;
	}
}

static int relay_fwd_folup(struct port *q, struct ptp_message *msg)
{
	struct port *p;

	clock_gettime(CLOCK_MONOTONIC, &msg->ts.host);

	for (p = clock_first_port(q->clock); p; p = LIST_NEXT(p, list)) {
		if (tc_blocked(q, p, msg)) {
			continue;
		}
		relay_complete(q, p, msg, tmv_zero());
	}
	return 0;
}

static int relay_fwd_event(struct port *q, struct ptp_message *msg)
{
	tmv_t egress, ingress = msg->hwts.ts, residence;
	struct ptp_message *dup;
	struct port *p;
	int cnt, err;
	double rr;

	clock_gettime(CLOCK_MONOTONIC, &msg->ts.host);

	dup = msg_allocate();
	if (!dup)
		return 1;

	memcpy(dup, msg, sizeof(*dup));
	dup->refcnt = 1;

	/* First send the event message out. */
	for (p = clock_first_port(q->clock); p; p = LIST_NEXT(p, list)) {
		if (tc_blocked(q, p, msg)) {
			continue;
		}
		dup->header.sourcePortIdentity.clockIdentity = p->portIdentity.clockIdentity;
		dup->header.sourcePortIdentity.portNumber = htons(p->portIdentity.portNumber);
		cnt = transport_send(p->trp, &p->fda, TRANS_DEFER_EVENT, dup);
		if (cnt <= 0) {
			pr_err("failed to forward event from port %hd to %hd",
				portnum(q), portnum(p));
			port_dispatch(p, EV_FAULT_DETECTED, 0);
		}
	}

	msg_put(dup);

	/* Go back and gather the transmit time stamps. */
	for (p = clock_first_port(q->clock); p; p = LIST_NEXT(p, list)) {
		if (tc_blocked(q, p, msg)) {
			continue;
		}
		err = transport_txts(&p->fda, msg);
		if (err || !msg_sots_valid(msg)) {
			pr_err("failed to fetch txts on port %hd to %hd event",
				portnum(q), portnum(p));
			port_dispatch(p, EV_FAULT_DETECTED, 0);
			continue;
		}
		ts_add(&msg->hwts.ts, p->tx_timestamp_offset);
		egress = msg->hwts.ts;
		residence = tmv_sub(egress, ingress);
		residence = tmv_add(residence, clock_get_path_delay(q->clock));
		rr = clock_rate_ratio(q->clock);
		if (rr != 1.0) {
			residence = dbl_tmv(tmv_dbl(residence) * rr);
		}
		relay_complete(q, p, msg, residence);
	}

	return 0;
}

static int relay_delay_request(struct port *p)
{
	switch (p->state) {
	case PS_INITIALIZING:
	case PS_FAULTY:
	case PS_DISABLED:
		return 0;
	case PS_LISTENING:
	case PS_PRE_MASTER:
	case PS_MASTER:
	case PS_PASSIVE:
	case PS_UNCALIBRATED:
	case PS_SLAVE:
	case PS_GRAND_MASTER:
		break;
	}
	return port_delay_request(p);
}

void relay_dispatch(struct port *p, enum fsm_event event, int mdiff)
{
	if (!port_state_update(p, event, mdiff)) {
		if (clock_best_local(p->clock)) {
			if (event == EV_RS_GRAND_MASTER) {
				set_tmo_log(p->fda.fd[FD_MANNO_TIMER], 1, -10); /*~1ms*/
				port_set_sync_tx_tmo(p);
			}
			return;
		}

		switch (p->state) {
		case PS_PRE_MASTER:
			port_clr_tmo(p->fda.fd[FD_ANNOUNCE_TIMER]);
			port_clr_tmo(p->fda.fd[FD_SYNC_RX_TIMER]);
			/* Leave FD_DELAY_TIMER running. */
			port_clr_tmo(p->fda.fd[FD_MANNO_TIMER]);
			port_clr_tmo(p->fda.fd[FD_SYNC_TX_TIMER]);
			break;
		case PS_MASTER:
		case PS_GRAND_MASTER:
			port_clr_tmo(p->fda.fd[FD_ANNOUNCE_TIMER]);
			port_clr_tmo(p->fda.fd[FD_SYNC_RX_TIMER]);
			/* Leave FD_DELAY_TIMER running. */
			port_clr_tmo(p->fda.fd[FD_QUALIFICATION_TIMER]);
			port_clr_tmo(p->fda.fd[FD_MANNO_TIMER]);
			port_clr_tmo(p->fda.fd[FD_SYNC_TX_TIMER]);
			break;
		default:
			;
		};

		return;
	}
	if (!portnum(p)) {
		/* UDS needs no timers. */
		return;
	}

	port_clr_tmo(p->fda.fd[FD_ANNOUNCE_TIMER]);
	port_clr_tmo(p->fda.fd[FD_SYNC_RX_TIMER]);
	/* Leave FD_DELAY_TIMER running. */
	port_clr_tmo(p->fda.fd[FD_QUALIFICATION_TIMER]);
	port_clr_tmo(p->fda.fd[FD_MANNO_TIMER]);
	port_clr_tmo(p->fda.fd[FD_SYNC_TX_TIMER]);

	/*
	 * Handle the side effects of the state transition.
	 */
	switch (p->state) {
	case PS_INITIALIZING:
		break;
	case PS_FAULTY:
	case PS_DISABLED:
		port_disable(p);
		break;
	case PS_LISTENING:
		port_set_announce_tmo(p);
		port_set_delay_tmo(p);
		break;
	case PS_PRE_MASTER:
		port_set_qualification_tmo(p);
		break;
	case PS_MASTER:
	case PS_GRAND_MASTER:
		set_tmo_log(p->fda.fd[FD_MANNO_TIMER], 1, -10); /*~1ms*/
		port_set_sync_tx_tmo(p);
		break;
	case PS_PASSIVE:
		port_set_announce_tmo(p);
		break;
	case PS_UNCALIBRATED:
	case PS_SLAVE:
		port_set_announce_tmo(p);
		break;
	};
}

static int path_trace_insert(struct port *p, struct ptp_message *m)
{
	struct path_trace_tlv *ptt;
	uint16_t length;
	uint8_t *ptr;

	ptr = m->announce.suffix;
	if (!ptr) {
		pr_err("TLV on %s not allowed", msg_type_string(msg_type(m)));
		return -1;
	}

	/* Check that the message buffer has enough room for the new TLV. */
	if ((unsigned long)(ptr + sizeof(struct ClockIdentity)) >
	    (unsigned long)(&m->tail_room)) {
		pr_debug("cannot fit TLV of length into message");
		return -1;
	}

	ptt = (struct path_trace_tlv *) ptr;
	if (ntohs(m->header.messageLength) +
	    sizeof(struct ClockIdentity) > 1500) {
		uint16_t ptt_len = ntohs(ptt->length);

		length = ntohs(m->header.messageLength);
		length -= ptt_len;
		length -= 4;
		m->header.messageLength = htons(length);

		return -1;
	}

	length = ntohs(ptt->length);
	length += sizeof(struct ClockIdentity);
	ptt->length = htons(length);
	ptt->cid[length / sizeof(struct ClockIdentity) - 1] =
		clock_identity(p->clock);

	length = ntohs(m->header.messageLength);
	length += sizeof(struct ClockIdentity);
	m->header.messageLength = htons(length);

	return 0;
}

enum fsm_event relay_event(struct port *p, int fd_index)
{
	int cnt, fd = p->fda.fd[fd_index];
	enum fsm_event event = EV_NONE;
	struct ptp_message *msg, *dup;
	int err = 0;

	switch (fd_index) {
	case FD_ANNOUNCE_TIMER:
	case FD_SYNC_RX_TIMER:
		pr_debug("port %hu: %s timeout", portnum(p),
			 fd_index == FD_SYNC_RX_TIMER ? "rx sync" : "announce");
		if (p->best) {
			fc_clear(p->best);
		}
		port_set_announce_tmo(p);
		return EV_ANNOUNCE_RECEIPT_TIMEOUT_EXPIRES;

	case FD_DELAY_TIMER:
		pr_debug("port %hu: delay timeout", portnum(p));
		port_set_delay_tmo(p);
		tc_prune(p);
		return relay_delay_request(p) ? EV_FAULT_DETECTED : EV_NONE;

	case FD_QUALIFICATION_TIMER:
		pr_debug("port %hu: qualification timeout", portnum(p));
		return EV_QUALIFICATION_TIMEOUT_EXPIRES;

	case FD_MANNO_TIMER:
		if (clock_best_local(p->clock)) {
			pr_debug("port %hu: master tx announce timeout", portnum(p));
			port_set_manno_tmo(p);
			return port_tx_announce(p, NULL, p->seqnum.announce++) ?
				EV_FAULT_DETECTED : EV_NONE;
		}
		return EV_NONE;
	case FD_SYNC_TX_TIMER:
		if (clock_best_local(p->clock)) {
			pr_debug("port %hu: master sync timeout", portnum(p));
			port_set_sync_tx_tmo(p);
			return port_tx_sync(p, NULL, p->seqnum.sync++) ?
				EV_FAULT_DETECTED : EV_NONE;
		}
		break;

	case FD_UNICAST_REQ_TIMER:
	case FD_UNICAST_SRV_TIMER:
		pr_err("unexpected timer expiration");
		return EV_NONE;

	case FD_RTNL:
		pr_debug("port %hu: received link status notification", portnum(p));
		rtnl_link_status(fd, p->name, port_link_status, p);
		if (p->link_status == (LINK_UP|LINK_STATE_CHANGED)) {
			return EV_FAULT_CLEARED;
		} else if ((p->link_status == (LINK_DOWN|LINK_STATE_CHANGED)) ||
			   (p->link_status & TS_LABEL_CHANGED)) {
			return EV_FAULT_DETECTED;
		} else {
			return EV_NONE;
		}
	}

	msg = msg_allocate();
	if (!msg) {
		return EV_FAULT_DETECTED;
	}
	msg->hwts.type = p->timestamping;

	cnt = transport_recv(p->trp, fd, msg);
	if (cnt <= 0) {
		pr_err("port %hu: recv message failed", portnum(p));
		msg_put(msg);
		return EV_FAULT_DETECTED;
	}
	if (msg_sots_valid(msg)) {
		ts_add(&msg->hwts.ts, -p->rx_timestamp_offset);
	}
	if (msg_unicast(msg)) {
		pl_warning(600, "cannot switch unicast messages!");
		msg_put(msg);
		return EV_NONE;
	}

	dup = msg_duplicate(msg, cnt);
	if (!dup) {
		msg_put(msg);
		return EV_NONE;
	}
	if (tc_ignore(p, dup)) {
		msg_put(dup);
		dup = NULL;
	}

	switch (msg_type(msg)) {
	case SYNC:
		if (dup) {
			err = process_sync(p, dup);
		}
		if (!err && relay_fwd_event(p, msg)) {
			event = EV_FAULT_DETECTED;
			break;
		}

		break;
	case DELAY_REQ:
		break;
	case PDELAY_REQ:
		if (dup && process_pdelay_req(p, dup)) {
			event = EV_FAULT_DETECTED;
		}
		break;
	case PDELAY_RESP:
		if (dup && process_pdelay_resp(p, dup)) {
			event = EV_FAULT_DETECTED;
		}
		break;
	case FOLLOW_UP:
		if (dup) {
			err = process_follow_up(p, dup);
		}
		if (!err && relay_fwd_folup(p, msg)) {
			event = EV_FAULT_DETECTED;
			break;
		}
		break;
	case DELAY_RESP:
		break;
	case PDELAY_RESP_FOLLOW_UP:
		if (dup) {
			process_pdelay_resp_fup(p, dup);
		}
		break;
	case ANNOUNCE:
		if (p->path_trace_enabled && path_trace_insert(p, msg)) {
			pr_err("port %hu: append path trace failed", portnum(p));
		}

		if (relay_forward(p, msg)) {
			event = EV_FAULT_DETECTED;
			break;
		}
		if (dup && process_announce(p, dup)) {
			event = EV_STATE_DECISION_EVENT;
		}
		break;
	case SIGNALING:
	case MANAGEMENT:
		if (relay_forward(p, msg)) {
			event = EV_FAULT_DETECTED;
		}
		break;
	}

	msg_put(msg);
	if (dup) {
		msg_put(dup);
	}
	return event;
}
