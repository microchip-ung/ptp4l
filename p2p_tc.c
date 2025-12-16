/**
 * @file p2p_tc.c
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

static int p2p_delay_request(struct port *p)
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
	case PS_PASSIVE_SLAVE:
	case PS_GRAND_MASTER:
		break;
	}
	return port_delay_request(p);
}

void p2p_dispatch(struct port *p, enum fsm_event event, int mdiff)
{
	if (!port_state_update(p, event, mdiff)) {
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
		break;
	case PS_PASSIVE:
		port_set_announce_tmo(p);
		break;
	case PS_UNCALIBRATED:
	case PS_SLAVE:
		port_set_announce_tmo(p);
		break;
	case PS_PASSIVE_SLAVE:
		break;
	};
}

enum fsm_event p2p_event(struct port *p, int fd_index)
{
	int cnt, fd = p->fda.fd[fd_index];
	enum fsm_event event = EV_NONE;
	struct ptp_message *msg, *dup;
	struct port *red_master;

	switch (fd_index) {
	case FD_ANNOUNCE_TIMER:
	case FD_SYNC_RX_TIMER:
		pr_debug("%s: %s timeout", p->log_name,
			 fd_index == FD_SYNC_RX_TIMER ? "rx sync" : "announce");
		if (p->best) {
			fc_clear(p->best);
		}
		port_set_announce_tmo(p);
		return EV_ANNOUNCE_RECEIPT_TIMEOUT_EXPIRES;

	case FD_DELAY_TIMER:
		pr_debug("%s: delay timeout", p->log_name);
		port_set_delay_tmo(p);
		tc_prune(p);
		return p2p_delay_request(p) ? EV_FAULT_DETECTED : EV_NONE;

	case FD_QUALIFICATION_TIMER:
		pr_debug("%s: qualification timeout", p->log_name);
		return EV_QUALIFICATION_TIMEOUT_EXPIRES;

	case FD_MANNO_TIMER:
	case FD_SYNC_TX_TIMER:
	case FD_UNICAST_REQ_TIMER:
	case FD_UNICAST_SRV_TIMER:
		pr_err("unexpected timer expiration");
		return EV_NONE;

	case FD_RTNL:
		pr_debug("%s: received link status notification", p->log_name);
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

	/* In redundancy, slave ports do not receive msgs from
	 * network directly. Only red master does.
	 */
	if (red_hsr_slave_port(p)) {
		pr_warning("Error: red slave %s receives msgs: fd_index=%d",
			   p->name, fd_index);
		return EV_NONE;
	}

	msg = msg_allocate();
	if (!msg) {
		return EV_FAULT_DETECTED;
	}
	msg->hwts.type = p->timestamping;

	if (red_hsr_port(p))
		cnt = transport_red_recv(p->trp, fd, msg);
	else
		cnt = transport_recv(p->trp, fd, msg);

	if (cnt <= 0) {
		pr_err("%s: recv message failed", p->log_name);
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

	if (red_hsr_port(p)) {
		/* Save the red master port that receives the msg */
		red_master = p;

		pr_debug("port %s: received %s msg",
			 port_name(p),
			 msg_type_string(msg_type(msg)));

		switch (msg_type(msg)) {
		case ANNOUNCE:
		case SYNC:
		case FOLLOW_UP:
		case PDELAY_REQ:
		case PDELAY_RESP:
		case PDELAY_RESP_FOLLOW_UP:
			/* The red slave port that is going to
			 * process the received msg.
			 */
			p = red_rx_msg_get_port(p, msg);
			if (!p) {
				pr_err("Msg %d: no red port found",
				       msg_type(msg));
				msg_put(msg);
				return EV_NONE;
			} else {
				pr_notice("Chosen slave port: %s", port_name(p));
			}
			/* Save the red slave port so that port_dispatch
			 * will dispatch the right port to process the
			 * event, e.g. EV_STATE_DECISION, returned after
			 * processing the received message.
			 */
			red_master->red_dispatch_port = p;
			break;
		case DELAY_REQ:
		case DELAY_RESP:
		case SIGNALING:
		case MANAGEMENT:
		default:
			break;
		}
	}

	switch (msg_type(msg)) {
	case SYNC:
		if (tc_fwd_sync(p, msg)) {
			event = EV_FAULT_DETECTED;
			break;
		}
		if (dup) {
			process_sync(p, dup);
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
		if (tc_fwd_folup(p, msg)) {
			event = EV_FAULT_DETECTED;
			break;
		}
		if (dup) {
			process_follow_up(p, dup);
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
		if (tc_forward(p, msg)) {
			event = EV_FAULT_DETECTED;
			break;
		}
		if (dup && process_announce(p, dup)) {
			event = EV_STATE_DECISION_EVENT;
		}
		break;
	case SIGNALING:
	case MANAGEMENT:
		if (tc_forward(p, msg)) {
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
