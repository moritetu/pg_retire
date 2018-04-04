/*-------------------------------------------------------------------------
 *
 * pg_retire.c
 *		Terminate current backend process when client socket is broken.
 *
 * While backend process is running commands, client process may be down. But
 * backend process cannot normally notice client down until backend reads the next
 * command. It may cause a problem that client cannot retry transaction immediately.
 * When pg_retire detects client down while running command, cancels transaction,
 * and then the backend exits.
 *
 * IDENTIFICATION
 *	  contrib/pg_retire/pg_retire.c
 *
 *-------------------------------------------------------------------------
 */

#include "postgres.h"

#include <limits.h>
#include <unistd.h>

#include "miscadmin.h"
#include "utils/guc.h"
#include "libpq/auth.h"
#include "libpq/libpq.h"
#include "libpq/pqsignal.h"
#include "libpq/pqformat.h"
#include "utils/timeout.h"
#include "utils/timestamp.h"
#include "parser/analyze.h"
#include "storage/ipc.h"
#include "access/parallel.h"


PG_MODULE_MAGIC;


/* Could not register a timeout handler */
#define TIMEOUT_INVALID()		(MyTimeoutId == MAX_TIMEOUTS)
#define MILLISECONDS(sec)		(sec * 1000)

/* Size that dummy packet can be stored */
#define WBUFSIZE	128

/*
 * Do not schedule alarm in the interrupt pending.
 */
#define RETURN_IF_INTERRUPT_PENDING	\
do { \
	if (!ParallelMessagePending && InterruptPending) \
		return; \
} while (0);

/*
 * Data container for ParameterStatus message.
 */
typedef struct CharBuffer
{
	char 	buf[WBUFSIZE];	/* Fixed buffer size */
	int 	pos;			/* Next writing position in buf */
} CharBuffer;

/*----- GUC variables -----*/

/* If true, pg_retire is enabled */
static bool pg_retire_enable;
/* Interval seconds to do sanity check of client */
static int pg_retire_interval;	/* seconds */

/*---- Local variables ----*/

/* Saved hook values in case of unload */
static ClientAuthentication_hook_type prev_ClientAuthentication = NULL;
static post_parse_analyze_hook_type prev_post_parse_analyze = NULL;

/*
 * TimeoutId used by pg_retire. TimeoutId never exceeds MAX_TIMEOUTS.
 * If TimeoutId equals to MAX_TIMEOUTS, it means to be invalid.
 */
static TimeoutId MyTimeoutId = MAX_TIMEOUTS;

/*----- Function declarations -----*/
void _PG_init(void);
void _PG_fini(void);

static void pg_retire_ClientAuthentication(Port *port, int status);
static void pg_retire_post_parse_analyze(ParseState *pstate, Query *query);
static void pg_retire_alarm_handler(void);
static bool maybeScheduleAlarm(void);
static bool doSanityCheck(void);
static void cancelTransaction(void);
static int send_dummy_message_to_frontend(void);
static int write_cbuf(CharBuffer *pb, void *buf, size_t len);
static int flush_cbuf(CharBuffer *pb, Port *port);

/*
 * pg_retire_ClientAuthentication: ClientAuthentication_hook
 *
 * If client authentication done successfully, register pg_retire timeout handler.
 */
static void
pg_retire_ClientAuthentication(Port *port, int status)
{
	/*
	 * If a hook exists, just call it.
	 */
	if (prev_ClientAuthentication)
		prev_ClientAuthentication(port, status);

	/*
	 * OK, client authentication has completed successfully.
	 * Now, we enable a timer in order to watch the client.
	 */
	if (status == STATUS_OK)
	{
		/*
		 * Register my timeout handler. If cannot register the timeout
		 * handler, current process will exit at error level 'FATAL'.
		 * For more detail, see 'utils/timeout.c'.
		 */
		if (TIMEOUT_INVALID())
		{
			MyTimeoutId = RegisterTimeout(USER_TIMEOUT, pg_retire_alarm_handler);

			ereport(DEBUG3,
					(errmsg("registered pg_retire timer: id %d", MyTimeoutId)));
		}
	}

}

/*
 * pg_retire_post_parse_analyze: post_parse_analyze_hook
 *		Enable a timer for sanity check.
 *
 * May be appropriate to enable a timer after reading command.
 */
static void
pg_retire_post_parse_analyze(ParseState *pstate, Query *query)
{
	if (prev_post_parse_analyze)
		prev_post_parse_analyze(pstate, query);

	if (TIMEOUT_INVALID() || !pg_retire_enable)
		return;

	RETURN_IF_INTERRUPT_PENDING;

	/*
	 * Skip utility statement.
	 */
	if (query->commandType == CMD_UTILITY)
		return;

	if (maybeScheduleAlarm())
		ereport(DEBUG3,
				(errmsg("scheduled pg_retire alarm after %d seconds again",
						pg_retire_interval)));
}

/*
 * pg_retire_alarm_handler
 *		Called from SIGALRM signal handler.
 *
 * In the alarm handler, do sanity check of the client and cancel current
 * transaction if the client is down.
 */
static void
pg_retire_alarm_handler(void)
{
	int save_errno = errno;

	/*
	 * If query has been already canceled or the backend is terminating,
	 * do not do sanity check.
	 */
	RETURN_IF_INTERRUPT_PENDING;

	PG_SETMASK(&BlockSig);

	if (doSanityCheck())
	{
		/*
		 * If current transaction is still running, reschedule alarm.
		 * Because sanity check may be needed more than one time.
		 */
		if (maybeScheduleAlarm())
			ereport(DEBUG3,
					(errmsg("rescheduled pg_retire alarm after %d seconds again",
							pg_retire_interval)));
	}
	else
	{
		/*
		 * Failed to write dummy parameter status. The client may be down,
		 * so cancel current transaction here.
		 * In the sanity check, InterruptPending and ClientConnectionLost flags
		 * may be already set. But we send a signal considering the case where
		 * the backend is waiting for process latch. When the backend receives
		 * SIGINT, it will call StatementCancelHandler.
		 */
		cancelTransaction();
	}

	PG_SETMASK(&UnBlockSig);

	errno = save_errno;
}

/*
 * maybeScheduleAlarm
 *		Schedule alarm if necessary.
 *
 * If the registered time had been fired, reschedule alarm. Normally,
 * after backend send the result of a query to client, it exits. But
 * client that disconnects logically, like which has a feature of
 * connection pooling, keeps current connection. Therefore, we must
 * compare current time with firing time before reschedule alarm.
 */
static bool
maybeScheduleAlarm(void)
{
	bool timer_fired;
	TimestampTz now;
	TimestampTz fin_time;

	/*
	 * Timeout disabled.
	 */
	if (TIMEOUT_INVALID())
		return false;

	timer_fired = get_timeout_indicator(MyTimeoutId, false);

	/*
	 * Reschedule if previous timer had been fired.
	 */
	if (timer_fired)
	{
		enable_timeout_after(MyTimeoutId, MILLISECONDS(pg_retire_interval));
		return true;
	}

	now = GetCurrentTimestamp();
	fin_time = get_timeout_finish_time(MyTimeoutId);

	/*
	 * Alarm may be already scheduled.
	 */
	if (fin_time != 0 && now < fin_time)
		return false;

	enable_timeout_after(MyTimeoutId, MILLISECONDS(pg_retire_interval));

	return true;
}

/*
 * pgrt_doSanityCheck
 *		Do sanity check of the client.
 *
 * Check if the client is alive by writing a dummy parameter to the
 * accepted socket descriptor. If the client has been already down,
 * write will fail. We may not be able to notice in the first write,
 * because system does not deny to write the half-closed socket.
 * In such a case, we will notice client down in the second write.
 */
static bool
doSanityCheck(void)
{
	int status;

	/*
	 * Send a dummy parameter status report to the client.
	 */
	status = send_dummy_message_to_frontend();

	if (status != 0)
		return false;

	return true;
}

/*
 * cancelTransaction
 *		Cancel current transaction.
 *
 * Send a SIGINT to itself.
 */
static void
cancelTransaction(void)
{
	int sig = SIGINT;

#ifdef HAVE_SETSID
	/* Try to signal whole process group */
	kill(-MyProcPid, sig);
#endif
	kill(MyProcPid, sig);
}

/*
 * send_dummy_message_to_frontend
 *		Send dummy parameter status to client.
 *
 * To check whether the client is still alive, send a unreserved dummy parameter
 * to the client. Normally, client receives it and ignores.
 */
static int
send_dummy_message_to_frontend(void)
{
	/*
	 * Dummy parameter name and value that pg_retire sends to client at the
	 * time of client check.
	 */
#define PGRETIRE_DUMMY_PAREMETER_NAME	"pg_retire_dummy_name"
#define PGRETIRE_DUMMY_PAREMETER_VALUE	"pg_retire_dummy_value"
#define PGRETIRE_KEEP_ALIVE_MESSAGE		"keep alive checking from pg_retire"

	CharBuffer cb;
	int r;
	int plen;

	cb.pos = 0;

	if (PG_PROTOCOL_MAJOR(MyProcPort->proto) >= 3)
	{
		/*
		 * Protocol version 3 or later supports ParameterStatus message.
		 * It starts with 'S', for more detail, see the latest public document.
		 */
		write_cbuf(&cb, "S", 1);
		plen = sizeof(PGRETIRE_DUMMY_PAREMETER_NAME) + sizeof(PGRETIRE_DUMMY_PAREMETER_VALUE) + sizeof(plen);
		plen = htonl(plen);
		write_cbuf(&cb, &plen, sizeof(plen));
		write_cbuf(&cb, PGRETIRE_DUMMY_PAREMETER_NAME, sizeof(PGRETIRE_DUMMY_PAREMETER_NAME));
		write_cbuf(&cb, PGRETIRE_DUMMY_PAREMETER_VALUE, sizeof(PGRETIRE_DUMMY_PAREMETER_VALUE));
	}
	else
	{
		/*
		 * Send a message with V2 protocol.
		 * See the following link about old protocol.
		 * http://dorn.org/docs/postgres/postgres/protocol21288.htm
		 */
		write_cbuf(&cb, "N", 1);
		write_cbuf(&cb, PGRETIRE_KEEP_ALIVE_MESSAGE, sizeof(PGRETIRE_KEEP_ALIVE_MESSAGE));
	}

	r = flush_cbuf(&cb, MyProcPort);

	return r;
}

/*
 * write_cbuf
 */
static int
write_cbuf(CharBuffer *cb, void *buf, size_t len)
{
	char *src = buf;
	char *dst = cb->buf + cb->pos;
	int i = 0;
	if (cb->pos + len <= WBUFSIZE)
	{
		while (i++ < len)
			*dst++ = *src++;

		cb->pos += len;
		return len;
	}
	return -1;
}

/*
 * flush_cbuf
 */
static int
flush_cbuf(CharBuffer *cb, Port *port)
{
	int wlen;
	int offset;
	int r;

	if (cb->pos == 0)
		return 0;

	wlen = cb->pos;
	offset = 0;

	for (;;)
	{
		/*
		 * Flush buffer here so that backend can decide whether it
		 * should terminate itself as soon as possible. This message is a bit,
		 * so an extra flush won't hurt much, probably...
		 */

#ifdef USE_SSL
		if (port->ssl_in_use)
		{
			ereport(DEBUG3,
					(errmsg("pg_retire does not support use in ssl")));
		}
		else
#endif
		{
			r = write(port->sock, cb->buf + offset, wlen);
		}

		if (r > 0)
		{
			wlen -= r;

			/* Write completed */
			if (wlen == 0)
				return 0;
			else if (wlen < 0)
			{
				ereport(WARNING,
						(errmsg("pg_retire failed to write keep alive packet")));
				return -1;
			}
			else
			{
				/* Write remained data */
				offset += r;
				continue;
			}
		}

		if (errno == EINTR)
			continue;

		if (errno == EAGAIN ||
			errno == EWOULDBLOCK)
		{
			return 0;
		}

		/*
		 * There was something wrong.
		 */
		return -1;
	}
}


/*
 * Module initialization function
 */
void
_PG_init(void)
{
	/*
	 * In order to use ClientAuthentication_hook, this library have to be loaded
	 * via shared_preload_libraries.
	 */
	if (!process_shared_preload_libraries_in_progress)
		return;

	/*
	 * Define (or redefine) custom GUC variables.
	 */
	DefineCustomBoolVariable("pg_retire.enable",
							"Enable monitoring a client with pg_retire.",
							NULL,
							&pg_retire_enable,
							false,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	DefineCustomIntVariable("pg_retire.interval",
							"Interval seconds to do sanity check of client.",
							NULL,
							&pg_retire_interval,
							10,		/* seconds */
							0,
							INT_MAX,
							PGC_USERSET,
							0,
							NULL,
							NULL,
							NULL);

	/*
	 * Install hooks.
	 */
	prev_ClientAuthentication = ClientAuthentication_hook;
	ClientAuthentication_hook = pg_retire_ClientAuthentication;
	prev_post_parse_analyze = post_parse_analyze_hook;
	post_parse_analyze_hook = pg_retire_post_parse_analyze;
}

/*
 * Module unload callback
 */
void
_PG_fini(void)
{
	/* Uninstall hooks. */
	ClientAuthentication_hook = prev_ClientAuthentication;
	post_parse_analyze_hook = prev_post_parse_analyze;
}
