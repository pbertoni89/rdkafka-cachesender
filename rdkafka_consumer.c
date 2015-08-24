#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>

#include "rdkafka.h"  /* for Kafka driver */

static int run = 1;
static rd_kafka_t *rk;

static void stop (int sig) {
	run = 0;
}

struct timeval tstart, tlast;
long totreceived = 0;

inline long compute_delta(struct timeval* t2, struct timeval* t1) {
    return ((long)(t2->tv_sec - t1->tv_sec)) * 1000000 +
           (t2->tv_usec - t1->tv_usec);
}

static void msg_consume (rd_kafka_message_t *rkmessage,
			 void *opaque) {
	if (rkmessage->err) {
		if (rkmessage->err == RD_KAFKA_RESP_ERR__PARTITION_EOF) {
			fprintf(stderr,
				"%% Consumer reached end of %s [%"PRId32"] "
			       "message queue at offset %"PRId64"\n",
			       rd_kafka_topic_name(rkmessage->rkt),
			       rkmessage->partition, rkmessage->offset);

			return;
		}

		fprintf(stderr, "%% Consume error for topic \"%s\" [%"PRId32"] "
		       "offset %"PRId64": %s\n",
		       rd_kafka_topic_name(rkmessage->rkt),
		       rkmessage->partition,
		       rkmessage->offset,
		       rd_kafka_message_errstr(rkmessage));
		return;
	}

	totreceived += rkmessage->len;
        struct timeval t2;
	gettimeofday(&t2, NULL);
#define LOG_EVERY_USEC 1000000
	if(compute_delta(&t2, &tlast) > LOG_EVERY_USEC) {
		fprintf(stdout, "received = %ld, throughput (b/s) = %ld\n",
			totreceived,
			8 * totreceived * 1000000L / compute_delta(&t2, &tstart));
		fflush(stdout);
		tlast = t2;
	}

/*
	fprintf(stdout, "%% Message (offset %"PRId64", %zd bytes):\n",
		rkmessage->offset, rkmessage->len);

	printf("Key: %.*s\n",
		(int)rkmessage->key_len, (char *)rkmessage->key);
	printf("%.*s\n",
		(int)rkmessage->len, (char *)rkmessage->payload);
*/
}

static void sig_usr1 (int sig) {
	rd_kafka_dump(stdout, rk);
}

int main (int argc, char **argv) {
	rd_kafka_topic_t *rkt;
	char *brokers = "localhost:9092";
	char *topic = NULL;
	int partition = RD_KAFKA_PARTITION_UA;
	int opt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	int64_t start_offset = 0;

	/* Kafka + topic configuration */
	conf = rd_kafka_conf_new();
	topic_conf = rd_kafka_topic_conf_new();

	while ((opt = getopt(argc, argv, "t:p:b:")) != -1) {
		switch (opt) {
		case 't':
			topic = optarg;
			break;
		case 'p':
			partition = atoi(optarg);
			break;
		case 'b':
			brokers = optarg;
			break;
		default:
			fprintf(stderr, "Invalid parameter\n");
			exit(-1);
		}
	}

	signal(SIGINT, stop);
	signal(SIGUSR1, sig_usr1);

	/* Create Kafka handle */
	if (!(rk = rd_kafka_new(RD_KAFKA_CONSUMER, conf,
				errstr, sizeof(errstr)))) {
		fprintf(stderr,
			"%% Failed to create new consumer: %s\n",
			errstr);
		exit(1);
	}

	/* Add brokers */
	if (rd_kafka_brokers_add(rk, brokers) == 0) {
		fprintf(stderr, "%% No valid brokers specified\n");
		exit(1);
	}

	/* Create topic */
	rkt = rd_kafka_topic_new(rk, topic, topic_conf);

	/* Start consuming */
	if (rd_kafka_consume_start(rkt, partition, start_offset) == -1){
		fprintf(stderr, "%% Failed to start consuming: %s\n",
			rd_kafka_err2str(rd_kafka_errno2err(errno)));
		exit(1);
	}

        gettimeofday(&tstart, NULL);
	tlast = tstart;

	while (run) {
		rd_kafka_message_t *rkmessage;

		/* Consume single message.
		 * See rdkafka_performance.c for high speed
		 * consuming of messages. */
		rkmessage = rd_kafka_consume(rkt, partition, 1000);
		if (!rkmessage) /* timeout */
			continue;

		msg_consume(rkmessage, NULL);

		/* Return message to rdkafka */
		rd_kafka_message_destroy(rkmessage);
	}

	/* Stop consuming */
	rd_kafka_consume_stop(rkt, partition);

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy handle */
	rd_kafka_destroy(rk);

	/* Let background threads clean up and terminate cleanly. */
	rd_kafka_wait_destroyed(2000);

	return 0;
}
