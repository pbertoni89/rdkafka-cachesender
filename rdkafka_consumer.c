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

static void sig_usr1 (int sig) {
	rd_kafka_dump(stdout, rk);
}

int main (int argc, char **argv) {
	rd_kafka_topic_t *rkt;
	char *brokers = "10.20.10.141:9092";
	char *topic = "wc";
	int partition = 0;
	int m_words_sent = 0;
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

// -------------------------------------------------.

		if (rkmessage->err)
		{
			printf("AAA");
			exit(0);
		}

		totreceived += rkmessage->len;
		struct timeval t2;
		gettimeofday(&t2, NULL);
		#define LOG_EVERY_USEC 1000000
		if(compute_delta(&t2, &tlast) > LOG_EVERY_USEC) {
			fprintf(stdout, "received = %ld B, %ld wps, throughput (b/s) = %ld\n",
					totreceived,
					m_words_sent * 1000000L / compute_delta(&t2, &tstart),
					8 * totreceived * 1000000L / compute_delta(&t2, &tstart));
			fflush(stdout);
			tlast = t2;
		}

		int payload_len = (int) rkmessage->len;
		//std::cout << WORDSKAFKA_NAME << "Message payload " << payload_len << "B, offset " << m_start_offset << std::endl;
		char * payload = (char*) rkmessage->payload;

		// we have retc + offset byte in the storage, process it
		int whitespace;
		char * p_word = NULL;

		for(whitespace = 0; whitespace < payload_len; whitespace ++)
		{
			if(isspace(payload[whitespace]))
			{
				payload[whitespace] = 0;
				if(p_word != NULL)
				{
					//std::string w(p_word);

					//BOTTLENECK std::remove_copy_if(w.begin(), w.end(), std::back_inserter(w), std::ptr_fun<int, int>(&std::ispunct));
					//auto remove_punct = [](char c) { return std::ispunct(static_cast<unsigned char>(c)); };
					//w.erase(std::remove_if(w.begin(), w.end(), remove_punct), w.end());

/*
					std::shared_ptr<std::map<std::string, uint32_t>> map = m_words_map->map();
					std::map<std::string, uint32_t>::iterator it = map->find(w);

					if(it != map->end())
						it->second ++;
					else
						map->insert(std::pair<std::string, uint32_t> (w, 1));

					if((m_words_sent % m_send_limit) == 0)
					{
						std::cout << "Sending map to aggregator\n";
						auto map_copy = m_words_map;
						if(map->find("xxxxx11111") == map->end())
						{
							map->insert(std::pair<std::string, uint32_t>("xxxxx11111", atoi(m_node_id.c_str())));
						}
						send_out_through(std::move(map_copy), m_gate_id);
						//m_map_sent ++;
					}
*/
					m_words_sent ++;
				}
				p_word = NULL;
			}
			else
				if(p_word == NULL)
					p_word = payload + whitespace;

// -------------------------------------------------.
		}
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
