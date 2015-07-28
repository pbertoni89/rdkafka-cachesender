#include <ctype.h>
#include <signal.h>
#include <string.h>
#include <unistd.h>
#include <stdlib.h>
#include <syslog.h>
#include <sys/time.h>
#include <errno.h>
#include <assert.h>

#define DEFAULT_LINES_TO_SEND 1
#define DEFAULT_LINE_LENGTH 100
#define DEFAULT_CHUNK_SIZE 16

#include "rdkafka.h"  /* for Kafka driver */

static volatile int run = 1;
static rd_kafka_t *rk;

static void stop (int sig)
{
	run = 0;
}

static void logger (const rd_kafka_t *rk, int level, const char *fac, const char *buf)
{
	struct timeval tv;
	gettimeofday(&tv, NULL);
	fprintf(stderr, "%u.%03u RDKAFKA-%i-%s: %s: %s\n",
			(int)tv.tv_sec,
			(int)(tv.tv_usec / 1000),
			level, fac, rd_kafka_name(rk), buf);
}

static void msg_delivered (rd_kafka_t *rk,
			void *payload, size_t len,
			rd_kafka_resp_err_t error_code,
			void *opaque, void *msg_opaque)
{
	if (error_code)
		fprintf(stderr, "%% Message delivery failed: %s\n", rd_kafka_err2str(error_code));
	fprintf(stderr, "%% Message delivered (%zd bytes)\n", len);
}

static void sig_usr1 (int sig)
{
	rd_kafka_dump(stdout, rk);
}

static void errexit(char* const msg)
{
	fprintf(stderr, "%s\n", msg);
	exit(-1);
}

int main (int argc, char **argv)
{
	rd_kafka_topic_t *rkt;
	char *brokers;
	char *topic = NULL;
	int partition = RD_KAFKA_PARTITION_UA;
	int opt;
	rd_kafka_conf_t *conf;
	rd_kafka_topic_conf_t *topic_conf;
	char errstr[512];
	char *filename_string = NULL;
	char *line_length_string = NULL;
	char *lines_to_send_string = NULL;
	int lines_to_send = DEFAULT_LINES_TO_SEND;
	int line_length = DEFAULT_LINE_LENGTH;

	/* Kafka configuration + topic */
	conf = rd_kafka_conf_new();
	topic_conf = rd_kafka_topic_conf_new();

	while ((opt = getopt(argc, argv, "t:p:b:f:l:n:")) != -1)
	{
		switch (opt)
		{
			case 't':
				topic = optarg;
				break;
			case 'p':
				partition = atoi(optarg);
				break;
			case 'b':
				brokers = optarg;
				break;
			case 'f':
				filename_string = optarg;
				break;
			case 'l':
				line_length_string = optarg;
				break;
			case 'n':
				lines_to_send_string = optarg;
				break;
			default:
				errexit("Invalid parameter");
		}
	}

	if(line_length_string != NULL)
	{
		line_length = atoi(line_length_string);
		if(line_length < 1)
			errexit("Invalid line length");
	}

	if(lines_to_send_string != NULL)
	{
		lines_to_send = atoi(lines_to_send_string);
		if(lines_to_send <= 0)
			errexit("Invalid lines to send number");
	}

	if(filename_string == NULL)
		errexit("A file name should be specified");

	char *memory = (char*) malloc(DEFAULT_CHUNK_SIZE);
	if(!memory)
		errexit("Cannot allocate memory");

	int mem_size = DEFAULT_CHUNK_SIZE;
	FILE *fptr = fopen(filename_string, "rb");
	if(!fptr)
		errexit("Error opening file");

	int file_size = 0;

	while(fptr)
	{
		int retc = fread(memory + file_size, 1, DEFAULT_CHUNK_SIZE, fptr);
		file_size += retc;

		if(retc < DEFAULT_CHUNK_SIZE)
		{
			if(feof(fptr))
			{
				fclose(fptr);
				fptr = NULL;
				break;
			}
			if(ferror(fptr))
				errexit("Error reading file");

			assert(0);
		}

		mem_size += DEFAULT_CHUNK_SIZE;
		memory = (char*) realloc(memory, mem_size);
		if(!memory)
			errexit("Cannot reallocate memory");
	}

	fprintf(stdout, "File is %d bytes\n", file_size);

	// now parse the memory and divide the content in batch with
	// line_length maximum length.
	int *offset_table = (int*) malloc(DEFAULT_CHUNK_SIZE);
	if(!offset_table)
		errexit("Cannot allocate memory");

	int curr_line = 0;
	int line_mem_size = DEFAULT_CHUNK_SIZE;
	int kk;
	int last_space_offset = 0;
	int last_line_offset = 0;

	for(kk = 0; kk < file_size; kk++)
	{
		if(kk - last_line_offset >= line_length)
		{
			if(last_space_offset == last_line_offset)
				errexit("Line too long, try increasing line length");

			offset_table[curr_line] = last_space_offset + 1;
			memory[last_space_offset] = '\n';
			curr_line++;
			if(curr_line * sizeof(int) >= line_mem_size)
			{
				line_mem_size += DEFAULT_CHUNK_SIZE;
				offset_table = (int*) realloc(offset_table, line_mem_size);
				if(!offset_table)
					errexit("Cannot reallocate memory");
			}
			last_space_offset ++;
			last_line_offset = last_space_offset;
		}
		if(memory[kk] == ' ')
			last_space_offset = kk;
	}
	if(last_line_offset < file_size)
	{
		offset_table[curr_line] = file_size;
		curr_line ++;
	}

	/*
	// compute average line length
	int line_offset = 0;
	double average_line_cum = 0;
	int average_line_num = 0;
	for(kk = 0; kk < curr_line; kk++)
	{
		int tosend = offset_table[kk] - line_offset;
		average_line_cum += tosend;
		average_line_num += 1;
		line_offset = offset_table[kk];
	}
	double average_line_length = average_line_cum / average_line_num;
	*/

	signal(SIGINT, stop);
	signal(SIGUSR1, sig_usr1);

	int sendcnt = 0;

	/* Set up a message delivery report callback.
	* It will be called once for each message, either on successful delivery to broker, or upon failure to deliver to broker. */
	rd_kafka_conf_set_dr_cb(conf, msg_delivered);

	/* Create Kafka handle */
	if (!(rk = rd_kafka_new(RD_KAFKA_PRODUCER, conf, errstr, sizeof(errstr))))
	{
		fprintf(stderr, "%% Failed to create new producer: %s\n", errstr);
		exit(1);
	}

	/* Set logger */
	rd_kafka_set_logger(rk, logger);
	rd_kafka_set_log_level(rk, LOG_DEBUG);

	/* Add brokers */
	if (rd_kafka_brokers_add(rk, brokers) == 0)
		errexit("%% No valid brokers specified");

	/* Create topic */
	rkt = rd_kafka_topic_new(rk, topic, topic_conf);

	int offset_table_size = curr_line;

	while (run)
	{
		int line_offset = 0;
		int kk;
		for(kk = 0; kk < offset_table_size; kk += lines_to_send)
		{
			if(run==false)
				break;

			char *memory_to_send = memory + line_offset;
			int tosend, index;

			if(kk > offset_table_size - lines_to_send)
				index = offset_table_size;
			else
				index = kk + lines_to_send;
			tosend = offset_table[index - 1] - line_offset;

			/*
			gettimeofday(&t2, NULL);
			if(bw > 0)
			{
				int adjust = delay_set_by_user + compute_delta(&t1, &t2);
				t1 = t2;
				if(adjust > 0 || delay > 0)
					delay += adjust;
			}

			if(compute_delta(&t2, &lastlog) > LOG_EVERY_USEC)
			{
				fprintf(stdout, "throughput (b/s) = %ld\n", 8 * totbyte * 1000000 / compute_delta(&t2, &tstart));
				fflush(stdout);
				lastlog = t2;
			}
			*/

			/* Send/Produce message. */
			if (rd_kafka_produce(rkt, partition, RD_KAFKA_MSG_F_COPY,
								/* Payload and length */
								memory_to_send, tosend,
								/* Optional key and its length */
								NULL, 0,
								/* Message opaque, provided in delivery report callback as msg_opaque. */
								NULL) == -1)
			{
				fprintf(stderr, "%% Failed to produce to topic %s partition %i: %s\n", rd_kafka_topic_name(rkt), partition, rd_kafka_err2str(rd_kafka_errno2err(errno)));
				/* Poll to handle delivery reports */
				rd_kafka_poll(rk, 0);
				continue;
			}

			sleep(1);

			fprintf(stderr, "%% Sent %d bytes to topic %s partition %i\n", tosend, rd_kafka_topic_name(rkt), partition);
			sendcnt++;
			/* Poll to handle delivery reports */
			rd_kafka_poll(rk, 0);

			/*
			if(bw > 0)
				if(delay > 0)
					usleep(delay);
			*/
			line_offset = offset_table[kk + lines_to_send -1];
		}
		/*
		size_t len = strlen(buf);
		if (buf[len-1] == '\n')
			buf[--len] = '\0';
		*/
	}

	/* Poll to handle delivery reports */
	rd_kafka_poll(rk, 0);

	fprintf(stdout, "Stopped receiving. Waiting to deliver every message in the topic...\n");

	/* Wait for messages to be delivered */
	while (run && rd_kafka_outq_len(rk) > 0)
		rd_kafka_poll(rk, 100);

	/* Destroy topic */
	rd_kafka_topic_destroy(rkt);

	/* Destroy the handle */
	rd_kafka_destroy(rk);

	/* Let background threads clean up and terminate cleanly. */
	rd_kafka_wait_destroyed(2000);

	return 0;
}
