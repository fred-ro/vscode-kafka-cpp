#include <iostream>
#include <string>
#include <cstdlib>
#include <cstdio>
#include <csignal>

#include <librdkafka/rdkafkacpp.h>
#include <librdkafka/rdkafka.h>

using namespace std;

static volatile sig_atomic_t run = 1;

static void sigterm(int sig) {
  run = 0;
}

class ExampleDeliveryReportCb : public RdKafka::DeliveryReportCb {
 public:
  void dr_cb(RdKafka::Message &message) {
    /* If message.err() is non-zero the message delivery failed permanently
     * for the message. */
    if (message.err())
      std::cerr << "% Message delivery failed: " << message.errstr()
                << std::endl;
    else
      std::cerr << "% Message delivered to topic " << message.topic_name()
                << " [" << message.partition() << "] at offset "
                << message.offset() << std::endl;
  }
};

rd_kafka_resp_err_t createTopic(RdKafka::Producer *producer, string name, int partitions, int replica) {
    rd_kafka_t *rd_kafka = producer->c_ptr();
    char strerr[512];
    rd_kafka_NewTopic_t *newt[1];
    const size_t newt_cnt = 1;
    newt[0] = rd_kafka_NewTopic_new(name.c_str(), partitions, replica, strerr, sizeof(strerr));
    rd_kafka_queue_t *rkqu = rd_kafka_queue_new(rd_kafka);
    rd_kafka_CreateTopics(rd_kafka, newt, newt_cnt, NULL, rkqu);
    rd_kafka_event_t *rkev = rd_kafka_queue_poll(rkqu, 20000);
    const rd_kafka_CreateTopics_result_t *res = rd_kafka_event_CreateTopics_result(rkev);
    size_t res_cnt;
    const rd_kafka_topic_result_t **terr = rd_kafka_CreateTopics_result_topics(res, &res_cnt);
    if (res_cnt != newt_cnt) {
        cerr << "create topics return more than 1 result" << endl;
        return RD_KAFKA_RESP_ERR_UNKNOWN;
    }
    rd_kafka_resp_err_t err = rd_kafka_topic_result_error(terr[0]);
    if (rd_kafka_topic_result_error(terr[0]) == RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) {
        cerr << "Topic already exists" << endl;
    }
    return err;
}

int main()
{
    const string topic = "topic-test";
    cout << "Starting..." << endl;

    RdKafka::Conf *conf = RdKafka::Conf::create(RdKafka::Conf::CONF_GLOBAL);
    string errstr;
    if (conf->set("bootstrap.servers", "kafka:9092", errstr) != RdKafka::Conf::CONF_OK)
    {
        cerr << errstr << endl;
        exit(1);
    }
    if (conf->set("debug", "broker,topic,msg", errstr) != RdKafka::Conf::CONF_OK)
    {
        cerr << errstr << endl;
        exit(1);
    }

    signal(SIGINT, sigterm);
    signal(SIGTERM, sigterm);

    ExampleDeliveryReportCb ex_dr_cb;

    if (conf->set("dr_cb", &ex_dr_cb, errstr) != RdKafka::Conf::CONF_OK)
    {
        std::cerr << errstr << std::endl;
        exit(1);
    }

    // Create producer instance.
    cout << "creating producer" << endl;
    RdKafka::Producer *producer = RdKafka::Producer::create(conf, errstr);
    if (!producer)
    {
        cerr << "Failed to create producer: " << errstr << endl;
        exit(1);
    }
    cout << "Producer created" << endl;
    delete conf;

    cout << "Creating topic" << endl;
    rd_kafka_resp_err_t err = createTopic(producer, topic, 3, 1);
    cout << "topic creation result: " << err << endl;
    if ((err != RD_KAFKA_RESP_ERR_TOPIC_ALREADY_EXISTS) && (err != RD_KAFKA_RESP_ERR_NO_ERROR)) {
        return -1;
    }

    // Read messages from stdin and produce to broker.
    cout << "% Type message value and hit enter "
         << "to produce message." << endl;

    for (std::string line; run && std::getline(std::cin, line);)
    {
        if (line.empty())
        {
            producer->poll(0);
            continue;
        }

        /*
     * Send/Produce message.
     * This is an asynchronous call, on success it will only
     * enqueue the message on the internal producer queue.
     * The actual delivery attempts to the broker are handled
     * by background threads.
     * The previously registered delivery report callback
     * is used to signal back to the application when the message
     * has been delivered (or failed permanently after retries).
     */
    retry:
        RdKafka::ErrorCode err = producer->produce(
            /* Topic name */
            topic,
            /* Any Partition: the builtin partitioner will be
         * used to assign the message to a topic based
         * on the message key, or random partition if
         * the key is not set. */
            RdKafka::Topic::PARTITION_UA,
            /* Make a copy of the value */
            RdKafka::Producer::RK_MSG_COPY /* Copy payload */,
            /* Value */
            const_cast<char *>(line.c_str()), line.size(),
            /* Key */
            NULL, 0,
            /* Timestamp (defaults to current time) */
            0,
            /* Message headers, if any */
            NULL,
            /* Per-message opaque value passed to
         * delivery report */
            NULL);

        if (err != RdKafka::ERR_NO_ERROR)
        {
            std::cerr << "% Failed to produce to topic " << topic << ": "
                      << RdKafka::err2str(err) << std::endl;

            if (err == RdKafka::ERR__QUEUE_FULL)
            {
                /* If the internal queue is full, wait for
         * messages to be delivered and then retry.
         * The internal queue represents both
         * messages to be sent and messages that have
         * been sent or failed, awaiting their
         * delivery report callback to be called.
         *
         * The internal queue is limited by the
         * configuration property
         * queue.buffering.max.messages */
                producer->poll(1000 /*block for max 1000ms*/);
                goto retry;
            }
        }
        else
        {
            std::cerr << "% Enqueued message (" << line.size() << " bytes) "
                      << "for topic " << topic << std::endl;
        }

        /* A producer application should continually serve
     * the delivery report queue by calling poll()
     * at frequent intervals.
     * Either put the poll call in your main loop, or in a
     * dedicated thread, or call it after every produce() call.
     * Just make sure that poll() is still called
     * during periods where you are not producing any messages
     * to make sure previously produced messages have their
     * delivery report callback served (and any other callbacks
     * you register). */
        producer->poll(0);
  }

  /* Wait for final messages to be delivered or fail.
   * flush() is an abstraction over poll() which
   * waits for all messages to be delivered. */
  std::cerr << "% Flushing final messages..." << std::endl;
  producer->flush(10 * 1000 /* wait for max 10 seconds */);

  if (producer->outq_len() > 0)
    std::cerr << "% " << producer->outq_len()
              << " message(s) were not delivered" << std::endl;

  delete producer;
  
    return 0;
}