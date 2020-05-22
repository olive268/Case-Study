package com.sigmoid.twitter.casestudy

import java.util.Properties
import java.util.concurrent.{BlockingQueue, LinkedBlockingQueue, TimeUnit}

import com.twitter.hbc.ClientBuilder
import com.twitter.hbc.core.endpoint.StatusesFilterEndpoint
import com.twitter.hbc.core.processor.StringDelimitedProcessor
import com.twitter.hbc.core.{Client, Hosts, HttpHosts}
import com.twitter.hbc.httpclient.auth.{Authentication, OAuth1}
import com.typesafe.config._
import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.{Logger, LoggerFactory}
object TwitterProducer {
  def main(args: Array[String]): Unit = {
    (new TwitterProducer).run()
  }
}

class TwitterProducer() {
  val config: Config = ConfigFactory.load()
  private val logger: Logger = LoggerFactory.getLogger(classOf[TwitterProducer].getName)
  //private val terms: List[String] = Lists.newArrayList("COVID-19","CORONA")


  def run(): Unit = {
    this.logger.info("Setup")

    // Setup your blocking queue
    val msgQueue: BlockingQueue[String] = new LinkedBlockingQueue(1000)


    //Create a twitter client
    val client: Client = this.createTwitterClient(msgQueue)

    //Attempts to establish a connection
    client.connect()

    //Create a Kafka Producer
    val producer: KafkaProducer[String, String] = this.createKafkaProducer
    
     //    Runtime.getRuntime.addShutdownHook(new Thread(() => {
    //      def stopnow() = {
    //        logger.info("stopping application")
    //        logger.info("shutting down client from twitter")
    //        client.stop()
    //        logger.info("closing producer")
    //        producer.close()
    //        logger.info("done")
    //      }
    //
    //      stopnow()
    //    }))

//loop to send tweets to kafka
    // on a different thread,or multiple different thread
    while ( {
      !client.isDone
    }) {
      var msg: String = null
      try msg = msgQueue.poll(5L, TimeUnit.SECONDS).asInstanceOf[String]
      catch {
        case variable: InterruptedException =>
          variable.printStackTrace()
          client.stop()
      }
      if (msg != null) {
        this.logger.info(msg)
        val result = new ProducerRecord[String, String]("twitter_tweet10",msg)
        producer.send(result)
      }
      this.logger.info("End of application")
    }
  }

  def createTwitterClient(msgQueue: BlockingQueue[String]): Client = {
    val hosebirdHosts: Hosts = new HttpHosts("https://stream.twitter.com")
    val hosebirdEndpoint: StatusesFilterEndpoint = new StatusesFilterEndpoint
    hosebirdEndpoint.trackTerms(config.getStringList("twitter.terms"))
    val hosebirdAuth: Authentication = new OAuth1(config.getString("twitter.consumerKey"),
      config.getString("twitter.consumerSecret"),
      config.getString("twitter.accessToken"),
      config.getString("twitter.accessTokenSecret"))
    val builder: ClientBuilder = (new ClientBuilder).name("Hosebird-Client-01").hosts(hosebirdHosts).authentication(hosebirdAuth).endpoint(hosebirdEndpoint).processor(new StringDelimitedProcessor(msgQueue))
    val hosebirdClient: Client = builder.build
    hosebirdClient
  }


  def createKafkaProducer: KafkaProducer[String, String] = {


    val bootstrapservers = "127.0.0.1:9092"
    val properties = new Properties
    properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapservers)
    properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, classOf[StringSerializer].getName)
    properties.setProperty(ProducerConfig.ENABLE_IDEMPOTENCE_CONFIG, "true")
    properties.setProperty(ProducerConfig.ACKS_CONFIG, "all")
    properties.setProperty(ProducerConfig.RETRIES_CONFIG, Integer.toString(Integer.MAX_VALUE))
    properties.setProperty(ProducerConfig.MAX_IN_FLIGHT_REQUESTS_PER_CONNECTION, "5")

    // high throughput producer (at the expense of a bit of latency and cpu usage)
    properties.setProperty(ProducerConfig.COMPRESSION_TYPE_CONFIG, "snappy")
    properties.setProperty(ProducerConfig.LINGER_MS_CONFIG, "20")
    properties.setProperty(ProducerConfig.BATCH_SIZE_CONFIG, Integer.toString(32 * 1024))
    val producer: KafkaProducer[String, String] = new KafkaProducer(properties)
    producer
  }
}

