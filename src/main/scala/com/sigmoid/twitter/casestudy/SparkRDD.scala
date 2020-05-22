package com.sigmoid.twitter.casestudy
//package com.sigmoid.casestudy


import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.streaming.Trigger
import org.apache.spark.sql.{ForeachWriter, Row, SparkSession}
import org.bson.json.JsonParseException
import org.mongodb.scala.bson.{BsonDocument, BsonInt32}
import org.mongodb.scala.model.{Filters, FindOneAndUpdateOptions}
import org.mongodb.scala.{Document, MongoClient, MongoCollection, MongoDatabase, MongoSocketReadException, Observer, bson}
import play.api.libs.json.Json


object SparkRDD {


  val mongoClient : MongoClient =MongoClient()
  val database: MongoDatabase = mongoClient.getDatabase("DB")
  val newCollection1: MongoCollection[Document] = database.getCollection("CountryAndCount")
  val newCollection2: MongoCollection[Document] = database.getCollection("CountryCountAndDate")
  val newCollection3: MongoCollection[Document] = database.getCollection("WordCount")
  val newCollection4: MongoCollection[Document] = database.getCollection("WordCountCountry")


  val countriesSet=SetOfCountries.countriesSet
  val common_words=SetOfStopWords.common_words


  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)

    val spark = SparkSession
      .builder
      .appName("TweetStream")
      .master("local[*]")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")


    // This is the Spark Structured Streaming + Kafka integration
    // Do not have to explicitly use the Consumer API to consume from kafka

    val ds = spark.readStream.format("kafka")
      .option("kafka.bootstrap.servers", "localhost:9092")
      .option("subscribe", "twitter_tweet10")
      .option("startingOffsets", "latest")
      .load()


    /*
	This is the schema of a consuming a ProducerRecord from kafka. Value is the actual payload and
	the rest of the fields are metadata
	root
	 |-- key: binary (nullable = true)
	 |-- value: binary (nullable = true)
	 |-- topic: string (nullable = true)
	 |-- partition: integer (nullable = true)
	 |-- offset: long (nullable = true)
	 |-- timestamp: timestamp (nullable = true)
	 |-- timestampType: integer (nullable = true)
	 At this point, our key and values are in UTF8/binary, which was serialized this way by the
	 KafkaProducer for transmission of data through the kafka brokers.
	 "Keys/Values are always deserialized as byte arrays with ByteArrayDeserializer.
	 Use DataFrame operations to explicitly deserialize the keys/values"
	*/

    // Transforms and preprocessing can be done here

    val selectds = ds.selectExpr("CAST(value AS STRING)") // deserialize binary back to String type

    val result= selectds.select("value")


    // // We must create a custom sink for MongoDB
    // // ForeachWriter is the contract for a foreach writer that is a streaming format that controls streaming writes.
    val customwriter = new ForeachWriter[Row] {
      def open(partitionId: Long, version: Long): Boolean = {
        true
      }

      def process(record: Row): Unit = {
        // Write string to connection
        try{
          val full_tweet =record(0).toString
          val result=formatTweets(full_tweet)
          val time=result._1
          val country=result._2
          val filtered_text=result._3

          if(country!=null && filtered_text!="" && filtered_text!=null){

            println(s"{country : $country, time: $time, text: $filtered_text}")

            putInCollection1(country)
            putInCollection2(country,time)

            putInCollection4(country,filtered_text)

          }
          if(filtered_text!=null && filtered_text!="")
            putInCollection3(filtered_text)



        }
        catch {
          case e: JsonParseException => println("BAD JSON FORMAT")
          case e: MongoSocketReadException=>println("SOMETHING BAD HAPPENED")
          case e: ArrayIndexOutOfBoundsException=>println("INVALID COUNTRY FORMAT")
          case e: NullPointerException=> println("WRONG TWEET TEXT")
          case e: NumberFormatException=> println("BAD TIMESTAMP")
        }

      }
      def close(errorOrNull: Throwable): Unit = {
        println("Closing..")
      }
    }

    result.writeStream
      .foreach(customwriter)
      .trigger(Trigger.ProcessingTime("2 seconds"))
      .start()
      .awaitTermination()

    // spark.stop()

  }

  def formatTweets(full_tweet: String):(String,String,String)={

    val parser = Json.parse(full_tweet)

    val rawCountry = parser \ "user" \ "location".toString
    val country = rawCountry.toString

    val rawTime = parser \ "created_at"
    var date = rawTime.toString.substring(5, rawTime.toString.length - 21) + "," + rawTime.toString.substring(rawTime.toString.length - 6, rawTime.toString.length - 1)

    if(date==null||date==""){
      date=getDateFromTimestamp()
    }

    def getDateFromTimestamp(): String ={
      val timestamp = parser \ "timestamp_ms"
      val timeInMillis=timestamp.toString().replaceAll("\"","").toLong
      import java.time.Instant
      val instant=Instant.ofEpochMilli(timeInMillis).toString.substring(0,10)

      val monthMap=Map("01"->"Jan","02"->"Feb","03"->"Mar","04"->"Apr","05"->"May","06"->"Jun","07"->"Jul","08"->"Aug","09"->"Sep","10"->"Oct","11"->"Nov","12"->"Dec")
      val month = instant.split("-")(1)
      val monthInWords=monthMap(month)

      return (monthInWords+" "+instant.split("-")(2)+", "+instant.split("-")(0))
    }


    var country1 = country.replaceAll("\"","")

    val countryFields = country1.split(",")

    var country2 =countryFields(countryFields.length-1).trim


    //country2 = "\"" + country2 + "\""

    val rawText = parser \ "text"
    val text = rawText.toString()


    var filtered_text = text.replaceAll("[-+.^!?:=;%,&\"(\\u00a9|\\u00ae|[\\u2000-\\u3300]|\\ud83c[\\ud000-\\udfff]|\\ud83d[\\ud000-\\udfff]|\\ud83e[\\ud000-\\udfff])]","")
      .toLowerCase
      .split(" ")
      .distinct
      .filterNot(x => common_words.contains(x))
      .filter(x => x.matches("^[a-zA-Z]*$"))
      .filter(x => x.length > 2)
      .mkString(" ")

    if(!countriesSet.contains(country2))
      return(null,null,filtered_text)

    if(filtered_text.isEmpty)
      return (date,country2,null)



    return (date,country2,filtered_text)
  }

  def putInCollection1(country:String){

    val doc:BsonDocument = BsonDocument()

    doc.append("country", new BsonDocument().append("count", BsonInt32(1)))

    val docu: BsonDocument = new bson.BsonDocument().append("$inc", new bson.BsonDocument().append("count", BsonInt32(1)))
    newCollection1.findOneAndUpdate(Filters.eq("country", country), docu, new FindOneAndUpdateOptions().upsert(true))
      .subscribe(new Observer[Document] {
        override def onNext(result: Document): Unit = println("")

        override def onError(e: Throwable): Unit = println("ERROR OCCURRED")

        override def onComplete(): Unit = println("INSERTED")
      })

    Thread.sleep(30)

  }

  def putInCollection2(country:String,date:String){

    val doc:BsonDocument = BsonDocument()

    val doc1=doc.append("country", new BsonDocument())
    doc1.append("date", new BsonDocument()).append("count",BsonInt32(1))

    val docu: BsonDocument = new bson.BsonDocument().append("$inc", new bson.BsonDocument().append("count", BsonInt32(1)))
    newCollection2.findOneAndUpdate(Filters.and(Filters.eq("country", country), Filters.eq("date", date)), docu, new FindOneAndUpdateOptions().upsert(true))
      .subscribe(new Observer[Document] {
        override def onNext(result: Document): Unit = println("")

        override def onError(e: Throwable): Unit = println("ERROR OCCURRED")

        override def onComplete(): Unit = println("INSERTED")
      })
//
   Thread.sleep(30)

  }

  def putInCollection3(text:String){

    val text_arr=text.split(" ")
    text_arr.foreach(putWordInDB)

    def putWordInDB(word:String){

      val doc:BsonDocument = BsonDocument()

      doc.append("word", new BsonDocument().append("count", BsonInt32(1)))

      val docu: BsonDocument = new bson.BsonDocument().append("$inc", new bson.BsonDocument().append("count", BsonInt32(1)))
      newCollection3.findOneAndUpdate(Filters.eq("word", word), docu, new FindOneAndUpdateOptions().upsert(true))
        .subscribe(new Observer[Document] {
          override def onNext(result: Document): Unit = println("")

          override def onError(e: Throwable): Unit = println("ERROR OCCURRED")

          override def onComplete(): Unit = println("INSERTED")
        })

     Thread.sleep(30)

    }
  }

  def putInCollection4(country:String,text:String){

    val text_arr=text.split(" ")
    text_arr.foreach(putCountryWordInDB)

    def putCountryWordInDB(word:String){

      val doc:BsonDocument = BsonDocument()
      val doc1=doc.append("country", new BsonDocument())
      doc1.append("word", new BsonDocument()).append("count",BsonInt32(1))

      val docu: BsonDocument = new bson.BsonDocument().append("$inc", new bson.BsonDocument().append("count", BsonInt32(1)))
      newCollection4.findOneAndUpdate(Filters.and(Filters.eq("country", country), Filters.eq("word", word)), docu, new FindOneAndUpdateOptions().upsert(true))
        .subscribe(new Observer[Document] {
          override def onNext(result: Document): Unit = println("")

          override def onError(e: Throwable): Unit = println("ERROR OCCURRED")

          override def onComplete(): Unit = println("INSERTED")
        })
//
     Thread.sleep(30)


    }
  }


}