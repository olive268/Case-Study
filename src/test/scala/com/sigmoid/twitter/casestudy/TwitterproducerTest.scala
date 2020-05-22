package com.sigmoid.twitter.casestudy

import com.typesafe.config.{Config, ConfigFactory}
import org.junit.Test

class TwitterproducerTest {

  val config: Config = ConfigFactory.load()
  @Test
  def test_consumerKey(): Unit ={

    val Consumer_Key="TZFsn3LDE2HgVhH7koDFlyta4"
    val found_key=config.getString("twitter.consumerKey")
    assert(Consumer_Key==found_key)
  }

  @Test
  def test_consumerSecret(): Unit ={

    val Consumer_Secret="4XI9K5uUYoWIa4OpcPal6fr3f4jvHAWaMxbqn37u34KGpoLvL2"
    val found_secret=config.getString("twitter.consumerSecret")
    assert(Consumer_Secret==found_secret)
  }

  @Test
  def test_accessToken(): Unit ={

    val access_Token="850526074475651072-LdzrMYPfbVqjpU8NHC2GgalsvbyxUlz"
    val found_token=config.getString("twitter.accessToken")
    assert(access_Token==found_token)
  }

  @Test
  def test_accessTokenSecret(): Unit ={

    val access_Tokensecret="ZUFihspoLiTCqWpcZolxZW32SU2tLpwOC4amVxA5MKsoh"
    val found_token=config.getString("twitter.accessTokenSecret")
    assert(access_Tokensecret==found_token)
  }

  @Test
  def test_validterm(): Unit ={
    val config: Config = ConfigFactory.load()
    val input_term="corona"
    val predicted_output=true
    val term=config.getStringList("twitter.terms")
    assert(predicted_output==term.contains(input_term))
  }


}
