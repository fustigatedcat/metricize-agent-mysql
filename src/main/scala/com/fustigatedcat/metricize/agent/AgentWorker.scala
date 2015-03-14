package com.fustigatedcat.metricize.agent

import java.sql.{ResultSet, ResultSetMetaData, DriverManager, Connection}

import akka.actor.Actor
import com.fustigatedcat.metricize.agent.intf.AgentWorkerInterface
import com.typesafe.config.Config
import org.json4s.JsonAST.{JString, JObject, JArray, JValue}
import org.json4s.native.{prettyJson, renderJValue}
import org.json4s.JsonDSL._

class AgentWorker(config : Config) extends AgentWorkerInterface {

  val fqdn = config.getString("fqdn")

  val username = config.getString("username")

  val password = config.getString("password")

  val port = config.getString("port")

  val jdbc = s"jdbc:mysql://$fqdn:$port/metricize"

  Class.forName("com.mysql.jdbc.Driver")

  def handleResultSet(rs : ResultSet, rsmd : ResultSetMetaData, out : List[JObject] = List()) : JArray = rs.next() match {
    case true => {
      val arr = JObject((for(col <- 0 until rsmd.getColumnCount) yield {
        rsmd.getColumnName(col + 1) -> (rs.getString(col + 1) : JValue)
      }).toList)
      handleResultSet(
        rs,
        rsmd,
        arr :: out
      )
    }
    case _ => out.reverse
  }

  def process(): Unit = {
    val conn = DriverManager.getConnection(jdbc, username, password)
    val stmt = conn.createStatement
    val rs = stmt.executeQuery("SELECT * FROM Customer")
    println(prettyJson(renderJValue(handleResultSet(rs, rs.getMetaData))))
  }

}