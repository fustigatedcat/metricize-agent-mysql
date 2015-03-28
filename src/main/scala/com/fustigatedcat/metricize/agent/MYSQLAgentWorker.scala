package com.fustigatedcat.metricize.agent

import com.fustigatedcat.metricize.agent.intf.{AgentResponse, AgentWorkerInterface}
import com.fustigatedcat.metricize.agent.mysql.MYSQLWorker
import com.typesafe.config.Config

class MYSQLAgentWorker(config : Config) extends AgentWorkerInterface {

  Class.forName("com.mysql.jdbc.Driver")

  val fqdn = config.getString("fqdn")

  val username = config.getString("username")

  val password = config.getString("password")

  val port = config.getString("port")

  val queryString = config.getString("queryString")

  val dbName = config.getString("dbName")

  val jdbc = s"jdbc:mysql://$fqdn:$port/$dbName"

  def needsRescheduling_? : Boolean = true

  def process(): AgentResponse = {
    MYSQLWorker.executeMysqlWorker(jdbc, username, password, queryString)
  }

}