package com.cloudera.sa.bsci.storage

import org.apache.kudu.client.KuduClient.KuduClientBuilder
import org.apache.kudu.client._

/**
 * Created by gmedasani on 4/25/17.
 */
object KuduDAO {

  /*
  Create and return a Kudu Client and Session.
   */
  def createClientAndSession(kuduMaster:String):(KuduClient,KuduSession) = {
    val client:KuduClient = new KuduClientBuilder(kuduMaster).build()
    val session:KuduSession = client.newSession()
    session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_SYNC)
    (client,session)
  }

  /*
  Create and return a upsert operation and PartialRow.
   */
  def createRow(client:KuduClient,tableName:String):(Upsert,PartialRow) = {
    val table:KuduTable = client.openTable(tableName)
    val upsert:Upsert = table.newUpsert()
    val row:PartialRow = upsert.getRow
    (upsert,row)
  }

  /*
  Apply the upsert operation in a given session.
   */
  def upsertRow(session:KuduSession,upsert:Upsert) = {
    session.apply(upsert)
  }

  /*
  Shutdown the client and close the session.
   */
  def closeClientAndSession(session:KuduSession,client:KuduClient)={
    session.close()
    client.shutdown()
  }

}
