package com.cloudera.sa.bsci.matmas

import com.cloudera.sa.bsci.storage.KuduDAO
import org.apache.kudu.client.{KuduSession, KuduClient}
import play.api.libs.json.{JsString, JsArray, Json, JsValue}
import scala.collection.mutable

/**
 * Created by gmedasani on 4/25/17.
 */
/*
Class to process material idocs and load them into Kudu.
 */
object SegmentLoader {

  /*
  Create a Json object given a string.
   */
  def createIdocJson(idocMessage:String):JsValue={
    val idocJson:JsValue = Json.parse(idocMessage)
    idocJson
  }

  /*
  Return the value for a given primary key field.
   */
  def getIdocJsonAndPrimaryKey(idocJson:JsValue,
                               parentPrimaryKeyName:String,
                               isMetadata:Boolean):(JsValue, String) = {
    if(!isMetadata){
      val parentPrimaryKey = idocJson.\\(parentPrimaryKeyName.toString)(0)
      val parentPrimaryKeyCleaned = parentPrimaryKey.toString().replaceAll("\"", "")
      (idocJson,parentPrimaryKeyCleaned)
    }
    else (idocJson,"")
  }

  /*
  Check if a segment exists in a idoc.
   */
  def checkIfSegmentsExist(idocJson:JsValue,
                           segmentName:String):Boolean = {
    val numberOfSegments = idocJson.\\(segmentName).length
    if(numberOfSegments == 0) false
    else true
  }


  /*
  Return segment or multiple segments that match given segment name in a idoc.
   */
  def extractSegments(idocJson:JsValue,
                      segmentName:String):Seq[JsValue] = {
    val idocSegments = idocJson.\\(segmentName).head
    if(idocSegments.isInstanceOf[JsArray]){
      val segments:Seq[JsValue] = idocSegments.asInstanceOf[JsArray].value
      segments
    }else{
      val segments:Seq[JsValue] = Seq(idocSegments)
      segments
    }
  }

  /*
  Return the key-value pairs of a segment. Only valid fields for a given table are considered in the returned
  key-value pairs.
   */
  def extractFieldValues(segment:JsValue,
                         tableName:String):mutable.HashMap[String,String] = {
    val validFields = MaterialTableMappings.getMaterialMasterValidFields().getOrElse(tableName,List())
    val segmentHashMap = new mutable.HashMap[String,String]()
    for (field <- validFields) {
      val value = segment \ field
      if (value.isInstanceOf[JsString]) {
        val cleanedValue = value.toString().trim.replaceAll("\"", "")
        segmentHashMap.put(field,cleanedValue)
      }
    }
    segmentHashMap
  }

  /*
  Persist the given key-value pairs for a segment in Kudu.
   */
  def insertIntoKudu(idocMessage:String,
                     tableName:String,
                     keyValues:mutable.HashMap[String,String],
                     clientAndSession:(KuduClient,KuduSession),
                     databaseName:String,
                     parentPrimaryKeyName:String,
                     parentPrimaryKeyValue:String,
                     secondaryPrimaryKeyName:String,
                     secondaryPrimaryKeyValue:String,
                     isMetadata:Boolean) = {
    val primaryKeys = MaterialTableMappings.getMaterialMasterPrimaryKeys().getOrElse(tableName,List())
    val upsertDAO = KuduDAO.createRow(clientAndSession._1,"impala::"+databaseName+"."+tableName)
    val row = upsertDAO._2
    for (primaryKey <- primaryKeys){
      if(primaryKey == parentPrimaryKeyName){
        row.addString(primaryKey,parentPrimaryKeyValue)
      }else if(primaryKey == secondaryPrimaryKeyName) {
        row.addString(primaryKey,secondaryPrimaryKeyValue)
      }
      else{
        row.addString(primaryKey,keyValues.getOrElse(primaryKey.toUpperCase,""))
        keyValues.remove(primaryKey.toUpperCase)
      }
    }
    for (key <- keyValues.keySet) row.addString(key.toLowerCase,keyValues.getOrElse(key,""))
    if(isMetadata){
      row.addString("idocjson",idocMessage.trim) //This key is not part of the keyValues from Idoc Json parsing
    }
    println(row)
    KuduDAO.upsertRow(clientAndSession._2,upsertDAO._1)
  }


  /*
  Function to load a segment into Kudu. Most of the processing of a idoc begins here.
  'complex' is a totally made up term for lack of better word. 'complex' in this sense simply means it has children.
   */
  def loadSegment(clientAndSession:(KuduClient,KuduSession),
                  idocMessage:String,
                  idocJson:JsValue,
                  segmentName:String,
                  kuduMaster:String,
                  database:String,
                  parentPrimaryKeyName:String,
                  secondaryPrimaryKeyName:String,
                  complexSegment:Boolean,
                  isMetadata:Boolean):Unit = {
    val idocJsonAndParentPrimaryKey = getIdocJsonAndPrimaryKey(idocJson,parentPrimaryKeyName.toUpperCase,isMetadata)
    val docSegment = idocJsonAndParentPrimaryKey._1
    val parentPrimaryKeyValue = idocJsonAndParentPrimaryKey._2
    val doesSegmentExist = checkIfSegmentsExist(docSegment, segmentName)
    if (doesSegmentExist){
      val tableName = MaterialTableMappings.getSegmentToTableMapping().getOrElse(segmentName,"")
      val segments = extractSegments(docSegment,segmentName)
      for (segment <- segments){
        val segmentKeyValues = extractFieldValues(segment,tableName)
        val secondaryPrimaryKeyValue = segmentKeyValues.getOrElse(secondaryPrimaryKeyName.toUpperCase,"")
        insertIntoKudu(idocMessage,tableName,segmentKeyValues,clientAndSession,database,parentPrimaryKeyName,parentPrimaryKeyValue,
          secondaryPrimaryKeyName,secondaryPrimaryKeyValue,isMetadata)
        if(complexSegment){
          loadChildSegments(idocMessage,segmentName,segment,clientAndSession,database,parentPrimaryKeyName,parentPrimaryKeyValue,
            secondaryPrimaryKeyName,secondaryPrimaryKeyValue,isMetadata)
        }
      }
    }
  }

  /*
  Function to load child segments of segments that are considered 'complex' into Kudu.
   */
  def loadChildSegments(idocMessage:String,
                        parentSegmentName:String,
                        parentSegment:JsValue,
                        clientAndSession:(KuduClient,KuduSession),
                        database:String,
                        parentPrimaryKeyName:String,
                        parentPrimaryKeyValue:String,
                        secondaryPrimaryKeyName:String,
                        secondaryPrimaryKeyValue:String,
                        isMetadata:Boolean): Unit ={
    val childSegmentNames = MaterialTableMappings.getSegmentChildren().get(parentSegmentName)
    for (name <- childSegmentNames){
      val childSegmentName = name.head
      val doesChildSegmentsExist = checkIfSegmentsExist(parentSegment,childSegmentName)
      if(doesChildSegmentsExist){
        val tableName = MaterialTableMappings.getSegmentToTableMapping().getOrElse(childSegmentName,"")
        val segments = extractSegments(parentSegment,childSegmentName)
        for (segment <- segments){
          val segmentKeyValues = extractFieldValues(segment,tableName)
          insertIntoKudu(idocMessage,tableName,segmentKeyValues,clientAndSession,database,parentPrimaryKeyName,parentPrimaryKeyValue,
            secondaryPrimaryKeyName,secondaryPrimaryKeyValue,isMetadata)
        }
      }
    }
  }


  /*
  Function to load mara segments into Kudu.
  Technically this can be removed and merged with loadSegment function itself.
   */
  def loadMaraSegment(clientAndSession:(KuduClient,KuduSession),
                      idocMessage:String,
                      idocJson:JsValue,
                      segmentNames:List[String],
                      kuduMaster:String,
                      database:String,
                      parentPrimaryKeyName:String,
                      secondaryPrimaryKeyName:String,
                      complexSegment:Boolean,
                      isMetadata:Boolean)={
    val idocJsonAndParentPrimaryKey = getIdocJsonAndPrimaryKey(idocJson,parentPrimaryKeyName.toUpperCase,isMetadata)
    val docSegment = idocJsonAndParentPrimaryKey._1
    val parentPrimaryKeyValue = idocJsonAndParentPrimaryKey._2
    val tableName = MaterialTableMappings.getSegmentToTableMapping().getOrElse(segmentNames(0),"")
    val segmentsKeyValues = new mutable.ListBuffer[mutable.HashMap[String,String]] ()
    for (segmentName <- segmentNames){
      val doesSegmentExist = checkIfSegmentsExist(docSegment, segmentName)
      if(doesSegmentExist){
        val segments = extractSegments(docSegment,segmentName)
        for (segment <- segments){
          val segmentKeyValues = extractFieldValues(segment,tableName)
          segmentsKeyValues += segmentKeyValues
        }
      }
    }
    val segmentsKeyValuesMap = collection.mutable.HashMap(segmentsKeyValues.flatten.toMap.toSeq: _*)
    insertIntoKudu(idocMessage,tableName,segmentsKeyValuesMap,clientAndSession,database,parentPrimaryKeyName,parentPrimaryKeyValue,
      secondaryPrimaryKeyName,"",isMetadata)
  }


  /*
  Function to load all the required material segments from material idoc. Only segments with two part primary Keys are
  specified here. For segments with three part primary keys, we pass in 'complex' as true which tells the loadSegment()
  to load it's child segments. Currently the code has a restriction of being able to traverse only upto a total
  depth of 3 from the parent json element. Future improvements can be made to deal with arbitrary number of primary
  keys and arbitrary depth level.
   */
  def loadSegments(idocMessage:String,
                   kuduMaster:String,
                   databaseName:String)={
    val clientAndSession = KuduDAO.createClientAndSession(kuduMaster)//Create a Kudu client and session
    val idocJson = createIdocJson(idocMessage)
    loadSegment(clientAndSession,idocMessage,idocJson,"EDI_DC40",kuduMaster,databaseName,"","",false,true)//Load rawmarastaging table. This is a metadata segment
    loadSegment(clientAndSession,idocMessage,idocJson,"E1MARCM",kuduMaster,databaseName,"matnr","werks",true,false)//Load marc, mard, mkal tables
    loadSegment(clientAndSession,idocMessage,idocJson,"E1MARMM",kuduMaster,databaseName,"matnr","meinh",true,false)//Load marm, mean tables
    loadSegment(clientAndSession,idocMessage,idocJson,"E1MLGNM",kuduMaster,databaseName,"matnr","lgnum",true,false)//Load mlgn, mlgt tables
    loadSegment(clientAndSession,idocMessage,idocJson,"E1MAKTM",kuduMaster,databaseName,"matnr","spras",false,false)//Load makt table
    loadSegment(clientAndSession,idocMessage,idocJson,"E1MBEWM",kuduMaster,databaseName,"matnr","bwkey",false,false)//Load mbew table
    loadSegment(clientAndSession,idocMessage,idocJson,"E1MLANM",kuduMaster,databaseName,"matnr","aland",false,false)//Load mlan table
    loadSegment(clientAndSession,idocMessage,idocJson,"E1MVKEM",kuduMaster,databaseName,"matnr","vkorg",false,false)//Load mvke table
    loadMaraSegment(clientAndSession,idocMessage,idocJson,List("E1MARAM","E1MARA1","ZEI_MARAM"),kuduMaster,databaseName,"matnr","",false,false) //Load mara table
    KuduDAO.closeClientAndSession(clientAndSession._2,clientAndSession._1)//Shutdown the client and session
  }

}
