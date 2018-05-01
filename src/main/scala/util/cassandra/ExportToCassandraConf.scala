package util.cassandra

case class ExportToCassandraConf(keyspace:String,
                                 tableName:String,
                                 insertStatement:String,
                                 createTableStatement:String,
                                 cassHosts:String,
                                 throughputInMBits:Int,
                                 throughputBufferSize:Int,
                                 distLockHost:String) extends Serializable