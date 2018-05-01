package util.cassandra

case class ExporterConf(keyspace:String,
                        tableName:String,
                        insertStatement:String,
                        createTableStatement:String,
                        cassHosts:String,
                        throughputInMBits:Int,
                        throughputBufferSize:Int,
                        distLockHost:String) extends Serializable