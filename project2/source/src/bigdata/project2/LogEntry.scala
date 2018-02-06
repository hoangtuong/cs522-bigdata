package bigdata.project2

import java.util.Date

// LogEntry: corressponding to a line of log
//
case class LogEntry(
    ipAddress: String, 
    userIdentifier: String,
    userId: String,
    datetime: Date,
    requestMethod: String,
    resourcePath: String,
    httpProtocol: String,
    responeStatus: Int,
    responseSize: Long
)
