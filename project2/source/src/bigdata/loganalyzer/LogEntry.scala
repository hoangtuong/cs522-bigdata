package bigdata.loganalyzer

import java.util.Date

// LogEntry: corressponding to a line of log
//
case class LogEntry(
    ipAddress: String, 
    userIdentifier: String,
    userId: String,
    datetime: Date,
    requestMethod: String,
    requestResource: String,
    httpProtocol: String,
    responeStatus: Int,
    responseSize: Long
)
