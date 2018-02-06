package bigdata.project2

import java.util.Date
import java.text.SimpleDateFormat
import java.util.regex.Pattern
import java.io.Serializable

class LogParser extends Serializable {
  private val REG_EXP = "^(\\S+) (\\S+) (\\S+) \\[([\\w:/]+\\s[+\\-]\\d{4})\\] \"(\\S+) (\\S+) (\\S+)\" (\\d{3}) (\\d+)"
  private val pattern = Pattern.compile(REG_EXP)
  private val dateFormater = new SimpleDateFormat("dd/MMM/yyyy:hh:mm:ss ZZ")
  
  // Sample log entry
  // 64.242.88.10 - - [07/Mar/2004:16:10:02 -0800] "GET /mailman/listinfo/hsdivision HTTP/1.1" 200 6291
   
  // Parse Date from a string
  def parseDate(dateString: String) : Date = {
    dateFormater.parse(dateString)
  }
  
  // Parse a line into a LogEntry object
  def parse(line: String) : LogEntry = {
    val matcher = pattern.matcher(line)

    if (matcher.find()) {
      LogEntry(matcher.group(1),
          matcher.group(2),
          matcher.group(3),
          parseDate(matcher.group(4)),
          matcher.group(5),
          matcher.group(6),
          matcher.group(7),
          matcher.group(8).toInt,
          matcher.group(9).toLong
          )
    } else {
      null
    }
  }
}
