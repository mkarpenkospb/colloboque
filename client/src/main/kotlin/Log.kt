import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import kotlinx.coroutines.*
import java.sql.DriverManager
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter


class Log(private val url: String) {
    private val currentQueries: MutableList<String> = ArrayList()
    private var cnt = 0

    val createLogTable = """CREATE TABLE IF NOT EXISTS LOG(
                            |ID INT PRIMARY KEY NOT NULL, 
                            |SQL_COMMAND TEXT NOT NULL,
                            |TIME TIMESTAMP DEFAULT CURRENT_TIMESTAMP);""".trimMargin()


    fun addQuery(query: String) {
        currentQueries.add(query)
        writeLog(query)
    }

    private fun writeLog(query: String) {

        val currTimestamp = DateTimeFormatter
                .ofPattern("yyyy-MM-dd HH:mm:ss.SS")
                .withZone(ZoneOffset.UTC)
                .format(Instant.now())

        val logQuery = """INSERT INTO LOG VALUES (${cnt}, '${query.replace("'", "''")}', {ts '${currTimestamp}'});"""

        DriverManager.getConnection(url).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(createLogTable)
                stmt.execute(logQuery)
                cnt++
            }
        }
    }

    fun clear() {
        currentQueries.clear()
    }

    fun getQueries(): MutableList<String> {
        return currentQueries;
    }

}

val clientLog = Log("jdbc:h2:/home/mkarpenko/bd1")



