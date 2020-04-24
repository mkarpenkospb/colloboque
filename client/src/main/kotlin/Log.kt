import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import kotlinx.coroutines.*
import java.sql.DriverManager
import java.time.Instant
import java.time.ZoneOffset
import java.time.format.DateTimeFormatter


class Log(private var url: String, createLogTable: String) {

    init {
        DriverManager.getConnection(url).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(createLogTable)
            }
        }
    }


    fun writeLog(query: String) {

        val logQuery = """INSERT INTO LOG(sql_command) VALUES ('${query.replace("'", "''")}');"""

        DriverManager.getConnection(url).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(logQuery)
            }
        }
    }

    fun clear(deleteTo: Int) {
        val deleteSynchronized = """delete from LOG where id <= $deleteTo;"""

        DriverManager.getConnection(url).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(deleteSynchronized)
            }
        }
    }

}





