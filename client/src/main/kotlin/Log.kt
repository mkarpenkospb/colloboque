import io.ktor.http.escapeIfNeeded
import java.sql.Connection
import java.sql.DriverManager


class Log(private var url: String, createLogTable: String) {

    init {
        DriverManager.getConnection(url).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(createLogTable)
            }
        }
    }


    fun writeLog(conn: Connection, queries: List<String>) {

        var logQuery: String

        conn.createStatement().use { stmt ->
            for (query in queries) {
                logQuery = """INSERT INTO LOG(sql_command) VALUES ('${query.replace("'", "''")}');"""
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





