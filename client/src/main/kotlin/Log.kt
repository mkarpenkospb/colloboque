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

        val logQuery = """INSERT INTO LOG(sql_command) VALUES ( ? );"""

        conn.prepareStatement(logQuery).use { stmt ->
            for (query in queries) {
                stmt.setString(1, query)
                stmt.execute()
            }
        }

    }

    fun clear(deleteTo: Int) {
        val deleteSynchronized = """delete from LOG where id <= ?;"""

        DriverManager.getConnection(url).use { conn ->
            conn.prepareStatement(deleteSynchronized).use {stmt ->
                stmt.setInt(1, deleteTo)
                stmt.execute()
            }
        }
    }

}





