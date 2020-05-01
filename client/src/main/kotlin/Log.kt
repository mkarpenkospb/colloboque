import java.sql.Connection
import java.sql.DriverManager


private const val DELETE_SYNCHRONIZED = "delete from LOG where id <= ?;"
private const val LOG_QUERY = "INSERT INTO LOG(sql_command) VALUES ( ? );"

class Log(private val connectionUrl: String, createLogTable: String) {

    init {
        DriverManager.getConnection(connectionUrl).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(createLogTable)
            }
        }
    }


    fun writeLog(conn: Connection, queries: List<String>) {
        conn.prepareStatement(LOG_QUERY).use { stmt ->
            for (query in queries) {
                stmt.setString(1, query)
                stmt.execute()
            }
        }
    }

    fun clear(deleteTo: Int) {
        DriverManager.getConnection(connectionUrl).use { conn ->
            conn.prepareStatement(DELETE_SYNCHRONIZED).use { stmt ->
                stmt.setInt(1, deleteTo)
                stmt.execute()
            }
        }
    }

}





