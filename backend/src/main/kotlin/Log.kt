import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection
import java.sql.DriverManager

private const val CREATE_LOG_TABLE = """CREATE TABLE IF NOT EXISTS LOG( 
                                        sync_num INT NOT NULL,
                                        sql_command TEXT NOT NULL,
                                        user_id TEXT NOT NULL);"""

private const val LOG_QUERY = "INSERT INTO LOG(sync_num, sql_command, user_id) VALUES (?, ?, ?);"

class Log(ds: HikariDataSource) {

    init {
        ds.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(CREATE_LOG_TABLE)
            }
        }
    }


    fun writeLog(conn: Connection, update: UpdateRequest) {
        conn.prepareStatement(LOG_QUERY).use { stmt ->
            for (query in update.statements) {
                stmt.setInt(1, update.sync_num + 1)
                stmt.setString(2, query)
                stmt.setString(3, update.user_id)
                stmt.execute()
            }
        }
    }

}





