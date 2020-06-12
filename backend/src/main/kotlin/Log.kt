import com.zaxxer.hikari.HikariDataSource
import java.sql.Connection

private const val CREATE_LOG_TABLE = """CREATE TABLE IF NOT EXISTS control_schema.LOG( 
                                        sync_num INT NOT NULL,
                                        q_id INT NOT NULL,
                                        sql_command TEXT NOT NULL,
                                        user_id TEXT NOT NULL);"""

private const val LOG_QUERY = "INSERT INTO control_schema.LOG(sync_num, q_id, sql_command, user_id) VALUES (?, ?, ?, ?);"

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
            for ((counter, query) in update.statements.withIndex()) { // as I watched queries way, the order here should be correct
                stmt.setInt(1, update.sync_num)
                stmt.setInt(2, counter)
                stmt.setString(3, query)
                stmt.setString(4, update.user_id)
                stmt.execute()
            }
        }
    }
}





