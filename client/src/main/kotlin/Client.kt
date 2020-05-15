import io.ktor.client.*
import io.ktor.client.engine.apache.*
import java.sql.DriverManager
import java.util.*

// ----------------- SQL queries constants ---------------------------

private const val CREATE_USER_ID_TABLE = """CREATE TABLE USER_ID(
                                        id INT PRIMARY KEY NOT NULL,      
                                         uuid TEXT NOT NULL);"""

private const val INSERT_UID = """INSERT INTO USER_ID(id, uuid) VALUES (0, ?)"""


private const val CREATE_LOG_TABLE = """CREATE TABLE IF NOT EXISTS LOG( 
                                        id bigint auto_increment,
                                        sql_command TEXT NOT NULL);"""

private const val CREATE_SYNCHRONISATION_TABLE = """CREATE TABLE IF NOT EXISTS SYNCHRONISATION(
                                                    id INT PRIMARY KEY NOT NULL,      
                                                    sync_num INT NOT NULL);"""

private const val INIT_SYNCHRONISATION_TABLE = """INSERT INTO SYNCHRONISATION VALUES (0, 0);"""

class Client (val CONNECTION_URL: String) {
    val USER_ID: String
    val LOG: Log
    val HTTP_CLIENT: HttpClient

    init {
        // ------------------ create user id if not exists -------------------
        transaction(this) { conn ->
            if (!existsTable(conn, "USER_ID")) {
                conn.createStatement().use { stmt ->
                    stmt.execute(CREATE_USER_ID_TABLE)
                }
                conn.prepareStatement(INSERT_UID).use { stmt ->
                    stmt.setString(1, UUID.randomUUID().toString())
                    stmt.execute()
                }
            }
        }

        USER_ID = getUserId(CONNECTION_URL)
        // --------------------------- create client log -------------------------
        LOG = Log(CONNECTION_URL, CREATE_LOG_TABLE)

        // --------------------------- create http client -------------------------
        HTTP_CLIENT = HttpClient(Apache) {
            followRedirects = true
        }

        // -------------------------- create sync table if not exists -------------
        transaction (this) { conn ->
            if (!existsTable(conn, "SYNCHRONISATION")) {
                conn.createStatement().use { stmt ->
                    stmt.execute(CREATE_SYNCHRONISATION_TABLE)
                    stmt.execute(INIT_SYNCHRONISATION_TABLE)
                }
            }
        }
    }

    fun getUserId(connectionUrl: String): String {
        DriverManager.getConnection(connectionUrl).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT uuid FROM USER_ID WHERE id=0;").use { res ->
                    res.next()
                    return res.getString(1)
                }
            }
        }
    }
}