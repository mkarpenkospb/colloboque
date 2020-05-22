import io.ktor.client.*
import io.ktor.client.engine.apache.*
import java.sql.DriverManager
import java.util.*

// ----------------- SQL queries constants ---------------------------

private const val CREATE_USER_ID_TABLE = """CREATE TABLE USER_ID(
                                         id INT PRIMARY KEY NOT NULL,      
                                         uuid TEXT NOT NULL);"""

private const val INSERT_UID = """INSERT INTO USER_ID(id, uuid) VALUES (0, ?)"""


private const val CREATE_SYNCHRONISATION_TABLE = """CREATE TABLE IF NOT EXISTS SYNCHRONISATION(
                                                    id INT PRIMARY KEY NOT NULL,      
                                                    sync_num INT NOT NULL);"""

private const val INIT_SYNCHRONISATION_TABLE = """INSERT INTO SYNCHRONISATION VALUES (0, 0);"""

private const val DROP_TABLE = """DROP TABLE IF EXISTS %s """;

private const val CLONE_TABLE = """CREATE TABLE %s AS SELECT * FROM %s """;


class Client (val connectionUrl: String) {
    val userId: String
    val log: Log
    val httpClient: HttpClient
    val txnManager = TransactionManager(connectionUrl)
    init {
        // ------------------ create user id if not exists -------------------
        txnManager.transaction { conn ->
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

        userId = getUserId(connectionUrl)
        // --------------------------- create client log -------------------------
        log = Log(connectionUrl)

        // --------------------------- create http client -------------------------
        httpClient = HttpClient(Apache) {
            followRedirects = true
        }

        // -------------------------- create sync table if not exists -------------
        txnManager.transaction { conn ->
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

    fun applyQueries(query: List<String>) {
        val queries = mutableListOf<String>()
        txnManager.transaction  { conn ->
            conn.createStatement().use { stmt ->
                for (sql in query) {
                    stmt.executeUpdate(sql)
                    queries.add(sql)
                }
            }
            log.writeLog(conn, queries)
        }
    }

    fun cloneTable(tableName: String) {
        val cloneName = "${tableName}_CLONE"
        txnManager.transaction {conn ->
            conn.createStatement().use { stmt -> // I can't use prepared stmt for table names
                stmt.execute(DROP_TABLE.format(cloneName))
                stmt.execute(CLONE_TABLE.format(cloneName, tableName))
            }
        }
    }
}