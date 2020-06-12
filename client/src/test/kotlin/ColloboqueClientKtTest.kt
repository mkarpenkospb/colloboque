import com.zaxxer.hikari.HikariDataSource
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.Assertions.*
import org.junit.jupiter.api.TestInstance
import org.junit.jupiter.api.TestInstance.*
import java.sql.DriverManager
import kotlinx.coroutines.delay
import org.junit.jupiter.api.BeforeEach
import java.util.*

private const val DROP_QUERY = """DROP TABLE IF EXISTS main_table;
                          DROP TABLE IF EXISTS main_table_clone;
                          DROP TABLE IF EXISTS synchronisation;
                          DROP TABLE IF EXISTS user_id;
                          DROP TABLE IF EXISTS log;
                          """

private const val CREATE_MAIN_TABLE = """CREATE TABLE IF NOT EXISTS MAIN_TABLE(
                                      id INT PRIMARY KEY NOT NULL, 
                                      first TEXT NOT NULL,
                                      last TEXT NOT NULL,
                                      age INT NOT NULL);"""

// default server settings
private const val databaseName = "template1"
private const val portNumber = 8080
private const val postgresHost = "localhost"
private const val postgresPort = 5432
private const val user = "tester"
private const val password = "test_password"
private const val table = "MAIN_TABLE"
private const val currentSchema = "schema1"

@TestInstance(Lifecycle.PER_CLASS)
internal class ColloboqueClientKtTest {
    val localConnectionUrl = "jdbc:h2:/home/mkarpenko/bd1";
    lateinit var sessionClient : Client
    lateinit var serverTask : ServerWorkingThread
    lateinit var serverDataSource : HikariDataSource


    // class for run server and client in the same time
    class ServerWorkingThread : Thread() {
        override fun run() {
            startServer(portNumber, postgresHost, postgresPort,
                    databaseName, user,
                    password, table)
        }
    }


    @BeforeEach
    fun setUp() {

        serverDataSource = connectPostgres(postgresHost, postgresPort, databaseName, user, password)
        clearServer()

        // remove all tables
        DriverManager.getConnection(localConnectionUrl).use { conn ->
            conn.createStatement().use { stmt ->
            stmt.execute(DROP_QUERY)
            }
        }

        sessionClient = Client(localConnectionUrl)

        serverTask = ServerWorkingThread()

        serverTask.start()

        // server needs time to start
        runBlocking {
            delay(1000L)
        }

    }


    private fun getLogTableContent() : List<String> {
        val queries = mutableListOf<String>()
        DriverManager.getConnection(sessionClient.connectionUrl).use { conn ->
            conn.createStatement().use { stmt ->
                stmt.executeQuery("SELECT id, sql_command FROM LOG ORDER BY id;").use { res ->
                    while (res.next()) {
                        queries.add(res.getString(2))
                    }
                }
            }
        }
        return queries
    }

    private fun clearServer() {
        serverDataSource.connection.use { conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(DROP_QUERY)
            }
        }
    }

    private fun createServerTable() {
        val queries = listOf(
                "INSERT INTO MAIN_TABLE (id, first, last, age) VALUES (16, 'Kate', 'Pirson', 25);",
                "INSERT INTO MAIN_TABLE (id, first, last, age) VALUES (17, 'Anna', 'Pirson', 26);",
                "INSERT INTO MAIN_TABLE (id, first, last, age) VALUES (18, 'Mary', 'Pirson', 27);"
        )

        transaction(serverDataSource) {conn ->
            conn.createStatement().use { stmt ->
                stmt.execute(CREATE_MAIN_TABLE)
                for (q in queries) {
                    stmt.execute(q)
                }
            }
        }
    }

    @Test
    fun actionSimulation() {
        val queries = listOf(
            "INSERT INTO MAIN_TABLE (id, first, last, age) VALUES (1, 'Kate', 'Pirson', 25);",
            "INSERT INTO MAIN_TABLE (id, first, last, age) VALUES (2, 'Anna', 'Pirson', 26);",
            "INSERT INTO MAIN_TABLE (id, first, last, age) VALUES (3, 'Mary', 'Pirson', 27);"
        )

        // clone table before apply changes
        sessionClient.cloneTable("MAIN_TABLE")

        // check MAIN_TABLE_CLONE exists
        assertTrue(existsTable(DriverManager.getConnection(localConnectionUrl),
                "MAIN_TABLE_CLONE"))

        sessionClient.applyQueries(queries)

        // check logging
        assertEquals(queries, getLogTableContent())

        // send queries to server and clear log
        sessionClient.log.clear(updateServer(postgresHost, portNumber, sessionClient))

        // check log table is empty
        assertTrue(getLogTableContent().isEmpty())

        // check sync numbers are same
        assertEquals(getSyncNum(DriverManager.getConnection(localConnectionUrl)),
                getSyncNum(serverDataSource.connection))

    }


    @Test
    fun loadTableFromServer() {
        createServerTable()
        loadTableFromServer(sessionClient, postgresHost, portNumber, "MAIN_TABLE")
        assertEquals(getSyncNum(DriverManager.getConnection(localConnectionUrl)),
                getSyncNum(serverDataSource.connection))

        // check table content is the same
        val scServer = Scanner(String(loadTableFromDB(serverDataSource.connection, "MAIN_TABLE")))
        val scClient = Scanner(String(loadTableFromDB(DriverManager.getConnection(localConnectionUrl), "MAIN_TABLE")))

        while (scServer.hasNext() || scClient.hasNext()){
            assertEquals(scServer.nextLine().toLowerCase(), scClient.nextLine().toLowerCase())
            assertEquals(scServer.hasNext(), scClient.hasNext())
        }
    }
}