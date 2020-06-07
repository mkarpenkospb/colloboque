import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.opencsv.CSVWriter
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.codec.binary.Base64
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import java.io.*
import java.sql.Connection
import java.sql.ResultSetMetaData

// setting queries
private const val CREATE_MAIN_TABLE = """CREATE TABLE IF NOT EXISTS MAIN_TABLE(
                                      id INT PRIMARY KEY NOT NULL, 
                                      first TEXT NOT NULL,
                                      last TEXT NOT NULL,
                                      age INT NOT NULL);"""

private const val CREATE_SYNCHRONISATION_TABLE = """CREATE TABLE IF NOT EXISTS SYNCHRONISATION(
                                                    id INT PRIMARY KEY NOT NULL,      
                                                    sync_num INT NOT NULL);"""


private const val INIT_SYNCHRONISATION_TABLE = """INSERT INTO SYNCHRONISATION VALUES (0, 0);"""


private const val CALL_FUNCTION = "{call clone_schema(?, ?, true)}"
private const val BASE_SCHEMA_NAME = "schema"

fun connectPostgres(host: String, port: Int, dataBase: String, currentSchema: String,
                    user: String, password: String): HikariDataSource {

    val ds = HikariDataSource()
    ds.jdbcUrl = "jdbc:postgresql://$host:$port/$dataBase?currentSchema=$currentSchema"
    ds.username = user
    ds.password = password
    ds.schema = currentSchema
    return ds
}

fun setUpServer(ds: HikariDataSource) {
    // -------------- create main table if not exists ----------------
    transaction(ds) { conn ->
        conn.createStatement().use { stmt ->
            stmt.execute(CREATE_MAIN_TABLE)
        }
    }

    // -------------------------- create sync table if not exists -------------
    transaction(ds) { conn ->
        if (!existsTable(conn, "synchronisation", ds.schema)) {
            conn.createStatement().use { stmt ->
                stmt.execute(CREATE_SYNCHRONISATION_TABLE)
                stmt.execute(INIT_SYNCHRONISATION_TABLE)
            }
        }
    }
}

fun updateDataBase(ds: HikariDataSource, jsonQueries: String, serverLog: ServerLog): Int {
    val update: UpdateRequest = jacksonObjectMapper().readValue(jsonQueries)
    var updatedSyncNum = -1
    ds.connection.use { conn ->
        conn.autoCommit = false
        val serverSyncNum = getSyncNum(conn)
        if (serverSyncNum == update.sync_num) {
            runUpdateRequest(conn, update)
            serverLog.writeLog(conn, update)
            conn.commit()
            conn.autoCommit = true
            updatedSyncNum = update.sync_num + 1
        } else if (serverSyncNum > update.sync_num) {
            return -2
        } else {
            TODO("Implement this branch too")
        }
    }
    return updatedSyncNum
}


// current temporary table name
fun mergeDataBase(ds: HikariDataSource, portNumber: Int, postgresHost: String,
                  postgresPort: Int, databaseName: String,
                  currentSchema: String, user: String, password: String,
                  oldClientTable: ByteArray, tableName: String = "TABLE2") : HikariDataSource {

    val mergeData: MergeRequest = jacksonObjectMapper().readValue(oldClientTable)
    val queries = mergeData.statements.toMutableList()
    val currentSchemaVersion = currentSchema.substring(BASE_SCHEMA_NAME.length).toInt()
    val newSchema = "$BASE_SCHEMA_NAME${currentSchemaVersion + 1}"

    // create and call function
    ds.connection.use { conn ->
        conn.createStatement().use {stmt->
            stmt.execute(CLONE_SCHEMA)
        }
        conn.prepareCall(CALL_FUNCTION).use { cstmt ->
            cstmt.setString(1, currentSchema)
            cstmt.setString(2, newSchema)
            cstmt.execute()
        }
    }

    // the same as in client part
    val tmp = createTempFile()
    tmp.writeText(String(Base64.decodeBase64(mergeData.csvbase64)))
    transaction(ds) { conn ->
        conn.createStatement().use { stmt ->
            stmt.execute("DROP TABLE IF EXISTS ${newSchema}.${tableName}")
            stmt.execute("""CREATE TABLE IF NOT EXISTS ${newSchema}.${tableName} (
                            id INT PRIMARY KEY NOT NULL,
                            first TEXT NOT NULL,
                            last TEXT NOT NULL,
                            age INT NOT NULL);
                            """)
        }
        CopyManager(conn.unwrap(BaseConnection::class.java))
                .copyIn(
                        "COPY ${newSchema}.${tableName} FROM STDIN (FORMAT csv, HEADER)",
                        BufferedReader(FileReader(tmp.absolutePath))
                )
    }
    tmp.delete()

    val dsNew = connectPostgres(postgresHost, postgresPort, databaseName, newSchema, user, password)
    // Select all the newest from log
    transaction(dsNew) { conn ->
        conn.createStatement().use { stmt ->
            stmt.executeQuery(
                    """SELECT sql_command FROM LOG 
                       WHERE sync_num > ${mergeData.sync_num}
                       ORDER BY sync_num, q_id""").use { res ->
                while (res.next()) {
                    queries.add(res.getString(1))
                }
            }
        }
    }

    try {
        transaction(dsNew) { conn ->
            conn.createStatement().use { stmt ->
                for (sql in queries) {
                    stmt.execute(sql)
                }
                updateSyncNum(conn, getSyncNum(conn) + 1)
            }
        }
        return dsNew
    } catch (e : Throwable) {
        TODO("action in case merging failed")
    }
}

fun runUpdateRequest(conn: Connection, update: UpdateRequest) {
    conn.createStatement().use { stmt ->
        for (q in update.statements) {
            stmt.execute(q)
        }
    }
    updateSyncNum(conn, update.sync_num + 1)
}


fun <T> transaction(ds: HikariDataSource, code: (Connection) -> T): T {
    return ds.connection.use { conn ->
        conn.autoCommit = false
        code(conn).also {
            conn.commit()
            conn.autoCommit = true
        }
    }
}

