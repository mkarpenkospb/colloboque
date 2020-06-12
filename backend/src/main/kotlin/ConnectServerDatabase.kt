import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue
import com.opencsv.CSVWriter
import com.zaxxer.hikari.HikariDataSource
import org.apache.commons.codec.binary.Base64
import org.postgresql.copy.CopyManager
import org.postgresql.core.BaseConnection
import java.io.*
import java.sql.Connection
import java.sql.Connection.TRANSACTION_SERIALIZABLE
import java.sql.ResultSetMetaData

// setting queries
private const val CREATE_SCHEMA = "CREATE SCHEMA IF NOT EXISTS control_schema"
private const val BASE_SCHEMA_NAME = "schema"
private const val CREATE_SYNCHRONISATION_TABLE = """CREATE TABLE IF NOT EXISTS control_schema.synchronisation(
                                                    id INT PRIMARY KEY NOT NULL,      
                                                    sync_num INT NOT NULL);"""
private const val INIT_SYNCHRONISATION_TABLE = """INSERT INTO control_schema.synchronisation VALUES (0, 0);"""
private const val CREATE_CURRENT_SCHEMA_TABLE = """CREATE TABLE IF NOT EXISTS control_schema."current_schema"(
                                                    id INT PRIMARY KEY NOT NULL,      
                                                    schema_name TEXT NOT NULL);"""
private const val INIT_CURRENT_SCHEMA_TABLE = """INSERT INTO control_schema."current_schema" VALUES (0, 'schema0');"""

private const val CREATE_MAIN_TABLE = """CREATE TABLE IF NOT EXISTS MAIN_TABLE (
                                      id INT PRIMARY KEY NOT NULL, 
                                      first TEXT NOT NULL,
                                      last TEXT NOT NULL,
                                      age INT NOT NULL);"""



fun connectPostgres(host: String, port: Int, dataBase: String, user: String, password: String): HikariDataSource {

    val ds = HikariDataSource()
    ds.jdbcUrl = "jdbc:postgresql://$host:$port/$dataBase"
    ds.username = user
    ds.password = password

    return ds
}

fun setUpServer(ds: HikariDataSource) {
    // -------------- create and complete control schema
    transaction(ds, "public") { conn ->
        conn.createStatement().use { stmt ->
            stmt.execute(CREATE_SCHEMA)
        }
    }

    transaction(ds, "control_schema") { conn ->
        if (!existsTable(conn, "current_schema", "control_schema")) {
            conn.createStatement().use { stmt ->
                stmt.execute(CREATE_CURRENT_SCHEMA_TABLE)
                stmt.execute(INIT_CURRENT_SCHEMA_TABLE)
            }
        }
    }

    transaction(ds, "control_schema") { conn ->
        if (!existsTable(conn, "synchronisation", "control_schema")) {
            conn.createStatement().use { stmt ->
                stmt.execute(CREATE_SYNCHRONISATION_TABLE)
                stmt.execute(INIT_SYNCHRONISATION_TABLE)
            }
        }
    }


    transaction(ds) { conn ->
        conn.createStatement().use { stmt ->
            stmt.execute("CREATE SCHEMA IF NOT EXISTS ${getCurrentSchema(conn)}")
        }
    }

    // -------------- create main table if not exists ----------------
    transaction(ds) { conn ->
        conn.createStatement().use { stmt ->
            stmt.execute(CREATE_MAIN_TABLE)
        }
    }
}

fun updateDataBase(ds: HikariDataSource, jsonQueries: String, serverLog: ServerLog): Int {
    val update: UpdateRequest = jacksonObjectMapper().readValue(jsonQueries)
    return ds.connection.use { conn ->
        conn.autoCommit = false
        setSchema(conn, getCurrentSchema(conn))
        val serverSyncNum = getSyncNum(conn)
        when {
            serverSyncNum == update.sync_num -> {
                runUpdateRequest(conn, update)
                serverLog.writeLog(conn, update)
                conn.commit()
                conn.autoCommit = true
                update.sync_num + 1
            }
            serverSyncNum > update.sync_num -> {
                -2
            }
            else -> {
                -1
            }
        }
    }
}


// current temporary table name


fun mergeDataBase(ds: HikariDataSource, mergeRequestBytes: ByteArray, tableName: String = "MAIN_TABLE") {
    val mergeData: MergeRequest = jacksonObjectMapper().readValue(mergeRequestBytes)
    val queriesFromClient = mergeData.statements.toMutableList()
    val queriesFromLog = mutableListOf<String>()

    // create a new empty schema
    transaction(ds) { conn ->
        val currentSchemaVersion = getCurrentSchema(conn).substring(BASE_SCHEMA_NAME.length).toInt()
        val newSchemaName = "${BASE_SCHEMA_NAME}${currentSchemaVersion + 1}"
        conn.createStatement().use { stmt ->
            stmt.execute("CREATE SCHEMA $newSchemaName")
        }
        updateCurrentSchema(conn, newSchemaName);
    }



    transaction(ds) { conn ->
        conn.createStatement().use { stmt ->
            stmt.execute("DROP TABLE IF EXISTS $tableName")
            stmt.execute("""CREATE TABLE $tableName (
                            id INT PRIMARY KEY NOT NULL,
                            first TEXT NOT NULL,
                            last TEXT NOT NULL,
                            age INT NOT NULL);
                            """)
        }
        CopyManager(conn.unwrap(BaseConnection::class.java))
                .copyIn(
                        "COPY $tableName FROM STDIN (FORMAT csv, HEADER)",
                        Base64.decodeBase64(mergeData.csvbase64).inputStream()
                )
    }

    // Select all the newest from log

    transaction(ds, "control_schema") { conn ->
        conn.prepareStatement("""SELECT sql_command FROM log 
                       WHERE sync_num > ?
                       ORDER BY sync_num, q_id""").use { stmt ->
            stmt.setInt(1, mergeData.sync_num)
            stmt.executeQuery().use { res ->
                while (res.next()) {
                    queriesFromLog.add(res.getString(1))
                }
            }
        }
    }

    try {
        transaction(ds) { conn ->
            conn.transactionIsolation = TRANSACTION_SERIALIZABLE
            transaction(ds) { connConcurrent ->
                connConcurrent.transactionIsolation = TRANSACTION_SERIALIZABLE
                connConcurrent.createStatement().use { stmtConcurrent ->
                    for (sql in queriesFromLog) {
                        stmtConcurrent.execute(sql)
                    }
                }
            }

            conn.createStatement().use { stmt ->
                for (sql in queriesFromClient) {
                    stmt.execute(sql)
                }
                updateSyncNum(conn, getSyncNum(conn) + 1)
            }
        }
    } catch (e: Throwable) {
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



fun updateSyncNum(conn: Connection, syncNum: Int) {
    conn.prepareStatement("UPDATE control_schema.SYNCHRONISATION SET sync_num=?").use { stmt ->
        stmt.setInt(1, syncNum)
        stmt.execute()
    }
}


fun getSyncNum(conn: Connection): Int {
    conn.createStatement().use { stmt ->
        stmt.executeQuery("SELECT sync_num FROM control_schema.SYNCHRONISATION WHERE id=0;").use { res ->
            res.next()
            return res.getInt(1)
        }
    }
}

fun getCurrentSchema(conn: Connection): String {
    conn.createStatement().use { stmt ->
        stmt.executeQuery("SELECT schema_name FROM control_schema.CURRENT_SCHEMA WHERE id=0;").use { res ->
            res.next()
            return res.getString(1)
        }
    }
}


fun <T> transaction(ds: HikariDataSource, schema: String? = null, code: (Connection) -> T): T {
    return ds.connection.use { conn ->
        conn.autoCommit = false
        setSchema(conn, schema ?: getCurrentSchema(conn))
        code(conn).also {
            conn.commit()
            conn.autoCommit = true
        }
    }
}


fun loadTableFromDB(ds: HikariDataSource, tableName: String): ByteArray {
    ds.connection.use { conn ->
        conn.autoCommit = false
        conn.createStatement().use { stmt ->
            val query =
                    """
                    SELECT * FROM $tableName
                    """
            stmt.executeQuery(query).use { rs ->
                val rsmd: ResultSetMetaData = rs.metaData

                val colNames = arrayOfNulls<String>(rsmd.columnCount)

                for (i in 1..rsmd.columnCount) {
                    colNames[i - 1] = rsmd.getColumnName(i)
                }
                val queryByteArray = ByteArrayOutputStream()

                CSVWriter(BufferedWriter(OutputStreamWriter(queryByteArray)),
                        CSVWriter.DEFAULT_SEPARATOR,
                        CSVWriter.NO_QUOTE_CHARACTER,
                        CSVWriter.DEFAULT_ESCAPE_CHARACTER,
                        CSVWriter.DEFAULT_LINE_END).use { csvWriter ->

                    csvWriter.writeNext(colNames)
                    while (rs.next()) {
                        val getLine = arrayOfNulls<String>(rsmd.columnCount)
                        for (i in 1..rsmd.columnCount) {
                            getLine[i - 1] = rs.getString(i)
                        }
                        csvWriter.writeNext(getLine)
                    }
                }
                val syncNum = getSyncNum(conn)
                conn.commit()
                conn.autoCommit = true
                return jacksonObjectMapper().writeValueAsBytes(ReplicationResponse(
                        Base64.encodeBase64(queryByteArray.toByteArray()), syncNum))
            }
        }
    }
}

fun existsTable(conn: Connection, tableName: String, schema: String? = null): Boolean {
    val rs = conn.metaData.getTables(null, schema, tableName, null)
    while (rs.next()) {
        return true
    }
    return false
}

fun setSchema(conn: Connection, schema: String) {
    conn.createStatement().use { stmt ->
        stmt.execute("SET search_path to $schema") // cannot use a table name in prepared stmt
    }
}

fun updateCurrentSchema(conn: Connection, schema: String) {
    conn.prepareStatement("""UPDATE control_schema."current_schema" SET schema_name=?""").use { stmt ->
        stmt.setString(1, schema)
        stmt.execute()
    }
}

