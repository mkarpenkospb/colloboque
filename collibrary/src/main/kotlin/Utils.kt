import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.sql.Connection
import java.sql.ResultSetMetaData
import com.opencsv.CSVWriter

fun existsTable(conn: Connection, tableName: String, schema: String? = null): Boolean {
    val rs = conn.metaData.getTables(null, schema, tableName, null)
    while (rs.next()) {
        return true
    }
    return false
}

fun getSyncNum(conn: Connection) : Int {
    conn.createStatement().use { stmt ->
        stmt.executeQuery("SELECT sync_num FROM SYNCHRONISATION WHERE id=0;").use { res ->
            res.next()
            return res.getInt(1)
        }
    }
}

fun updateSyncNum(conn: Connection, syncNum: Int) {
    conn.prepareStatement("UPDATE SYNCHRONISATION SET sync_num=?").use { stmt ->
        stmt.setInt(1, syncNum)
        stmt.execute()
    }
}

fun loadTableFromDB(conn: Connection, tableName: String): ByteArray {
    conn.autoCommit = false
    conn.createStatement().use { stmt ->
        val query =
                """
                SELECT * FROM $tableName ORDER BY id
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
            conn.commit()
            conn.autoCommit = true
            return queryByteArray.toByteArray()
        }
    }
}
