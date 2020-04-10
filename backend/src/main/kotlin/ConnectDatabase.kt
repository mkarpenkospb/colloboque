import com.opencsv.CSVWriter
import com.zaxxer.hikari.HikariDataSource
import java.io.BufferedWriter
import java.io.ByteArrayOutputStream
import java.io.OutputStreamWriter
import java.sql.ResultSetMetaData
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import com.fasterxml.jackson.module.kotlin.readValue


fun connectPostgres(host: String, port: Int,
                    dataBase: String, user: String, password: String): HikariDataSource {

    val url = "jdbc:postgresql://$host:$port/$dataBase"

    val ds = HikariDataSource()
    ds.jdbcUrl = url
    ds.username = user
    ds.password = password

    return ds
}

data class UpdatePost(val statements: Array<String>)

fun UpdateDataBase(ds: HikariDataSource, jsonQueries: String) {
    val mapper = jacksonObjectMapper()
    val update: UpdatePost = mapper.readValue(jsonQueries)

    ds.connection.use { conn ->
        conn.createStatement().use { stmt ->
            for (q in update.statements) {
                stmt.execute(q)
            }
        }
    }

}



fun loadTableFromDB(ds : HikariDataSource, tableName : String ): ByteArrayOutputStream {

   ds.connection.use { conn ->
        conn.createStatement().use { stmt ->
            val query =
                """
                SELECT * FROM $tableName
                """
            stmt.executeQuery(query).use {rs ->
                val rsmd: ResultSetMetaData = rs.metaData

                val colNames = arrayOfNulls<String>(rsmd.columnCount)

                for (i in 1..rsmd.columnCount) {
                    colNames[i-1] = rsmd.getColumnName(i)
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
                    return queryByteArray
                    }
            }
        }
    }
}

