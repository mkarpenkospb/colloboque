import java.sql.DriverManager
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper

fun importTable(url: String, tableName: String, tableData: ByteArray) {

    val tmp = createTempFile()
    tmp.writeBytes(tableData)

    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            val sql =
                    """
                     CREATE TABLE $tableName AS SELECT * FROM CSVREAD('$tmp');
                    """
            stmt.executeUpdate(sql)
        }
    }

    tmp.delete()
}


data class UpdatePost(val statements: List<String>)

// expected queries as a kind of parametr
fun updateRequest(): String {
    return jacksonObjectMapper().writeValueAsString(
            UpdatePost(
                    listOf(
                            "INSERT INTO table2 (id, first, last, age) VALUES (15, 'Kate', 'Pirson', 19);",
                            "INSERT INTO table2 (id, first, last, age) VALUES (16, 'Anna', 'Pirson', 199);",
                            "INSERT INTO table2 (id, first, last, age) VALUES (17, 'Mary', 'Pirson', 20);"
                    )
            )
    )
}