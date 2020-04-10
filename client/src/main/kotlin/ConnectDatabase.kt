import java.sql.DriverManager
import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import java.io.File

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
}


data class UpdatePost(val statements: Array<String>)

// expected queries as a kind of parametr
fun UpdateServerDatabase() : String {
    val mapper = jacksonObjectMapper()
    val queries = arrayOf(
            "INSERT INTO table2 (id, first, last, age) VALUES (12, 'Kate', 'Pirson', 19);",
            "INSERT INTO table2 (id, first, last, age) VALUES (13, 'Anna', 'Pirson', 199);",
            "INSERT INTO table2 (id, first, last, age) VALUES (14, 'Mary', 'Pirson', 20);"
    )

    val post = UpdatePost(queries)
    val jsonPost = mapper.writeValueAsString(post)
    return jsonPost
}
