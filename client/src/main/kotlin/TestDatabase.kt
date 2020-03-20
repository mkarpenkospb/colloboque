import java.sql.DriverManager
import java.sql.ResultSet




fun main(args: Array<String>) {
    createFirst()
}

fun createFirst() {
    val url = "jdbc:h2:mem:"
    DriverManager.getConnection(url).use { conn ->
        conn.createStatement().use { stmt ->
            val sql =
             """
             CREATE TABLE   REGISTRATION
            (id INTEGER not NULL,
             first VARCHAR(255),
             last VARCHAR(255),
             age INTEGER,
             PRIMARY KEY ( id ))
            """
            stmt.executeUpdate(sql)
            stmt.executeUpdate("""
            INSERT INTO REGISTRATION(id, first, last, age) 
            VALUES(1, 'Maria', 'Karpenko', 26 )
            """)
            stmt.executeQuery("SELECT * from REGISTRATION").use { res ->
                while (res.next()) {
                    println("${res.getString(1)}  ${res.getString(2)}  ${res.getString(3)}  ${res.getString(4)}")
                }
            }
        }
    }
}

