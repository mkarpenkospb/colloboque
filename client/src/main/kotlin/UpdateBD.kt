import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import java.sql.DriverManager


class UpdateBD : CliktCommand() {
    private val dbPath by option("-f", help="Path to database")
    private val command by option("-c", help="Update command")

    override fun run() {
        val url = "jdbc:h2:$dbPath"
        val sqlQuery = command;
        DriverManager.getConnection(url).use {conn->
            conn.createStatement().use {stmt ->
                stmt.executeUpdate(sqlQuery)
            }
        }
    }
}

fun main(args: Array<String>) = UpdateBD().main(args)