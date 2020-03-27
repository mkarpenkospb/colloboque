import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.zaxxer.hikari.HikariDataSource


class ConnectDatabase : CliktCommand() {
    private val host by option("-h", help="host")
    private val portName by option("-p", help="Name of the port").int()
    private val dataBase by option("-d", help="Name of the database")
    private val user by option("-u", help="Username")
    private val password by option("-pwd", help="User password")

    override fun run() {
        connectPostgres(host, portName, dataBase, user, password);
    }
}


fun connectPostgres(host: String?, portName: Int?, dataBase: String?, user: String?, password: String?) {
    var port: String = ""
    if (portName != null)
        port = ":$portName"

    val url = "jdbc:postgresql://$host$port/$dataBase"

    val ds = HikariDataSource()
    ds.jdbcUrl = url
    ds.username = user
    if (password != null)
        ds.password = password
}

fun main(args: Array<String>) = ConnectDatabase().main(args)