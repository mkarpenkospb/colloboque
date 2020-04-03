import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.zaxxer.hikari.HikariDataSource
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*


class ColloboqueServer : CliktCommand() {
    private val portName by option("-p", help="Name of the port").int()

    override fun run() {
        portName?.let { startServer(it) }
    }

}


fun startServer(portName: Int) {
    val server = embeddedServer(Netty, port = portName) {
        routing {
            get("/") {
                call.respondText("Hello!")
            }
            get("/table") {

                val host = call.parameters["host"]
                val port = call.parameters["port"]?.toInt()
                val database = call.parameters["database"]
                val user = call.parameters["user"]
                val password = call.parameters["password"]
                val table = call.parameters["table"]

                val ds = connectPostgres(host, port, database, user, password)
                val responseString = loadTableFromDB(ds, table)

                call.respondText(responseString)
            }
        }
    }
    server.start(wait = true)
}


fun main(args: Array<String>) = ColloboqueServer().main(args)