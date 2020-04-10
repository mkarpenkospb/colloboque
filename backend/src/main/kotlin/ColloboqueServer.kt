import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*


class ColloboqueServer : CliktCommand() {
    private val portName by option("--server-port", help="Name of the port").int()
    private val postgresHost by option("--pg-host", help="host").default("localhost")
    private val postgresPort by option("--pg-port", help="Number of the port").int().default(5432)

    override fun run() {
        portName?.let { startServer(it, postgresHost, postgresPort) }
    }

}


fun startServer(portName: Int, postgresHost: String, postgresPort : Int) {
    val server = embeddedServer(Netty, port = portName) {
        routing {
            get("/") {
                call.respondText("Hello!")
            }
            get("/table") {

                val database = call.parameters["database"]
                val user = call.parameters["user"]
                val password = call.parameters["password"]
                val table = call.parameters["table"]

                val ds = connectPostgres(postgresHost, postgresPort, database, user, password)
                call.respondBytes(loadTableFromDB(ds, table).toByteArray())
            }
        }
    }
    server.start(wait = true)
}


fun main(args: Array<String>) = ColloboqueServer().main(args)