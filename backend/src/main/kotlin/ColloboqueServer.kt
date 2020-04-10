import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.default
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import io.ktor.application.*
import io.ktor.response.*
import io.ktor.routing.*
import io.ktor.server.engine.*
import io.ktor.server.netty.*
import java.io.File


class ColloboqueServer : CliktCommand() {
    private val portName by option("--server-port", help="Name of the port").int()
    private val host by option("--pg-host", help="host").default("localhost")
    private val port by option("--pg-port", help="Number of the port").int().default(5432)

    override fun run() {
        portName?.let { startServer(it, host, port) }
    }

}


fun startServer(portName: Int, host: String, port : Int) {
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

                val ds = connectPostgres(host, port, database, user, password)
                loadTableFromDB(ds, table)

                call.respondFile(File("/tmp/saveClientQuery.csv"))
            }
        }
    }
    server.start(wait = true)
}


fun main(args: Array<String>) = ColloboqueServer().main(args)