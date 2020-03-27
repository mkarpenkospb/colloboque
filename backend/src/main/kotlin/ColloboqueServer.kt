import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option
import com.github.ajalt.clikt.parameters.types.int
import com.zaxxer.hikari.HikariDataSource
import io.ktor.application.call
import io.ktor.http.ContentType
import io.ktor.response.respondText
import io.ktor.routing.get
import io.ktor.routing.routing
import io.ktor.server.engine.embeddedServer
import io.ktor.server.netty.Netty
import java.sql.DriverManager


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
                call.respondText("Hello, word!", ContentType.Text.Html)
            }
            get("/echo") {
                val msg = call.parameters["msg"]
                call.respondText("$msg", ContentType.Text.Html)
            }
        }
    }
    server.start(wait = true)
}


fun main(args: Array<String>) = ColloboqueServer().main(args)