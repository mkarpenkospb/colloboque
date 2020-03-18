import com.github.ajalt.clikt.core.CliktCommand
import com.github.ajalt.clikt.parameters.options.option

class ReturnString : CliktCommand() {
    private val inStr by option("-c", help="A string you want to see again")

    override fun run() {
        echo(inStr)
    }
}

fun main(args: Array<String>) = ReturnString().main(args)

