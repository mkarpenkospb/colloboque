import com.fasterxml.jackson.module.kotlin.jacksonObjectMapper
import io.ktor.client.HttpClient
import io.ktor.client.request.post
import kotlinx.coroutines.*


class Journal {
    private val currentQueries: MutableList<String> = ArrayList()
    private val history: MutableList<String> = ArrayList()
    private val localLog: MutableList<logItem> = ArrayList()

    class logItem(
            val query: String,
            val status: String,
            val errorMsg: String
    )

    fun addQuery(query: String) {
        currentQueries.add(query)
        localLog.add(logItem(query, "success", ""))
    }

    fun addFailQuery(query: String, error: String) {
        localLog.add(logItem(query, "fail", error))
    }

    fun updateRequest(url: String, client: HttpClient) {

        runBlocking {
            client.post<String>(url) {
                body = jacksonObjectMapper().writeValueAsString(
                        UpdatePost(currentQueries)
                )
            }
        }

        history.addAll(currentQueries)
        currentQueries.clear()
    }
}

val clientJournal = Journal()



