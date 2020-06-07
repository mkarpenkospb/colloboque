
data class ReplicationResponse(val csvbase64: ByteArray, val sync_num: Int)
data class UpdateRequest(val statements: List<String>, val sync_num: Int, val user_id: String)
data class MergeRequest(val statements: List<String>, val csvbase64: ByteArray,
                        val sync_num: Int, val user_id: String)