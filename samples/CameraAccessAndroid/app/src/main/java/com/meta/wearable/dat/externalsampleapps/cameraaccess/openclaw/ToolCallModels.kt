package com.meta.wearable.dat.externalsampleapps.cameraaccess.openclaw

import org.json.JSONArray
import org.json.JSONObject

// Tool Result

sealed class ToolResult {
    data class Success(val result: String) : ToolResult()
    data class Failure(val error: String) : ToolResult()

    fun toJSON(): JSONObject = when (this) {
        is Success -> JSONObject().put("result", result)
        is Failure -> JSONObject().put("error", error)
    }
}

// Tool Call Status (for UI)

sealed class ToolCallStatus {
    data object Idle : ToolCallStatus()
    data class Executing(val name: String) : ToolCallStatus()
    data class Completed(val name: String) : ToolCallStatus()
    data class Failed(val name: String, val error: String) : ToolCallStatus()
    data class Cancelled(val name: String) : ToolCallStatus()

    val displayText: String
        get() = when (this) {
            is Idle -> ""
            is Executing -> "Running: $name..."
            is Completed -> "Done: $name"
            is Failed -> "Failed: $name - $error"
            is Cancelled -> "Cancelled: $name"
        }

    val isActive: Boolean
        get() = this is Executing
}

// OpenClaw Connection State

sealed class OpenClawConnectionState {
    data object NotConfigured : OpenClawConnectionState()
    data object Checking : OpenClawConnectionState()
    data object Connected : OpenClawConnectionState()
    data class Unreachable(val message: String) : OpenClawConnectionState()
}

// Agent selector — maps display names to OpenClaw session keys

enum class OpenClawAgent(val displayName: String, val sessionKey: String) {
    SPOCK("Spock (CTO)", "agent:cto:glass"),
    WREN("Wren (COO)", "agent:coo:glass"),
    MASON("Mason (CMO)", "agent:cmo:glass"),
    GRAYSON("Grayson (CRO)", "agent:cro:glass"),
    MAIN("Default Agent", "agent:main:glass");

    companion object {
        fun fromSessionKey(key: String): OpenClawAgent =
            entries.firstOrNull { it.sessionKey == key } ?: MAIN
    }
}