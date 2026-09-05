package com.meta.wearable.dat.externalsampleapps.cameraaccess.livekit

import android.Manifest
import android.app.Application
import android.content.pm.PackageManager
import android.graphics.Bitmap
import android.graphics.BitmapFactory
import android.graphics.ImageFormat
import android.graphics.Matrix
import android.graphics.Rect
import android.graphics.YuvImage
import android.util.Log
import androidx.annotation.OptIn
import androidx.camera.camera2.interop.ExperimentalCamera2Interop
import androidx.core.content.ContextCompat
import androidx.lifecycle.AndroidViewModel
import androidx.lifecycle.ProcessLifecycleOwner
import androidx.lifecycle.viewModelScope
import com.meta.wearable.dat.camera.StreamSession
import com.meta.wearable.dat.camera.startStreamSession
import com.meta.wearable.dat.camera.types.StreamConfiguration
import com.meta.wearable.dat.camera.types.StreamSessionState
import com.meta.wearable.dat.camera.types.VideoQuality
import com.meta.wearable.dat.core.Wearables
import com.meta.wearable.dat.core.selectors.AutoDeviceSelector
import com.meta.wearable.dat.core.selectors.DeviceSelector
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.CaptureSource
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.GatewayApi
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.IntelligenceEngine
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.SettingsManager
import com.meta.wearable.dat.externalsampleapps.cameraaccess.stream.StreamingService
import com.meta.wearable.dat.externalsampleapps.cameraaccess.wearables.WearablesInit
import io.livekit.android.LiveKit
import io.livekit.android.events.RoomEvent
import io.livekit.android.events.collect
import io.livekit.android.room.Room
import io.livekit.android.room.participant.Participant
import io.livekit.android.room.track.CameraPosition
import io.livekit.android.room.track.LocalVideoTrack
import io.livekit.android.room.track.LocalVideoTrackOptions
import io.livekit.android.room.track.Track
import io.livekit.android.room.track.video.CameraCapturerUtils
import java.io.ByteArrayOutputStream
import java.io.IOException
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicReference
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.Job
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.MutableStateFlow
import kotlinx.coroutines.flow.StateFlow
import kotlinx.coroutines.flow.asStateFlow
import kotlinx.coroutines.flow.update
import kotlinx.coroutines.launch
import kotlinx.coroutines.withContext
import livekit.org.webrtc.CameraXHelper
import livekit.org.webrtc.VideoFrame
import livekit.org.webrtc.VideoSink
import livekit.org.webrtc.getCameraX
import okhttp3.MediaType.Companion.toMediaType
import okhttp3.OkHttpClient
import okhttp3.Request
import okhttp3.RequestBody.Companion.toRequestBody
import org.json.JSONObject

sealed class SessionState {
    data object Disconnected : SessionState()
    data object Connecting : SessionState()
    data object Connected : SessionState()
    data class Failed(val message: String) : SessionState()
}

/**
 * What the agent is doing right now, surfaced so a dead worker is visible
 * instead of an empty room that politely ignores you. Driven by agent
 * presence in the room plus the standard "lk.agent.state" attribute the
 * agents framework publishes (listening / thinking / speaking).
 */
enum class AgentStatus {
    NONE, // no active call
    WAITING, // call is up, agent hasn't joined the room
    STARTING, // agent joined, model session still initializing
    LISTENING,
    THINKING,
    SPEAKING,
    LEFT, // agent was here and disconnected mid-call
}

/**
 * One transcribed utterance from the "lk.transcription" text stream. Interim
 * segments update in place (same segment id); a final segment lingers briefly
 * and then clears.
 */
data class Caption(
    val segmentId: String,
    val text: String,
    val fromAgent: Boolean,
    val isFinal: Boolean,
)

/**
 * A generative UI card published by the agent's show_card tool on the "vc.ui"
 * text-stream topic. Schema source of truth: CARD_TOOL_SCHEMA in
 * agent/main.py. Unknown types render as info; unknown versions render
 * best-effort from whichever fields are present.
 */
data class UiCard(
    val uuid: String,
    val version: Int,
    val type: String,
    val title: String?,
    val value: String?,
    val body: String?,
    val facts: List<CardFact>,
    val items: List<CardItem>,
    val imageUrl: String?,
    val fallbackText: String,
    // Live-view URL for "live" cards (Browser Use live browser view). Rendered
    // in a WebView instead of static content.
    val url: String? = null,
    // Fetched lazily for image cards; arrives via a state update.
    val image: Bitmap? = null,
)

data class CardFact(val label: String, val value: String)

data class CardItem(
    val glyph: String?,
    val title: String,
    val subtitle: String?,
    val trailing: String?,
)

data class LiveKitUiState(
    val state: SessionState = SessionState.Disconnected,
    val agentStatus: AgentStatus = AgentStatus.NONE,
    val localVideoTrack: LocalVideoTrack? = null,
    // Camera-only preview while no call is active. The camera IS this app;
    // hanging up stops the listening, not the seeing.
    val previewTrack: LocalVideoTrack? = null,
    val frozenFrame: Bitmap? = null,
    val zoomFactor: Float = 1f,
    // Video published from the glasses stream instead of the phone camera.
    // The rest of the call (mic, speaker, agent) is identical.
    val isGlassesSource: Boolean = false,
    val glassesStreaming: Boolean = false,
    val caption: Caption? = null,
    val card: UiCard? = null,
) {
    val isActive: Boolean
        get() = state == SessionState.Connected || state == SessionState.Connecting

    val displayTrack: LocalVideoTrack?
        get() = localVideoTrack ?: previewTrack
}

/**
 * The entire voice+vision client, post-migration: join a LiveKit room, publish
 * mic and camera, subscribe to the agent's audio. Everything the direct
 * connection hand-rolled -- echo cancellation, interruption, turn-taking,
 * reconnection -- lives in WebRTC and the server-side agent now. What remains
 * on the phone is a room ticket and two track toggles.
 */
@OptIn(ExperimentalCamera2Interop::class)
class LiveKitSessionViewModel(
    application: Application,
) : AndroidViewModel(application) {

    companion object {
        private const val TAG = "LiveKitSession"
        private const val AGENT_STATE_ATTRIBUTE = "lk.agent.state"
        private const val MAX_ZOOM = 8f
        private const val FROZEN_JPEG_QUALITY = 90
        private const val TRANSCRIPTION_TOPIC = "lk.transcription"
        private const val CARD_TOPIC = "vc.ui"
        private const val TRANSCRIPTION_FINAL_ATTRIBUTE = "lk.transcription_final"
        private const val TRANSCRIPTION_SEGMENT_ATTRIBUTE = "lk.segment_id"
        private const val CAPTION_LINGER_MS = 4000L
    }

    private val _uiState = MutableStateFlow(LiveKitUiState())
    val uiState: StateFlow<LiveKitUiState> = _uiState.asStateFlow()

    // CameraX-backed capture so zoom can go through CameraControl.
    private val cameraProvider = CameraXHelper.createCameraProvider(ProcessLifecycleOwner.get())

    val room: Room = LiveKit.create(application).apply {
        // A video-call SDK defaults to the selfie camera; this app is a pair
        // of eyes on the world, so it opens on the back camera.
        videoTrackCaptureDefaults = LocalVideoTrackOptions(position = CameraPosition.BACK)
    }

    private val frameGrabber = FrameGrabber()
    private var grabberTrack: LocalVideoTrack? = null

    // MARK: Glasses feed (DAT stream -> LiveKit bridge)

    private val glassesSelector: DeviceSelector = AutoDeviceSelector()
    private var glassesSession: StreamSession? = null
    private val glassesFeedJobs = mutableListOf<Job>()

    // Wi-Fi Direct link negotiation between phone and glasses is flaky in
    // dense RF; when it fails the stream starves on BLE and dies. Rather than
    // a permanent "Waiting" that needs a manual back-out, restart the feed
    // after a stall. Recovery resets the budget; three failures rest.
    private var glassesRetryJob: Job? = null
    private var glassesRetryCount = 0
    private val glassesStallMs = 10_000L
    private val glassesMaxRetries = 3

    // Capturer of whichever glasses track is current (preview or call); the
    // DAT collector pushes into it. A disposed capturer drops pushes, so a
    // stale reference during track handoff is harmless.
    @Volatile private var glassesCapturer: GlassesVideoCapturer? = null

    private val httpClient = OkHttpClient.Builder()
        .callTimeout(20, TimeUnit.SECONDS)
        .build()

    init {
        if (cameraProvider.isSupported(application)) {
            CameraCapturerUtils.registerCameraProvider(cameraProvider)
        }
        // Room events arrive on SDK coroutines; each handler re-derives agent
        // status from the room, so ordering races collapse into "recompute
        // from current truth".
        viewModelScope.launch {
            room.events.collect { event ->
                when (event) {
                    is RoomEvent.ParticipantConnected -> refreshAgentStatus()
                    is RoomEvent.ParticipantDisconnected -> {
                        if (event.participant.kind == Participant.Kind.AGENT &&
                            _uiState.value.state == SessionState.Connected
                        ) {
                            _uiState.update { it.copy(agentStatus = AgentStatus.LEFT) }
                        } else {
                            refreshAgentStatus()
                        }
                    }
                    is RoomEvent.ParticipantAttributesChanged -> {
                        if (event.participant.kind == Participant.Kind.AGENT) {
                            refreshAgentStatus()
                        }
                    }
                    else -> {}
                }
            }
        }
        registerTranscriptionHandler()
        registerCardHandler()
    }

    // MARK: Generative UI cards (agent show_card tool, "vc.ui" text streams)

    private var dismissedCardUuid: String? = null

    /**
     * Each stream carries one complete JSON card payload. The same uuid
     * replaces that card in place; a dismissal holds until any new publish
     * arrives. Handlers are per-Room and survive disconnects, so this
     * registers exactly once.
     */
    private fun registerCardHandler() {
        room.registerTextStreamHandler(CARD_TOPIC) { receiver, _ ->
            viewModelScope.launch {
                val builder = StringBuilder()
                try {
                    receiver.flow.collect { chunk -> builder.append(chunk) }
                } catch (e: Exception) {
                    Log.w(TAG, "card stream ${receiver.info.id} failed: ${e.message}")
                    return@launch
                }
                handleCardPayload(builder.toString())
            }
        }
        Log.d(TAG, "card handler registered for topic $CARD_TOPIC")
    }

    private fun handleCardPayload(payload: String) {
        val json = try {
            JSONObject(payload)
        } catch (e: Exception) {
            Log.w(TAG, "malformed card payload ignored (${payload.length} bytes)")
            return
        }
        // Programmatic dismissal: {"uuid":..., "dismiss":true} clears the shown
        // card (e.g. the browse live view when the task finishes). A later card
        // publish supersedes the dismissal, matching dismissCard().
        if (json.optBoolean("dismiss", false)) {
            val uuid = json.optString("uuid").takeIf { it.isNotEmpty() }
            Log.d(TAG, "card dismiss control received uuid=$uuid")
            uuid?.let { dismissedCardUuid = it }
            _uiState.update { it.copy(card = null) }
            return
        }
        val card = try {
            parseCard(json)
        } catch (e: Exception) {
            null
        }
        if (card == null) {
            Log.w(TAG, "malformed card payload ignored (${payload.length} bytes)")
            return
        }
        Log.d(TAG, "card received uuid=${card.uuid} type=${card.type} bytes=${payload.length}")
        // Any new publish (same or different uuid) supersedes a dismissal.
        dismissedCardUuid?.let { Log.d(TAG, "card dismissal of $it superseded by new publish") }
        dismissedCardUuid = null
        _uiState.update { it.copy(card = card) }
        card.imageUrl?.let { url -> fetchCardImage(card.uuid, url) }
    }

    private fun parseCard(json: JSONObject): UiCard? {
        val uuid = json.optString("uuid").takeIf { it.isNotEmpty() } ?: return null
        val facts = json.optJSONArray("facts")?.let { array ->
            (0 until array.length()).mapNotNull { index ->
                val fact = array.optJSONObject(index) ?: return@mapNotNull null
                val label = fact.optString("label")
                val value = fact.optString("value")
                if (label.isEmpty() && value.isEmpty()) null else CardFact(label, value)
            }
        } ?: emptyList()
        val items = json.optJSONArray("items")?.let { array ->
            (0 until array.length()).mapNotNull { index ->
                val item = array.optJSONObject(index) ?: return@mapNotNull null
                val title = item.optString("title").takeIf { it.isNotEmpty() } ?: return@mapNotNull null
                CardItem(
                    glyph = item.optString("glyph").takeIf { it.isNotEmpty() },
                    title = title,
                    subtitle = item.optString("subtitle").takeIf { it.isNotEmpty() },
                    trailing = item.optString("trailing").takeIf { it.isNotEmpty() },
                )
            }
        } ?: emptyList()
        return UiCard(
            uuid = uuid,
            version = json.optInt("version", 1),
            type = json.optString("type", "info"),
            title = json.optString("title").takeIf { it.isNotEmpty() },
            value = json.optString("value").takeIf { it.isNotEmpty() },
            body = json.optString("body").takeIf { it.isNotEmpty() },
            facts = facts,
            items = items,
            imageUrl = json.optString("image_url").takeIf { it.startsWith("http") },
            fallbackText = json.optString("fallback_text"),
            url = json.optString("url").takeIf { it.startsWith("http") },
        )
    }

    fun dismissCard() {
        val uuid = _uiState.value.card?.uuid ?: return
        dismissedCardUuid = uuid
        Log.d(TAG, "card dismissed uuid=$uuid")
        _uiState.update { it.copy(card = null) }
    }

    private fun fetchCardImage(uuid: String, url: String) {
        viewModelScope.launch(Dispatchers.IO) {
            val bitmap = try {
                httpClient.newCall(Request.Builder().url(url).build()).execute().use { response ->
                    if (!response.isSuccessful) {
                        Log.w(TAG, "card image fetch failed: HTTP ${response.code}")
                        null
                    } else {
                        response.body?.bytes()?.let { BitmapFactory.decodeByteArray(it, 0, it.size) }
                    }
                }
            } catch (e: Exception) {
                Log.w(TAG, "card image fetch failed: ${e.message}")
                null
            } ?: return@launch
            _uiState.update { state ->
                val card = state.card
                if (card?.uuid == uuid && card.imageUrl == url) {
                    state.copy(card = card.copy(image = bitmap))
                } else {
                    state
                }
            }
        }
    }

    // MARK: Captions (agents transcription text streams)

    private var captionClearJob: Job? = null

    /**
     * The agents worker publishes live transcriptions as text streams on
     * "lk.transcription": one stream per segment, chunks arriving
     * incrementally; the same segment id repeats across interim streams until
     * one carries lk.transcription_final. Handlers are per-Room and survive
     * disconnects, so this registers exactly once.
     */
    private fun registerTranscriptionHandler() {
        room.registerTextStreamHandler(TRANSCRIPTION_TOPIC) { receiver, fromIdentity ->
            val attributes = receiver.info.attributes
            val segmentId = attributes[TRANSCRIPTION_SEGMENT_ATTRIBUTE] ?: receiver.info.id
            val isFinal = attributes[TRANSCRIPTION_FINAL_ATTRIBUTE]?.toBoolean() ?: false
            val fromAgent = room.remoteParticipants[fromIdentity]?.kind == Participant.Kind.AGENT ||
                fromIdentity.value.startsWith("agent-")
            Log.d(
                TAG,
                "transcription stream ${receiver.info.id} segment=$segmentId from=${fromIdentity.value} " +
                    "agent=$fromAgent final=$isFinal attrs=$attributes",
            )
            viewModelScope.launch {
                val builder = StringBuilder()
                try {
                    receiver.flow.collect { chunk ->
                        builder.append(chunk)
                        updateCaption(segmentId, builder.toString(), fromAgent, isFinal = false)
                    }
                } catch (e: Exception) {
                    Log.w(TAG, "transcription stream ${receiver.info.id} failed: ${e.message}")
                }
                Log.d(TAG, "transcription segment $segmentId done final=$isFinal len=${builder.length}")
                updateCaption(segmentId, builder.toString(), fromAgent, isFinal = isFinal)
            }
        }
        Log.d(TAG, "transcription handler registered for topic $TRANSCRIPTION_TOPIC")
    }

    private fun updateCaption(segmentId: String, text: String, fromAgent: Boolean, isFinal: Boolean) {
        if (text.isBlank()) return
        captionClearJob?.cancel()
        _uiState.update { it.copy(caption = Caption(segmentId, text, fromAgent, isFinal)) }
        if (isFinal) {
            captionClearJob = viewModelScope.launch {
                delay(CAPTION_LINGER_MS)
                _uiState.update { state ->
                    if (state.caption?.segmentId == segmentId) state.copy(caption = null) else state
                }
            }
        }
    }

    private fun refreshAgentStatus() {
        _uiState.update { state ->
            if (state.state != SessionState.Connected) {
                return@update state.copy(agentStatus = AgentStatus.NONE)
            }
            val agent = room.remoteParticipants.values.firstOrNull { it.kind == Participant.Kind.AGENT }
            val status = if (agent == null) {
                if (state.agentStatus == AgentStatus.LEFT) AgentStatus.LEFT else AgentStatus.WAITING
            } else {
                when (agent.attributes[AGENT_STATE_ATTRIBUTE]) {
                    "listening" -> AgentStatus.LISTENING
                    "thinking" -> AgentStatus.THINKING
                    "speaking" -> AgentStatus.SPEAKING
                    // "initializing", "idle", or the attribute not yet set
                    else -> AgentStatus.STARTING
                }
            }
            state.copy(agentStatus = status)
        }
    }

    // MARK: Call lifecycle

    // The engine the current call was dialed with, so a settings change can be
    // detected and applied by redialing.
    private var connectedEngine: IntelligenceEngine? = null
    private var autoStarted = false

    fun start() {
        val current = _uiState.value.state
        if (current != SessionState.Disconnected && current !is SessionState.Failed) return
        viewModelScope.launch { connectInternal() }
    }

    /**
     * Phone mode joins the room on sight -- camera, mic and the assistant all
     * come up together (mirrors iOS StreamSessionView's launch task). Runs
     * once per stint in phone mode; returns whether a start was initiated.
     * Declines (without consuming the one-shot) until the runtime permissions
     * exist, so the first-ever launch auto-starts right after the grant
     * instead of failing into a dead call.
     */
    fun autoStartIfNeeded(): Boolean {
        if (autoStarted) return false
        // The phone camera needs CAMERA; glasses video does not (mic stays
        // the phone's in both modes).
        val cameraNeeded = SettingsManager.captureSource == CaptureSource.PHONE
        val granted = ContextCompat.checkSelfPermission(
            getApplication(),
            Manifest.permission.RECORD_AUDIO,
        ) == PackageManager.PERMISSION_GRANTED &&
            (
                !cameraNeeded ||
                    ContextCompat.checkSelfPermission(
                        getApplication(),
                        Manifest.permission.CAMERA,
                    ) == PackageManager.PERMISSION_GRANTED
                )
        if (!granted) return false
        autoStarted = true
        start()
        return true
    }

    /**
     * The brain is chosen at session start (room-token metadata), so a live
     * call redials itself to apply an engine switch -- the user flips a toggle
     * in Settings and seconds later the other model picks up.
     */
    fun redialIfEngineChanged() {
        val selected = SettingsManager.intelligenceEngine
        if (_uiState.value.state != SessionState.Connected) return
        if (connectedEngine == null || connectedEngine == selected) return
        viewModelScope.launch {
            disconnectInternal()
            connectInternal()
        }
    }

    private suspend fun connectInternal() {
        if (!SettingsManager.isGatewayConfigured) {
            _uiState.update { it.copy(state = SessionState.Failed("Gateway not configured. Check Settings.")) }
            return
        }
        val glasses = SettingsManager.captureSource == CaptureSource.GLASSES
        _uiState.update { it.copy(state = SessionState.Connecting, isGlassesSource = glasses) }
        stopPreview()
        try {
            val engine = SettingsManager.intelligenceEngine
            val ticket = fetchTicket(engine)
            room.connect(ticket.url, ticket.token)
            connectedEngine = engine
            room.localParticipant.setMicrophoneEnabled(true)
            // Video failure (emulator, permission denied, glasses hiccup)
            // degrades to voice-only rather than killing the call.
            try {
                val track = if (glasses) {
                    ensureGlassesFeed()
                    val capturer = GlassesVideoCapturer()
                    val glassesTrack = room.localParticipant.createVideoTrack(
                        name = "glasses",
                        capturer = capturer,
                    )
                    glassesCapturer = capturer
                    glassesTrack.startCapture()
                    room.localParticipant.publishVideoTrack(glassesTrack)
                    glassesTrack
                } else {
                    room.localParticipant.setCameraEnabled(true)
                    room.localParticipant
                        .getTrackPublication(Track.Source.CAMERA)?.track as? LocalVideoTrack
                }
                attachGrabber(track)
                _uiState.update { it.copy(localVideoTrack = track) }
            } catch (e: Exception) {
                Log.w(TAG, "video unavailable, voice-only: ${e.message}")
            }
            _uiState.update { it.copy(state = SessionState.Connected, zoomFactor = 1f) }
            refreshAgentStatus()
        } catch (e: Exception) {
            Log.w(TAG, "call failed: ${e.message}")
            _uiState.update {
                it.copy(
                    state = SessionState.Failed(e.message ?: "connection failed"),
                    agentStatus = AgentStatus.NONE,
                    localVideoTrack = null,
                )
            }
            room.disconnect()
            // Even a failed call leaves the user with eyes.
            startPreview()
        }
    }

    fun stop() {
        viewModelScope.launch {
            disconnectInternal()
            startPreview()
        }
    }

    private fun disconnectInternal() {
        room.disconnect()
        attachGrabber(null)
        connectedEngine = null
        captionClearJob?.cancel()
        dismissedCardUuid = null
        _uiState.update {
            it.copy(
                state = SessionState.Disconnected,
                agentStatus = AgentStatus.NONE,
                localVideoTrack = null,
                frozenFrame = null,
                zoomFactor = 1f,
                caption = null,
                card = null,
            )
        }
    }

    /**
     * Full teardown when leaving the current capture mode: hang up AND
     * release the video source. The next stint auto-starts again.
     */
    fun leave() {
        autoStarted = false
        viewModelScope.launch {
            disconnectInternal()
            stopPreview()
            stopGlassesFeed()
            glassesCapturer = null
            _uiState.update { LiveKitUiState() }
        }
    }

    // MARK: Preview (local, unpublished video between calls)

    fun startPreview() {
        val state = _uiState.value
        val idle = state.state == SessionState.Disconnected || state.state is SessionState.Failed
        if (!idle || state.previewTrack != null) return
        val glasses = SettingsManager.captureSource == CaptureSource.GLASSES
        try {
            val track = if (glasses) {
                ensureGlassesFeed()
                val capturer = GlassesVideoCapturer()
                room.localParticipant.createVideoTrack(
                    name = "glasses_preview",
                    capturer = capturer,
                ).also { glassesCapturer = capturer }
            } else {
                room.localParticipant.createVideoTrack(
                    options = LocalVideoTrackOptions(position = CameraPosition.BACK),
                )
            }
            track.startCapture()
            attachGrabber(track)
            _uiState.update { it.copy(previewTrack = track, zoomFactor = 1f, isGlassesSource = glasses) }
        } catch (e: Exception) {
            Log.w(TAG, "preview video unavailable: ${e.message}")
        }
    }

    private fun stopPreview() {
        val track = _uiState.value.previewTrack ?: return
        _uiState.update { it.copy(previewTrack = null) }
        if (grabberTrack == track) attachGrabber(null)
        try {
            track.stopCapture()
            track.dispose()
        } catch (e: Exception) {
            Log.w(TAG, "preview stop failed: ${e.message}")
        }
    }

    // MARK: Glasses feed

    /**
     * Starts the DAT streaming session that produces glasses frames and fans
     * them into whichever LiveKit track is current. Kept alive across calls
     * and Settings visits (like the phone camera preview); torn down by
     * leave(). The foreground service keeps frames flowing when the screen
     * locks, matching the old DAT streaming path.
     */
    private fun ensureGlassesFeed() {
        if (glassesSession != null) return
        WearablesInit.ensure(getApplication())
        StreamingService.start(getApplication())
        val session = Wearables.startStreamSession(
            getApplication(),
            glassesSelector,
            StreamConfiguration(videoQuality = VideoQuality.MEDIUM, 24),
        )
        glassesSession = session
        // Conversion is a plain memcpy but runs per frame; keep it off main.
        glassesFeedJobs += viewModelScope.launch(Dispatchers.Default) {
            var frames = 0L
            session.videoStream.collect { frame ->
                if (frames == 0L || frames % 100 == 0L) {
                    Log.i(TAG, "glasses frame #$frames ${frame.width}x${frame.height}")
                }
                frames++
                glassesCapturer?.pushI420(frame.buffer, frame.width, frame.height)
            }
        }
        glassesFeedJobs += viewModelScope.launch {
            session.state.collect { sessionState ->
                Log.i(TAG, "glasses session state: $sessionState")
                _uiState.update { it.copy(glassesStreaming = sessionState == StreamSessionState.STREAMING) }
                if (sessionState == StreamSessionState.STREAMING) {
                    glassesRetryCount = 0
                    glassesRetryJob?.cancel()
                    glassesRetryJob = null
                } else {
                    scheduleGlassesRetry()
                }
            }
        }
    }

    private fun scheduleGlassesRetry() {
        if (glassesRetryJob?.isActive == true) return
        glassesRetryJob = viewModelScope.launch {
            kotlinx.coroutines.delay(glassesStallMs)
            if (glassesSession == null || uiState.value.glassesStreaming) return@launch
            if (glassesRetryCount >= glassesMaxRetries) {
                Log.w(TAG, "glasses feed stalled; retry budget exhausted")
                return@launch
            }
            glassesRetryCount++
            Log.i(TAG, "glasses feed stalled ${glassesStallMs}ms; restart ${glassesRetryCount}/$glassesMaxRetries")
            // Clear the handle first so stopGlassesFeed doesn't cancel the
            // coroutine that is performing the restart.
            glassesRetryJob = null
            stopGlassesFeed()
            ensureGlassesFeed()
        }
    }

    private fun stopGlassesFeed() {
        glassesRetryJob?.cancel()
        glassesRetryJob = null
        if (glassesSession == null) return
        glassesFeedJobs.forEach { it.cancel() }
        glassesFeedJobs.clear()
        try {
            glassesSession?.close()
        } catch (e: Exception) {
            Log.w(TAG, "glasses session close failed: ${e.message}")
        }
        glassesSession = null
        StreamingService.stop(getApplication())
        _uiState.update { it.copy(glassesStreaming = false) }
    }

    // MARK: Freeze (pin a frame for the model to refer to)

    // While frozen, the screen shows the pinned frame and the published video
    // is muted: the model receives no newer frames, so the pinned one stays
    // the most recent thing it has seen -- "this" means the frame the user
    // pinned.

    fun toggleFreeze() {
        if (_uiState.value.frozenFrame != null) unfreeze() else freeze()
    }

    fun freeze() {
        if (_uiState.value.frozenFrame != null) return
        frameGrabber.requestFrame { bitmap ->
            if (bitmap == null) return@requestFrame
            viewModelScope.launch(Dispatchers.Main) {
                if (_uiState.value.frozenFrame != null) return@launch
                _uiState.update { it.copy(frozenFrame = bitmap) }
                if (_uiState.value.state == SessionState.Connected) {
                    room.localParticipant.getTrackPublication(Track.Source.CAMERA)?.muted = true
                }
            }
        }
    }

    fun unfreeze() {
        if (_uiState.value.frozenFrame == null) return
        _uiState.update { it.copy(frozenFrame = null) }
        if (_uiState.value.state == SessionState.Connected) {
            room.localParticipant.getTrackPublication(Track.Source.CAMERA)?.muted = false
        }
    }

    private fun attachGrabber(track: LocalVideoTrack?) {
        grabberTrack?.removeRenderer(frameGrabber)
        grabberTrack = track
        track?.addRenderer(frameGrabber)
    }

    // MARK: Zoom

    /**
     * Zoom applied at the sensor through whichever camera track is live (call
     * or preview), so what you pinch into is what the model sees. Capped at
     * 8x: past that the lens is upscaling, not resolving.
     */
    fun zoomBy(factor: Float) {
        // Glasses have no camera control; zoom is a phone-camera feature.
        if (_uiState.value.isGlassesSource) return
        val track = _uiState.value.displayTrack ?: return
        val camera = track.capturer.getCameraX()?.value ?: return
        val zoomState = camera.cameraInfo.zoomState.value ?: return
        val floor = maxOf(zoomState.minZoomRatio, 1f)
        val ceiling = minOf(zoomState.maxZoomRatio, MAX_ZOOM)
        val target = (_uiState.value.zoomFactor * factor).coerceIn(floor, ceiling)
        if (target == _uiState.value.zoomFactor) return
        camera.cameraControl.setZoomRatio(target)
        _uiState.update { it.copy(zoomFactor = target) }
    }

    // MARK: Room ticket

    private data class Ticket(val url: String, val room: String, val token: String)

    /**
     * The gateway holds the LiveKit secret and mints a short-lived per-user
     * room JWT. The engine choice (which realtime model answers) rides along
     * and comes back inside the token as participant metadata for the worker.
     */
    private suspend fun fetchTicket(engine: IntelligenceEngine): Ticket = withContext(Dispatchers.IO) {
        val baseUrl = SettingsManager.gatewayBaseUrl.trimEnd('/')
        val body = JSONObject()
            .put("engine", engine.value)
            .apply {
                // For OpenClaw engine, pass the agent session key for routing
                if (engine == IntelligenceEngine.OPENCLAW) {
                    put("agentSessionKey", SettingsManager.openClawSessionKey)
                }
            }
            .toString()
            .toRequestBody("application/json".toMediaType())
        val request = Request.Builder()
            .url("$baseUrl/livekit-token")
            .header("Authorization", "Bearer ${SettingsManager.gatewayToken}")
            .post(body)
            .build()
        httpClient.newCall(request).execute().use { response ->
            val text = response.body?.string().orEmpty()
            if (response.code == 401 && SettingsManager.accountEmail != null) {
                // A signed-in account the gateway no longer honors (revoked or
                // reverted to pending): drop to the gate instead of a dead call.
                SettingsManager.accountStatus = "revoked"
                SettingsManager.signOut()
                throw IOException("Your account is no longer active")
            }
            if (!response.isSuccessful) {
                throw IOException(GatewayApi.errorMessage(text) ?: "gateway error (${response.code})")
            }
            val json = JSONObject(text)
            Ticket(
                url = json.getString("url"),
                room = json.optString("room"),
                token = json.getString("token"),
            )
        }
    }

    override fun onCleared() {
        super.onCleared()
        attachGrabber(null)
        stopPreview()
        stopGlassesFeed()
        room.disconnect()
        room.release()
        CameraCapturerUtils.unregisterCameraProvider(cameraProvider)
    }

    /**
     * One-shot frame capture so a freeze pins exactly what is on screen.
     * Conversion to Bitmap happens only when a pin is taken.
     */
    private class FrameGrabber : VideoSink {
        private val pending = AtomicReference<((Bitmap?) -> Unit)?>(null)

        fun requestFrame(callback: (Bitmap?) -> Unit) {
            pending.set(callback)
        }

        override fun onFrame(frame: VideoFrame) {
            val callback = pending.getAndSet(null) ?: return
            val bitmap = try {
                videoFrameToBitmap(frame)
            } catch (t: Throwable) {
                Log.w(TAG, "frame conversion failed: ${t.message}")
                null
            }
            callback(bitmap)
        }

        private fun videoFrameToBitmap(frame: VideoFrame): Bitmap? {
            val i420 = frame.buffer.toI420() ?: return null
            try {
                val width = i420.width
                val height = i420.height
                val chromaWidth = (width + 1) / 2
                val chromaHeight = (height + 1) / 2
                val nv21 = ByteArray(width * height + 2 * chromaWidth * chromaHeight)

                val dataY = i420.dataY.duplicate()
                for (row in 0 until height) {
                    dataY.position(row * i420.strideY)
                    dataY.get(nv21, row * width, width)
                }
                val dataU = i420.dataU.duplicate()
                val dataV = i420.dataV.duplicate()
                val rowU = ByteArray(chromaWidth)
                val rowV = ByteArray(chromaWidth)
                var offset = width * height
                for (row in 0 until chromaHeight) {
                    dataU.position(row * i420.strideU)
                    dataU.get(rowU, 0, chromaWidth)
                    dataV.position(row * i420.strideV)
                    dataV.get(rowV, 0, chromaWidth)
                    for (col in 0 until chromaWidth) {
                        nv21[offset++] = rowV[col]
                        nv21[offset++] = rowU[col]
                    }
                }

                val jpegBytes = ByteArrayOutputStream().use { stream ->
                    YuvImage(nv21, ImageFormat.NV21, width, height, null)
                        .compressToJpeg(Rect(0, 0, width, height), FROZEN_JPEG_QUALITY, stream)
                    stream.toByteArray()
                }
                val bitmap = BitmapFactory.decodeByteArray(jpegBytes, 0, jpegBytes.size) ?: return null

                // The sensor delivers landscape buffers; renderers apply the
                // frame's rotation tag at display time. Converting raw pixels
                // skips that step, so apply it here or every portrait pin
                // comes out sideways.
                if (frame.rotation == 0) return bitmap
                val matrix = Matrix().apply { postRotate(frame.rotation.toFloat()) }
                return Bitmap.createBitmap(bitmap, 0, 0, bitmap.width, bitmap.height, matrix, true)
            } finally {
                i420.release()
            }
        }
    }
}
