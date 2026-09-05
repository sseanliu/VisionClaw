package com.meta.wearable.dat.externalsampleapps.cameraaccess.ui

import android.view.HapticFeedbackConstants
import androidx.compose.animation.AnimatedVisibility
import androidx.compose.animation.core.Animatable
import androidx.compose.animation.core.animateDpAsState
import androidx.compose.animation.core.tween
import androidx.compose.animation.fadeIn
import androidx.compose.animation.fadeOut
import androidx.compose.foundation.Image
import androidx.compose.foundation.background
import androidx.compose.foundation.border
import androidx.compose.foundation.clickable
import androidx.compose.foundation.gestures.detectTapGestures
import androidx.compose.foundation.gestures.detectTransformGestures
import androidx.compose.foundation.gestures.detectVerticalDragGestures
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Box
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.heightIn
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.layout.sizeIn
import androidx.compose.foundation.layout.statusBarsPadding
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.shape.CircleShape
import androidx.compose.foundation.shape.RoundedCornerShape
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.filled.Call
import androidx.compose.material.icons.filled.CallEnd
import androidx.compose.material.icons.filled.Close
import androidx.compose.material.icons.filled.Settings
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.Text
import androidx.compose.runtime.Composable
import androidx.compose.runtime.DisposableEffect
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.draw.clip
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.graphics.asImageBitmap
import androidx.compose.ui.graphics.graphicsLayer
import androidx.compose.ui.input.pointer.pointerInput
import androidx.compose.ui.layout.ContentScale
import androidx.compose.ui.platform.LocalConfiguration
import androidx.compose.ui.platform.LocalDensity
import androidx.compose.ui.platform.LocalView
import androidx.compose.ui.semantics.contentDescription
import androidx.compose.ui.semantics.semantics
import androidx.compose.ui.text.font.FontStyle
import androidx.compose.ui.text.font.FontWeight
import androidx.compose.ui.text.style.TextAlign
import androidx.compose.ui.text.style.TextOverflow
import androidx.compose.ui.unit.dp
import androidx.compose.ui.unit.sp
import androidx.compose.ui.viewinterop.AndroidView
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import androidx.lifecycle.viewmodel.compose.viewModel
import com.meta.wearable.dat.externalsampleapps.cameraaccess.livekit.AgentStatus
import com.meta.wearable.dat.externalsampleapps.cameraaccess.livekit.Caption
import com.meta.wearable.dat.externalsampleapps.cameraaccess.livekit.LiveKitSessionViewModel
import com.meta.wearable.dat.externalsampleapps.cameraaccess.livekit.LiveKitUiState
import com.meta.wearable.dat.externalsampleapps.cameraaccess.livekit.SessionState
import com.meta.wearable.dat.externalsampleapps.cameraaccess.livekit.UiCard
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.SettingsManager
import com.meta.wearable.dat.externalsampleapps.cameraaccess.wearables.GlassesIssue
import io.livekit.android.renderer.TextureViewRenderer
import io.livekit.android.room.Room
import io.livekit.android.room.track.VideoTrack
import livekit.org.webrtc.RendererCommon

/**
 * Phone-mode main screen under LiveKit, and the app's front door: the camera
 * preview + call button IS the home screen. Joins the room on sight -- camera,
 * mic and the assistant all come up together; everything intelligent lives
 * server-side.
 */
@Composable
fun LiveKitStreamScreen(
    onOpenSettings: () -> Unit,
    modifier: Modifier = Modifier,
    // Glasses condition rendered inline in the placeholder area; the app has
    // one voice, so glasses state never arrives as system alarm styling.
    glassesIssue: GlassesIssue? = null,
    viewModel: LiveKitSessionViewModel = viewModel(),
) {
    val uiState by viewModel.uiState.collectAsStateWithLifecycle()

    LaunchedEffect(Unit) {
        if (!viewModel.autoStartIfNeeded()) {
            // Re-entering from Settings: apply an engine switch by redialing,
            // and make sure the between-calls preview is up.
            viewModel.redialIfEngineChanged()
            viewModel.startPreview()
        }
    }

    // Haptics mirror iOS's sensoryFeedback grammar and key off state changes,
    // not button presses, so programmatic transitions buzz too. The null
    // previous value swallows the initial composition (e.g. remounting from
    // Settings mid-call must not re-buzz).
    val view = LocalView.current
    var previousState by remember { mutableStateOf<SessionState?>(null) }
    LaunchedEffect(uiState.state) {
        val previous = previousState
        previousState = uiState.state
        if (previous == null || previous == uiState.state) return@LaunchedEffect
        when (uiState.state) {
            SessionState.Connected -> view.performHapticFeedback(HapticFeedbackConstants.CONFIRM)
            is SessionState.Failed -> view.performHapticFeedback(HapticFeedbackConstants.REJECT)
            else -> {}
        }
    }
    val isFrozen = uiState.frozenFrame != null
    var previousFrozen by remember { mutableStateOf<Boolean?>(null) }
    LaunchedEffect(isFrozen) {
        val previous = previousFrozen
        previousFrozen = isFrozen
        if (previous == null || previous == isFrozen) return@LaunchedEffect
        view.performHapticFeedback(
            if (isFrozen) HapticFeedbackConstants.LONG_PRESS else HapticFeedbackConstants.CLOCK_TICK,
        )
    }

    Box(modifier = modifier.fillMaxSize().background(Color.Black)) {
        val track = uiState.displayTrack
        if (track != null) {
            // Pinch zoom drives the phone camera; glasses have no camera
            // control, so the gesture is not installed for them.
            val zoomModifier = if (uiState.isGlassesSource) {
                Modifier
            } else {
                Modifier.pointerInput(Unit) {
                    detectTransformGestures { _, _, zoom, _ -> viewModel.zoomBy(zoom) }
                }
            }
            VideoTrackView(
                room = viewModel.room,
                track = track,
                modifier = Modifier
                    .fillMaxSize()
                    .then(zoomModifier)
                    .pointerInput(Unit) {
                        detectTapGestures(onLongPress = { viewModel.toggleFreeze() })
                    },
            )
        }

        if (uiState.isGlassesSource && !uiState.glassesStreaming &&
            uiState.frozenFrame == null && uiState.state != SessionState.Connecting &&
            uiState.state !is SessionState.Failed
        ) {
            val (title, caption) = when (glassesIssue) {
                GlassesIssue.MetaAiMissing ->
                    "Meta AI app required" to "Install it to connect your glasses."
                GlassesIssue.PermissionDenied ->
                    "Glasses permission needed" to "Allow it in the Meta AI app."
                is GlassesIssue.DeviceUpdateRequired ->
                    "Glasses update required" to
                        "Device '${glassesIssue.deviceName}' requires an update to work with this app."
                GlassesIssue.Reconnecting ->
                    "Reconnecting to glasses" to "Video will appear when your glasses start streaming."
                null ->
                    "Waiting for glasses video" to "Video will appear when your glasses start streaming."
            }
            Column(
                modifier = Modifier.align(Alignment.Center).padding(horizontal = 32.dp),
                horizontalAlignment = Alignment.CenterHorizontally,
                verticalArrangement = Arrangement.spacedBy(8.dp),
            ) {
                Text(
                    text = title,
                    color = Color.White,
                    fontSize = 17.sp,
                    fontWeight = FontWeight.SemiBold,
                )
                Text(
                    text = caption,
                    color = Color.White.copy(alpha = 0.7f),
                    fontSize = 13.sp,
                    textAlign = TextAlign.Center,
                )
            }
        }

        if (uiState.zoomFactor > 1.05f) {
            Text(
                text = String.format("%.1fx", uiState.zoomFactor),
                color = Color.White,
                fontSize = 13.sp,
                fontWeight = FontWeight.SemiBold,
                modifier = Modifier
                    .align(Alignment.TopStart)
                    .statusBarsPadding()
                    .padding(start = 16.dp, top = 16.dp)
                    .background(Color.Black.copy(alpha = 0.45f), RoundedCornerShape(50))
                    .padding(horizontal = 10.dp, vertical = 6.dp),
            )
        }

        when (val state = uiState.state) {
            is SessionState.Failed -> {
                Column(
                    modifier = Modifier.align(Alignment.Center).padding(horizontal = 32.dp),
                    horizontalAlignment = Alignment.CenterHorizontally,
                    verticalArrangement = Arrangement.spacedBy(12.dp),
                ) {
                    Text(
                        text = "Not connected",
                        color = Color.White,
                        fontSize = 17.sp,
                        fontWeight = FontWeight.SemiBold,
                    )
                    Text(
                        text = state.message,
                        color = Color.White.copy(alpha = 0.7f),
                        fontSize = 13.sp,
                        textAlign = TextAlign.Center,
                    )
                }
            }
            SessionState.Connecting -> {
                Column(
                    modifier = Modifier.align(Alignment.Center),
                    horizontalAlignment = Alignment.CenterHorizontally,
                    verticalArrangement = Arrangement.spacedBy(16.dp),
                ) {
                    CircularProgressIndicator(color = Color.White)
                    Text(
                        text = "Connecting",
                        color = Color.White.copy(alpha = 0.7f),
                        fontSize = 15.sp,
                    )
                }
            }
            else -> {}
        }

        // Pinned frame floats as a card over the still-live view: the user
        // keeps their bearings, and the caption doubles as the release
        // affordance. The model is seeing nothing newer than this frame, so
        // screen and model agree on what "this" means.
        uiState.frozenFrame?.let { frozen ->
            Box(
                modifier = Modifier
                    .fillMaxSize()
                    .background(Color.Black.copy(alpha = 0.55f))
                    .clickable { viewModel.unfreeze() },
                contentAlignment = Alignment.Center,
            ) {
                Column(
                    horizontalAlignment = Alignment.CenterHorizontally,
                    verticalArrangement = Arrangement.spacedBy(16.dp),
                ) {
                    Image(
                        bitmap = frozen.asImageBitmap(),
                        contentDescription = "Frozen frame",
                        modifier = Modifier
                            .sizeIn(maxWidth = 300.dp, maxHeight = 480.dp)
                            .clip(RoundedCornerShape(20.dp))
                            .border(2.dp, Color.White.copy(alpha = 0.9f), RoundedCornerShape(20.dp)),
                    )
                    Text(
                        text = "Tap anywhere to return to live",
                        color = Color.White.copy(alpha = 0.85f),
                        fontSize = 15.sp,
                    )
                }
            }
        }

        // Agent liveness, top and center: a call can connect perfectly and
        // still be an empty room if the worker never dispatches. The pill
        // makes the difference visible -- stuck on "Waiting for agent" means
        // the backend is down, not that the model is ignoring you.
        if (uiState.state == SessionState.Connected && uiState.agentStatus != AgentStatus.NONE) {
            AgentStatusPill(
                status = uiState.agentStatus,
                modifier = Modifier
                    .align(Alignment.TopCenter)
                    .statusBarsPadding()
                    .padding(top = 16.dp),
            )
        }

        IconButton(
            onClick = onOpenSettings,
            modifier = Modifier
                .align(Alignment.TopEnd)
                .statusBarsPadding()
                .padding(end = 8.dp),
        ) {
            Icon(
                imageVector = Icons.Default.Settings,
                contentDescription = "Settings",
                tint = Color.White.copy(alpha = 0.85f),
                modifier = Modifier
                    .background(Color.Black.copy(alpha = 0.35f), CircleShape)
                    .padding(8.dp),
            )
        }

        // Generative UI card from the agent's show_card tool: one card at a
        // time floating over the upper portion, below the status pill, clear
        // of the gear and the bottom controls. A new uuid replaces with a
        // quick fade; the same uuid updates in place.
        var lastUiCard by remember { mutableStateOf<UiCard?>(null) }
        uiState.card?.let { lastUiCard = it }
        AnimatedVisibility(
            visible = uiState.card != null,
            enter = fadeIn(),
            exit = fadeOut(),
            modifier = Modifier
                .align(Alignment.TopCenter)
                .statusBarsPadding()
                .padding(top = 64.dp),
        ) {
            lastUiCard?.let { held ->
                UiCardView(
                    card = uiState.card ?: held,
                    onDismiss = { viewModel.dismissCard() },
                )
            }
        }

        // Captions, above the buttons: the most recent utterance, updating in
        // place as chunks arrive. Final segments linger in the view model for
        // a few seconds; this only animates the fade.
        var lastCaption by remember { mutableStateOf<Caption?>(null) }
        uiState.caption?.let { lastCaption = it }
        AnimatedVisibility(
            visible = uiState.caption != null && SettingsManager.showCaptions,
            enter = fadeIn(),
            exit = fadeOut(),
            modifier = Modifier
                .align(Alignment.BottomCenter)
                .navigationBarsPadding()
                .padding(start = 24.dp, end = 24.dp, bottom = 104.dp),
        ) {
            lastCaption?.let { caption -> CaptionBubble(caption) }
        }

        // Shutter front and center: pinning what you see is the primary act.
        Box(
            modifier = Modifier
                .align(Alignment.BottomCenter)
                .navigationBarsPadding()
                .padding(bottom = 24.dp),
        ) {
            FreezeButton(
                isFrozen = uiState.frozenFrame != null,
                onClick = { viewModel.toggleFreeze() },
            )
        }
        Row(
            modifier = Modifier
                .align(Alignment.BottomStart)
                .navigationBarsPadding()
                .padding(start = 24.dp, bottom = 32.dp),
        ) {
            LiveKitCallButton(
                uiState = uiState,
                onStart = { viewModel.start() },
                onStop = { viewModel.stop() },
            )
        }
        // Video mute toggle: voice-only mode to save glasses battery.
        // Only shown during an active call.
        if (uiState.state is SessionState.Connected) {
            Row(
                modifier = Modifier
                    .align(Alignment.BottomEnd)
                    .navigationBarsPadding()
                    .padding(end = 24.dp, bottom = 32.dp),
            ) {
                VideoMuteButton(
                    isMuted = uiState.videoMuted,
                    onClick = { viewModel.toggleVideoMute() },
                )
            }
        }
    }
}

/**
 * One generative card, rendered from whichever schema fields are present so
 * unknown types and future versions degrade to an info layout; a card with
 * nothing renderable shows its fallback text. Dismiss via the corner X or a
 * swipe up. Internally scrollable past ~45% of the screen height.
 */
@Composable
private fun UiCardView(
    card: UiCard,
    onDismiss: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val maxCardHeight = (LocalConfiguration.current.screenHeightDp * 0.45f).dp
    val dismissThreshold = with(LocalDensity.current) { 64.dp.toPx() }
    // New uuid = quick fade in of the replacement; same uuid = in-place
    // content update with no re-animation (the effect key doesn't change).
    val alpha = remember { Animatable(1f) }
    LaunchedEffect(card.uuid) {
        alpha.snapTo(0f)
        alpha.animateTo(1f, animationSpec = tween(200))
    }
    val hasContent = card.title != null || card.value != null || card.body != null ||
        card.facts.isNotEmpty() || card.items.isNotEmpty() || card.image != null

    Column(
        modifier = modifier
            .fillMaxWidth(0.9f)
            .heightIn(max = maxCardHeight)
            .graphicsLayer { this.alpha = alpha.value }
            .background(Color.Black.copy(alpha = 0.65f), RoundedCornerShape(20.dp))
            .pointerInput(Unit) {
                var dragTotal = 0f
                detectVerticalDragGestures(
                    onDragStart = { dragTotal = 0f },
                    onVerticalDrag = { _, dragAmount -> dragTotal += dragAmount },
                    onDragEnd = { if (dragTotal < -dismissThreshold) onDismiss() },
                )
            }
            .padding(start = 16.dp, end = 8.dp, top = 8.dp, bottom = 16.dp)
            .semantics { contentDescription = card.fallbackText },
    ) {
        Row(verticalAlignment = Alignment.CenterVertically) {
            Text(
                text = card.title.orEmpty(),
                color = Color.White,
                fontSize = 15.sp,
                fontWeight = FontWeight.SemiBold,
                maxLines = 2,
                overflow = TextOverflow.Ellipsis,
                modifier = Modifier.weight(1f).padding(top = 6.dp),
            )
            IconButton(onClick = onDismiss, modifier = Modifier.size(32.dp)) {
                Icon(
                    imageVector = Icons.Default.Close,
                    contentDescription = "Dismiss card",
                    tint = Color.White.copy(alpha = 0.7f),
                    modifier = Modifier.size(16.dp),
                )
            }
        }
        if (card.type == "live" && card.url != null) {
            // Live browser view (Browser Use). The URL runs JavaScript and opens
            // a WebSocket to stream the remote browser, so JS + DOM storage are
            // required. Fixed height so it floats mid-screen like a result card.
            AndroidView(
                factory = { ctx ->
                    android.webkit.WebView(ctx).apply {
                        settings.javaScriptEnabled = true
                        settings.domStorageEnabled = true
                        webViewClient = android.webkit.WebViewClient()
                        loadUrl(card.url)
                    }
                },
                onRelease = { it.destroy() },
                modifier = Modifier
                    .fillMaxWidth()
                    .height(320.dp)
                    .clip(RoundedCornerShape(12.dp)),
            )
        } else {
        Column(
            modifier = Modifier
                .verticalScroll(rememberScrollState())
                .padding(end = 8.dp),
            verticalArrangement = Arrangement.spacedBy(8.dp),
        ) {
            if (!hasContent) {
                Text(
                    text = card.fallbackText,
                    color = Color.White,
                    fontSize = 14.sp,
                )
            }
            card.value?.let {
                Text(
                    text = it,
                    color = Color.White,
                    fontSize = 34.sp,
                    fontWeight = FontWeight.SemiBold,
                )
            }
            card.body?.let {
                Text(
                    text = it,
                    color = Color.White.copy(alpha = 0.75f),
                    fontSize = 13.sp,
                )
            }
            card.image?.let {
                Image(
                    bitmap = it.asImageBitmap(),
                    contentDescription = card.fallbackText,
                    contentScale = ContentScale.FillWidth,
                    modifier = Modifier
                        .fillMaxWidth()
                        .clip(RoundedCornerShape(12.dp)),
                )
            }
            card.facts.forEach { fact ->
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                ) {
                    Text(
                        text = fact.label,
                        color = Color.White.copy(alpha = 0.6f),
                        fontSize = 13.sp,
                    )
                    Text(
                        text = fact.value,
                        color = Color.White,
                        fontSize = 13.sp,
                        textAlign = TextAlign.End,
                        modifier = Modifier.padding(start = 12.dp),
                    )
                }
            }
            card.items.forEach { item ->
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    item.glyph?.let {
                        Text(
                            text = it,
                            color = Color.White,
                            fontSize = 15.sp,
                            modifier = Modifier.padding(end = 10.dp),
                        )
                    }
                    Column(modifier = Modifier.weight(1f)) {
                        Text(text = item.title, color = Color.White, fontSize = 14.sp)
                        item.subtitle?.let {
                            Text(text = it, color = Color.White.copy(alpha = 0.6f), fontSize = 12.sp)
                        }
                    }
                    item.trailing?.let {
                        Text(
                            text = it,
                            color = Color.White.copy(alpha = 0.75f),
                            fontSize = 13.sp,
                            modifier = Modifier.padding(start = 12.dp),
                        )
                    }
                }
            }
        }
        }
    }
}

/**
 * The most recent utterance, speaker-differentiated: agent text plain white,
 * user text dimmed italic. Compose has no head-ellipsize for multiline text,
 * so a long segment keeps its tail (the newest words) behind a leading
 * ellipsis, capped at two lines.
 */
@Composable
private fun CaptionBubble(
    caption: Caption,
    modifier: Modifier = Modifier,
) {
    val display = if (caption.text.length > 90) {
        "…" + caption.text.takeLast(90).trimStart()
    } else {
        caption.text
    }
    Text(
        text = display,
        color = if (caption.fromAgent) Color.White else Color.White.copy(alpha = 0.75f),
        fontStyle = if (caption.fromAgent) FontStyle.Normal else FontStyle.Italic,
        fontSize = 15.sp,
        textAlign = TextAlign.Center,
        maxLines = 2,
        overflow = TextOverflow.Ellipsis,
        modifier = modifier
            .background(Color.Black.copy(alpha = 0.55f), RoundedCornerShape(12.dp))
            .padding(horizontal = 12.dp, vertical = 8.dp),
    )
}

/** Renders a local video track full-bleed through the SDK's TextureView. */
@Composable
private fun VideoTrackView(
    room: Room,
    track: VideoTrack,
    modifier: Modifier = Modifier,
) {
    var renderer by remember { mutableStateOf<TextureViewRenderer?>(null) }

    AndroidView(
        modifier = modifier,
        factory = { context ->
            TextureViewRenderer(context).also {
                room.initVideoRenderer(it)
                it.setScalingType(RendererCommon.ScalingType.SCALE_ASPECT_FILL)
                renderer = it
            }
        },
    )

    DisposableEffect(track, renderer) {
        val view = renderer
        view?.let { track.addRenderer(it) }
        onDispose { view?.let { track.removeRenderer(it) } }
    }

    DisposableEffect(Unit) {
        onDispose { renderer?.release() }
    }
}

/** One glance answers "is anything actually listening to me right now?" */
@Composable
private fun AgentStatusPill(
    status: AgentStatus,
    modifier: Modifier = Modifier,
) {
    val label = when (status) {
        AgentStatus.WAITING -> "Waiting for agent"
        AgentStatus.STARTING -> "Agent starting"
        AgentStatus.LISTENING -> "Listening"
        AgentStatus.THINKING -> "Thinking"
        AgentStatus.SPEAKING -> "Speaking"
        AgentStatus.LEFT -> "Agent left the call"
        AgentStatus.NONE -> ""
    }
    val dotColor = when (status) {
        AgentStatus.LISTENING -> AppColor.Green
        AgentStatus.THINKING -> AppColor.Yellow
        AgentStatus.SPEAKING -> AppColor.DeepBlue
        AgentStatus.LEFT -> AppColor.Red
        else -> null
    }

    Row(
        modifier = modifier
            .background(Color.Black.copy(alpha = 0.45f), RoundedCornerShape(50)),
        verticalAlignment = Alignment.CenterVertically,
        horizontalArrangement = Arrangement.spacedBy(8.dp),
    ) {
        Box(modifier = Modifier.padding(start = 12.dp)) {
            if (dotColor != null) {
                Box(
                    modifier = Modifier
                        .size(8.dp)
                        .clip(CircleShape)
                        .background(dotColor),
                )
            } else {
                CircularProgressIndicator(
                    modifier = Modifier.size(12.dp),
                    color = Color.White,
                    strokeWidth = 2.dp,
                )
            }
        }
        Text(
            text = label,
            color = Color.White,
            fontSize = 13.sp,
            fontWeight = FontWeight.SemiBold,
            modifier = Modifier.padding(end = 12.dp, top = 7.dp, bottom = 7.dp),
        )
    }
}

/** Same call semantics as before: green to connect, red to hang up. */
@Composable
private fun LiveKitCallButton(
    uiState: LiveKitUiState,
    onStart: () -> Unit,
    onStop: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val connecting = uiState.state == SessionState.Connecting
    Box(
        modifier = modifier
            .size(48.dp)
            .clip(CircleShape)
            .background(if (uiState.isActive) AppColor.Red.copy(alpha = 0.9f) else AppColor.Green.copy(alpha = 0.9f))
            .clickable(enabled = !connecting) {
                if (uiState.isActive) onStop() else onStart()
            },
        contentAlignment = Alignment.Center,
    ) {
        if (connecting) {
            CircularProgressIndicator(
                modifier = Modifier.size(20.dp),
                color = Color.White,
                strokeWidth = 2.dp,
            )
        } else {
            Icon(
                imageVector = if (uiState.isActive) Icons.Default.CallEnd else Icons.Default.Call,
                contentDescription = if (uiState.isActive) "End call" else "Start call",
                tint = Color.White,
            )
        }
    }
}

/** Camera-app shutter: tap to pin the current frame, tap again to release. */
@Composable
private fun FreezeButton(
    isFrozen: Boolean,
    onClick: () -> Unit,
    modifier: Modifier = Modifier,
) {
    val color = if (isFrozen) AppColor.Yellow else Color.White
    val innerSize by animateDpAsState(if (isFrozen) 50.dp else 54.dp, label = "shutter")
    Box(
        modifier = modifier
            .size(68.dp)
            .border(4.dp, color, CircleShape)
            .clip(CircleShape)
            .clickable { onClick() },
        contentAlignment = Alignment.Center,
    ) {
        Box(
            modifier = Modifier
                .size(innerSize)
                .clip(CircleShape)
                .background(color),
        )
    }
}

/**
 * Video mute toggle: voice-only mode to save glasses battery.
 * When muted, the glasses stop streaming video but voice continues.
 * Camera icon with a slash when muted, plain camera when active.
 */
@Composable
private fun VideoMuteButton(
    isMuted: Boolean,
    onClick: () -> Unit,
    modifier: Modifier = Modifier,
) {
    IconButton(
        onClick = onClick,
        modifier = modifier
            .size(56.dp)
            .clip(CircleShape)
            .background(Color.Black.copy(alpha = 0.5f)),
    ) {
        if (isMuted) {
            // Video off: camera with slash
            Icon(
                painter = painterResource(id = android.R.drawable.ic_menu_camera),
                contentDescription = "Video muted — tap to resume",
                tint = Color.White,
                modifier = Modifier.size(24.dp),
            )
        } else {
            // Video on: camera
            Icon(
                painter = painterResource(id = android.R.drawable.ic_menu_camera),
                contentDescription = "Video on — tap to mute",
                tint = AppColor.Yellow,
                modifier = Modifier.size(24.dp),
            )
        }
    }
}
