package com.meta.wearable.dat.externalsampleapps.cameraaccess.ui

import androidx.activity.compose.BackHandler
import androidx.compose.foundation.clickable
import androidx.compose.foundation.layout.Arrangement
import androidx.compose.foundation.layout.Column
import androidx.compose.foundation.layout.Row
import androidx.compose.foundation.layout.Spacer
import androidx.compose.foundation.layout.fillMaxSize
import androidx.compose.foundation.layout.fillMaxWidth
import androidx.compose.foundation.layout.height
import androidx.compose.foundation.layout.navigationBarsPadding
import androidx.compose.foundation.layout.padding
import androidx.compose.foundation.layout.size
import androidx.compose.foundation.rememberScrollState
import androidx.compose.foundation.text.KeyboardOptions
import androidx.compose.foundation.verticalScroll
import androidx.compose.material.icons.Icons
import androidx.compose.material.icons.automirrored.filled.ArrowBack
import androidx.compose.material.icons.filled.ChevronRight
import androidx.compose.material3.AlertDialog
import androidx.compose.material3.CircularProgressIndicator
import androidx.compose.material3.ExperimentalMaterial3Api
import androidx.compose.material3.Icon
import androidx.compose.material3.IconButton
import androidx.compose.material3.MaterialTheme
import androidx.compose.material3.OutlinedTextField
import androidx.compose.material3.SegmentedButton
import androidx.compose.material3.SegmentedButtonDefaults
import androidx.compose.material3.SingleChoiceSegmentedButtonRow
import androidx.compose.material3.Switch
import androidx.compose.material3.Text
import androidx.compose.material3.TextButton
import androidx.compose.material3.TopAppBar
import androidx.compose.runtime.Composable
import androidx.compose.runtime.LaunchedEffect
import androidx.compose.runtime.getValue
import androidx.compose.runtime.mutableStateOf
import androidx.compose.runtime.remember
import androidx.compose.runtime.setValue
import androidx.compose.ui.Alignment
import androidx.compose.ui.Modifier
import androidx.compose.ui.graphics.Color
import androidx.compose.ui.text.font.FontFamily
import androidx.compose.ui.text.input.KeyboardType
import androidx.compose.ui.unit.dp
import androidx.lifecycle.compose.collectAsStateWithLifecycle
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.CaptureSource
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.GatewayApi
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.GatewayStatus
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.IntelligenceEngine
import com.meta.wearable.dat.externalsampleapps.cameraaccess.settings.SettingsManager
import com.meta.wearable.dat.externalsampleapps.cameraaccess.openclaw.OpenClawAgent

private enum class SettingsSubScreen { CONNECTED_APPS, RECENT_TASKS, GATEWAY, OPENCLAW }

@Composable
fun SettingsScreen(
    onBack: () -> Unit,
    modifier: Modifier = Modifier,
) {
    var subScreen by remember { mutableStateOf<SettingsSubScreen?>(null) }

    when (subScreen) {
        SettingsSubScreen.CONNECTED_APPS -> ConnectedAppsScreen(onBack = { subScreen = null })
        SettingsSubScreen.RECENT_TASKS -> RecentTasksScreen(onBack = { subScreen = null })
        SettingsSubScreen.GATEWAY -> GatewaySettingsScreen(onBack = { subScreen = null })
        SettingsSubScreen.OPENCLAW -> OpenClawSettingsScreen(onBack = { subScreen = null })
        null -> SettingsMainScreen(
            onBack = onBack,
            onOpen = { subScreen = it },
            modifier = modifier,
        )
    }
}

@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun SettingsMainScreen(
    onBack: () -> Unit,
    onOpen: (SettingsSubScreen) -> Unit,
    modifier: Modifier = Modifier,
) {
    val captureSource by SettingsManager.captureSourceFlow.collectAsStateWithLifecycle()
    var intelligenceEngine by remember { mutableStateOf(SettingsManager.intelligenceEngine) }
    var showCaptions by remember { mutableStateOf(SettingsManager.showCaptions) }
    var gatewayStatus by remember { mutableStateOf<GatewayStatus>(GatewayStatus.Checking) }
    var showResetDialog by remember { mutableStateOf(false) }

    LaunchedEffect(Unit) {
        gatewayStatus = GatewayStatus.Checking
        gatewayStatus = GatewayApi.checkStatus()
    }

    BackHandler { onBack() }

    Column(modifier = modifier.fillMaxSize()) {
        TopAppBar(
            title = { Text("Settings") },
            navigationIcon = {
                IconButton(onClick = onBack) {
                    Icon(Icons.AutoMirrored.Filled.ArrowBack, contentDescription = "Back")
                }
            },
        )

        Column(
            modifier = Modifier
                .fillMaxSize()
                .verticalScroll(rememberScrollState())
                .padding(horizontal = 16.dp)
                .navigationBarsPadding(),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            // Camera section: applies immediately -- the root scaffold observes
            // the same flow and swaps the capture pipeline live.
            SectionHeader("Camera")
            SingleChoiceSegmentedButtonRow(modifier = Modifier.fillMaxWidth()) {
                CaptureSource.entries.forEachIndexed { index, source ->
                    SegmentedButton(
                        selected = source == captureSource,
                        onClick = { SettingsManager.captureSource = source },
                        shape = SegmentedButtonDefaults.itemShape(
                            index = index,
                            count = CaptureSource.entries.size,
                        ),
                    ) {
                        Text(source.label)
                    }
                }
            }
            FooterText(
                if (captureSource == CaptureSource.GLASSES) {
                    "Streams from your Meta glasses. Connecting them happens on the main screen."
                } else {
                    "Uses this phone's camera. The app opens straight into it, with voice ready."
                },
            )

            // Intelligence section
            SectionHeader("Intelligence")
            SingleChoiceSegmentedButtonRow(modifier = Modifier.fillMaxWidth()) {
                IntelligenceEngine.entries.forEachIndexed { index, engine ->
                    SegmentedButton(
                        selected = engine == intelligenceEngine,
                        onClick = {
                            intelligenceEngine = engine
                            SettingsManager.intelligenceEngine = engine
                        },
                        shape = SegmentedButtonDefaults.itemShape(
                            index = index,
                            count = IntelligenceEngine.entries.size,
                        ),
                    ) {
                        Text(engine.label)
                    }
                }
            }
            FooterText(
                if (intelligenceEngine == IntelligenceEngine.OPENAI) {
                    "OpenAI gpt-realtime. Applies to the next call."
                } else {
                    "Google Gemini Live. Applies to the next call."
                },
            )
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Text("Show captions", style = MaterialTheme.typography.bodyLarge)
                Switch(
                    checked = showCaptions,
                    onCheckedChange = {
                        showCaptions = it
                        SettingsManager.showCaptions = it
                    },
                )
            }

            // Gateway status + navigation rows
            SectionHeader("Gateway")
            Row(
                modifier = Modifier.fillMaxWidth(),
                horizontalArrangement = Arrangement.SpaceBetween,
                verticalAlignment = Alignment.CenterVertically,
            ) {
                Text("Status", style = MaterialTheme.typography.bodyLarge)
                GatewayStatusLabel(status = gatewayStatus)
            }
            NavigationRow("Connected Apps") { onOpen(SettingsSubScreen.CONNECTED_APPS) }
            NavigationRow("Recent Tasks") { onOpen(SettingsSubScreen.RECENT_TASKS) }
            // The URL ships with a working default and the token is captured
            // on first launch, so most people never need to see them;
            // surfacing them as primary fields made a configured setup look
            // like one awaiting setup.
            NavigationRow("Gateway settings") { onOpen(SettingsSubScreen.GATEWAY) }

            // OpenClaw direct connection (alternative to hosted gateway)
            SectionHeader("OpenClaw")
            NavigationRow("Direct Connection") { onOpen(SettingsSubScreen.OPENCLAW) }

            // Reset
            TextButton(onClick = { showResetDialog = true }) {
                Text("Reset to Defaults", color = Color.Red)
            }

            Spacer(modifier = Modifier.height(32.dp))
        }
    }

    if (showResetDialog) {
        AlertDialog(
            onDismissRequest = { showResetDialog = false },
            title = { Text("Reset Settings") },
            text = { Text("This will reset all settings to the values built into the app.") },
            confirmButton = {
                TextButton(onClick = {
                    SettingsManager.resetAll()
                    intelligenceEngine = SettingsManager.intelligenceEngine
                    showCaptions = SettingsManager.showCaptions
                    showResetDialog = false
                }) {
                    Text("Reset", color = Color.Red)
                }
            },
            dismissButton = {
                TextButton(onClick = { showResetDialog = false }) {
                    Text("Cancel")
                }
            },
        )
    }
}

@Composable
private fun GatewayStatusLabel(
    status: GatewayStatus,
    modifier: Modifier = Modifier,
) {
    when (status) {
        GatewayStatus.Checking ->
            CircularProgressIndicator(modifier = modifier.size(16.dp), strokeWidth = 2.dp)
        GatewayStatus.Ready ->
            Text("Connected", color = AppColor.Green, modifier = modifier)
        GatewayStatus.NotConfigured ->
            Text("Not set up", color = MaterialTheme.colorScheme.onSurfaceVariant, modifier = modifier)
        GatewayStatus.Unauthorized ->
            Text("Token rejected", color = AppColor.Red, modifier = modifier)
        is GatewayStatus.Unreachable ->
            Text(status.detail, color = AppColor.Red, modifier = modifier)
    }
}

@Composable
private fun NavigationRow(
    label: String,
    onClick: () -> Unit,
) {
    Row(
        modifier = Modifier
            .fillMaxWidth()
            .clickable(onClick = onClick)
            .padding(vertical = 12.dp),
        horizontalArrangement = Arrangement.SpaceBetween,
        verticalAlignment = Alignment.CenterVertically,
    ) {
        Text(label, style = MaterialTheme.typography.bodyLarge)
        Icon(
            imageVector = Icons.Default.ChevronRight,
            contentDescription = null,
            tint = MaterialTheme.colorScheme.onSurfaceVariant,
        )
    }
}

/** Gateway URL + token, tucked away like iOS's disclosure group. */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun GatewaySettingsScreen(
    onBack: () -> Unit,
    modifier: Modifier = Modifier,
) {
    var gatewayBaseUrl by remember { mutableStateOf(SettingsManager.gatewayBaseUrl) }
    var gatewayToken by remember { mutableStateOf(SettingsManager.gatewayToken) }
    var webrtcSignalingURL by remember { mutableStateOf(SettingsManager.webrtcSignalingURL) }
    val accountEmail = SettingsManager.accountEmail
    var signedOut by remember { mutableStateOf(false) }

    fun saveAndClose() {
        SettingsManager.gatewayBaseUrl = gatewayBaseUrl.trim()
        SettingsManager.webrtcSignalingURL = webrtcSignalingURL.trim()
        // Signing out already cleared the token; re-saving the stale field
        // value would silently sign the user back in.
        if (!signedOut) SettingsManager.gatewayToken = gatewayToken.trim()
        onBack()
    }

    BackHandler { saveAndClose() }

    Column(modifier = modifier.fillMaxSize()) {
        TopAppBar(
            title = { Text("Gateway settings") },
            navigationIcon = {
                IconButton(onClick = { saveAndClose() }) {
                    Icon(Icons.AutoMirrored.Filled.ArrowBack, contentDescription = "Back")
                }
            },
        )

        Column(
            modifier = Modifier
                .fillMaxSize()
                .verticalScroll(rememberScrollState())
                .padding(horizontal = 16.dp)
                .navigationBarsPadding(),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            SectionHeader("Gateway")
            if (accountEmail != null && !signedOut) {
                Row(
                    modifier = Modifier.fillMaxWidth(),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Column {
                        Text("Signed in as", style = MaterialTheme.typography.bodySmall,
                            color = MaterialTheme.colorScheme.onSurfaceVariant)
                        Text(accountEmail, style = MaterialTheme.typography.bodyLarge)
                    }
                    TextButton(onClick = {
                        SettingsManager.signOut()
                        signedOut = true
                        onBack()
                    }) { Text("Sign out") }
                }
            }
            MonoTextField(
                value = gatewayBaseUrl,
                onValueChange = { gatewayBaseUrl = it },
                label = "Gateway URL",
                placeholder = "https://gateway.example.com",
                keyboardType = KeyboardType.Uri,
            )
            if (accountEmail == null) {
                MonoTextField(
                    value = gatewayToken,
                    onValueChange = { gatewayToken = it },
                    label = "Access Token",
                    placeholder = "Your gateway access token",
                )
            }

            // Glasses live POV streaming; unrelated to the gateway but equally
            // rarely touched.
            SectionHeader("WebRTC")
            MonoTextField(
                value = webrtcSignalingURL,
                onValueChange = { webrtcSignalingURL = it },
                label = "Signaling URL",
                placeholder = "wss://your-server.example.com",
                keyboardType = KeyboardType.Uri,
            )

            Spacer(modifier = Modifier.height(32.dp))
        }
    }
}

/** OpenClaw direct connection: URL, token, and agent picker. */
@OptIn(ExperimentalMaterial3Api::class)
@Composable
private fun OpenClawSettingsScreen(
    onBack: () -> Unit,
    modifier: Modifier = Modifier,
) {
    var openClawBaseUrl by remember { mutableStateOf(SettingsManager.openClawBaseUrl) }
    var openClawToken by remember { mutableStateOf(SettingsManager.openClawToken) }
    var openClawSessionKey by remember { mutableStateOf(SettingsManager.openClawSessionKey) }
    var selectedAgent by remember { mutableStateOf(OpenClawAgent.fromSessionKey(openClawSessionKey)) }

    fun saveAndClose() {
        SettingsManager.openClawBaseUrl = openClawBaseUrl.trim()
        SettingsManager.openClawToken = openClawToken.trim()
        SettingsManager.openClawSessionKey = selectedAgent.sessionKey
        onBack()
    }

    BackHandler { saveAndClose() }

    Column(modifier = modifier.fillMaxSize()) {
        TopAppBar(
            title = { Text("OpenClaw Connection") },
            navigationIcon = {
                IconButton(onClick = { saveAndClose() }) {
                    Icon(Icons.AutoMirrored.Filled.ArrowBack, contentDescription = "Back")
                }
            },
        )

        Column(
            modifier = Modifier
                .fillMaxSize()
                .verticalScroll(rememberScrollState())
                .padding(horizontal = 16.dp)
                .navigationBarsPadding(),
            verticalArrangement = Arrangement.spacedBy(16.dp),
        ) {
            SectionHeader("Gateway")
            MonoTextField(
                value = openClawBaseUrl,
                onValueChange = { openClawBaseUrl = it },
                label = "OpenClaw URL",
                placeholder = "https://openclaw.wembassy.com",
                keyboardType = KeyboardType.Uri,
            )
            MonoTextField(
                value = openClawToken,
                onValueChange = { openClawToken = it },
                label = "Gateway Token",
                placeholder = "Your OpenClaw gateway token",
            )

            SectionHeader("Agent")
            FooterText("Routes messages to a specific Wembassy agent.")
            OpenClawAgent.entries.forEach { agent ->
                Row(
                    modifier = Modifier
                        .fillMaxWidth()
                        .clickable { selectedAgent = agent }
                        .padding(vertical = 8.dp),
                    horizontalArrangement = Arrangement.SpaceBetween,
                    verticalAlignment = Alignment.CenterVertically,
                ) {
                    Text(
                        text = agent.displayName,
                        style = MaterialTheme.typography.bodyLarge,
                        color = if (selectedAgent == agent) MaterialTheme.colorScheme.primary else MaterialTheme.colorScheme.onSurface,
                    )
                    if (selectedAgent == agent) {
                        Text("✓", color = MaterialTheme.colorScheme.primary)
                    }
                }
            }

            Spacer(modifier = Modifier.height(32.dp))
        }
    }
}

@Composable
internal fun SectionHeader(title: String) {
    Text(
        text = title,
        style = MaterialTheme.typography.titleSmall,
        color = MaterialTheme.colorScheme.primary,
    )
}

@Composable
internal fun FooterText(text: String) {
    Text(
        text = text,
        style = MaterialTheme.typography.bodySmall,
        color = MaterialTheme.colorScheme.onSurfaceVariant,
    )
}

@Composable
private fun MonoTextField(
    value: String,
    onValueChange: (String) -> Unit,
    label: String,
    placeholder: String,
    keyboardType: KeyboardType = KeyboardType.Text,
) {
    OutlinedTextField(
        value = value,
        onValueChange = onValueChange,
        label = { Text(label) },
        placeholder = { Text(placeholder) },
        modifier = Modifier.fillMaxWidth(),
        textStyle = MaterialTheme.typography.bodyMedium.copy(fontFamily = FontFamily.Monospace),
        singleLine = true,
        keyboardOptions = KeyboardOptions(keyboardType = keyboardType),
    )
}
