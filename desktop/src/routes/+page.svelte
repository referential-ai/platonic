<script lang="ts">
	import { onDestroy, onMount } from 'svelte';
	import { FolderOpen, RefreshCw, SquarePen } from '@lucide/svelte';
	import {
		bootstrap,
		cancelRun,
		decideApproval,
		listSessions,
		pickWorkspace,
		pollRun,
		readSession,
		recoverRun,
		submitMessage
	} from '$lib/bridge';
	import ApprovalModal from '$lib/components/ApprovalModal.svelte';
	import Composer from '$lib/components/Composer.svelte';
	import SessionList from '$lib/components/SessionList.svelte';
	import Transcript from '$lib/components/Transcript.svelte';
	import { Button } from '$lib/components/ui/button';
	import type {
		ApprovalAction,
		BootstrapView,
		DesktopError,
		DesktopEvent,
		DesktopEventPage,
		DesktopPendingApproval,
		DesktopRun,
		DesktopTranscript,
		RunState
	} from '$lib/desktop';
	import {
		applyRunEvent,
		applyRunStatus,
		applyTypedSnapshot,
		createRunState,
		type ExactRunState,
		type RunEvent
	} from '$lib/run-state';

	type ReadyBootstrap = Extract<BootstrapView, { state: 'ready' }>;
	type Screen =
		| { state: 'loading' }
		| { state: 'needs_workspace'; reason: string | null }
		| { state: 'unavailable'; code: string; message: string }
		| { state: 'ready'; data: ReadyBootstrap };

	let screen = $state<Screen>({ state: 'loading' });
	let selectedSessionId: string | null = $state(null);
	let loadingSessionId: string | null = $state(null);
	let transcript: DesktopTranscript | null = $state(null);
	let liveRun: ExactRunState | null = $state(null);
	let liveSessionIndex: number | null = $state(null);
	let pendingApproval: DesktopPendingApproval | null = $state(null);
	let composerMessage = $state('');
	let actionError: string | null = $state(null);
	let persistentActionError = false;
	let approvalError: string | null = $state(null);
	let selectingWorkspace = $state(false);
	let submitting = $state(false);
	let decidingApproval = $state(false);
	let canceling = $state(false);
	let workspaceGeneration = 0;
	let pickerGeneration = 0;
	let selectionGeneration = 0;
	let submissionGeneration = 0;
	let pollTimer: ReturnType<typeof setTimeout> | null = null;
	let daemonTimer: ReturnType<typeof setTimeout> | null = null;

	const workspaceName = $derived(
		screen.state === 'ready'
			? screen.data.workspaceRoot.split(/[\\/]/).filter(Boolean).at(-1) || screen.data.workspaceRoot
			: null
	);
	const displayRuns = $derived.by(() => {
		const currentLive = liveRun as ExactRunState | null;
		const runs = transcript?.runs.map((run) => {
			const state =
				currentLive?.runId === run.runId
					? currentLive
					: applyTypedSnapshot(createRunState(run.runId, run.status), run);
			return { runId: run.runId, sessionIndex: run.sessionIndex, status: state.status, entries: state.entries };
		}) ?? [];
		if (currentLive && !runs.some((run) => run.runId === currentLive.runId)) {
			runs.push({
				runId: currentLive.runId,
				sessionIndex: liveSessionIndex ?? 0,
				status: currentLive.status,
				entries: currentLive.entries
			});
		}
		return runs;
	});
	const latestStatus = $derived.by(
		() => (liveRun as ExactRunState | null)?.status ?? displayRuns.at(-1)?.status ?? null
	);
	const activeRunId = $derived.by(() => {
		const currentLive = liveRun as ExactRunState | null;
		return currentLive && isActive(currentLive.status) ? currentLive.runId : null;
	});
	const composerDisabled = $derived(
		loadingSessionId !== null || submitting || (latestStatus !== null && isActive(latestStatus))
	);

	onMount(() => {
		void loadBootstrap();
	});

	onDestroy(() => {
		workspaceGeneration += 1;
		pickerGeneration += 1;
		clearDaemonCheck();
		invalidateSelection();
	});

	function asDesktopError(error: unknown): DesktopError {
		if (
			typeof error === 'object' &&
			error !== null &&
			'code' in error &&
			'message' in error &&
			typeof error.code === 'string' &&
			typeof error.message === 'string'
		) {
			return { code: error.code, message: error.message };
		}
		return {
			code: 'desktop_error',
			message: error instanceof Error ? error.message : String(error)
		};
	}

	function isActive(status: RunState): boolean {
		return status === 'running' || status === 'cancel_requested';
	}

	function showUnavailable(failure: DesktopError): void {
		workspaceGeneration += 1;
		clearDaemonCheck();
		invalidateSelection();
		resetChat();
		screen = { state: 'unavailable', code: failure.code, message: failure.message };
	}

	function leaveReadyOnFailure(failure: DesktopError): boolean {
		if (failure.code !== 'daemon_unavailable' && failure.code !== 'desktop_already_open') {
			return false;
		}
		showUnavailable(failure);
		return true;
	}

	function isCurrent(workspace: number, selection: number, runId?: string): boolean {
		return (
			workspace === workspaceGeneration &&
			selection === selectionGeneration &&
			(runId === undefined || liveRun?.runId === runId)
		);
	}

	function clearPoll(): void {
		if (pollTimer !== null) clearTimeout(pollTimer);
		pollTimer = null;
	}

	function clearDaemonCheck(): void {
		if (daemonTimer !== null) clearTimeout(daemonTimer);
		daemonTimer = null;
	}

	function invalidateSelection(): number {
		clearPoll();
		selectionGeneration += 1;
		submissionGeneration += 1;
		loadingSessionId = null;
		submitting = false;
		decidingApproval = false;
		canceling = false;
		return selectionGeneration;
	}

	function resetChat(): void {
		selectedSessionId = null;
		transcript = null;
		liveRun = null;
		liveSessionIndex = null;
		pendingApproval = null;
		composerMessage = '';
		clearActionError();
		approvalError = null;
	}

	function clearActionError(): void {
		actionError = null;
		persistentActionError = false;
	}

	function setActionError(message: string, persistent = false): void {
		if (persistentActionError && !persistent) return;
		actionError = message;
		persistentActionError = persistent;
	}

	function initializeReady(result: ReadyBootstrap): void {
		resetChat();
		screen = { state: 'ready', data: result };
		scheduleDaemonCheck(workspaceGeneration, 1_000);
		const initialRunId = result.selectedRun?.runId;
		const initial =
			result.sessions.find((session) => session.runId === initialRunId) ?? result.sessions[0];
		if (initial) void selectSession(initial.sessionId);
	}

	async function loadBootstrap(): Promise<void> {
		const generation = ++workspaceGeneration;
		clearDaemonCheck();
		invalidateSelection();
		resetChat();
		screen = { state: 'loading' };
		try {
			const result = await bootstrap();
			if (generation !== workspaceGeneration) return;
			if (result.state === 'needs_workspace') {
				screen = result;
				return;
			}
			initializeReady(result);
		} catch (error) {
			if (generation === workspaceGeneration) {
				showUnavailable(asDesktopError(error));
			}
		}
	}

	async function chooseWorkspace(): Promise<void> {
		const picker = ++pickerGeneration;
		workspaceGeneration += 1;
		clearDaemonCheck();
		invalidateSelection();
		resetChat();
		screen = { state: 'loading' };
		selectingWorkspace = true;
		try {
			const result = await pickWorkspace();
			if (picker !== pickerGeneration) return;
			if (!result) {
				await loadBootstrap();
				return;
			}
			if (result.state === 'ready') initializeReady(result);
			else screen = result;
		} catch (error) {
			if (picker === pickerGeneration) {
				const failure = asDesktopError(error);
				if (leaveReadyOnFailure(failure)) return;
				showUnavailable(failure);
			}
		} finally {
			if (picker === pickerGeneration) selectingWorkspace = false;
		}
	}

	async function selectSession(sessionId: string): Promise<void> {
		if (screen.state !== 'ready' || sessionId === selectedSessionId) return;
		const workspace = workspaceGeneration;
		const selection = invalidateSelection();
		selectedSessionId = sessionId;
		transcript = null;
		liveRun = null;
		liveSessionIndex = null;
		pendingApproval = null;
		composerMessage = '';
		clearActionError();
		approvalError = null;
		loadingSessionId = sessionId;

		try {
			const loaded = await readSession(sessionId);
			if (!isCurrent(workspace, selection) || selectedSessionId !== sessionId) return;
			transcript = loaded;
			const latest = loaded.runs.at(-1) ?? null;
			if (latest && isActive(latest.status)) {
				liveRun = applyTypedSnapshot(createRunState(latest.runId, latest.status), latest);
				liveSessionIndex = latest.sessionIndex;
				await recoverWithRetry(latest.runId, workspace, selection);
			}
		} catch (error) {
			if (isCurrent(workspace, selection)) {
				const failure = asDesktopError(error);
				if (!leaveReadyOnFailure(failure)) setActionError(failure.message);
			}
		} finally {
			if (isCurrent(workspace, selection) && loadingSessionId === sessionId) {
				loadingSessionId = null;
			}
		}
	}

	function newChat(): void {
		invalidateSelection();
		resetChat();
	}

	async function submitChat(message: string): Promise<void> {
		if (screen.state !== 'ready' || submitting) return;
		const workspace = workspaceGeneration;
		const previousSelection = selectionGeneration;
		const previousSessionId = selectedSessionId;
		const submitToken = ++submissionGeneration;
		submitting = true;
		clearActionError();
		try {
			const result = await submitMessage(message, previousSessionId);
			if (
				workspace !== workspaceGeneration ||
				previousSelection !== selectionGeneration ||
				submitToken !== submissionGeneration
			) return;

			const selection = invalidateSelection();
			selectedSessionId = result.sessionId;
			pendingApproval = null;
			composerMessage = '';
			loadingSessionId = result.sessionId;
			liveSessionIndex = previousSessionId
				? (transcript?.runs.at(-1)?.sessionIndex ?? -1) + 1
				: 0;
			const optimistic: DesktopRun = {
				runId: result.runId,
				sessionIndex: liveSessionIndex,
				status: result.status,
				entries: [{ kind: 'user', text: message }]
			};
			transcript = previousSessionId
				? { runs: [...(transcript?.runs ?? []), optimistic] }
				: { runs: [optimistic] };
			liveRun = applyTypedSnapshot(createRunState(result.runId, result.status), optimistic);

			try {
				const loaded = await readSession(result.sessionId);
				if (isCurrent(workspace, selection, result.runId)) {
					transcript = loaded;
					const run = loaded.runs.find((candidate) => candidate.runId === result.runId);
					if (run) {
						liveRun = applyTypedSnapshot(createRunState(run.runId, run.status), run);
						liveSessionIndex = run.sessionIndex;
					}
				}
			} catch (error) {
				if (isCurrent(workspace, selection, result.runId)) {
					const failure = asDesktopError(error);
					if (!leaveReadyOnFailure(failure)) setActionError(failure.message);
				}
			}

			if (isCurrent(workspace, selection, result.runId)) {
				loadingSessionId = null;
				void refreshSessions(workspace, selection);
				schedulePoll(result.runId, 0, workspace, selection, 0);
			}
		} catch (error) {
			if (
				workspace === workspaceGeneration &&
				previousSelection === selectionGeneration &&
				submitToken === submissionGeneration
			) {
				const failure = asDesktopError(error);
				if (!leaveReadyOnFailure(failure)) setActionError(failure.message);
			}
		} finally {
			if (submitToken === submissionGeneration) submitting = false;
		}
	}

	function schedulePoll(
		runId: string,
		fromOffset: number,
		workspace: number,
		selection: number,
		delay: number
	): void {
		if (!isCurrent(workspace, selection, runId)) return;
		clearPoll();
		pollTimer = setTimeout(() => {
			pollTimer = null;
			void pollExactRun(runId, fromOffset, workspace, selection);
		}, delay);
	}

	function scheduleRecovery(
		runId: string,
		workspace: number,
		selection: number,
		clearRecoveredError: boolean,
		delay: number
	): void {
		if (!isCurrent(workspace, selection, runId)) return;
		clearPoll();
		pollTimer = setTimeout(() => {
			pollTimer = null;
			void recoverWithRetry(runId, workspace, selection, clearRecoveredError);
		}, delay);
	}

	async function pollExactRun(
		runId: string,
		fromOffset: number,
		workspace: number,
		selection: number
	): Promise<void> {
		if (!isCurrent(workspace, selection, runId)) return;
		try {
			const page = await pollRun(runId, fromOffset);
			if (!isCurrent(workspace, selection, runId)) return;
			const approvalChanged = applyEventPage(page);
			if (approvalChanged || !isActive(page.status)) {
				await recoverWithRetry(runId, workspace, selection);
				return;
			}
			if (!persistentActionError) actionError = null;
			const fullPage = page.nextOffset - page.fromOffset >= 128;
			schedulePoll(runId, page.nextOffset, workspace, selection, fullPage ? 0 : 100);
		} catch (error) {
			if (!isCurrent(workspace, selection, runId)) return;
			const failure = asDesktopError(error);
			if (leaveReadyOnFailure(failure)) return;
			if (failure.code === 'lagged') {
				await recoverWithRetry(runId, workspace, selection);
				return;
			}
			setActionError(failure.message);
			schedulePoll(runId, fromOffset, workspace, selection, 500);
		}
	}

	function applyEventPage(page: DesktopEventPage): boolean {
		if (!liveRun || liveRun.runId !== page.runId) return false;
		let state = liveRun;
		let approvalChanged = false;
		for (const event of page.events) {
			if (event.kind === 'approval_requested') {
				approvalChanged = true;
				continue;
			}
			if (event.kind === 'cancel_requested') {
				pendingApproval = null;
				state = applyRunStatus(state, 'cancel_requested');
				continue;
			}
			if (event.kind === 'approval') {
				approvalChanged = true;
				if (
					pendingApproval?.runId === page.runId &&
					pendingApproval.toolCallId === event.callId
				) pendingApproval = null;
			}
			state = applyRunEvent(state, toRunEvent(page.runId, event));
		}
		liveRun = applyRunStatus(state, page.status);
		if (!isActive(liveRun.status)) pendingApproval = null;
		updateSessionStatus(page.runId, liveRun.status);
		return approvalChanged;
	}

	function toRunEvent(
		runId: string,
		event: Exclude<DesktopEvent, { kind: 'approval_requested' | 'cancel_requested' }>
	): RunEvent {
		switch (event.kind) {
			case 'assistant_delta':
				return {
					kind: 'assistant_delta',
					runId,
					offset: event.offset,
					deltaIndex: event.deltaIndex,
					assistantOrdinal: event.step,
					text: event.text
				};
			case 'assistant_committed':
				return { kind: 'assistant', runId, assistantOrdinal: event.step, text: event.text };
			case 'tool_call':
				return {
					kind: 'tool_call',
					runId,
					callId: event.callId,
					tool: event.tool,
					inputPreview: event.inputPreview
				};
			case 'tool_result':
				return { kind: 'tool_result', runId, callId: event.callId, summary: event.summary };
			case 'approval':
				return {
					kind: 'approval',
					runId,
					callId: event.callId,
					decision: event.decision,
					actorId: event.actorId,
					reason: event.reason
				};
			case 'policy_denied':
				return { kind: 'policy_denied', runId, callId: event.callId, reason: event.reason };
			case 'tool_failed':
				return { kind: 'tool_failed', runId, callId: event.callId, error: event.error };
		}
	}

	async function recoverWithRetry(
		runId: string,
		workspace: number,
		selection: number,
		clearRecoveredError = true
	): Promise<void> {
		if (!isCurrent(workspace, selection, runId)) return;
		try {
			await recoverAndPoll(runId, workspace, selection, clearRecoveredError);
		} catch (error) {
			if (!isCurrent(workspace, selection, runId)) return;
			const failure = asDesktopError(error);
			if (leaveReadyOnFailure(failure)) return;
			setActionError(failure.message);
			scheduleRecovery(runId, workspace, selection, clearRecoveredError, 500);
		}
	}

	async function recoverAndPoll(
		runId: string,
		workspace: number,
		selection: number,
		clearRecoveredError = true
	): Promise<void> {
		let approvalChanged = true;
		let nextOffset = 0;
		while (approvalChanged && isCurrent(workspace, selection, runId)) {
			const recovery = await recoverRun(runId);
			if (!isCurrent(workspace, selection, runId)) return;
			replaceTranscriptRun(recovery.run);
			liveRun = applyTypedSnapshot(liveRun ?? createRunState(runId), recovery.run);
			liveSessionIndex = recovery.run.sessionIndex;
			pendingApproval = recovery.pendingApproval;
			approvalError = null;
			approvalChanged = applyEventPage(recovery.page);
			nextOffset = recovery.page.nextOffset;
		}
		if (!isCurrent(workspace, selection, runId) || !liveRun) return;
		if (!isActive(liveRun.status)) {
			pendingApproval = null;
			await refreshSelectedSession(workspace, selection);
			void refreshSessions(workspace, selection);
			return;
		}
		if (clearRecoveredError && !persistentActionError) actionError = null;
		schedulePoll(runId, nextOffset, workspace, selection, 100);
	}

	function replaceTranscriptRun(run: DesktopRun): void {
		const runs = transcript?.runs.filter((candidate) => candidate.runId !== run.runId) ?? [];
		runs.push(run);
		runs.sort((left, right) => left.sessionIndex - right.sessionIndex);
		transcript = { runs };
	}

	async function refreshSelectedSession(workspace: number, selection: number): Promise<void> {
		const sessionId = selectedSessionId;
		if (!sessionId) return;
		try {
			const loaded = await readSession(sessionId);
			if (isCurrent(workspace, selection) && selectedSessionId === sessionId) transcript = loaded;
		} catch (error) {
			if (isCurrent(workspace, selection)) {
				const failure = asDesktopError(error);
				if (!leaveReadyOnFailure(failure)) setActionError(failure.message);
			}
		}
	}

	async function refreshSessions(workspace: number, selection: number): Promise<void> {
		try {
			const sessions = await listSessions();
			if (isCurrent(workspace, selection) && screen.state === 'ready') {
				screen = { state: 'ready', data: { ...screen.data, sessions } };
			}
		} catch (error) {
			if (isCurrent(workspace, selection)) {
				const failure = asDesktopError(error);
				if (!leaveReadyOnFailure(failure)) setActionError(failure.message);
			}
		}
	}

	function scheduleDaemonCheck(workspace: number, delay: number): void {
		clearDaemonCheck();
		if (workspace !== workspaceGeneration || screen.state !== 'ready') return;
		daemonTimer = setTimeout(() => {
			daemonTimer = null;
			void checkDaemon(workspace);
		}, delay);
	}

	async function checkDaemon(workspace: number): Promise<void> {
		if (workspace !== workspaceGeneration || screen.state !== 'ready') return;
		try {
			await listSessions();
		} catch (error) {
			if (workspace !== workspaceGeneration || screen.state !== 'ready') return;
			const failure = asDesktopError(error);
			if (leaveReadyOnFailure(failure)) return;
			setActionError(failure.message);
		}
		if (workspace === workspaceGeneration && screen.state === 'ready') {
			scheduleDaemonCheck(workspace, 1_000);
		}
	}

	function updateSessionStatus(runId: string, status: RunState): void {
		if (screen.state !== 'ready') return;
		screen = {
			state: 'ready',
			data: {
				...screen.data,
				sessions: screen.data.sessions.map((session) =>
					session.runId === runId ? { ...session, status } : session
				)
			}
		};
	}

	async function decide(action: ApprovalAction): Promise<void> {
		if (!pendingApproval || decidingApproval) return;
		const approval = pendingApproval;
		const workspace = workspaceGeneration;
		const selection = selectionGeneration;
		decidingApproval = true;
		clearActionError();
		approvalError = null;
		try {
			await decideApproval(
				approval.runId,
				approval.toolCallId,
				action,
				action === 'deny' ? 'approval denied by desktop client' : null
			);
			if (!isCurrent(workspace, selection, approval.runId)) return;
			await recoverWithRetry(approval.runId, workspace, selection);
		} catch (error) {
			if (!isCurrent(workspace, selection, approval.runId)) return;
			const failure = asDesktopError(error);
			if (leaveReadyOnFailure(failure)) return;
			if (failure.code === 'not_found') {
				setActionError(failure.message, true);
				await recoverWithRetry(approval.runId, workspace, selection, false);
			} else {
				approvalError = failure.message;
			}
		} finally {
			if (isCurrent(workspace, selection, approval.runId)) decidingApproval = false;
		}
	}

	async function requestCancel(): Promise<void> {
		if (!activeRunId || canceling) return;
		const runId = activeRunId;
		const workspace = workspaceGeneration;
		const selection = selectionGeneration;
		canceling = true;
		clearActionError();
		try {
			const result = await cancelRun(runId);
			if (isCurrent(workspace, selection, runId) && liveRun) {
				liveRun = applyRunStatus(liveRun, result.status);
				updateSessionStatus(runId, liveRun.status);
			}
		} catch (error) {
			if (isCurrent(workspace, selection, runId)) {
				const failure = asDesktopError(error);
				if (!leaveReadyOnFailure(failure)) setActionError(failure.message);
			}
		} finally {
			if (isCurrent(workspace, selection, runId)) canceling = false;
		}
	}
</script>

<svelte:head><title>{workspaceName ? `${workspaceName} - Plato Agent` : 'Plato Agent'}</title></svelte:head>

<main>
	<header class="app-header" inert={pendingApproval !== null}>
		<div class="brand" aria-label="Plato Agent">
			<span class="brand-mark">P</span>
			<span>Plato Agent</span>
		</div>
		{#if screen.state === 'ready'}
			<div class="workspace-label" title={screen.data.workspaceRoot}>
				<span class="connection-dot"></span>
				<span>{workspaceName}</span>
			</div>
		{/if}
		<div class="header-actions">
			{#if screen.state === 'ready'}
				<span class="daemon-version">daemon {screen.data.daemonVersion}</span>
			{/if}
			<Button
				variant="ghost"
				size="icon"
				title="Choose workspace"
				aria-label="Choose workspace"
				disabled={selectingWorkspace || screen.state === 'loading'}
				onclick={chooseWorkspace}
			>
				<FolderOpen />
			</Button>
		</div>
	</header>

	{#if screen.state === 'loading'}
		<section class="center-state" aria-live="polite">
			<div class="loading-mark"></div>
			<p>Connecting to Platonic...</p>
		</section>
	{:else if screen.state === 'needs_workspace'}
		<section class="center-state">
			<div class="state-icon"><FolderOpen size={22} /></div>
			<h1>Select a workspace</h1>
			{#if screen.reason}<p class="state-detail">{screen.reason}</p>{/if}
			<Button disabled={selectingWorkspace} onclick={chooseWorkspace}>
				<FolderOpen data-icon="inline-start" />
				{selectingWorkspace ? 'Opening...' : 'Choose folder'}
			</Button>
		</section>
	{:else if screen.state === 'unavailable'}
		<section class="center-state" role="alert">
			<div class="offline-mark"></div>
			<h1>{screen.code === 'desktop_already_open' ? 'Workspace already open' : 'Daemon unavailable'}</h1>
			<p class="state-detail">{screen.message}</p>
			<div class="state-actions">
				<Button onclick={loadBootstrap}><RefreshCw data-icon="inline-start" />Reconnect</Button>
				<Button variant="outline" disabled={selectingWorkspace} onclick={chooseWorkspace}>
					<FolderOpen data-icon="inline-start" />Choose folder
				</Button>
			</div>
		</section>
	{:else}
		<div class="app-shell" inert={pendingApproval !== null}>
			<aside>
				<div class="pane-title">
					<div>
						<h1>Chats</h1>
						<span>{screen.data.sessions.length}</span>
					</div>
					<button class="new-chat" type="button" title="New chat" aria-label="New chat" onclick={newChat}>
						<SquarePen size={16} />
					</button>
				</div>
				{#if screen.data.sessions.length === 0}
					<p class="empty-list">No chats yet.</p>
				{:else}
					<SessionList
						sessions={screen.data.sessions}
						{selectedSessionId}
						{loadingSessionId}
						onselect={selectSession}
					/>
				{/if}
			</aside>
			<section class="run-pane">
				<header class="run-header">
					<div>
						<h2>{selectedSessionId ? 'Chat' : 'New chat'}</h2>
						{#if selectedSessionId}<span>{selectedSessionId}</span>{/if}
					</div>
					{#if latestStatus}
						<span class={`run-status status-${latestStatus}`}>{latestStatus.replace('_', ' ')}</span>
					{/if}
				</header>
				<div class="chat-body">
					{#if loadingSessionId}
						<div class="loading-run" aria-live="polite">Loading chat...</div>
					{:else if displayRuns.length > 0}
						<Transcript runs={displayRuns} {activeRunId} />
					{:else}
						<div class="empty-run-pane">
							{selectedSessionId ? 'This chat has no transcript.' : 'Start a new chat.'}
						</div>
					{/if}
				</div>
				<Composer
					bind:message={composerMessage}
					disabled={composerDisabled}
					{submitting}
					activeStatus={latestStatus}
					canCancel={activeRunId !== null && loadingSessionId === null && !canceling && latestStatus !== 'cancel_requested'}
					error={actionError}
					onsubmit={submitChat}
					oncancel={requestCancel}
				/>
			</section>
		</div>
		{#if pendingApproval}
			<ApprovalModal
				approval={pendingApproval}
				busy={decidingApproval}
				error={approvalError}
				ongrant={() => void decide('grant')}
				ondeny={() => void decide('deny')}
			/>
		{/if}
	{/if}
</main>
