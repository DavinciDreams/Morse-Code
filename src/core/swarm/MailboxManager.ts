import type { TeammateMessage } from "@roo-code/types"

import { InMemoryMailbox } from "./InMemoryMailbox"

/**
 * Manages one InMemoryMailbox per swarm session.
 * A session's mailbox is only created when the session opts in to persistent
 * workers (`persistent: true` on spawnConcurrentChildren).
 *
 * All public methods are no-ops when the session has no mailbox, so callers
 * don't need to guard against missing sessions.
 */
export class MailboxManager {
	private mailboxes = new Map<string, InMemoryMailbox>()

	createMailbox(sessionId: string): void {
		if (!this.mailboxes.has(sessionId)) {
			this.mailboxes.set(sessionId, new InMemoryMailbox())
		}
	}

	getMailbox(sessionId: string): InMemoryMailbox | undefined {
		return this.mailboxes.get(sessionId)
	}

	/**
	 * Called by a worker when it finishes a turn.
	 * Stores an `idle_notification` in the worker's own queue so the leader's
	 * `on(WorkerIdle)` handler can read it if needed.
	 */
	async notifyIdle(
		sessionId: string,
		workerId: string,
		summary: string,
		payload?: Record<string, unknown>,
	): Promise<void> {
		const mailbox = this.mailboxes.get(sessionId)
		if (!mailbox) return
		await mailbox.send(`leader:${sessionId}`, {
			type: "idle_notification",
			from: workerId,
			to: `leader:${sessionId}`,
			payload: { workerId, summary, ...payload },
			ts: Date.now(),
		})
	}

	/** Leader assigns a new task to an idle worker. */
	async assignTask(sessionId: string, workerId: string, message: string): Promise<void> {
		const mailbox = this.mailboxes.get(sessionId)
		if (!mailbox) throw new Error(`[MailboxManager] No mailbox for session "${sessionId}"`)
		await mailbox.send(workerId, {
			type: "task_assignment",
			from: `leader:${sessionId}`,
			to: workerId,
			payload: { message },
			ts: Date.now(),
		})
	}

	/** Leader tells a worker to stop after its current idle period. */
	async shutdownWorker(sessionId: string, workerId: string): Promise<void> {
		const mailbox = this.mailboxes.get(sessionId)
		if (!mailbox) return
		await mailbox.send(workerId, {
			type: "shutdown_request",
			from: `leader:${sessionId}`,
			to: workerId,
			ts: Date.now(),
		})
	}

	/**
	 * Waits for the worker's next `task_assignment` or `shutdown_request`.
	 * Returns null if the timeout fires first or the session has no mailbox.
	 */
	async waitForNextMessage(
		sessionId: string,
		workerId: string,
		opts?: { timeoutMs?: number },
	): Promise<TeammateMessage | null> {
		const mailbox = this.mailboxes.get(sessionId)
		if (!mailbox) return null
		return mailbox.waitForMessage(workerId, ["task_assignment", "shutdown_request"], opts)
	}

	destroyMailbox(sessionId: string): void {
		this.mailboxes.get(sessionId)?.dispose()
		this.mailboxes.delete(sessionId)
	}

	dispose(): void {
		for (const mailbox of this.mailboxes.values()) mailbox.dispose()
		this.mailboxes.clear()
	}
}
