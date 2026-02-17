import type SignalingConnection from "./serverconn";
import type { CandidateMessage, OfferMessage } from "./serverconn";

export const defaultStun = [
    "stun:stun.l.google.com:19302",
    "stun:stun1.l.google.com:19302",
    "stun:stun.cloudflare.com:3478",
];

const sessions = new Map<string, SessionState>();
const earlyRemoteCandidates = new Map<string, (RTCIceCandidateInit | null)[]>();

export async function sendFiles({ signaling, targetPeerId, iceServers }: SendFilesObject) {
    if (!targetPeerId) {
        throw new Error("targetPeerId is required");
    }

    const resolvedIceServers = resolveIceServers(iceServers);
    try {
        await sendFilesAttempt({ signaling, targetPeerId, iceServers: resolvedIceServers, policy: "all" });
    } catch (error) {
        if (!hasTurnServer(resolvedIceServers)) {
            throw error;
        }

        console.warn("Initial ICE attempt failed; retrying with TURN relay-only policy", error);
        await sendFilesAttempt({ signaling, targetPeerId, iceServers: resolvedIceServers, policy: "relay" });
    }
}

type SendFilesAttemptObject = {
    signaling: SignalingConnection;
    targetPeerId: string;
    iceServers: RTCIceServer[];
    policy: RTCIceTransportPolicy;
};

async function sendFilesAttempt({ signaling, targetPeerId, iceServers, policy }: SendFilesAttemptObject) {
    const sessionId = createSessionId();
    const peerConnection = await createPeerConnection(iceServers, policy);
    registerSession(sessionId, peerConnection);
    attachCandidateRelay(peerConnection, signaling, sessionId, targetPeerId);

    try {
        const dataChannel = peerConnection.createDataChannel("data");
        dataChannel.binaryType = "arraybuffer";
        const dataChannelOpened = waitDataChannelOpen(dataChannel);
        const connected = waitPeerConnected(peerConnection, iceServers);

        console.log("Creating offer...");
        const offer = await peerConnection.createOffer();
        await peerConnection.setLocalDescription(offer);

        const localSdp = peerConnection.localDescription?.sdp;
        if (!localSdp) {
            throw new Error("missing local SDP for offer");
        }

        signaling.send({
            type: "OFFER",
            sessionId,
            target: targetPeerId,
            sdp: localSdp,
        });

        console.log("Waiting for answer...");
        const answer = await signaling.waitForAnswer(sessionId);
        if (!answer.sdp) {
            throw new Error("received empty SDP answer");
        }

        await peerConnection.setRemoteDescription({
            type: "answer",
            sdp: answer.sdp,
        });

        await flushPendingCandidates(sessionId);
        await Promise.all([dataChannelOpened, connected]);

        console.log("Data channel opened");
    } catch (error) {
        sessions.delete(sessionId);
        peerConnection.close();
        throw error;
    }
}

export type ReceiveFilesObject = {
    signaling: SignalingConnection;
    offer: OfferMessage;
    iceServers?: RTCIceServer[];
};

export async function receiveFiles({ signaling, offer, iceServers }: ReceiveFilesObject) {
    console.log("Accepting offer from", offer.peer.id);

    const resolvedIceServers = resolveIceServers(iceServers);
    const peerConnection = await createPeerConnection(resolvedIceServers, "all");
    registerSession(offer.sessionId, peerConnection);
    attachCandidateRelay(peerConnection, signaling, offer.sessionId, offer.peer.id);

    const dataChannelPromise = new Promise<RTCDataChannel>((resolve) => {
        peerConnection.ondatachannel = (event) => {
            resolve(event.channel);
        };
    });

    const connected = waitPeerConnected(peerConnection, resolvedIceServers);

    await peerConnection.setRemoteDescription({
        type: "offer",
        sdp: offer.sdp,
    });

    await flushPendingCandidates(offer.sessionId);

    console.log("Creating answer...");
    const answer = await peerConnection.createAnswer();
    await peerConnection.setLocalDescription(answer);

    const localSdp = peerConnection.localDescription?.sdp;
    if (!localSdp) {
        throw new Error("missing local SDP for answer");
    }

    signaling.send({
        type: "ANSWER",
        sessionId: offer.sessionId,
        target: offer.peer.id,
        sdp: localSdp,
    });

    const dataChannel = await dataChannelPromise;
    dataChannel.binaryType = "arraybuffer";
    await waitDataChannelOpen(dataChannel);
    await connected;

    console.log("Receiver data channel opened");
}

export async function handleRemoteCandidate(message: CandidateMessage) {
    const session = sessions.get(message.sessionId);
    if (!session) {
        const queued = earlyRemoteCandidates.get(message.sessionId) ?? [];
        queued.push(message.candidate);
        earlyRemoteCandidates.set(message.sessionId, queued);
        return;
    }

    if (!session.connection.remoteDescription) {
        session.pendingCandidates.push(message.candidate);
        return;
    }

    try {
        await session.connection.addIceCandidate(message.candidate);
    } catch (error) {
        console.error("Failed to add ICE candidate", error);
    }
}

export const recieveFiles = receiveFiles;

export type SendFilesObject = {
    signaling: SignalingConnection;
    targetPeerId: string;
    iceServers?: RTCIceServer[];
};

type SessionState = {
    connection: RTCPeerConnection;
    pendingCandidates: (RTCIceCandidateInit | null)[];
};

export function getDefaultIceServers(): RTCIceServer[] {
    const servers: RTCIceServer[] = [{ urls: defaultStun }];

    const turnUrls = splitCsv(import.meta.env.VITE_TURN_URLS);
    const turnUsername = import.meta.env.VITE_TURN_USERNAME as string | undefined;
    const turnCredential = import.meta.env.VITE_TURN_CREDENTIAL as string | undefined;

    if (turnUrls.length > 0) {
        const turnServer: RTCIceServer = {
            urls: turnUrls,
        };

        if (turnUsername) {
            turnServer.username = turnUsername;
        }
        if (turnCredential) {
            turnServer.credential = turnCredential;
        }

        servers.push(turnServer);
    }

    return servers;
}

function resolveIceServers(iceServers?: RTCIceServer[]): RTCIceServer[] {
    if (iceServers && iceServers.length > 0) {
        return normalizeIceServers(iceServers);
    }

    return normalizeIceServers(getDefaultIceServers());
}

async function createPeerConnection(iceServers: RTCIceServer[], iceTransportPolicy: RTCIceTransportPolicy) {
    const peerConnection = new RTCPeerConnection({ iceServers, iceTransportPolicy });

    peerConnection.onicecandidateerror = (event) => {
        console.error("ICE candidate error:", event);
    };

    peerConnection.oniceconnectionstatechange = () => {
        console.log("ICE connection state change", peerConnection.iceConnectionState, iceServers);
    };

    return peerConnection;
}

function createSessionId() {
    if (typeof crypto !== "undefined" && typeof crypto.randomUUID === "function") {
        return crypto.randomUUID();
    }

    return Math.random().toString(36).slice(2) + Date.now().toString(36);
}

function attachCandidateRelay(
    peerConnection: RTCPeerConnection,
    signaling: SignalingConnection,
    sessionId: string,
    targetPeerId: string,
) {
    peerConnection.onicecandidate = (event) => {
        const candidate = event.candidate ? event.candidate.toJSON() : null;
        signaling.send({
            type: "CANDIDATE",
            sessionId,
            target: targetPeerId,
            candidate,
        });
    };
}

function registerSession(sessionId: string, connection: RTCPeerConnection) {
    const session: SessionState = {
        connection,
        pendingCandidates: [],
    };

    sessions.set(sessionId, session);

    const early = earlyRemoteCandidates.get(sessionId);
    if (early && early.length > 0) {
        session.pendingCandidates.push(...early);
        earlyRemoteCandidates.delete(sessionId);
    }

    connection.addEventListener("connectionstatechange", () => {
        if (
            connection.connectionState === "closed" ||
            connection.connectionState === "failed" ||
            connection.connectionState === "disconnected"
        ) {
            sessions.delete(sessionId);
        }
    });
}

async function flushPendingCandidates(sessionId: string) {
    const session = sessions.get(sessionId);
    if (!session || !session.connection.remoteDescription) {
        return;
    }

    while (session.pendingCandidates.length > 0) {
        const candidate = session.pendingCandidates.shift();
        if (candidate === undefined) {
            continue;
        }

        try {
            await session.connection.addIceCandidate(candidate);
        } catch (error) {
            console.error("Failed to apply buffered ICE candidate", error);
        }
    }
}

function waitDataChannelOpen(dataChannel: RTCDataChannel) {
    if (dataChannel.readyState === "open") {
        return Promise.resolve();
    }

    return new Promise<void>((resolve, reject) => {
        const timeoutId = window.setTimeout(() => {
            reject(new Error("timed out waiting for data channel"));
        }, 30000);

        dataChannel.onopen = () => {
            window.clearTimeout(timeoutId);
            resolve();
        };
    });
}

function waitPeerConnected(peerConnection: RTCPeerConnection, iceServers: RTCIceServer[]) {
    if (peerConnection.connectionState === "connected") {
        return Promise.resolve();
    }

    return new Promise<void>((resolve, reject) => {
        const timeoutId = window.setTimeout(() => {
            reject(new Error(`timed out waiting for peer connection (iceServers=${JSON.stringify(iceServers)})`));
        }, 30000);

        peerConnection.addEventListener("connectionstatechange", () => {
            if (peerConnection.connectionState === "connected") {
                window.clearTimeout(timeoutId);
                resolve();
            }

            if (peerConnection.connectionState === "failed") {
                window.clearTimeout(timeoutId);
                reject(
                    new Error(
                        `peer connection failed (iceState=${peerConnection.iceConnectionState}). ` +
                            "Direct path failed; verify TURN settings from signaling HELLO or client overrides",
                    ),
                );
            }
        });
    });
}

function splitCsv(input: string | undefined): string[] {
    if (!input) {
        return [];
    }

    return input
        .split(",")
        .map((entry) => entry.trim())
        .filter((entry) => entry.length > 0);
}

function normalizeIceServers(iceServers: RTCIceServer[]): RTCIceServer[] {
    return iceServers.map((server) => ({
        ...server,
        urls: Array.isArray(server.urls) ? [...server.urls] : server.urls,
    }));
}

function hasTurnServer(iceServers: RTCIceServer[]): boolean {
    return iceServers.some((server) => {
        const urls = Array.isArray(server.urls) ? server.urls : [server.urls];
        return urls.some((url) => typeof url === "string" && url.toLowerCase().startsWith("turn:"));
    });
}
