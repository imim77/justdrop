import SignalingConnection, {
    type ClientInfo,
    type ClientInfoWithoutId,
    type OfferMessage,
    type WsServerMessage,
} from "./serverconn";
import { getDefaultIceServers, handleRemoteCandidate, receiveFiles, sendFiles } from "./webrtc";

type SetupConnectionObject = {
    url: string;
    info: ClientInfoWithoutId;
};

export const store = {
    loopStarted: false,
    proposedClient: null as ClientInfoWithoutId | null,
    signaling: null as SignalingConnection | null,
    client: null as ClientInfo | null,
    peers: [] as ClientInfo[],
    iceServers: [] as RTCIceServer[],
};

export async function setupConnection({ url, info }: SetupConnectionObject) {
    store.proposedClient = info;

    if (store.loopStarted) {
        return;
    }

    store.loopStarted = true;
    void connectionLoop(url);
}

export function stopConnectionLoop() {
    store.loopStarted = false;
    if (store.signaling) {
        store.signaling.close();
    }
}

export async function connectToPeer(peerId: string) {
    if (!store.signaling) {
        throw new Error("signaling is not connected");
    }

    await sendFiles({
        signaling: store.signaling,
        targetPeerId: peerId,
        iceServers: resolveStoreIceServers(),
    });
}

async function connectionLoop(url: string) {
    while (store.loopStarted) {
        try {
            if (!store.proposedClient) {
                throw new Error("missing proposed client info");
            }

            const signaling = await SignalingConnection.connect({
                url,
                info: store.proposedClient,
                onMessage: (message) => {
                    void handleMessage(message);
                },
                onClose: resetConnectionState,
            });

            store.signaling = signaling;
            await signaling.waitUntilClose();
        } catch (error) {
            console.error("Signaling loop error", error);
            resetConnectionState();
        }

        if (!store.loopStarted) {
            break;
        }

        await sleep(1000);
    }
}

async function handleMessage(message: WsServerMessage) {
    switch (message.type) {
        case "HELLO":
            store.client = message.client;
            store.peers = message.peers;
            store.iceServers =
                message.iceServers && message.iceServers.length > 0
                    ? message.iceServers
                    : getDefaultIceServers();
            return;
        case "JOIN":
            upsertPeer(message.peer);
            return;
        case "UPDATE":
            upsertPeer(message.peer);
            return;
        case "LEFT":
            store.peers = store.peers.filter((peer) => peer.id !== message.peerId);
            return;
        case "OFFER":
            await acceptOffer(message);
            return;
        case "CANDIDATE":
            await handleRemoteCandidate(message);
            return;
        case "ANSWER":
        case "ERROR":
            return;
    }
}

async function acceptOffer(offer: OfferMessage) {
    if (!store.signaling) {
        return;
    }

    try {
        await receiveFiles({
            signaling: store.signaling,
            offer,
            iceServers: resolveStoreIceServers(),
        });
    } catch (error) {
        console.error("Failed to accept offer", error);
    }
}

function upsertPeer(peer: ClientInfo) {
    const index = store.peers.findIndex((candidate) => candidate.id === peer.id);
    if (index === -1) {
        store.peers = [...store.peers, peer];
        return;
    }

    const peers = [...store.peers];
    peers[index] = peer;
    store.peers = peers;
}

function resetConnectionState() {
    store.signaling = null;
    store.client = null;
    store.peers = [];
    store.iceServers = [];
}

function sleep(ms: number) {
    return new Promise<void>((resolve) => {
        window.setTimeout(resolve, ms);
    });
}

function resolveStoreIceServers(): RTCIceServer[] {
    if (store.iceServers.length > 0) {
        return store.iceServers;
    }
    return getDefaultIceServers();
}
