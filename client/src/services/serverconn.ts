export default class SignalingConnection {
    private _socket: WebSocket;
    private _pendingAnswers: Map<string, PendingAnswer> = new Map();

    private constructor(socket: WebSocket) {
        this._socket = socket;
    }

    static async connect({ url, info, onMessage, onClose }: ConnectObject): Promise<SignalingConnection> {
        console.log(`Connecting to ${url}`);

        const socket = await new Promise<WebSocket>((resolve, reject) => {
            const ws = new WebSocket(url);
            ws.onopen = () => resolve(ws);
            ws.onerror = (event) => reject(event);
        });

        console.log("Signaling connection established");
        const instance = new SignalingConnection(socket);

        socket.onmessage = (event) => {
            let message: WsServerMessage;
            try {
                message = JSON.parse(event.data) as WsServerMessage;
            } catch (error) {
                console.error("Failed to parse signaling message", error);
                return;
            }

            console.log(`WS in: ${event.data}`);

            if (message.type === "ANSWER") {
                const pending = instance._pendingAnswers.get(message.sessionId);
                if (pending) {
                    instance._pendingAnswers.delete(message.sessionId);
                    window.clearTimeout(pending.timeoutId);
                    pending.resolve(message);
                }
            }

            onMessage(message);
        };

        socket.onclose = () => {
            console.log("Signaling connection closed");
            instance.rejectPendingAnswers();
            if (onClose) {
                onClose();
            }
        };

        const normalizedInfo = normalizeClientInfo(info);
        if (normalizedInfo) {
            instance.send({
                type: "UPDATE",
                info: normalizedInfo,
            });
        }

        return instance;
    }

    send(message: WsClientMessage) {
        const encoded = JSON.stringify(message);
        console.log(`WS out: ${encoded}`);
        this._socket.send(encoded);
    }

    waitForAnswer(sessionId: string, timeoutMs: number = 30000): Promise<AnswerMessage> {
        return new Promise<AnswerMessage>((resolve, reject) => {
            if (this._pendingAnswers.has(sessionId)) {
                reject(new Error(`already waiting for answer on session ${sessionId}`));
                return;
            }

            const timeout = window.setTimeout(() => {
                this._pendingAnswers.delete(sessionId);
                reject(new Error(`timed out waiting for answer on session ${sessionId}`));
            }, timeoutMs);

            this._pendingAnswers.set(sessionId, {
                timeoutId: timeout,
                resolve,
                reject,
            });
        });
    }

    waitUntilClose() {
        return new Promise<void>((resolve) => {
            this._socket.addEventListener("close", () => {
                resolve();
            });
        });
    }

    close() {
        this._socket.close();
    }

    private rejectPendingAnswers() {
        for (const pending of this._pendingAnswers.values()) {
            window.clearTimeout(pending.timeoutId);
            pending.reject(new Error("signaling connection closed"));
        }
        this._pendingAnswers.clear();
    }
}

type ConnectObject = {
    url: string;
    info?: ClientInfoWithoutId | string;
    onMessage: OnMessageCallback;
    onClose?: () => void;
};

type OnMessageCallback = (message: WsServerMessage) => void;

type PendingAnswer = {
    timeoutId: number;
    resolve: (message: AnswerMessage) => void;
    reject: (reason?: unknown) => void;
};

export type ClientInfoWithoutId = {
    alias: string;
    deviceModel?: string;
    deviceType?: string;
    token?: string;
};

export type ClientInfo = ClientInfoWithoutId & { id: string };

export type WsClientMessage = WsClientSdpMessage | WsClientCandidateMessage | WsClientUpdateMessage;

type WsClientSdpMessage = {
    type: "OFFER" | "ANSWER";
    sessionId: string;
    target: string;
    sdp: string;
};

type WsClientCandidateMessage = {
    type: "CANDIDATE";
    sessionId: string;
    target: string;
    candidate: RTCIceCandidateInit | null;
};

type WsClientUpdateMessage = {
    type: "UPDATE";
    info: ClientInfoWithoutId;
};

export type WsServerMessage =
    | HelloMessage
    | JoinMessage
    | LeftMessage
    | UpdateMessage
    | OfferMessage
    | AnswerMessage
    | CandidateMessage
    | ErrorMessage;

export type HelloMessage = {
    type: "HELLO";
    client: ClientInfo;
    peers: ClientInfo[];
    iceServers?: IceServerInfo[];
};

export type JoinMessage = {
    type: "JOIN";
    peer: ClientInfo;
};

export type UpdateMessage = {
    type: "UPDATE";
    peer: ClientInfo;
};

export type LeftMessage = {
    type: "LEFT";
    peerId: string;
};

export type WsServerSdpMessage = {
    peer: ClientInfo;
    sessionId: string;
    sdp: string;
};

export type OfferMessage = WsServerSdpMessage & { type: "OFFER" };
export type AnswerMessage = WsServerSdpMessage & { type: "ANSWER" };

export type CandidateMessage = {
    type: "CANDIDATE";
    peer: ClientInfo;
    sessionId: string;
    candidate: RTCIceCandidateInit | null;
};

export type ErrorMessage = {
    type: "ERROR";
    code: number;
};

export type IceServerInfo = {
    urls: string[];
    username?: string;
    credential?: string;
};

function normalizeClientInfo(info?: ClientInfoWithoutId | string): ClientInfoWithoutId | null {
    if (!info) {
        return null;
    }

    if (typeof info === "string") {
        const alias = info.trim();
        if (alias.length === 0) {
            return null;
        }
        return { alias };
    }

    return info;
}
