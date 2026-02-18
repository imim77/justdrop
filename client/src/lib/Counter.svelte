<script lang="ts">
  import { onMount } from "svelte";
  import SignalingConnection, { type ClientInfo, type WsServerMessage } from "../services/serverconn";
  import { getDefaultIceServers, handleRemoteCandidate, receiveFiles, sendFiles } from "../services/webrtc";
  import { FileChunker, FileDigester, type ReceivedFile } from "../services/files";

  let connection = $state<SignalingConnection | null>(null);
  let peers = $state<ClientInfo[]>([]);
  let myId = $state("Connecting...");
  let status = $state("Offline");
  let alias = $state(`peer-${Math.random().toString(36).slice(2, 7)}`);
  let serverIceServers = $state<RTCIceServer[]>([]);

  const connectingPeerIds = new Set<string>();
  const connectedPeerIds = new Set<string>();

  onMount(() => {
    void connect();

    return () => {
      if (connection) {
        connection.close();
      } 
    };
  });

  async function connect() {
    try {
      connection = await SignalingConnection.connect({
        url: `ws://${location.hostname}:9000/ws`,
        info: { alias },
        onMessage: (message) => {
          void handleMessage(message);
        },
        onClose: () => {
          status = "Offline";
          myId = "Disconnected";
          peers = [];
          serverIceServers = [];
          connectingPeerIds.clear();
          connectedPeerIds.clear();
          connection = null;
        },
      });
      status = "Online";
    } catch (error) {
      console.error("Failed to connect signaling", error);
      status = "Offline";
    }
  }

  async function handleMessage(message: WsServerMessage) {
    switch (message.type) {
      case "HELLO":
        myId = message.client.id;
        peers = message.peers;
        serverIceServers =
          message.iceServers && message.iceServers.length > 0
            ? message.iceServers
            : getDefaultIceServers();
        for (const peer of message.peers) {
          void maybeAutoConnect(peer);
        }
        return;
      case "JOIN":
        upsertPeer(message.peer);
        void maybeAutoConnect(message.peer);
        return;
      case "UPDATE":
        upsertPeer(message.peer);
        void maybeAutoConnect(message.peer);
        return;
      case "LEFT":
        peers = peers.filter((peer) => peer.id !== message.peerId);
        connectingPeerIds.delete(message.peerId);
        connectedPeerIds.delete(message.peerId);
        return;
      case "OFFER":
        if (!connection) {
          return;
        }
        try {
          await receiveFiles({
            signaling: connection,
            offer: message,
            iceServers: resolveIceServers(),
          });
          connectingPeerIds.delete(message.peer.id);
          connectedPeerIds.add(message.peer.id);
        } catch (error) {
          console.error("Failed to process offer", error);
        }
        return;
      case "CANDIDATE":
        await handleRemoteCandidate(message);
        return;
      case "ANSWER":
        return;
      case "ERROR":
        console.error("Signaling server error", message.code);
        return;
    }
  }

  function upsertPeer(peer: ClientInfo) {
    if (peer.id === myId) {
      return;
    }

    const index = peers.findIndex((currentPeer) => currentPeer.id === peer.id);
    if (index === -1) {
      peers = [...peers, peer];
      return;
    }

    const updated = [...peers];
    updated[index] = peer;
    peers = updated;
  }

  async function maybeAutoConnect(peer: ClientInfo) {
    if (!connection || !myId || peer.id === myId) {
      return;
    }

    if (!shouldInitiateForPeer(peer.id)) {
      return;
    }

    if (connectingPeerIds.has(peer.id) || connectedPeerIds.has(peer.id)) {
      return;
    }

    connectingPeerIds.add(peer.id);
    try {
      await sendFiles({
        signaling: connection,
        targetPeerId: peer.id,
        iceServers: resolveIceServers(),
      });
      connectingPeerIds.delete(peer.id);
      connectedPeerIds.add(peer.id);
      console.log("Peer connection established as sender", peer.id);
    } catch (error) {
      connectingPeerIds.delete(peer.id);
      console.error("Failed to establish sender connection", error);
    }
  }

  function resolveIceServers(): RTCIceServer[] {
    return [...(serverIceServers.length > 0 ? serverIceServers : getDefaultIceServers())];
  }

  function shouldInitiateForPeer(peerId: string): boolean {
    if (!myId || myId === "Connecting..." || myId === "Disconnected") {
      return false;
    }
    return myId < peerId;
  }



</script>

<div class="container">
  <p>Signaling status: {status}</p>
  <p>My ID: <code>{myId}</code></p>
  <p>Auto-connect: enabled</p>

  <h3>Peers</h3>
  {#if peers.length === 0}
    <p>No peers connected yet.</p>
  {:else}
    <ul>
      {#each peers as peer}
        <li>
          {peer.alias || "anonymous"} - <code>{peer.id}</code>
        </li>
      {/each}
    </ul>
  {/if}
</div>



<style>
  .container {
    padding: 1rem;
    border: 1px solid #ccc;
    border-radius: 8px;
  }

  ul {
    margin: 0;
    padding-left: 1.2rem;
  }

  li {
    margin-top: 0.5rem;
  }

</style>
