

export class FileChunker {
    private chunkSize: number;
    private maxPartitionSize: number;
    private offset = 0;
    private partitionSize = 0;
    readonly file: File;

    constructor(file: File) {
        this.file = file;
        this.chunkSize = 64000;
        this.maxPartitionSize = 1e6;
    }

    get progress(): number {
        if (this.file.size === 0) {
            return 1;
        }
        return this.offset / this.file.size;
    }

    get isFileEnd(): boolean {
        return this.offset >= this.file.size;
    }

    get isPartitionEnd(): boolean {
        return this.partitionSize >= this.maxPartitionSize;
    }

    readChunk(): Promise<ArrayBuffer> {
        return new Promise((resolve, reject) => {
            const reader = new FileReader();
            const chunk = this.file.slice(this.offset, this.offset + this.chunkSize);

            reader.onload = (event) => {
                const result = event.target?.result;
                if (!(result instanceof ArrayBuffer)) {
                    reject(new Error("failed to read file chunk"));
                    return;
                }
                resolve(result);
            };

            reader.onerror = () => reject(reader.error ?? new Error("failed to read file chunk"));
            reader.readAsArrayBuffer(chunk);
        });
    }

    async *partition(): AsyncGenerator<ArrayBuffer, void, unknown> {
        this.partitionSize = 0;

        while (!this.isFileEnd && !this.isPartitionEnd) {
            const chunk = await this.readChunk();
            this.offset += chunk.byteLength;
            this.partitionSize += chunk.byteLength;
            yield chunk;
        }
    }

    repeatPartition(): void {
        this.offset = Math.max(0, this.offset - this.partitionSize);
    }

    repeatPratition(): void {
        this.repeatPartition();
    }
}

export type FileMeta = {
    size: number;
    mime?: string;
    name: string;
}

export type ReceivedFile = {
    name: string;
    mime: string;
    size: number;
    blob: Blob;
}

export class FileDigester {
    private readonly buffer: ArrayBuffer[] = [];
    private bytesReceivedInternal = 0;
    private settled = false;
    private readonly mime: string;

    private resolveDone!: (file: ReceivedFile) => void;
    private rejectDone!: (reason?: unknown) => void;

    readonly done: Promise<ReceivedFile>;

    constructor(private readonly meta: FileMeta) {
        if (meta.size < 0) {
            throw new Error("meta.size must be >= 0");
        }

        this.mime = meta.mime && meta.mime.length > 0 ? meta.mime : "application/octet-stream";

        this.done = new Promise((resolve, reject) => {
            this.resolveDone = resolve;
            this.rejectDone = reject;
        });

        if (this.meta.size === 0) {
            this.complete();
        }
    }

    get bytesReceived() {
        return this.bytesReceivedInternal;
    }

    get progress() {
        if (this.meta.size === 0) {
            return 1;
        }
        return this.bytesReceivedInternal / this.meta.size;
    }

    get isDone(){
        return this.settled && this.bytesReceivedInternal === this.meta.size;
    }

    unchunk(chunk: ArrayBuffer){
        if (this.settled) {
            return;
        }

        if (chunk.byteLength === 0) {
            return;
        }

        const remaining = this.meta.size - this.bytesReceivedInternal;
        if (remaining <= 0) {
            this.complete();
            return;
        }

        if (chunk.byteLength > remaining) {
            this.abort(new Error("received chunk larger than expected remaining bytes"));
            return;
        }

        this.buffer.push(chunk);
        this.bytesReceivedInternal += chunk.byteLength;

        if (this.bytesReceivedInternal === this.meta.size) {
            this.complete();
        }
    }

    abort(reason?: unknown){
        if (this.settled) {
            return;
        }

        this.settled = true;
        this.rejectDone(reason ?? new Error("file digest aborted"));
    }

    private complete(): void {
        if (this.settled) {
            return;
        }

        this.settled = true;
        const blob = new Blob(this.buffer, { type: this.mime });
        this.resolveDone({
            name: this.meta.name,
            mime: this.mime,
            size: this.meta.size,
            blob,
        });
    }
}
