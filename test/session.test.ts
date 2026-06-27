import { describe, it, expect } from 'vitest';
import crypto from 'crypto';
import { Duplex } from 'stream';
import { createClientSession, createServerSession, YamuxSession, YamuxStream } from '../src/session';
import { createSocketPair } from './helpers';

function readAll(stream: Duplex): Promise<Buffer> {
    return new Promise((resolve, reject) => {
        const chunks: Buffer[] = [];
        stream.on('data', (c: Buffer) => chunks.push(c));
        stream.on('end', () => resolve(Buffer.concat(chunks)));
        stream.on('error', reject);
    });
}

async function newPair(): Promise<{ server: YamuxSession; client: YamuxSession; cleanup: () => void }> {
    const [clientSide, serverSide, cleanup] = await createSocketPair();
    const server = createServerSession(serverSide);
    const client = createClientSession(clientSide);
    server.on('error', () => {});
    client.on('error', () => {});
    return {
        server,
        client,
        cleanup: () => {
            server.close();
            client.close();
            cleanup();
        },
    };
}

describe('yamux session', () => {
    it('openAndAcceptStream transfers small payload', async () => {
        const { server, client, cleanup } = await newPair();
        const payload = Buffer.from('hello gwk yamux');

        const accepted = new Promise<Buffer>((resolve) => {
            client.on('stream', (stream: YamuxStream) => resolve(readAll(stream)));
        });

        const s = server.openStream();
        s.write(payload);
        s.end();

        const got = await accepted;
        expect(got.equals(payload)).toBe(true);
        cleanup();
    });

    it('singleStreamLarge transfers 512KB', async () => {
        const { server, client, cleanup } = await newPair();
        const payload = crypto.randomBytes(512 * 1024);

        const accepted = new Promise<Buffer>((resolve) => {
            client.on('stream', (stream: YamuxStream) => resolve(readAll(stream)));
        });

        const s = server.openStream();
        s.end(payload);

        const got = await accepted;
        expect(got.length).toBe(payload.length);
        expect(got.equals(payload)).toBe(true);
        cleanup();
    });

    it('concurrentStreams20 keep data independent', async () => {
        const { server, client, cleanup } = await newPair();
        const count = 20;
        const payload = crypto.randomBytes(50 * 1024);

        let received = 0;
        const allDone = new Promise<void>((resolve, reject) => {
            client.on('stream', (stream: YamuxStream) => {
                readAll(stream)
                    .then((buf) => {
                        if (!buf.equals(payload)) {
                            reject(new Error('payload mismatch'));
                            return;
                        }
                        received += 1;
                        if (received === count) resolve();
                    })
                    .catch(reject);
            });
        });

        for (let i = 0; i < count; i++) {
            const s = server.openStream();
            s.end(payload);
        }

        await allDone;
        expect(received).toBe(count);
        cleanup();
    });

    it('streamEnd fires end on remote side', async () => {
        const { server, client, cleanup } = await newPair();

        const ended = new Promise<void>((resolve) => {
            client.on('stream', (stream: YamuxStream) => {
                stream.on('data', () => {});
                stream.on('end', () => resolve());
            });
        });

        const s = server.openStream();
        s.write(Buffer.from('bye'));
        s.end();

        await ended;
        cleanup();
    });

    it('streamDestroy closes remote side', async () => {
        const { server, client, cleanup } = await newPair();

        // 远端流在被 reset 后应触发 close/error，下面任意其一即可。
        const closed = new Promise<void>((resolve) => {
            client.on('stream', (stream: YamuxStream) => {
                stream.on('data', () => {});
                stream.on('error', () => resolve());
                stream.on('close', () => resolve());
            });
        });

        const s = server.openStream();
        s.on('error', () => {});
        s.write(Buffer.from('boom'));
        // 让对端先收到流再 destroy
        setTimeout(() => s.destroy(new Error('reset')), 50);

        await closed;
        cleanup();
    });

    it('sessionClose destroys open streams', async () => {
        const { server, client, cleanup } = await newPair();

        const remoteStream = await new Promise<YamuxStream>((resolve) => {
            client.on('stream', (stream: YamuxStream) => {
                stream.on('data', () => {});
                resolve(stream);
            });
            const s = server.openStream();
            s.on('error', () => {});
            s.write(Buffer.from('hi'));
        });

        // 会话关闭后远端流应被销毁（close/error 任一触发即可）。
        const closed = new Promise<void>((resolve) => {
            remoteStream.on('close', () => resolve());
            remoteStream.on('error', () => resolve());
        });
        server.close();

        await closed;
        cleanup();
    });
});
