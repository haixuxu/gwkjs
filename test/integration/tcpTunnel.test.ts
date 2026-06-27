import { describe, it, expect, afterEach } from 'vitest';
import net from 'net';
import crypto from 'crypto';
import Server from '../../src/Server';
import { doHandshake } from '../../src/protocol/handshake';
import { createClientSession, YamuxSession, YamuxStream } from '../../src/session';
import { TunnelOpts } from '../../src/types';
import { startEchoServer } from '../helpers';

interface Ctx {
    closeFns: Array<() => void>;
}

const ctx: Ctx = { closeFns: [] };

afterEach(() => {
    ctx.closeFns.splice(0).forEach((fn) => {
        try {
            fn();
        } catch {
            // ignore
        }
    });
});

// 启动真实 Server 的控制通道监听，返回端口。
function startServer(): Promise<number> {
    const srv = new Server({ serverHost: '127.0.0.1', serverPort: 0 });
    return new Promise((resolve) => {
        const ln = net.createServer((conn) => srv.handleConection(conn));
        ln.listen(0, '127.0.0.1', () => {
            ctx.closeFns.push(() => ln.close());
            resolve((ln.address() as net.AddressInfo).port);
        });
    });
}

// 模拟 gwk 客户端：握手 + yamux client，accept 到的流中继到本地服务。
async function runClient(ctrlPort: number, tunopts: TunnelOpts): Promise<string> {
    const socket = net.connect(ctrlPort, '127.0.0.1');
    await new Promise<void>((resolve) => socket.once('connect', () => resolve()));
    const message = await doHandshake(socket, 'test:test123', tunopts);
    const session: YamuxSession = createClientSession(socket);
    ctx.closeFns.push(() => {
        session.close();
        socket.destroy();
    });
    session.on('stream', (stream: YamuxStream) => {
        const local = net.connect(tunopts.localPort, tunopts.localIp);
        stream.pipe(local);
        local.pipe(stream);
        local.on('error', () => stream.destroy());
        stream.on('error', () => local.destroy());
        local.on('close', () => stream.destroy());
        stream.on('close', () => local.destroy());
    });
    return message;
}

function remotePortFromMsg(msg: string): number {
    const idx = msg.lastIndexOf(':');
    return Number(msg.slice(idx + 1));
}

function roundTrip(remotePort: number, payload: Buffer): Promise<Buffer> {
    return new Promise((resolve, reject) => {
        const tryConnect = (attempt: number) => {
            const conn = net.connect(remotePort, '127.0.0.1');
            conn.once('connect', () => {
                const chunks: Buffer[] = [];
                let total = 0;
                conn.on('data', (c) => {
                    chunks.push(c);
                    total += c.length;
                    if (total >= payload.length) {
                        conn.destroy();
                        resolve(Buffer.concat(chunks));
                    }
                });
                conn.on('error', reject);
                conn.write(payload);
            });
            conn.once('error', (err) => {
                if (attempt < 40) {
                    setTimeout(() => tryConnect(attempt + 1), 25);
                } else {
                    reject(err);
                }
            });
        };
        tryConnect(0);
    });
}

describe('tcp tunnel integration', () => {
    it('tcpTunnelE2E echoes payload back', async () => {
        const echo = await startEchoServer();
        ctx.closeFns.push(echo.close);
        const ctrlPort = await startServer();

        const tunopts: TunnelOpts = {
            name: 'tcp1',
            tunType: 0x1,
            protocol: 'tcp',
            localIp: '127.0.0.1',
            localPort: echo.port,
            remotePort: 0,
        };
        const msg = await runClient(ctrlPort, tunopts);
        const remotePort = remotePortFromMsg(msg);

        const payload = Buffer.from('hello gwk yamux integration');
        const got = await roundTrip(remotePort, payload);
        expect(got.equals(payload)).toBe(true);
    });

    it('tcpTunnelLargePayload transfers 1MB', async () => {
        const echo = await startEchoServer();
        ctx.closeFns.push(echo.close);
        const ctrlPort = await startServer();

        const tunopts: TunnelOpts = {
            name: 'tcp1',
            tunType: 0x1,
            protocol: 'tcp',
            localIp: '127.0.0.1',
            localPort: echo.port,
            remotePort: 0,
        };
        const msg = await runClient(ctrlPort, tunopts);
        const remotePort = remotePortFromMsg(msg);

        const payload = crypto.randomBytes(1024 * 1024);
        const got = await roundTrip(remotePort, payload);
        expect(got.length).toBe(payload.length);
        expect(got.equals(payload)).toBe(true);
    });

    it('tcpTunnelConcurrent10 keeps connections independent', async () => {
        const echo = await startEchoServer();
        ctx.closeFns.push(echo.close);
        const ctrlPort = await startServer();

        const tunopts: TunnelOpts = {
            name: 'tcp1',
            tunType: 0x1,
            protocol: 'tcp',
            localIp: '127.0.0.1',
            localPort: echo.port,
            remotePort: 0,
        };
        const msg = await runClient(ctrlPort, tunopts);
        const remotePort = remotePortFromMsg(msg);

        const results = await Promise.all(
            Array.from({ length: 10 }, (_, i) => {
                const payload = Buffer.from(`conn-${i}-payload-${i * 7}`);
                return roundTrip(remotePort, payload).then((got) => got.equals(payload));
            })
        );
        expect(results.every(Boolean)).toBe(true);
    });
});
