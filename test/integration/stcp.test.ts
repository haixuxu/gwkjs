import { describe, it, expect, afterEach } from 'vitest';
import net from 'net';
import Server from '../../src/Server';
import { doHandshake } from '../../src/protocol/handshake';
import { createClientSession, YamuxSession, YamuxStream } from '../../src/session';
import { TunnelOpts } from '../../src/types';
import { getFreePort, startEchoServer } from '../helpers';

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

async function connectCtrl(ctrlPort: number, tunopts: TunnelOpts): Promise<{ session: YamuxSession; message: string }> {
    const socket = net.connect(ctrlPort, '127.0.0.1');
    await new Promise<void>((resolve) => socket.once('connect', () => resolve()));
    const message = await doHandshake(socket, 'test:test123', tunopts);
    const session = createClientSession(socket);
    session.on('error', () => {});
    ctx.closeFns.push(() => {
        session.close();
        socket.destroy();
    });
    return { session, message };
}

// stcp 左端：接收服务端转发来的流，并中继到本地服务（echo）。
async function runStcpLeft(ctrlPort: number, tunopts: TunnelOpts): Promise<void> {
    const { session } = await connectCtrl(ctrlPort, tunopts);
    session.on('stream', (stream: YamuxStream) => {
        const local = net.connect(tunopts.localPort, tunopts.localIp);
        stream.pipe(local);
        local.pipe(stream);
        local.on('error', () => stream.destroy());
        stream.on('error', () => local.destroy());
        local.on('close', () => stream.destroy());
        stream.on('close', () => local.destroy());
    });
}

// stcp 右端：本地监听 bindPort，每个连接主动向服务端开流。
async function runStcpRight(ctrlPort: number, tunopts: TunnelOpts): Promise<void> {
    const { session } = await connectCtrl(ctrlPort, tunopts);
    const server = net.createServer((socket) => {
        const stream = session.openStream();
        socket.pipe(stream);
        stream.pipe(socket);
        socket.on('error', () => stream.destroy());
        stream.on('error', () => socket.destroy());
        socket.on('close', () => stream.destroy());
        stream.on('close', () => socket.destroy());
    });
    await new Promise<void>((resolve) => {
        server.listen(tunopts.bindPort, tunopts.bindIp, () => resolve());
    });
    ctx.closeFns.push(() => server.close());
}

function roundTrip(port: number, payload: Buffer): Promise<Buffer> {
    return new Promise((resolve, reject) => {
        const tryConnect = (attempt: number) => {
            const conn = net.connect(port, '127.0.0.1');
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

describe('stcp tunnel integration', () => {
    it('stcpPeerConnect relays data left<->right via secretKey', async () => {
        const echo = await startEchoServer();
        ctx.closeFns.push(echo.close);
        const ctrlPort = await startServer();
        const bindPort = await getFreePort();

        const leftTun: TunnelOpts = {
            name: 'stcp-left',
            tunType: 0x4,
            protocol: 'stcp',
            localIp: '127.0.0.1',
            localPort: echo.port,
            secretKey: 'mysecret',
        };
        const rightTun: TunnelOpts = {
            name: 'stcp-right',
            tunType: 0x4,
            protocol: 'stcp',
            localIp: '127.0.0.1',
            localPort: 0,
            secretKey: 'mysecret',
            bindIp: '127.0.0.1',
            bindPort,
        };

        // 先注册左端，确保右端流到达时对端已存在。
        await runStcpLeft(ctrlPort, leftTun);
        await runStcpRight(ctrlPort, rightTun);

        const payload = Buffer.from('hello gwk stcp tunnel');
        const got = await roundTrip(bindPort, payload);
        expect(got.equals(payload)).toBe(true);
    });
});
