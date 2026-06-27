import { describe, it, expect, afterEach } from 'vitest';
import net from 'net';
import dgram from 'dgram';
import Server from '../../src/Server';
import { doHandshake } from '../../src/protocol/handshake';
import { createClientSession, YamuxSession, YamuxStream } from '../../src/session';
import { TunnelOpts } from '../../src/types';
import { bindStreamSocket } from '../../src/utils/socket';
import { UdpOverTcpReadStream } from '../../src/stream/udp2tcp';
import { parseIpAddrBuffer } from '../../src/utils/ipaddr';
import { getFreeUdpPort, startUdpEchoServer } from '../helpers';

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

// 模拟 gwk 客户端的 UDP 隧道处理：复刻 Client.handleUdpStream 的核心逻辑，
// 但不依赖控制台输出，避免在非 TTY 环境下出错。
function handleUdpStream(tunnelConf: TunnelOpts, stream: YamuxStream) {
    const udpclientMap = new Map<string, dgram.Socket>();
    const listenData = (buff: Buffer) => {
        const udpaddrbuf = buff.slice(0, 6);
        const ipaddr = parseIpAddrBuffer(udpaddrbuf);
        const key = `${ipaddr.addr}|${ipaddr.port}`;
        if (!udpclientMap.has(key)) {
            const client = dgram.createSocket('udp4');
            client.bind();
            udpclientMap.set(key, client);
            const rst = new UdpOverTcpReadStream(client, udpaddrbuf);
            rst.pipe(stream);
        }
        const socket = udpclientMap.get(key) as dgram.Socket;
        socket.send(buff.slice(6), tunnelConf.localPort, tunnelConf.localIp);
    };
    bindStreamSocket(
        stream as any,
        listenData,
        () => {},
        () => {}
    );
    ctx.closeFns.push(() => udpclientMap.forEach((s) => s.close()));
}

async function runUdpClient(ctrlPort: number, tunopts: TunnelOpts): Promise<string> {
    const socket = net.connect(ctrlPort, '127.0.0.1');
    await new Promise<void>((resolve) => socket.once('connect', () => resolve()));
    const message = await doHandshake(socket, 'test:test123', tunopts);
    const session: YamuxSession = createClientSession(socket);
    session.on('error', () => {});
    ctx.closeFns.push(() => {
        session.close();
        socket.destroy();
    });
    session.on('stream', (stream: YamuxStream) => handleUdpStream(tunopts, stream));
    return message;
}

// 通过一个 UDP socket 向远端隧道端口发包，等待回显。
function udpRoundTrip(remotePort: number, payload: Buffer): Promise<Buffer> {
    return new Promise((resolve, reject) => {
        const cli = dgram.createSocket('udp4');
        ctx.closeFns.push(() => {
            try {
                cli.close();
            } catch {
                // ignore
            }
        });
        const timer = setTimeout(() => reject(new Error('udp roundtrip timeout')), 8000);
        cli.on('message', (msg) => {
            clearTimeout(timer);
            resolve(msg);
        });
        cli.on('error', (err) => {
            clearTimeout(timer);
            reject(err);
        });
        cli.send(payload, remotePort, '127.0.0.1');
    });
}

describe('udp tunnel integration', () => {
    it('udpTunnelE2E echoes payload back', async () => {
        const echo = await startUdpEchoServer();
        ctx.closeFns.push(echo.close);
        const ctrlPort = await startServer();
        const remotePort = await getFreeUdpPort();

        const tunopts: TunnelOpts = {
            name: 'udp1',
            tunType: 0x3,
            protocol: 'udp',
            localIp: '127.0.0.1',
            localPort: echo.port,
            remotePort,
        };
        await runUdpClient(ctrlPort, tunopts);

        const payload = Buffer.from('hello gwk udp tunnel');
        const got = await udpRoundTrip(remotePort, payload);
        expect(got.equals(payload)).toBe(true);
    });

    it('udpMultiClient keeps each client echo independent', async () => {
        const echo = await startUdpEchoServer();
        ctx.closeFns.push(echo.close);
        const ctrlPort = await startServer();
        const remotePort = await getFreeUdpPort();

        const tunopts: TunnelOpts = {
            name: 'udp1',
            tunType: 0x3,
            protocol: 'udp',
            localIp: '127.0.0.1',
            localPort: echo.port,
            remotePort,
        };
        await runUdpClient(ctrlPort, tunopts);

        const results = await Promise.all(
            Array.from({ length: 3 }, (_, i) => {
                const payload = Buffer.from(`udp-client-${i}`);
                return udpRoundTrip(remotePort, payload).then((got) => got.equals(payload));
            })
        );
        expect(results.every(Boolean)).toBe(true);
    });
});
