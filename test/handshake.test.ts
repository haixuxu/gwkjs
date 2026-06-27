import { describe, it, expect } from 'vitest';
import { doHandshake, doServerHandshake, readFrame } from '../src/protocol/handshake';
import { TunnelOpts } from '../src/types';
import { createSocketPair } from './helpers';

const tcpTun: TunnelOpts = {
    name: 'tcp1',
    tunType: 0x1,
    protocol: 'tcp',
    localIp: '127.0.0.1',
    localPort: 8080,
    remotePort: 9090,
};

const webTun: TunnelOpts = {
    name: 'web1',
    tunType: 0x2,
    protocol: 'web',
    localIp: '127.0.0.1',
    localPort: 8080,
    subdomain: 'demo',
};

describe('handshake', () => {
    it('authSuccess + tunnelTcpSuccess', async () => {
        const [client, server, cleanup] = await createSocketPair();
        const serverDone = doServerHandshake(
            server,
            async () => 'ok',
            async (frame) => {
                expect(frame.tunType).toBe(0x1);
                expect(frame.name).toBe('tcp1');
                expect(frame.port).toBe(9090);
                return 'tcp://127.0.0.1:9090';
            }
        );
        const message = await doHandshake(client, 'test:test123', tcpTun);
        expect(message).toBe('tcp://127.0.0.1:9090');
        const result = await serverDone;
        expect(result.token).toBe('test:test123');
        cleanup();
    });

    it('tunnelWebSuccess returns http message', async () => {
        const [client, server, cleanup] = await createSocketPair();
        const serverDone = doServerHandshake(
            server,
            async () => 'ok',
            async (frame) => {
                expect(frame.tunType).toBe(0x2);
                expect(frame.subdomain).toBe('demo');
                return 'http://demo.example.com/';
            }
        );
        const message = await doHandshake(client, 'tok', webTun);
        expect(message).toMatch(/^http:\/\//);
        await serverDone;
        cleanup();
    });

    it('authFailed throws on client', async () => {
        const [client, server, cleanup] = await createSocketPair();
        const serverDone = doServerHandshake(
            server,
            async () => {
                throw new Error('bad token');
            },
            async () => 'unused'
        ).catch(() => 'server rejected');
        await expect(doHandshake(client, 'wrong', tcpTun)).rejects.toThrow(/auth failed/);
        expect(await serverDone).toBe('server rejected');
        cleanup();
    });

    it('tunnelRejected throws on client', async () => {
        const [client, server, cleanup] = await createSocketPair();
        const serverDone = doServerHandshake(
            server,
            async () => 'ok',
            async () => {
                throw new Error('port in use');
            }
        ).catch(() => 'server rejected');
        await expect(doHandshake(client, 'tok', tcpTun)).rejects.toThrow(/tunnel failed/);
        expect(await serverDone).toBe('server rejected');
        cleanup();
    });

    it('readFrame times out without data', async () => {
        const [client, , cleanup] = await createSocketPair();
        await expect(readFrame(client, 200)).rejects.toThrow(/timeout/);
        cleanup();
    });

    it('handshakeAuthTimeout throws when server never replies to AUTH_REQ', async () => {
        const [client, server, cleanup] = await createSocketPair();
        // 服务端收到 AUTH_REQ 但不回应，doHandshake 在等待 AUTH_RES 时超时。
        server.on('data', () => {});
        await expect(doHandshake(client, 'tok', tcpTun)).rejects.toThrow(/handshake timeout/);
        cleanup();
    });

    it('handshakeTunnelTimeout throws when server stops after AUTH_RES', async () => {
        const [client, server, cleanup] = await createSocketPair();
        // 服务端只完成 auth（doServerHandshake 内部读取 TUNNEL_REQ 但 tunnelHandler 永不 resolve），
        // 客户端发出 TUNNEL_REQ 后等待 TUNNEL_RES 超时。
        // tunnelHandler 永不 resolve，故 doServerHandshake 也不会结束，这里不能 await 它。
        void doServerHandshake(
            server,
            async () => 'ok',
            () => new Promise<string>(() => {})
        ).catch(() => 'server pending');
        await expect(doHandshake(client, 'tok', tcpTun)).rejects.toThrow(/handshake timeout/);
        cleanup();
    });

    it('socketCloseDuringHandshake rejects', async () => {
        const [client, server, cleanup] = await createSocketPair();
        const p = readFrame(client, 5000);
        server.destroy();
        await expect(p).rejects.toThrow();
        cleanup();
    });
});
