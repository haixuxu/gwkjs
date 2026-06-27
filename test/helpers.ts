import net from 'net';
import dgram from 'dgram';

/**
 * 建立一对已连接的 TCP socket（loopback），返回 [clientSide, serverSide]。
 * 用于在不依赖完整 Server/Client 的情况下测试握手与 yamux 会话。
 */
export function createSocketPair(): Promise<[net.Socket, net.Socket, () => void]> {
    return new Promise((resolve, reject) => {
        const srv = net.createServer((serverSide) => {
            // 吞掉测试 socket 上的连接级错误（如对端 destroy 引发的 ECONNRESET），
            // 避免在会话关闭场景下抛出 uncaught exception。
            serverSide.on('error', () => {});
            clientSide.on('error', () => {});
            const cleanup = () => {
                clientSide.destroy();
                serverSide.destroy();
                srv.close();
            };
            resolve([clientSide, serverSide, cleanup]);
        });
        let clientSide: net.Socket;
        srv.on('error', reject);
        srv.listen(0, '127.0.0.1', () => {
            const { port } = srv.address() as net.AddressInfo;
            clientSide = net.connect(port, '127.0.0.1');
        });
    });
}

/** 获取一个当前空闲的 TCP 端口。 */
export function getFreePort(): Promise<number> {
    return new Promise((resolve, reject) => {
        const srv = net.createServer();
        srv.on('error', reject);
        srv.listen(0, '127.0.0.1', () => {
            const { port } = srv.address() as net.AddressInfo;
            srv.close(() => resolve(port));
        });
    });
}

/** 启动一个 TCP echo 服务，返回端口与关闭函数。 */
export function startEchoServer(): Promise<{ port: number; close: () => void }> {
    return new Promise((resolve, reject) => {
        const srv = net.createServer((sock) => {
            sock.on('error', () => sock.destroy());
            sock.pipe(sock);
        });
        srv.on('error', reject);
        srv.listen(0, '127.0.0.1', () => {
            const { port } = srv.address() as net.AddressInfo;
            resolve({ port, close: () => srv.close() });
        });
    });
}

/** 获取一个当前空闲的 UDP 端口。 */
export function getFreeUdpPort(): Promise<number> {
    return new Promise((resolve, reject) => {
        const sock = dgram.createSocket('udp4');
        sock.on('error', reject);
        sock.bind(0, '127.0.0.1', () => {
            const { port } = sock.address() as net.AddressInfo;
            sock.close(() => resolve(port));
        });
    });
}

/** 启动一个 UDP echo 服务，返回端口与关闭函数。 */
export function startUdpEchoServer(): Promise<{ port: number; close: () => void }> {
    return new Promise((resolve, reject) => {
        const sock = dgram.createSocket('udp4');
        sock.on('error', reject);
        sock.on('message', (msg, rinfo) => {
            sock.send(msg, rinfo.port, rinfo.address);
        });
        sock.bind(0, '127.0.0.1', () => {
            const { port } = sock.address() as net.AddressInfo;
            resolve({ port, close: () => sock.close() });
        });
    });
}
