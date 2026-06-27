import net from 'net';
import { TunnelOpts } from '../types';
import {
    AUTH_REQ,
    AUTH_RES,
    TUNNEL_REQ,
    TUNNEL_RES,
    Frame,
    encode,
    decode,
} from './index';

const HANDSHAKE_TIMEOUT = 5 * 1000;

/**
 * 用 2 字节长度前缀发送一个握手帧。握手阶段仍沿用旧的分帧格式，
 * 与 Go 端 transport.TcpTransport 完全一致；yamux 接管 socket 之前仅用于 AUTH / TUNNEL 协商。
 */
export function sendFrame(socket: net.Socket, frame: Frame): void {
    const payload = encode(frame);
    const len = payload.length;
    const buf = Buffer.concat([Buffer.from([len >> 8, len % 256]), payload]);
    if (!socket.writable) {
        throw new Error('socket is not writable');
    }
    socket.write(buf);
}

/**
 * 读取一个 2 字节长度前缀的帧，读到完整帧后立刻移除监听器；
 * 若一次 data 事件里附带了多余字节（例如对端紧接着发来的下一帧或 yamux 数据），
 * 用 socket.unshift 放回内部缓冲区，保证后续读取方（下一次握手或 yamux）能拿到这些字节。
 */
export function readFrame(socket: net.Socket, timeout = HANDSHAKE_TIMEOUT): Promise<Frame> {
    return new Promise<Frame>((resolve, reject) => {
        let cache = Buffer.alloc(0);

        const timer = setTimeout(() => {
            cleanup();
            reject(new Error('handshake timeout'));
        }, timeout);

        const cleanup = () => {
            clearTimeout(timer);
            socket.removeListener('data', onData);
            socket.removeListener('error', onError);
            socket.removeListener('close', onClose);
            socket.removeListener('end', onEnd);
        };

        const onData = (chunk: Buffer) => {
            cache = Buffer.concat([cache, chunk]);
            if (cache.length < 2) return;
            const datalen = cache[0] * 256 + cache[1];
            if (cache.length < datalen + 2) return;
            const packet = cache.subarray(2, datalen + 2);
            const rest = cache.subarray(datalen + 2);
            cleanup();
            if (rest.length > 0) {
                socket.unshift(rest);
            }
            try {
                resolve(decode(Buffer.from(packet)));
            } catch (err) {
                reject(err as Error);
            }
        };
        const onError = (err: Error) => {
            cleanup();
            reject(err);
        };
        const onClose = () => {
            cleanup();
            reject(new Error('socket closed during handshake'));
        };
        const onEnd = () => {
            cleanup();
            reject(new Error('socket ended during handshake'));
        };

        socket.on('data', onData);
        socket.once('error', onError);
        socket.once('close', onClose);
        socket.once('end', onEnd);
    });
}

function buildTunnelReqFrame(tunopts: TunnelOpts): Frame {
    const port = tunopts.tunType === 0x2 ? 0 : tunopts.remotePort;
    const subdomain = tunopts.tunType === 0x2 ? tunopts.subdomain : '';
    let secretKey = tunopts.secretKey;
    if (tunopts.tunType === 0x4) {
        if (tunopts.bindIp && tunopts.bindPort) {
            secretKey = 'stcp_right_' + secretKey;
        } else {
            secretKey = 'stcp_left_' + secretKey;
        }
    }
    return { type: TUNNEL_REQ, tunType: tunopts.tunType, name: tunopts.name, port, subdomain, secretKey };
}

/**
 * 客户端握手：发送 AUTH_REQ / TUNNEL_REQ 并等待响应。
 * 完成后 socket 上不残留任何持久监听器，可直接交给 yamux。
 * @returns 服务端在 TUNNEL_RES 中返回的 message（隧道地址信息）。
 */
export async function doHandshake(socket: net.Socket, token: string, tunopts: TunnelOpts): Promise<string> {
    sendFrame(socket, { type: AUTH_REQ, status: 0, token });
    const authRes = await readFrame(socket);
    if (authRes.type !== AUTH_RES || authRes.status !== 0x1) {
        throw new Error('auth failed: ' + (authRes.message || 'unknown'));
    }

    sendFrame(socket, buildTunnelReqFrame(tunopts));
    const tunnelRes = await readFrame(socket);
    if (tunnelRes.type !== TUNNEL_RES || tunnelRes.status !== 0x1) {
        throw new Error('tunnel failed: ' + (tunnelRes.message || 'unknown'));
    }
    return tunnelRes.message || '';
}

export type AuthHandler = (token: string) => Promise<string>;
export type TunnelHandler = (frame: Frame) => Promise<string>;

export interface ServerHandshakeResult {
    token: string;
    tunnelFrame: Frame;
    message: string;
}

/**
 * 服务端握手：接收 AUTH_REQ / TUNNEL_REQ，调用业务 handler 后回送响应。
 * tunnelHandler 应在返回前完成隧道注册（监听端口 / 绑定子域名），
 * 失败时抛错即可，握手函数会回送 STATUS_FAILED 并向上抛出。
 */
export async function doServerHandshake(
    socket: net.Socket,
    authHandler: AuthHandler,
    tunnelHandler: TunnelHandler
): Promise<ServerHandshakeResult> {
    const authReq = await readFrame(socket);
    if (authReq.type !== AUTH_REQ) {
        throw new Error('protocol error: expected AUTH_REQ');
    }
    const token = authReq.token || '';
    let authMsg = '';
    try {
        authMsg = await authHandler(token);
    } catch (err) {
        sendFrame(socket, { type: AUTH_RES, status: 0x2, message: (err as Error).message });
        throw err;
    }
    sendFrame(socket, { type: AUTH_RES, status: 0x1, message: authMsg || 'ok' });

    const tunnelReq = await readFrame(socket);
    if (tunnelReq.type !== TUNNEL_REQ) {
        throw new Error('protocol error: expected TUNNEL_REQ');
    }
    let message = '';
    try {
        message = await tunnelHandler(tunnelReq);
    } catch (err) {
        sendFrame(socket, { type: TUNNEL_RES, status: 0x2, message: (err as Error).message });
        throw err;
    }
    sendFrame(socket, { type: TUNNEL_RES, status: 0x1, message });

    return { token, tunnelFrame: tunnelReq, message };
}
