import net from 'net';
import { Client, Server, YamuxSession, YamuxStream } from '@bbk47/yamux';

export type { YamuxSession, YamuxStream };

/**
 * 在握手完成后的 raw socket 上创建客户端 yamux 会话。
 * 客户端使用奇数 stream id，与 hashicorp/yamux 的 Go 服务端互通。
 */
export function createClientSession(socket: net.Socket): YamuxSession {
    return Client(socket);
}

/**
 * 在握手完成后的 raw socket 上创建服务端 yamux 会话。
 * 服务端使用偶数 stream id。
 */
export function createServerSession(socket: net.Socket): YamuxSession {
    return Server(socket);
}
