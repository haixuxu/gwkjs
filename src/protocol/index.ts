import { Frame } from './frame';
export * from './frame';

export const AUTH_REQ = 0x0; // start auth
export const AUTH_RES = 0x1; // auth response

export const TUNNEL_REQ = 0xa6; // start tunnel
export const TUNNEL_RES = 0xa9; // response tunnel

/**
 * 握手帧格式（仍使用 2 字节长度前缀分帧，与 Go 端 transport.TcpTransport 一致）。
 * yamux 接管 raw socket 之前，仅用于 AUTH / TUNNEL 协商。
 *
 * @param {*} AUTH_REQ frame
 * |<--type[1]-->|--status(1)--|<------auth token------>|
 * |----- 1 -----|------0------|----------s2------------|
 *
 * @param {*} AUTH_RES frame
 * |<--type[1]-->|--status(1)--|<--------message------->|
 * |----- 1 -----|-----1/2-----|----------s2------------|
 *
 * @param {*} TUNNEL_REQ frame
 * |<--type[1]-->|----pro----|----- port/subdomain-----|
 * |----- 1 -----|----- 1----|--------name:port--------|
 * |----- 1 -----|----- 1----|--------name:domain------|
 *
 * @param {*} TUNNEL_RES frame
 * |<--type[1]-->|----status----|------message-------|
 * |----- 1 -----|----- 1-------|--------------------|
 */

export function encode(frame: Frame): Buffer {
    const type = frame.type;
    const prefix = Buffer.from([type]);
    if (type === AUTH_REQ) {
        const statusBuf = Buffer.from([frame.status as number]);
        return Buffer.concat([prefix, statusBuf, Buffer.from(frame.token as string)]);
    } else if (type === AUTH_RES) {
        const statusBuf = Buffer.from([frame.status as number]);
        return Buffer.concat([prefix, statusBuf, Buffer.from(frame.message as string)]);
    } else if (type === TUNNEL_REQ) {
        const probuf = Buffer.from([frame.tunType as number]);
        let message = '';
        if (frame.tunType === 0x2) {
            message = `${frame.name}:${frame.subdomain}`;
        } else if (frame.tunType === 0x1 || frame.tunType === 0x3) {
            // 0x1:tcp, 0x3:udp
            message = `${frame.name}:${frame.port}`;
        } else if (frame.tunType === 0x4) {
            message = `${frame.name}:${frame.secretKey}`;
        }
        return Buffer.concat([prefix, probuf, Buffer.from(message)]);
    } else if (type === TUNNEL_RES) {
        const statusBuf = Buffer.from([frame.status as number]);
        const messageBuf = Buffer.from(frame.message as string);
        return Buffer.concat([prefix, statusBuf, messageBuf]);
    }
    throw new Error('unknown frame type: ' + type);
}

export function decode(data: Buffer): Frame {
    const type = data[0];
    if (type === AUTH_REQ) {
        const token = data.slice(2);
        return { type, token: token.toString(), status: 0 };
    } else if (type === AUTH_RES) {
        const message = data.slice(2).toString();
        return { type, message, status: data[1] };
    } else if (type === TUNNEL_REQ) {
        const proto = data[1];
        let message = data.slice(2).toString();
        let parts = message.split(':');
        let port = 0;
        let subdomain = '';
        let secretKey = '';
        if (proto === 0x1 || proto === 0x3) {
            port = Number(parts[1]);
        } else if (proto === 0x2) {
            subdomain = parts[1];
        } else if (proto === 0x4) {
            secretKey = parts[1];
        }
        return { type, tunType: proto, name: parts[0], port, subdomain, secretKey };
    } else if (type === TUNNEL_RES) {
        const status = data[1];
        const message = data.slice(2).toString();
        return { type, status, message };
    }
    throw new Error('unknown frame type: ' + type);
}
