import { describe, it, expect } from 'vitest';
import {
    AUTH_REQ,
    AUTH_RES,
    TUNNEL_REQ,
    TUNNEL_RES,
    encode,
    decode,
    Frame,
} from '../../src/protocol';

// 编码后再解码，验证关键字段无损往返。
function roundTrip(frame: Frame): Frame {
    return decode(encode(frame));
}

describe('protocol encode/decode', () => {
    it('encodeDecodeAuthReq restores token', () => {
        const got = roundTrip({ type: AUTH_REQ, status: 0, token: 'test:test123' });
        expect(got.type).toBe(AUTH_REQ);
        expect(got.token).toBe('test:test123');
    });

    it('encodeDecodeAuthResOk keeps status=1 and message', () => {
        const got = roundTrip({ type: AUTH_RES, status: 0x1, message: 'success' });
        expect(got.type).toBe(AUTH_RES);
        expect(got.status).toBe(0x1);
        expect(got.message).toBe('success');
    });

    it('encodeDecodeAuthResFail keeps status=2', () => {
        const got = roundTrip({ type: AUTH_RES, status: 0x2, message: 'bad token' });
        expect(got.type).toBe(AUTH_RES);
        expect(got.status).toBe(0x2);
        expect(got.message).toBe('bad token');
    });

    it('encodeDecodeTunnelReqTcp restores name and port', () => {
        const got = roundTrip({ type: TUNNEL_REQ, tunType: 0x1, name: 'tcp1', port: 9090 });
        expect(got.type).toBe(TUNNEL_REQ);
        expect(got.tunType).toBe(0x1);
        expect(got.name).toBe('tcp1');
        expect(got.port).toBe(9090);
    });

    it('encodeDecodeTunnelReqWeb restores subdomain', () => {
        const got = roundTrip({ type: TUNNEL_REQ, tunType: 0x2, name: 'web1', subdomain: 'demo' });
        expect(got.type).toBe(TUNNEL_REQ);
        expect(got.tunType).toBe(0x2);
        expect(got.name).toBe('web1');
        expect(got.subdomain).toBe('demo');
    });

    it('encodeDecodeTunnelReqUdp restores name and port', () => {
        const got = roundTrip({ type: TUNNEL_REQ, tunType: 0x3, name: 'udp1', port: 5353 });
        expect(got.type).toBe(TUNNEL_REQ);
        expect(got.tunType).toBe(0x3);
        expect(got.name).toBe('udp1');
        expect(got.port).toBe(5353);
    });

    it('encodeDecodeTunnelReqStcp restores secretKey', () => {
        const got = roundTrip({ type: TUNNEL_REQ, tunType: 0x4, name: 'stcp1', secretKey: 'stcp_left_mysecret' });
        expect(got.type).toBe(TUNNEL_REQ);
        expect(got.tunType).toBe(0x4);
        expect(got.name).toBe('stcp1');
        expect(got.secretKey).toBe('stcp_left_mysecret');
    });

    it('encodeDecodeTunnelRes restores status and message', () => {
        const got = roundTrip({ type: TUNNEL_RES, status: 0x1, message: 'tcp://127.0.0.1:9090' });
        expect(got.type).toBe(TUNNEL_RES);
        expect(got.status).toBe(0x1);
        expect(got.message).toBe('tcp://127.0.0.1:9090');
    });

    it('encode throws on unknown frame type', () => {
        expect(() => encode({ type: 0xff } as Frame)).toThrow(/unknown frame type/);
    });

    it('decode throws on unknown frame type', () => {
        expect(() => decode(Buffer.from([0xff, 0x00]))).toThrow(/unknown frame type/);
    });
});
