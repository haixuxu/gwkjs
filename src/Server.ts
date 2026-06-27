import net from 'net';
import tls from 'tls';
import dgram from 'dgram';
import { ConnectObj, GankServerOpts, tuntype2Str } from './types/index';
import getCustomLogger, { Logger } from './utils/logger';
import { HeaderTransform, HttpReq } from './stream/header';
import { getRamdomUUID } from './utils/uuid';
import { bindStreamSocket, tcpsocketSend } from './utils/socket';
import { buildIpAddrBuffer, parseIpAddrBuffer } from './utils/ipaddr';
import { Frame } from './protocol';
import { doServerHandshake } from './protocol/handshake';
import { createServerSession, YamuxStream } from './session';

class Server {
    private listenPort: number;
    listenHttpPort: number | undefined;
    listenHttpsPort: number | undefined;
    tlsOpts: { ca: string | undefined; key: string | undefined; cert: string | undefined };
    webTunnels: Record<string, ConnectObj>;
    stcpTunnels: Record<string, ConnectObj>;
    serverHost: string;
    connectMap: Record<string, ConnectObj>;
    logger: Logger;
    constructor(opts: GankServerOpts) {
        this.listenPort = opts.serverPort || 4443;
        this.listenHttpPort = opts.httpAddr;
        this.listenHttpsPort = opts.httpsAddr;
        this.serverHost = opts.serverHost || 'gank007.com';
        this.tlsOpts = {
            ca: opts.tlsCA,
            key: opts.tlsKey,
            cert: opts.tlsCrt,
        };
        this.webTunnels = {};
        this.stcpTunnels = {};
        this.connectMap = {};
        this.logger = getCustomLogger('s>', 'debug');
    }

    releaseConn(conn: ConnectObj) {
        if (conn.server) {
            this.logger.info(`release tunnel unlisten on :${conn.remotePort}`);
            // socket 'close' 与 session 'close' 都会触发 releaseConn，置空避免对
            // dgram socket 二次 close 抛出 ERR_SOCKET_DGRAM_NOT_RUNNING。
            const server = conn.server;
            conn.server = undefined;
            try {
                server.close();
            } catch {
                // already closed
            }
        }
        const fulldomain = conn.fulldomain;
        if (fulldomain) {
            delete this.webTunnels[fulldomain];
            this.logger.info(`release tunnel unbind   on :${conn.fulldomain}`);
        }
        if (conn.secretKey) {
            delete this.stcpTunnels[conn.secretKey];
            this.logger.info(`release tunnel unbind   on :${conn.secretKey}`);
        }
    }

    handleAuth(token: string): Promise<string> {
        // console.log('handleAuth:', token);
        return Promise.resolve('success');
    }

    transformSocket(connobj: ConnectObj, socket2: net.Socket) {
        if (!connobj.session) {
            socket2.destroy();
            return;
        }
        try {
            this.logger.info(`handle socket for tunnel:${connobj.url}`);
            const stream = connobj.session.openStream();
            this.logger.info('create stream for', connobj.name);
            socket2.pipe(stream);
            stream.pipe(socket2);
            socket2.on('close', () => stream.destroy());
            socket2.on('error', () => stream.destroy());
            stream.on('close', () => socket2.destroy());
            stream.on('error', () => socket2.destroy());
        } catch (err) {
            this.logger.info('err:', (err as Error).message);
            socket2.write('service invalid!');
            socket2.destroy();
        }
    }

    transformUdpSocket(connobj: ConnectObj, msg: Buffer, rinfo: dgram.RemoteInfo, udpsocket: dgram.Socket) {
        const clientHostPort = `${rinfo.address}:${rinfo.port}`;
        // 只使用一个stream, 避免多次创建
        if (!connobj.udpstream && connobj.session) {
            this.logger.info('create stream for', connobj.name);
            const stream = connobj.session.openStream();
            connobj.udpstream = stream;
            const listenStreamData = (data: Buffer) => {
                const udpaddrbuf = data.slice(0, 6);
                const ipaddr = parseIpAddrBuffer(udpaddrbuf);
                const rawdata = data.slice(6);
                udpsocket.send(rawdata, ipaddr.port, ipaddr.addr, (err) => {
                    if (err) {
                        console.log('err;', err);
                    }
                });
            };
            const logmsg: any = (err?: Error) => this.logger.info((err && err.message) || 'udp stream closed');
            bindStreamSocket(stream as any, listenStreamData, logmsg, logmsg);
        }
        if (!connobj.udpstream) return;
        this.logger.info(`handle client[udp:${clientHostPort}] packet for tunnel:${connobj.url} msglen:${msg.length}`);
        const stream = connobj.udpstream;
        const udpaddr = buildIpAddrBuffer(rinfo.address, rinfo.port);
        const udppacket = Buffer.concat([udpaddr, msg]);
        tcpsocketSend(stream, udppacket); // udp msg => stream
    }

    handleUdpTunnel(connobj: ConnectObj, fm: Frame): Promise<string> {
        return new Promise((resolve, reject) => {
            const server = dgram.createSocket('udp4');
            server.on('message', (msg: Buffer, rinfo: dgram.RemoteInfo) => {
                this.transformUdpSocket(connobj, msg, rinfo, server);
            });
            server.on('error', function (err) {
                reject(err);
            });
            server.bind(fm.port, () => {
                this.logger.info('udp tunnel listen on :' + fm.port);
                connobj.server = server;
                connobj.url = 'udp://' + this.serverHost + ':' + fm.port;
                connobj.name = fm.name;
                connobj.remotePort = fm.port;
                resolve(connobj.url);
            });
        });
    }

    handleTcpTunnel(connobj: ConnectObj, fm: Frame): Promise<string> {
        return new Promise((resolve, reject) => {
            const server = net.createServer((socket2) => this.transformSocket(connobj, socket2));
            server.listen(fm.port, () => {
                const addr = server.address();
                const port = addr && typeof addr === 'object' ? addr.port : (fm.port as number);
                this.logger.info('tcp tunnel listen on :' + port);
                connobj.server = server;
                connobj.url = 'tcp://' + this.serverHost + ':' + port;
                connobj.name = fm.name;
                connobj.remotePort = port;
                resolve(connobj.url);
            });
            server.on('error', function (err) {
                reject(err);
            });
        });
    }

    handleWebTunnel(connobj: ConnectObj, fm: Frame): Promise<string> {
        return new Promise((resolve, reject) => {
            if (!fm.subdomain) {
                const err = Error('subdomain missing');
                this.logger.error(err.message);
                reject(err);
                return;
            }
            const subdomainfull = fm.subdomain + '.' + this.serverHost;

            if (this.webTunnels[subdomainfull]) {
                const err = Error('subdomain existed!');
                this.logger.error(err.message);
                reject(err);
                return;
            }
            connobj.url = `http://${subdomainfull}/`;
            connobj.name = fm.name;
            connobj.fulldomain = subdomainfull;
            this.webTunnels[subdomainfull] = connobj;
            resolve(connobj.url);
        });
    }

    handleStcpTunnel(connobj: ConnectObj, fm: Frame): Promise<string> {
        return new Promise((resolve, reject) => {
            if (!fm.secretKey) {
                const err = Error('secretKey missing');
                this.logger.error(err.message);
                reject(err);
                return;
            }
            const secretKey = fm.secretKey;
            if (this.stcpTunnels[secretKey]) {
                const err = Error('secretKey existed!');
                this.logger.error(err.message);
                reject(err);
                return;
            }
            connobj.url = `${secretKey}`;
            connobj.name = fm.name;
            connobj.secretKey = secretKey;
            this.stcpTunnels[secretKey] = connobj;
            // session 就绪后再监听右端 stream，避免空指针
            connobj.activate = () => this.handleStcpDispatch(connobj, secretKey);
            resolve(connobj.url);
        });
    }

    handleStcpDispatch(connobj: ConnectObj, secretKey: string) {
        if (!/stcp_right/.test(secretKey)) return;

        const leftkey = secretKey.replace(/stcp_right/, 'stcp_left');
        // dispatch right stcp peer to left stcp peer;
        const listenStream = (rightStream: YamuxStream) => {
            try {
                const peerConnObj = this.stcpTunnels[leftkey];
                if (!peerConnObj || !peerConnObj.session) {
                    rightStream.destroy();
                    return;
                }
                const stream = peerConnObj.session.openStream();
                this.logger.info(`create stream for ${connobj.name} ${secretKey}=>${leftkey}`);
                rightStream.pipe(stream);
                stream.pipe(rightStream);
                rightStream.on('close', () => stream.destroy());
                rightStream.on('error', () => stream.destroy());
                stream.on('close', () => rightStream.destroy());
                stream.on('error', () => rightStream.destroy());
            } catch (err) {
                this.logger.info('err:', (err as Error).message);
                rightStream.destroy();
            }
        };

        connobj.session.on('stream', listenStream);
    }

    handleTunReq(connectObj: ConnectObj, fm: Frame): Promise<string> {
        this.logger.info('tunnel req:' + JSON.stringify(fm));
        connectObj.type = tuntype2Str[fm.tunType as number];
        if (fm.tunType === 0x1) {
            return this.handleTcpTunnel(connectObj, fm);
        } else if (fm.tunType === 0x2) {
            return this.handleWebTunnel(connectObj, fm);
        } else if (fm.tunType === 0x3) {
            return this.handleUdpTunnel(connectObj, fm);
        } else if (fm.tunType === 0x4) {
            return this.handleStcpTunnel(connectObj, fm);
        }
        return Promise.reject(Error('unknown tunnel type'));
    }

    handleConection(socket: net.Socket) {
        const connectObj: ConnectObj = { session: undefined as any, socket, url: '', rtt: 0 };
        const cid = getRamdomUUID();

        doServerHandshake(
            socket,
            (token: string) => this.handleAuth(token),
            (frame: Frame) => this.handleTunReq(connectObj, frame)
        )
            .then(() => {
                const session = createServerSession(socket);
                connectObj.session = session;
                this.connectMap[cid] = connectObj;

                session.on('error', (err: Error) => this.logger.info('session err:', err.message));
                session.on('close', () => {
                    delete this.connectMap[cid];
                    this.releaseConn(connectObj);
                });

                if (connectObj.activate) {
                    connectObj.activate();
                }
            })
            .catch((err: Error) => {
                this.logger.info('handshake err:', err.message);
                this.releaseConn(connectObj);
                socket.destroy();
            });

        socket.on('close', () => {
            delete this.connectMap[cid];
            this.releaseConn(connectObj);
        });
        socket.on('error', (err: Error) => this.logger.info('socket err:', err.message));
    }

    handleHttpRequest(socket: net.Socket) {
        const self = this;
        const headerTransformer = new HeaderTransform(transformReq);
        const pipestream = socket.pipe(headerTransformer);

        headerTransformer.on('error', function (err: Error) {
            socket.write(`HTTP/1.1 200 OK\r\n\r\n${err.message}!`);
            socket.destroy();
        });

        function transformReq(req: HttpReq) {
            let host = req.host;
            host = host.replace(/:\d+$/, '');
            const connobj = self.webTunnels[host];
            if (!connobj) {
                throw Error('service host missing');
            }
            handleConn(connobj, host);
            return req;
        }

        function handleConn(connobj: ConnectObj, host: string) {
            try {
                if (!connobj.session) {
                    throw Error('tunnel not ready');
                }
                const stream = connobj.session.openStream();
                self.logger.info('create stream on tunnel:', connobj.name);
                pipestream.pipe(stream);
                stream.pipe(socket);
                stream.on('close', () => {
                    self.logger.info('stream close====', host);
                    socket.destroy();
                });
                stream.on('error', () => socket.destroy());
                socket.on('error', function (err) {
                    stream.destroy(err);
                });
            } catch (err) {
                self.logger.info('err:', err);
                let msg = (err as Error).message;
                if (/ECONNREFUSED/.test(msg)) {
                    socket.write(`HTTP/1.1 502 Bad Gateway\r\n\r\n502 Bad Gateway, message:${msg}!`);
                } else if (/ETIMEDOUT/.test(msg)) {
                    socket.write(`HTTP/1.1 504 Gateway Timeout\r\n\r\n504 Gateway Timeout, message:${msg}!`);
                } else {
                    socket.write(`HTTP/1.1 200 OK\r\n\r\n${msg}, please service is on!`);
                }
                socket.destroy();
            }
        }
    }
    initTunnelServer() {
        const server = net.createServer(this.handleConection.bind(this));
        server.listen(this.listenPort, () => {
            this.logger.info('server listen on 127.0.0.1:' + this.listenPort);
        });
    }
    initHttpServer() {
        if (this.listenHttpPort) {
            const httpServer = net.createServer(this.handleHttpRequest.bind(this));
            httpServer.listen(this.listenHttpPort, () => {
                this.logger.info('http server listen on 127.0.0.1:' + this.listenHttpPort);
            });
        }
        if (this.listenHttpsPort) {
            const httpsServer = tls.createServer(this.tlsOpts, this.handleHttpRequest.bind(this));
            httpsServer.listen(this.listenHttpsPort, () => {
                this.logger.info('https server listen on 127.0.0.1:' + this.listenHttpsPort);
            });
        }
    }

    bootstrap() {
        this.initTunnelServer();
        this.initHttpServer();
    }
}

export default Server;
