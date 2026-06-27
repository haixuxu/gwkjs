import net from 'net';
import dgram from 'dgram';
import { GankClientOpts, TunnelOpts } from './types/index';
import getCustomLogger, { Logger } from './utils/logger';
import chalk from 'chalk';
import printer from './utils/printer';
import { bindStreamSocket } from './utils/socket';
import { UdpOverTcpReadStream } from './stream/udp2tcp';
import { parseIpAddrBuffer } from './utils/ipaddr';
import { doHandshake } from './protocol/handshake';
import { createClientSession, YamuxSession, YamuxStream } from './session';

class Client {
    serverHost: string;
    serverPort: number;
    authToken: string;
    tunnelsMap: Record<string, TunnelOpts>;
    logger: Logger;
    constructor(opts: GankClientOpts) {
        this.serverHost = opts.serverHost;
        this.serverPort = opts.serverPort || 4443;
        this.authToken = opts.authtoken || 'test:test123';
        this.tunnelsMap = opts.tunnels || {};
        this.logger = getCustomLogger('c>', 'debug');
    }
    handleUdpStream(tunnelConf: TunnelOpts, stream: YamuxStream) {
        const successMsg = tunnelConf.successMsg as string;
        const localPort = tunnelConf.localPort;
        const udpclientMap = new Map();
        const listenData = (buff: Buffer) => {
            const udpaddrbuf = buff.slice(0, 6);
            const ipaddr = parseIpAddrBuffer(udpaddrbuf);
            const udpclientAddr = `${ipaddr.addr}|${ipaddr.port}`;
            if (!udpclientMap.has(udpclientAddr)) {
                const client = dgram.createSocket('udp4');
                client.bind(); // bind random udp port
                const obj = { udpsocket: client, lastAt: Date.now() };
                udpclientMap.set(udpclientAddr, obj);
                const rst = new UdpOverTcpReadStream(client, udpaddrbuf);
                rst.pipe(stream);
            }
            const udpcliObj = udpclientMap.get(udpclientAddr);
            udpcliObj.lastAt = Date.now();
            udpcliObj.udpsocket.send(buff.slice(6), localPort, tunnelConf.localIp, (err: any) => {
                if (err) {
                    this.updateConsole(tunnelConf, `${successMsg} ${chalk.red('->|')}`);
                    stream.destroy(Error(err.message));
                } else {
                    this.updateConsole(tunnelConf, `${successMsg} ${chalk.green('<->')}`);
                }
            });
        };
        const listenError = (err: Error) => {
            this.updateConsole(tunnelConf, `${err.message} ${chalk.red('->|<-')}`);
        };
        bindStreamSocket(stream as any, listenData, listenError, () => listenError(Error('stream closed')));
    }
    handleTcpStream(tunnelConf: TunnelOpts, stream: YamuxStream) {
        const successMsg = tunnelConf.successMsg as string;
        const localPort = tunnelConf.localPort;
        this.updateConsole(tunnelConf, `${successMsg} ${chalk.green('->')}`);
        const localsocket = new net.Socket();
        let aborted = false;
        localsocket.connect(localPort, tunnelConf.localIp, () => {
            if (aborted) return;
            clearTimeout(timeoutid);
            this.updateConsole(tunnelConf, `${successMsg} ${chalk.green('<->')}`);
            stream.pipe(localsocket);
            localsocket.pipe(stream);
        });
        localsocket.on('close', () => {
            this.updateConsole(tunnelConf, successMsg);
            stream.destroy();
        });
        localsocket.on('error', (err) => stream.destroy(err));
        stream.on('close', () => localsocket.destroy());
        stream.on('error', () => localsocket.destroy());

        var timeoutid = setTimeout(() => {
            aborted = true;
            this.updateConsole(tunnelConf, `${successMsg} ${chalk.yellow('->')}`);
            localsocket.emit('error', Error('socket ETIMEDOUT!'));
        }, 15 * 1000);
    }

    handleStream(tunnelConf: TunnelOpts, stream: YamuxStream) {
        if (tunnelConf.tunType === 0x3) {
            this.handleUdpStream(tunnelConf, stream);
        } else {
            this.handleTcpStream(tunnelConf, stream);
        }
    }
    setupStcpBindPort(session: YamuxSession, tunnelConf: TunnelOpts) {
        const server = net.createServer((socket) => {
            const stream = session.openStream();
            socket.pipe(stream);
            stream.pipe(socket);
            socket.on('close', () => stream.destroy());
            socket.on('error', (err) => stream.destroy(err));
            stream.on('close', () => socket.destroy());
            stream.on('error', () => socket.destroy());
        });
        const hostname: string = tunnelConf.bindIp as string;
        server.listen(tunnelConf.bindPort, hostname, () => {
            // this.logger.info(`stcp local listen on ${tunnelConf.bindIp}:${tunnelConf.bindPort}`);
        });
        server.on('error', (err) => {
            this.logger.error(err);
        });
        tunnelConf.server = server;
    }

    async setupTunnel(tunnelConf: TunnelOpts) {
        const targetSocket = new net.Socket();
        this.updateConsole(tunnelConf, 'connecting');
        targetSocket.connect(this.serverPort, this.serverHost, async () => {
            try {
                this.updateConsole(tunnelConf, 'connect ok, starting auth');
                const message = await doHandshake(targetSocket, this.authToken, tunnelConf);

                const proto = tunnelConf.tunType === 0x3 ? 'udp' : 'tcp';
                const localPort = tunnelConf.localPort || tunnelConf.bindPort;
                const showIp = tunnelConf.bindPort ? tunnelConf.bindIp : tunnelConf.localIp;
                const successMsg = `${chalk.green('ok')}: ${message} <=> ${proto}://${showIp}:${localPort} ${tunnelConf.bindPort ? 'LISTEN' : ''}`;
                tunnelConf.successMsg = successMsg;
                this.updateConsole(tunnelConf, successMsg);

                const session = createClientSession(targetSocket);
                session.on('error', (err: Error) => {
                    this.updateConsole(tunnelConf, `tunnel ${chalk.red('err')}:${err.message}`);
                });

                if (tunnelConf.tunType === 0x4 && tunnelConf.bindIp && tunnelConf.bindPort) {
                    // stcp_right: 本地监听 bindPort，主动开流到服务端
                    this.setupStcpBindPort(session, tunnelConf);
                } else {
                    // tcp / web / udp / stcp_left: 接收服务端开的流
                    session.on('stream', (stream: YamuxStream) => this.handleStream(tunnelConf, stream));
                }
            } catch (err) {
                this.updateConsole(tunnelConf, `handshake ${chalk.red('err')}:${(err as Error).message}`);
                targetSocket.destroy();
            }
        });
        targetSocket.on('error', (err: Error) => {
            this.updateConsole(tunnelConf, err.message);
        });
        targetSocket.on('close', () => {
            if (tunnelConf.server) {
                tunnelConf.server.close(); // release
                tunnelConf.server = undefined;
            }
            setTimeout(() => this.setupTunnel(tunnelConf), 3000);
        });
    }

    updateConsole(tunopts: TunnelOpts, statusText: string) {
        tunopts.status = statusText;
        this.showConsole();
    }

    showConsole() {
        const keys = Object.keys(this.tunnelsMap);
        let message = 'tunnel list:\n';
        keys.forEach((key: string) => {
            const tunnelConf = this.tunnelsMap[key];
            message += tunnelConf.name?.padEnd(16) + '';
            message += tunnelConf.status + '\n';
        });
        printer.printStatus(message);
    }

    bootstrap() {
        Object.values(this.tunnelsMap).forEach((temp: TunnelOpts) => this.setupTunnel(temp));
    }
}

export default Client;
