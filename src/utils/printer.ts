
// @ts-ignore
import stringWidth from 'string-width';

let isFirstStatusPrinted = false;

let lastMsg = '';

// In-place status rendering only works on a real terminal. When stdout is a pipe/file
// (nohup, systemd, pm2, docker without -t, CI), the TTY-only methods are missing and
// process.stdout.columns is undefined, which previously crashed or spun in an infinite loop.
const isInteractive = (): boolean =>
    Boolean(process.stdout.isTTY) && typeof (process.stdout as any).cursorTo === 'function';

function getLastMsgLines() {
    const columns = process.stdout.columns || 80;
    const lines = lastMsg.split('\n');
    let count = 0;
    lines.forEach((line: string) => {
        count++;

        const len = stringWidth(line);
        if (len > columns) {
            count += Math.floor(len / columns);
        }
    });

    return count;
}

var printer = {
    printStatus(message: string) {
        if (!isInteractive()) {
            // Plain, append-only output for non-TTY: skip repaints, only log when changed.
            if (message !== lastMsg) {
                process.stdout.write(message + '\n');
                lastMsg = message;
            }
            return;
        }
        if (!isFirstStatusPrinted) {
            isFirstStatusPrinted = true;
            process.stdout.write('\n');
        } else {
            const lines = getLastMsgLines();
            for (var i = 0; i < lines; i++) {
                if (i !== 0) {
                    process.stdout.moveCursor(0, -1); // 1b5b3141
                }
                process.stdout.clearLine(0); // 1b5b304b
            }
        }
        process.stdout.cursorTo(0); // == process.stdout.write(Buffer.from('1b5b3130303044','hex))
        process.stdout.write(message);
        lastMsg = message;
    },
};

export default printer;
