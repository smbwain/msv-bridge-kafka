import {Bus} from './microbus';
import * as serializeError from 'serialize-error';
import {createServer, createConnection, Server} from 'net';
import safeEventsToPromise from './safe-events-to-promise';
import * as defer from 'defer-promise';
import {Profiler} from "msv-logger";
import {BridgeInterface, ServiceExportedMethods, BridgeOptions} from 'msv';

type Defer<T> = {
    promise: Promise<T>;
    resolve: (value: T) => void;
    reject: (err: Error) => void;
    timeout?: number,
    profiler: Profiler
};

type TaskSendData = {
    key: string,
    data: any,
    successEvent?: string,
    errorEvent?: string,
    rpcSender?: {
        host: string,
        port: number
    }
};

class FastNetServer {
    private address : {
        host: string,
        port: number
    };
    private listener : (data: any) => void;
    private server : Server;

    /**
     * @param host
     * @param port
     * @param listener
     */
    constructor({host, port}, listener) {
        this.address = {host, port};
        this.listener = listener;
    }

    async listen() {
        this.server = createServer(socket => {
            const data = [];
            socket.setEncoding('utf8');
            socket.on('data', piece => data.push(piece));
            socket.on('end', () => {
                this.listener(JSON.parse(data.join('')));
            });
        });
        await new Promise((resolve, reject) => {
            this.server.listen(this.address, err => err ? reject(err) : resolve());
        });
        this.address.port = this.server.address().port;
    }

    async close() {
        if (this.server) {
            await new Promise((resolve, reject) => {
                this.server.close(err => err ? reject(err) : resolve());
            });
        }
    }

    getAddress() {
        return {...this.address};
    }
}

class FastNetClient {
    async send({host, port}, data) {
        const connection = createConnection({
            host,
            port
        });
        await safeEventsToPromise(connection, 'connect', ['error', 'timeout']);
        connection.end(JSON.stringify(data));
        await safeEventsToPromise(connection, 'close', ['error', 'timeout']);
    }
}

class KafkaBridge implements BridgeInterface {
    public logger;
    private profiling : boolean;
    private myAddress : {
        host: string,
        port: number
    };
    private myExternalAddress : {
        host: string,
        port: number
    };
    private waitingTasks : {
        [key: string]: Defer<any>
    } = {};
    private consumers = {};
    private netClient : FastNetClient;
    private netServer : FastNetServer;
    private bus : Bus;
    private taskResponseTimeout : number;
    private taskCnt : number = 0;

    constructor({config, logger} : BridgeOptions) {
        this.logger = logger;
        this.profiling = !!parseInt(config.get('profiling', '0'));
        this.myAddress = {
            host: config.get('myAddress').split(':')[0],
            port: parseInt(config.get('myAddress').split(':')[1]) || undefined
        };
        // console.log('a1>>', this.myAddress);
        if(config.has('myExternalAddress')) {
            this.myExternalAddress = {
                host: config.get('myExternalAddress').split(':')[0],
                port: parseInt(config.get('myExternalAddress').split(':')[1]) || undefined
            };
            // console.log('a2>>', this.myExternalAddress);
        }
        this.netClient = new FastNetClient();
        this.netServer = new FastNetServer(this.myAddress, ({key, error, data}) => {
            const def = this.waitingTasks[key];
            if (!def) {
                this.logger.error(`No task found. Key: ${key}`);
                return;
            }
            if (def.timeout) {
                clearTimeout(def.timeout);
            }
            delete this.waitingTasks[key];
            def.profiler(`response received`);
            if (error) {
                // deserialize error
                const {message, ...rest} = error;
                const err = new Error(message);
                for(const propName in rest) {
                    if(rest.hasOwnProperty(propName)) {
                        err[propName] = rest[propName];
                    }
                }
                def.reject(err);
            } else {
                def.resolve(data);
            }
        });
        this.bus = new Bus({
            kafkaServers: config.get('servers'),
            appName: config.get('appName', 'default'),
            logger: this.logger
        });
        this.taskResponseTimeout = parseInt(config.get('taskResponseTimeout', '20000'));
    }

    public enable() {};
    public disable() {};

    async init() {
        await this.netServer.listen();
        this.myAddress = this.netServer.getAddress();
        this.logger.debug(`RPC listener is on ${this.myAddress.host}:${this.myAddress.port}`);
        await this.bus.init();
    }

    async startListening(id : string, {tasks, events} : ServiceExportedMethods) : Promise<void> {
        const promises = [];
        const consumers = this.consumers[id] = [];
        for (const taskName in tasks) {
            const handler = tasks[taskName].handler;
            promises.push(this.bus.consume(
                `task${taskName[0].toUpperCase()}${taskName.slice(1)}`,
                {
                    groupId: `msv_${id}`
                },
                async({key, rpcSender, data, successEvent, errorEvent}) => {
                    let res;
                    const profiler = this.logger.profiler(`in-task-${this.taskCnt++}`);
                    profiler(`start processing ${taskName}`);
                    try {
                        res = await handler(data);
                    } catch (err) {
                        const error = serializeError(err);
                        if (rpcSender) {
                            profiler('sending error response');
                            await this.netClient.send(rpcSender, {
                                key,
                                error
                            });
                            profiler(`finish error response`);
                        }
                        if (errorEvent) {
                            profiler('sending error event');
                            await this.send(errorEvent, {
                                key,
                                request: data,
                                error
                            });
                            profiler('finish sending error event');
                        }
                        return;
                    }
                    if (rpcSender) {
                        profiler('sending response');
                        await this.netClient.send(rpcSender, {
                            key,
                            data: res
                        });
                        profiler('finish response');
                    }
                    if (successEvent) {
                        profiler('sending event');
                        await this.send(successEvent, {
                            key,
                            request: data,
                            response: res
                        });
                        profiler('finish sending event');
                    }
                }
            ).then(consumer => {
                consumers.push(consumer);
            }));
        }
        for (const eventName in events) {
            const handler = events[eventName].handler;
            promises.push(this.bus.consume(
                `event${eventName[0].toUpperCase()}${eventName.slice(1)}`,
                {
                    groupId: `msv_${id}`
                },
                ({data}) => handler(data)
            ).then(consumer => {
                consumers.push(consumer)
            }));
        }

        await Promise.all(promises);
    }

    async stopListening(id) {
        if(this.consumers[id]) {
            await Promise.all(this.consumers[id].map(consumer => consumer.close()));
        }
        delete this.consumers[id];
    }

    async deinit() {
        await this.bus.closeConsumers();
        // todo: await for current jobs finish
        await this.netServer.close();
        await this.bus.deinit();
    }

    async run(
        taskName : string,
        data : {},
        {wait, successEvent, errorEvent} : {
            wait? : number | boolean,
            successEvent?: string,
            errorEvent?: string
        } = {}
    ) : Promise<any> {
        let def;
        const key = taskName + ':' + Math.random().toString().slice(2);
        const sendData : TaskSendData = {
            key,
            data,
            successEvent,
            errorEvent
        };
        if (wait == null) {
            wait = this.taskResponseTimeout;
        } else {
            wait = parseInt(wait as any) || 0;
        }
        if (wait) {
            sendData.rpcSender = this.myExternalAddress || this.myAddress;
            def = defer();
            this.waitingTasks[key] = def;
            def.profiler = this.logger.profiler(`ext-task-${key}`);
            def.profiler(`create new task ${taskName}`);
        }
        await this.bus.send(`task${taskName[0].toUpperCase()}${taskName.slice(1)}`, sendData);
        if (def) {
            def.timeout = setTimeout(() => {
                delete this.waitingTasks[key];
                def.profiler(`delete`);
                def.reject(new Error(`Task response timeout exceeded. Task: ${taskName}`));
            }, wait);
            return await def.promise;
        }
    }

    async send(eventName : string, data : any) : Promise<void> {
        await this.bus.send(`event${eventName[0].toUpperCase()}${eventName.slice(1)}`, {data});
    }
}

export default function () {
    return (options : BridgeOptions) => new KafkaBridge(options);
};