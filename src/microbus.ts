
import * as Kafka from 'no-kafka';
import {Logger} from 'msv-logger';

export type Consumer = {
    close: () => Promise<void>;
};

export class Bus {
    private appName : string;
    private consumers : (Kafka.GroupConsumer)[];
    private kafkaServers : string;
    public logger : Logger;
    private logFunction : (type: string, ...rest: any[]) => void;
    private producer : Kafka.Producer;

    constructor({
        kafkaServers,
        appName,
        logger
    } : {
        kafkaServers: string,
        appName: string
        logger: Logger
    }) {
        this.appName = appName;
        this.consumers = [];
        this.kafkaServers = kafkaServers;
        this.logger = logger;

        const logMethods = {
            INFO: this.logger.log,
            DEBUG: this.logger.debug,
            WARN: this.logger.warn,
            ERROR: this.logger.error
        };
        this.logFunction = (msgType, ...args) => {
            (logMethods[msgType] || this.logger.warn)(...args);
        };
    }

    async init() : Promise<void> {
        this.producer = new Kafka.Producer({
            connectionString: this.kafkaServers,
            /*partitioner: function(name, list, message) {
                return Math.floor(Math.random() * list.length); // send each message to random partition
            }*/
            logger: {
                logLevel: 4,
                logFunction: this.logFunction
            }
        });
        await this.producer.init();
    }

    /**
     * Send message(s) to topic
     */
    async send(topic : string, messages : {} | {}[]) {
        if(!Array.isArray(messages)) {
            messages = [messages];
        }
        this.logger && this.logger.debug(`Sending ${(messages as {}[]).length} messages to topic "${topic}"`);
        await this.producer.send((messages as {}[]).map(m => ({
            topic: topic,
            message: {
                key: '0',
                value: JSON.stringify(m)
            }
        })));
    }

    /**
     * Start to consume messages from topic
     */
    async consume(topic : string, options : {
        groupId?: string
    }, handler : (data: any) => Promise<void>) : Promise<Consumer> {
        let that = this;
        if(typeof options == 'function') {
            handler = options;
            options = {};
        }
        // await this._createTopics([topic]);
        let groupId = options.groupId || this.appName;
        // this.log(`Starting consume from topic "${topic}" (groupId: "${groupId}")`);
        let consumer = new Kafka.GroupConsumer({
            connectionString: this.kafkaServers,
            groupId: groupId,
            logger: {
                logLevel: 4,
                logFunction: this.logFunction
            },
            idleTimeout: 0
        });
        await consumer.init([{
            strategy: new Kafka.DefaultAssignmentStrategy(),
            subscriptions: [topic],
            handler: async(messageSet, topic, partition) => {
                for(let m of messageSet) {
                    // console.log('m>', m);
                    that.logger && this.logger.debug(`Received message from topic ${topic}`);
                    try {
                        await handler(JSON.parse((m as any).message.value));
                    } catch (err) {
                        this.logger.error(err);
                    }
                    await consumer.commitOffset({topic: topic, partition: partition, offset: (m as any).offset/*, metadata: 'optional'*/});
                }
            }
        }]);
        this.consumers.push(consumer);
        return {
            close: async () => {
                await this.closeConsumer(consumer);
            }
        };
    }

    private async closeConsumer(consumer) {
        this.consumers = this.consumers.filter(item => item != consumer);
        await consumer.end();
    }

    async closeConsumers() {
        await Promise.all(this.consumers.map(consumer => this.closeConsumer(consumer)));
    }

    async deinit() {
        await this.closeConsumers();
        await this.producer.end();
    }
}