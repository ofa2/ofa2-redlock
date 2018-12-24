import _ from 'lodash';
import redis from 'redis';
import Redlock from 'redlock';

interface IConfig {
  [key: string]: any;
}

// eslint-disable-next-line no-unused-vars
export default function lift(this: { config: IConfig; redlock?: Redlock }) {
  let { config } = this;
  let { prefix, connection: connectionName } = config.redlock;

  let connectionConfig = config.connections[connectionName];

  if (!connectionConfig) {
    throw new Error(`Undefined connection ${connectionName}`);
  }

  let clients = _.map(connectionConfig.hosts, (host: string) => {
    let options = _.assign({}, connectionConfig);
    delete options.hosts;
    options = _.assign({ port: 6379 }, options, host);
    return redis.createClient(options);
  });

  let redlock = new Redlock(
    // you should have one client for each redis node
    // in your cluster
    clients,
    _.extend(
      {
        // the expected clock drift; for more details
        // see http://redis.io/topics/distlock
        driftFactor: 0.01,

        // the max number of times Redlock will attempt
        // to lock a resource before erroring
        retryCount: 3,

        // the time in ms between attempts
        retryDelay: 200,
      },
      config.redlock.options
    )
  );

  let originLock = redlock.lock.bind(redlock);
  redlock.lock = function lock(resource: string, ttl: number) {
    let lockKey = prefix + resource;
    return originLock(lockKey, ttl);
  };

  this.redlock = redlock;
}
