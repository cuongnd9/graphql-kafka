import { Kafka } from 'kafkajs';
import { PubSub } from 'graphql-subscriptions';

const withUnsubscribe = (asyncIterator: any, onCancel: any) => {
  const asyncReturn = asyncIterator.return;

  // eslint-disable-next-line no-param-reassign
  asyncIterator.return = () => {
    onCancel();
    return asyncReturn ? asyncReturn.call(asyncIterator) : Promise.resolve({
      value: undefined,
      done: true
    });
  };

  return asyncIterator;
};

const kafka = new Kafka({
  clientId: 'my-app',
  brokers: ['0.0.0.0:9092'],
});
const pubsub = new PubSub();

const CHAT_CHANNEL = 'ABC_XYZ';

let chats = [
  {
    id: '1',
    from: '103cuong',
    content: 'hi',
    createdAt: '',
  },
];

const resolver = {
  Query: {
    chats: () => chats,
  },

  Mutation: {
    createChat: async (_: any, {
      content,
      from,
    }: any) => {
      const id = `_${
        Math.random()
          .toString(36)
          .substr(2, 9)}`;
      const newChat = {
        id,
        from,
        content,
        createdAt: new Date().toISOString(),
      };

      chats = [newChat, ...chats];
      chats = chats.splice(0, 8);

      const producer = kafka.producer();
      await producer.connect();
      await producer.send({
        topic: CHAT_CHANNEL,
        messages: [
          { value: JSON.stringify(newChat) },
        ],
      });
      await producer.disconnect();

      return newChat;
    },
  },

  Subscription: {
    messageSent: {
      subscribe: async () => {
        const consumer = kafka.consumer({ groupId: 'test-group' });
        await consumer.connect();
        // await consumer.subscribe({ topic: CHAT_CHANNEL, fromBeginning: true });
        await consumer.subscribe({ topic: CHAT_CHANNEL });

        await consumer.run({
          eachMessage: async ({ message }) => {
            const newChat = JSON.parse(message.value?.toString() || '{}');
            await pubsub.publish(CHAT_CHANNEL, { messageSent: newChat });
          },
        });
        return withUnsubscribe(pubsub.asyncIterator(CHAT_CHANNEL), async () => {
          await consumer.disconnect();
        });
      },
    },
  },
};

export default resolver;
