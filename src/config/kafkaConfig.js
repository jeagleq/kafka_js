const { Kafka } = require('kafkajs');
const dotenv = require('dotenv');
dotenv.config();

/**
 * Создает Kafka consumer с уникальным groupId для теста
 * @param {string} testId - Идентификатор теста (например, 'test1', 'test2')
 * @returns {KafkaConsumer} - Kafka consumer с уникальным groupId
 */
function createConsumer(testId) {
    const kafka = new Kafka({
        clientId: `my-app-${testId}`,
        brokers: [process.env.KAFKA_BROKER],
    });

    return kafka.consumer({ groupId: `test-group-${testId}` });
}

// Инициализируем общий Kafka-инстанс и продюсер
const kafka = new Kafka({
    clientId: 'my-app',
    brokers: [process.env.KAFKA_BROKER],
    // Если используется SASL/SSL, раскомментируй и добавь креды в .env
    // sasl: {
    //     mechanism: 'plain',
    //     username: process.env.KAFKA_API_KEY,
    //     password: process.env.KAFKA_API_SECRET,
    // },
    // ssl: true,
});

const producer = kafka.producer();
//const consumer = kafka.consumer({ groupId: 'test-group' });

module.exports = { kafka, producer, createConsumer };
