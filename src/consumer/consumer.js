const { createConsumer } = require('../config/kafkaConfig');
const dotenv = require('dotenv');
dotenv.config();

async function consumeMessage(topic = process.env.KAFKA_TOPIC, testId = 'test1', producerId = 'producer-1') {
    const consumer = createConsumer(testId);
    try {
        await consumer.connect();
        await consumer.subscribe({ topic: topic, fromBeginning: true });

        let receivedMessage;
        let receivedMessages = [];

        // Конфигурируем consumer, чтобы он получил только одно сообщение
        await consumer.run({
            eachMessage: async ({ message }) => {
                if (message.headers?.producerId?.toString() === producerId) {
                    console.log('Received:', message.value.toString());
                    receivedMessage = JSON.parse(message.value.toString());
                    receivedMessages.push(receivedMessage);
                }

            },
        });

        // Пауза, чтобы дать consumer время для обработки (вместо ожидания в consumer.run)
        await new Promise(resolve => setTimeout(resolve, 5000)); // Задержка в 5 секунд

        console.log(`Total messages received: ${receivedMessages.length}`); // Выводим количество сообщений

        return receivedMessage;
    } catch (error) {
        console.error('Error consuming message:', error);
        throw error; // Прокидываем ошибку дальше
    } finally {
        await consumer.disconnect(); // Отключаем consumer после обработки
    }
}

module.exports = { consumeMessage };

