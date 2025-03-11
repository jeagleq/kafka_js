const dotenv = require('dotenv');
const { validate } = require('jsonschema'); // Импортируем jsonschema для валидации
const { sendMessage } = require('../src/producer/producer'); // Импортируем sendMessage из producer.js
const { consumeMessage } = require('../src/consumer/consumer'); // Импортируем sendMessage из consumer.js
const fs = require('fs');
const path = require('path');

dotenv.config(); // Загружаем переменные окружения из .env файла
jest.setTimeout(120000); // Увеличиваем тайм-аут для интеграционных тестов

//топик
const topic = process.env.KAFKA_TOPIC
// Схема для валидации полученного сообщения
let messageSchema;

beforeAll(() => {
    // Загружаем схему перед выполнением тестов
    const schemaPath = path.join(__dirname, '../schemas', 'exampleSchema.json');
    if (!fs.existsSync(schemaPath)) {
        throw new Error(`Файл схемы ${schemaPath} не найден`);
    }
    const rawSchema = fs.readFileSync(schemaPath, 'utf8');
    messageSchema = JSON.parse(rawSchema);
});


test('Kafka producer sends and consumer receives a valid message', async () => {
    // Отправляем сообщение через продюсера
    const messageContent = await sendMessage('example.json', topic);
    console.log("Sending message:", messageContent);

    // Консьюмер начинает прослушивать сообщения
    const receivedMessage = await consumeMessage(topic, 'test1', 'producer-1')
    console.log("Received message after delay:", receivedMessage);

    // Валидация полученного сообщения
    const validationResult = validate(receivedMessage, messageSchema);
    expect(validationResult.valid).toBe(true); // Проверка, что сообщение соответствует схеме

    // Дополнительно, можно проверить поля
    expect(receivedMessage).toHaveProperty('text');
    expect(receivedMessage).toHaveProperty('timestamp');
    expect(typeof receivedMessage.text).toBe('string');
    expect(typeof receivedMessage.timestamp).toBe('number');
}, 90000);



