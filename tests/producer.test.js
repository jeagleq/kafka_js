const { sendMessage } = require('../src/producer/producer'); // Импортируем sendMessage из producer.js
const { validate } = require('jsonschema');
const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');

dotenv.config();

describe('Kafka Producer', () => {
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

    test('Producer should send valid JSON messages to Kafka', async () => {
        // Отправка сообщения
        const message = await sendMessage();

        // Путь к файлу с примерным сообщением, которое отправляется через продюсера
        // const messagePath = path.join(__dirname, '../messages/example.json');
        // const sentMessage = JSON.parse(fs.readFileSync(messagePath, 'utf8'));

        // Валидация отправленного сообщения с использованием схемы
        const validationResult = validate(message, messageSchema);

        // Проверка, что сообщение соответствует схеме
        expect(validationResult.valid).toBe(true);

        // Дополнительно, можно проверить конкретные поля
        expect(message).toHaveProperty('text');
        expect(message).toHaveProperty('timestamp');
        expect(typeof message.text).toBe('string');
        expect(typeof message.timestamp).toBe('number');
    });
});

