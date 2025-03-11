const { producer } = require('../config/kafkaConfig');
const dotenv = require('dotenv');
const fs = require('fs');
const path = require('path');
dotenv.config();

// Функция для загрузки JSON-файла с сообщением
function loadMessage(fileName) {
    const filePath = path.join(__dirname, '../../messages', fileName);
    if (!fs.existsSync(filePath)) {
        throw new Error(`Файл ${fileName} не найден в папке messages/`);
    }
    const rawData = fs.readFileSync(filePath);
    return JSON.parse(rawData);
}

// Функция для отправки сообщения в Kafka, теперь принимает topic как параметр
async function sendMessage(fileName = 'example.json', topic = process.env.KAFKA_TOPIC, producerId = 'producer-1') {
    await producer.connect();

    const message = loadMessage(fileName); // Загружаем сообщение из файла

    await producer.send({
        topic: topic, // Используем динамический topic
        messages: [{
            value: JSON.stringify(message),
            headers: { producerId: producerId },
        }],
    });

    console.log('Sent:', message);
    await producer.disconnect();

    return message; // Возвращаем message
}

// Для ручного запуска из CLI
if (require.main === module) {
    const fileName = process.argv[2] || 'example.json';
    const topic = process.argv[3] || process.env.KAFKA_TOPIC; // Топик из командной строки или по умолчанию
    sendMessage(fileName, topic).catch(console.error);
}

module.exports = { sendMessage };

