const fetch = require('node-fetch');
global.fetch = fetch;
global.Headers = fetch.Headers;

const express = require('express');
const multer = require('multer');
const amqp = require('amqplib/callback_api');
const cors = require('cors');

const app = express();
app.use(cors());

const fs = require('fs');
const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, 'data/');
    },
    filename: function (req, file, cb) {
        cb(null, file.originalname); // giữ tên gốc
    }
});
const upload = multer({ storage: storage });

const { imageConsumer, DB_FILE_PATH } = require('./image-consumer');
imageConsumer();

app.use(express.static('public'));

// Biến lưu trữ kết nối và channel RabbitMQ
let rabbitConnection = null;
let rabbitChannel = null;

// Hàm khởi tạo kết nối RabbitMQ
const initRabbitMQ = () => {
    amqp.connect('amqp://localhost', (error0, connection) => {
        if (error0) {
            console.error('Failed to connect to RabbitMQ:', error0);
            // Thử lại sau 5 giây
            setTimeout(initRabbitMQ, 5000);
            return;
        }

        rabbitConnection = connection;

        // Xử lý khi kết nối bị đóng
        connection.on('error', (err) => {
            console.error('RabbitMQ connection error:', err);
            rabbitConnection = null;
            rabbitChannel = null;
            setTimeout(initRabbitMQ, 5000);
        });

        connection.on('close', () => {
            console.error('RabbitMQ connection closed');
            rabbitConnection = null;
            rabbitChannel = null;
            setTimeout(initRabbitMQ, 5000);
        });

        // Tạo channel
        connection.createChannel((error1, channel) => {
            if (error1) {
                console.error('Failed to create RabbitMQ channel:', error1);
                connection.close();
                rabbitConnection = null;
                setTimeout(initRabbitMQ, 5000);
                return;
            }

            const queue = 'image-processing';
            channel.assertQueue(queue, {
                durable: false
            });

            rabbitChannel = channel;
            console.log('RabbitMQ connected and channel created');
        });
    });
};

// Khởi động kết nối RabbitMQ khi server khởi động
initRabbitMQ();

// Hàm gửi message tới RabbitMQ
const sendToQueue = (message, res) => {
    if (!rabbitChannel) {
        console.error('RabbitMQ channel not available');
        return res.status(500).send('RabbitMQ channel not available');
    }

    try {
        const queue = 'image-processing';
        rabbitChannel.sendToQueue(queue, Buffer.from(message), { persistent: true });
        console.log(" [x] Sent %s", message);
    } catch (err) {
        console.error('Failed to send message to queue:', err);
        return res.status(500).send('Failed to send message to RabbitMQ');
    }
};

app.post('/upload', upload.single('image'), (req, res) => {
    if (!req.file) {
        console.error("❌ Không nhận được file upload từ client");
        return res.status(400).json({ error: "No file uploaded" });
    }
    const filePath = req.file.path;
    const message = JSON.stringify({ filePath });

    const db = fs.existsSync(DB_FILE_PATH)
    ? JSON.parse(fs.readFileSync(DB_FILE_PATH, 'utf-8'))
    : [];
    const result = db.find(entry => entry.originalFilePath === filePath);
    if (result) {
        const pdfPath = result.pdfFilePath;
        return res.download(pdfPath);
    }

    // Gửi message tới RabbitMQ
    sendToQueue(message, res);

    // Chờ xử lý và kiểm tra kết quả trong db.json
    const checkResult = setInterval(() => {
        try {
            const db2 = fs.existsSync(DB_FILE_PATH)
                ? JSON.parse(fs.readFileSync(DB_FILE_PATH, 'utf-8'))
                : [];
            const result = db2.find(entry => entry.originalFilePath === filePath);

            if (result) {
                clearInterval(checkResult);
                const pdfPath = result.pdfFilePath;
                return res.download(pdfPath);
            }
        } catch (err) {
            clearInterval(checkResult);
            console.error('Error checking db.json:', err);
            return res.status(500).send('Error checking db.json');
        }
    }, 1000);
});

const PORT = process.env.PORT || 5000;
app.listen(PORT, () => {
    console.log(`Server is running on port ${PORT}`);
});

// Xử lý khi server tắt
process.on('SIGINT', () => {
    if (rabbitConnection) {
        rabbitConnection.close(() => {
            console.log('RabbitMQ connection closed');
            process.exit(0);
        });
    } else {
        process.exit(0);
    }
});