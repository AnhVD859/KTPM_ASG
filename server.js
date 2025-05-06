const express = require('express');
const multer = require('multer');
const amqp = require('amqplib/callback_api');
const cors = require('cors');
const fs = require('fs');
const path = require('path');

const DB_FILE_PATH = path.join(__dirname, 'db.json');

const app = express();
app.use(cors());
app.use(express.static('public'));

const storage = multer.diskStorage({
    destination: function (req, file, cb) {
        cb(null, 'data/');
    },
    filename: function (req, file, cb) {
        cb(null, file.originalname);
    }
});
const upload = multer({ storage: storage });

let rabbitConnection = null;
let rabbitChannel = null;

function initRabbitMQ() {
    return new Promise((resolve, reject) => {
        amqp.connect('amqp://localhost', (err, connection) => {
            if (err) {
                console.error('Không thể kết nối tới RabbitMQ:', err);
                return reject(err);
            }

            rabbitConnection = connection;

            connection.createChannel((err, channel) => {
                if (err) {
                    console.error('Không thể tạo channel RabbitMQ:', err);
                    return reject(err);
                }

                rabbitChannel = channel;
                channel.assertQueue('ocr_queue', { durable: true });
                channel.assertQueue('translation_queue', { durable: true });
                channel.assertQueue('pdf_queue', { durable: true });
                channel.assertQueue('result_queue', { durable: true });

                console.log('Đã kết nối thành công tới RabbitMQ');
                resolve();
            });
        });
    });
}

// API upload ảnh
app.post('/upload', upload.single('image'), (req, res) => {
    if (!req.file) {
        return res.status(400).json({ error: 'Không có file được tải lên' });
    }

    const db = fs.existsSync(DB_FILE_PATH) ? JSON.parse(fs.readFileSync(DB_FILE_PATH, 'utf-8')) : [];
    const result = db.find(entry => entry.originalFilePath === req.file.path);
    if (result && fs.existsSync(result.pdfFilePath)) {
        return res.download(pdfPath);
    }

    const imageData = {
        originalFilePath: req.file.path,
        timestamp: new Date().toISOString(),
        status: 'processing',
        id: Date.now().toString()
    };

    // Gửi job vào OCR queue
    rabbitChannel.sendToQueue('ocr_queue', Buffer.from(JSON.stringify(imageData)), {
        persistent: true
    });

    // Đợi kết quả từ result_queue, tạo consumer cho kết quả cuối cùng
    const resultConsumer = rabbitChannel.consume('result_queue', (msg) => {
        if (msg !== null) {
            const result = JSON.parse(msg.content.toString());
            if (result.id === imageData.id) {
                rabbitChannel.cancel(msg.fields.consumerTag);
                rabbitChannel.ack(msg);
                return res.download(result.pdfPath);
            } else {
                rabbitChannel.nack(msg);
            }
        }
    }, { noAck: false });
});

app.get('/status/:id', (req, res) => {
    const id = req.params.id;
    const dbFilePath = path.join(__dirname, 'db.json');

    if (!fs.existsSync(dbFilePath)) {
        return res.status(404).json({ error: 'Không tìm thấy dữ liệu' });
    }

    const db = JSON.parse(fs.readFileSync(dbFilePath, 'utf-8'));
    const entry = db.find(item => item.id === id);

    if (!entry) {
        return res.status(404).json({ error: 'Không tìm thấy job' });
    }

    res.json(entry);
});

const PORT = process.env.PORT || 5000;

async function startServer() {
    try {
        await initRabbitMQ();
        app.listen(PORT, () => {
            console.log(`Server đang chạy trên cổng ${PORT}`);
        });
    } catch (error) {
        console.error('Không thể khởi động server:', error);
        process.exit(1);
    }
}

startServer();
