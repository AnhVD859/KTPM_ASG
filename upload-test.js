const axios = require('axios');
const FormData = require('form-data');
const fs = require('fs-extra');
const path = require('path');
const createCsvWriter = require('csv-writer').createObjectCsvWriter;

// Cấu hình
const API_ENDPOINT = 'http://localhost:5000/upload';
const TEST_IMAGES_FOLDER = './test-image';
const CONCURRENCY = 20;
const RESULT_FILE = './upload-test-result.csv';

if (!fs.existsSync(TEST_IMAGES_FOLDER)) {
  console.error(`Không tìm thấy thư mục ảnh test: ${TEST_IMAGES_FOLDER}`);
  process.exit(1);
}

const imageFiles = fs.readdirSync(TEST_IMAGES_FOLDER)
  .filter(file => {
    const ext = path.extname(file).toLowerCase();
    return ['.jpg', '.jpeg', '.png', '.bmp', '.gif', '.tiff'].includes(ext);
  })
  .map(file => path.join(TEST_IMAGES_FOLDER, file));

if (imageFiles.length === 0) {
  console.error(`Không tìm thấy file ảnh nào trong thư mục: ${TEST_IMAGES_FOLDER}`);
  process.exit(1);
}

console.log(`Đã tìm thấy ${imageFiles.length} file ảnh để test.`);

const csvWriter = createCsvWriter({
  path: RESULT_FILE,
  header: [
    { id: 'requestId', title: 'Request ID' },
    { id: 'imageFile', title: 'Image File' },
    { id: 'startTime', title: 'Start Time' },
    { id: 'endTime', title: 'End Time' },
    { id: 'duration', title: 'Duration (ms)' },
    { id: 'status', title: 'Status' },
    { id: 'pdfUrl', title: 'PDF URL' }
  ]
});

async function sendRequest(requestId, imageFile) {
  const formData = new FormData();
  formData.append('image', fs.createReadStream(imageFile));

  const startTime = Date.now();
  const startTimeFormatted = new Date(startTime).toISOString();
  
  try {
    const response = await axios.post(API_ENDPOINT, formData, {
      headers: {
        ...formData.getHeaders()
      },
      timeout: 60000 
    });
    
    const endTime = Date.now();
    const endTimeFormatted = new Date(endTime).toISOString();
    const duration = endTime - startTime;
    
    return {
      requestId,
      imageFile: path.basename(imageFile),
      startTime: startTimeFormatted,
      endTime: endTimeFormatted,
      duration,
      status: response.status,
      pdfUrl: response.data && response.data.pdfUrl ? response.data.pdfUrl : 'N/A'
    };
  } catch (error) {
    const endTime = Date.now();
    const endTimeFormatted = new Date(endTime).toISOString();
    const duration = endTime - startTime;
    
    return {
      requestId,
      imageFile: path.basename(imageFile),
      startTime: startTimeFormatted,
      endTime: endTimeFormatted,
      duration,
      status: error.response ? error.response.status : 'ERROR',
      pdfUrl: 'ERROR'
    };
  }
}

async function runTest() {
  console.log(`Bắt đầu chạy test với ${imageFiles.length} request đồng thời`);

  const results = [];
  const startTimeTest = Date.now();
  
  const tasks = [];
  for (let i = 0; i < imageFiles.length; i++) {
    tasks.push({
      requestId: i + 1,
      imageFile: imageFiles[i]
    });
  }
  
  await processInBatches(tasks, async (task) => {
    const result = await sendRequest(task.requestId, task.imageFile);
    console.log(`Request #${task.requestId} - ${path.basename(task.imageFile)}: hoàn thành sau ${result.duration}ms với status ${result.status}`);
    results.push(result);
  }, CONCURRENCY);
  
  const endTimeTest = Date.now();
  const totalDuration = endTimeTest - startTimeTest;
  
  results.sort((a, b) => a.requestId - b.requestId);
  
  const successResults = results.filter(r => r.status === 200);
  const successCount = successResults.length;
  const avgDuration = successCount > 0 
    ? successResults.reduce((sum, r) => sum + r.duration, 0) / successCount 
    : 0;
  
  const minDuration = successCount > 0 
    ? Math.min(...successResults.map(r => r.duration)) 
    : 0;
  
  const maxDuration = successCount > 0 
    ? Math.max(...successResults.map(r => r.duration)) 
    : 0;
  
  console.log('\n===== KẾT QUẢ TEST =====');
  console.log(`Tổng thời gian chạy test: ${totalDuration}ms`);
  console.log(`Số lượng ảnh đã test: ${imageFiles.length}`);
  console.log(`Số request thành công: ${successCount}`);
  console.log(`Tỷ lệ thành công: ${(successCount / imageFiles.length * 100).toFixed(2)}%`);
  console.log(`Thời gian trung bình mỗi request: ${avgDuration.toFixed(2)}ms`);
  console.log(`Thời gian ngắn nhất: ${minDuration}ms`);
  console.log(`Thời gian dài nhất: ${maxDuration}ms`);
  console.log(`Throughput: ${(successCount / (totalDuration / 1000)).toFixed(2)} requests/giây`);
  
  await csvWriter.writeRecords(results);
  console.log(`\nKết quả chi tiết đã được lưu vào file: ${RESULT_FILE}`);
  
  return {
    results,
    stats: {
      totalRequests: imageFiles.length,
      successCount,
      avgDuration,
      minDuration,
      maxDuration,
      totalDuration
    }
  };
}

async function processInBatches(tasks, processFn, concurrency) {
  const tasksCopy = [...tasks];
  const running = [];
  
  async function processBatch() {
    if (tasksCopy.length === 0) return;
    
    const task = tasksCopy.shift();
    
    const promise = (async () => {
      try {
        await processFn(task);
      } catch (error) {
        console.error(`Lỗi khi xử lý task #${task.requestId}:`, error);
      } finally {
        const index = running.indexOf(promise);
        if (index !== -1) running.splice(index, 1);
        return processBatch();
      }
    })();
    
    running.push(promise);
    
    if (running.length < concurrency && tasksCopy.length > 0) {
      return processBatch();
    }
  }
  
  await processBatch();
  
  if (running.length > 0) {
    await Promise.all(running);
  }
}

async function main() {
  try {
    console.log('===== BẮT ĐẦU KIỂM TRA HIỆU NĂNG API =====');
    console.log(`Endpoint: ${API_ENDPOINT}`);
    console.log(`Thư mục ảnh test: ${TEST_IMAGES_FOLDER}`);
    console.log(`Số lượng request đồng thời tối đa: ${CONCURRENCY}`);
    console.log('---------------------------------------------');
    
    await runTest();
    
  } catch (error) {
    console.error('Lỗi khi chạy test:', error);
  }
}

main();
