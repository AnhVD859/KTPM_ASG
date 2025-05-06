// db-manager.js
const fs = require('fs');
const path = require('path');
const { promisify } = require('util');

const readFileAsync = promisify(fs.readFile);
const writeFileAsync = promisify(fs.writeFile);
const mkdirAsync = promisify(fs.mkdir);
const existsAsync = promisify(fs.exists);

class DbManager {
  constructor(dbFilePath) {
    this.dbFilePath = dbFilePath;
    this.writeLock = false;
    this.writeQueue = [];
    this.initialized = false;
    this.data = null;
  }

  /**
   * Khởi tạo database
   */
  async init() {
    if (this.initialized) return;

    try {
      // Đảm bảo thư mục tồn tại
      const dir = path.dirname(this.dbFilePath);
      const dirExists = await existsAsync(dir);
      if (!dirExists) {
        await mkdirAsync(dir, { recursive: true });
      }

      // Kiểm tra xem file DB đã tồn tại chưa
      const fileExists = await existsAsync(this.dbFilePath);

      if (fileExists) {
        // Đọc dữ liệu hiện có
        const content = await readFileAsync(this.dbFilePath, 'utf8');
        try {
          this.data = JSON.parse(content);
        } catch (err) {
          console.error('Lỗi khi phân tích DB:', err);
          this.data = [];
        }
      } else {
        // Khởi tạo file DB mới
        this.data = [];
        await writeFileAsync(this.dbFilePath, JSON.stringify(this.data, null, 2));
      }

      this.initialized = true;
    } catch (err) {
      console.error('Lỗi khi khởi tạo DB:', err);
      throw err;
    }
  }

  /**
   * Xử lý hàng đợi ghi
   */
  async _processWriteQueue() {
    if (this.writeQueue.length === 0) {
      this.writeLock = false;
      return;
    }

    const nextWrite = this.writeQueue.shift();
    try {
      await writeFileAsync(this.dbFilePath, JSON.stringify(this.data, null, 2));
      nextWrite.resolve();
    } catch (err) {
      nextWrite.reject(err);
    }

    // Tiếp tục xử lý hàng đợi
    setImmediate(() => this._processWriteQueue());
  }

  /**
   * Ghi dữ liệu vào DB
   */
  async write() {
    if (!this.initialized) {
      await this.init();
    }

    return new Promise((resolve, reject) => {
      this.writeQueue.push({ resolve, reject });

      if (!this.writeLock) {
        this.writeLock = true;
        this._processWriteQueue();
      }
    });
  }

  /**
   * Đọc toàn bộ dữ liệu từ DB
   */
  async readAll() {
    if (!this.initialized) {
      await this.init();
    }
    return [...this.data];
  }

  /**
   * Tìm một bản ghi theo điều kiện
   * @param {Function} predicate Hàm lọc để tìm bản ghi
   */
  async findOne(predicate) {
    if (!this.initialized) {
      await this.init();
    }
    return this.data.find(predicate) || null;
  }

  /**
   * Tìm tất cả bản ghi theo điều kiện
   * @param {Function} predicate Hàm lọc để tìm bản ghi
   */
  async findAll(predicate) {
    if (!this.initialized) {
      await this.init();
    }
    return predicate ? this.data.filter(predicate) : [...this.data];
  }

  /**
   * Thêm một bản ghi mới
   * @param {Object} record Bản ghi cần thêm
   */
  async insert(record) {
    if (!this.initialized) {
      await this.init();
    }
    this.data.push(record);
    return this.write().then(() => record);
  }

  /**
   * Cập nhật một bản ghi
   * @param {Function} predicate Hàm lọc để tìm bản ghi cần cập nhật
   * @param {Object|Function} updateData Dữ liệu cập nhật hoặc hàm tạo dữ liệu cập nhật
   */
  async update(predicate, updateData) {
    if (!this.initialized) {
      await this.init();
    }
    let updated = 0;

    this.data = this.data.map(item => {
      if (predicate(item)) {
        updated++;
        if (typeof updateData === 'function') {
          return { ...item, ...updateData(item) };
        }
        return { ...item, ...updateData };
      }
      return item;
    });

    if (updated > 0) {
      await this.write();
    }
    return updated;
  }

  /**
   * Cập nhật hoặc thêm mới một bản ghi
   * @param {Function} predicate Hàm lọc để tìm bản ghi
   * @param {Object} record Bản ghi để cập nhật/thêm mới
   */
  async upsert(predicate, record) {
    if (!this.initialized) {
      await this.init();
    }
    const existingRecord = this.data.find(predicate);
    
    if (existingRecord) {
      // Cập nhật bản ghi
      this.data = this.data.map(item => predicate(item) ? { ...item, ...record } : item);
    } else {
      // Thêm mới
      this.data.push(record);
    }
    
    await this.write();
    return existingRecord ? 'updated' : 'inserted';
  }

  /**
   * Xóa các bản ghi theo điều kiện
   * @param {Function} predicate Hàm lọc để tìm bản ghi cần xóa
   */
  async delete(predicate) {
    if (!this.initialized) {
      await this.init();
    }
    const initialLength = this.data.length;
    this.data = this.data.filter(item => !predicate(item));
    
    if (this.data.length !== initialLength) {
      await this.write();
    }
    
    return initialLength - this.data.length;
  }

  /**
   * Xóa tất cả dữ liệu
   */
  async clear() {
    if (!this.initialized) {
      await this.init();
    }
    const initialLength = this.data.length;
    this.data = [];
    await this.write();
    return initialLength;
  }
}

const DB_FILE_PATH = path.join(__dirname, 'db.json');
const dbManager = new DbManager(DB_FILE_PATH);
async function saveToDb(data) {
    try {
      await dbManager.upsert(
        item => item.originalFilePath === data.originalFilePath, 
        data
      );
    } catch (error) {
      console.error('Lỗi khi lưu vào DB:', error);
    }
  }
  

module.exports = {saveToDb};
