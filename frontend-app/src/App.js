import React, { useState, useRef } from 'react';
import axios from 'axios';
import './App.css';

function App() {
  const [file, setFile] = useState(null);
  const [preview, setPreview] = useState(null);
  const [isUploading, setIsUploading] = useState(false);
  const [uploadProgress, setUploadProgress] = useState(0);
  const [isDragActive, setIsDragActive] = useState(false);
  const [error, setError] = useState(null);
  const fileInputRef = useRef(null);

  const handleDragEnter = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragActive(true);
  };

  const handleDragLeave = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragActive(false);
  };

  const handleDragOver = (e) => {
    e.preventDefault();
    e.stopPropagation();
  };

  const handleDrop = (e) => {
    e.preventDefault();
    e.stopPropagation();
    setIsDragActive(false);
    
    const files = e.dataTransfer.files;
    if (files && files.length > 0) {
      handleFile(files[0]);
    }
  };

  const handleFileInput = (e) => {
    const files = e.target.files;
    if (files && files.length > 0) {
      handleFile(files[0]);
    }
  };

  const handleFile = (file) => {
    // Kiểm tra file là ảnh
    if (!file.type.match('image.*')) {
      setError('Vui lòng chọn file ảnh.');
      return;
    }
    
    // Kiểm tra kích thước file (tối đa 10MB)
    if (file.size > 10 * 1024 * 1024) {
      setError('Kích thước file vượt quá 10MB.');
      return;
    }
    
    setFile(file);
    setError(null);
    
    // Tạo preview
    const reader = new FileReader();
    reader.onload = (e) => {
      setPreview(e.target.result);
    };
    reader.readAsDataURL(file);
    
    // Tự động upload
    handleUpload(file);
  };

  const handleUpload = async (selectedFile) => {
    const fileToUpload = selectedFile || file;
    if (!fileToUpload) return;
    
    setIsUploading(true);
    setUploadProgress(0);
    
    const formData = new FormData();
    formData.append('image', fileToUpload);
    
    try {
      // Sử dụng axios với cấu hình theo dõi tiến trình
      const response = await axios.post('http://localhost:5000/upload', formData, {
        headers: {
          'Content-Type': 'multipart/form-data'
        },
        responseType: 'blob', // Quan trọng: Đảm bảo response được xử lý như một blob
        onUploadProgress: (progressEvent) => {
          const progress = Math.round((progressEvent.loaded / progressEvent.total) * 100);
          setUploadProgress(progress);
        }
      });
      
      // Khi upload và xử lý thành công
      setIsUploading(false);
      
      // Xử lý response là file PDF
      const blob = new Blob([response.data], { type: 'application/pdf' });
      const url = window.URL.createObjectURL(blob);
      
      // Tạo link tải xuống và click tự động
      const a = document.createElement('a');
      a.style.display = 'none';
      a.href = url;
      a.download = fileToUpload.name.replace(/\.[^/.]+$/, '') + '.pdf';
      document.body.appendChild(a);
      a.click();
      
      // Dọn dẹp
      window.URL.revokeObjectURL(url);
      document.body.removeChild(a);
      
    } catch (error) {
      console.error('Lỗi:', error);
      
      // Xử lý lỗi
      if (error.response) {
        // Server trả về response với status code nằm ngoài phạm vi 2xx
        setError(`Lỗi server: ${error.response.status}`);
      } else if (error.request) {
        // Request đã được tạo nhưng không nhận được response
        setError('Không nhận được phản hồi từ server. Vui lòng kiểm tra kết nối và thử lại.');
      } else {
        // Có lỗi khi thiết lập request
        setError('Có lỗi xảy ra khi gửi yêu cầu. Vui lòng thử lại.');
      }
      
      setIsUploading(false);
    }
  };

  const handleReset = () => {
    setFile(null);
    setPreview(null);
    setError(null);
    setUploadProgress(0);
    setIsUploading(false);
    if (fileInputRef.current) {
      fileInputRef.current.value = '';
    }
  };

  // Icon for upload
  const UploadIcon = () => (
    <svg width="64" height="64" viewBox="0 0 24 24" fill="none" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round">
      <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4" />
      <polyline points="17 8 12 3 7 8" />
      <line x1="12" y1="3" x2="12" y2="15" />
    </svg>
  );

  return (
    <div className="container">
      <div className="app-header">
        <h1 className="app-title">Chuyển đổi ảnh sang PDF</h1>
        <p className="app-subtitle">Tải lên ảnh và nhận về file PDF</p>
      </div>

      <div className="app-card">
        <div 
          className={`dropzone ${isDragActive ? 'dropzone-active' : ''}`}
          onDragEnter={handleDragEnter}
          onDragLeave={handleDragLeave}
          onDragOver={handleDragOver}
          onDrop={handleDrop}
          onClick={() => !isUploading && fileInputRef.current && fileInputRef.current.click()}
          style={{ cursor: isUploading ? 'default' : 'pointer', opacity: isUploading ? 0.7 : 1 }}
        >
          <input 
            type="file" 
            ref={fileInputRef}
            style={{ display: 'none' }} 
            accept="image/*"
            onChange={handleFileInput}
            disabled={isUploading}
          />
          
          <div className="dropzone-icon">
            <UploadIcon />
          </div>
          
          <p className="dropzone-text">
            {isDragActive ? 'Thả ảnh tại đây...' : isUploading
              ? 'Đang xử lý ảnh hiện tại...'
              : 'Kéo và thả ảnh vào đây, hoặc click để chọn ảnh'}
          </p>
          <p className="dropzone-hint">
            PNG, JPG, GIF tối đa 10MB
          </p>
        </div>
        
        {preview && (
          <div className="preview-container">
            <img 
              src={preview} 
              alt="Preview" 
              className="image-preview"
            />
            <div className="file-info">
              <span className="file-name">
                {file?.name}
              </span>
              <div className="button-group">
                <button 
                  className="button button-danger" 
                  onClick={handleReset} 
                  disabled={isUploading}
                >
                  Hủy
                </button>
                <button 
                  className="button button-primary" 
                  onClick={() => handleUpload()} 
                  disabled={!file || isUploading}
                >
                  {isUploading ? 'Đang xử lý...' : 'Tải lên lại'}
                </button>
              </div>
            </div>
          </div>
        )}
        
        {isUploading && (
          <div className="progress-container">
            <div className="progress-bar">
              <div 
                className="progress-fill" 
                style={{ width: `${uploadProgress}%` }}
              ></div>
            </div>
            <p className="progress-text">
              {uploadProgress < 100 
                ? `Đang tải lên: ${uploadProgress}%` 
                : "Đang xử lý ảnh thành PDF..."}
            </p>
          </div>
        )}
        
        {error && (
          <div className="error-message">
            {error}
          </div>
        )}
        
        <div className="help-section">
          <h3 className="help-title">Hướng dẫn</h3>
          <ul className="help-list">
            <li>Tải lên ảnh để tự động xử lý và nhận PDF</li>
            <li>Quá trình xử lý có thể mất vài giây tùy vào kích thước ảnh</li>
            <li>File PDF sẽ tự động tải về sau khi xử lý hoàn tất</li>
          </ul>
        </div>
      </div>
      
      <div className="footer">
        © 2025 Ứng dụng Chuyển đổi ảnh sang PDF. Đã bảo lưu mọi quyền.
      </div>
    </div>
  );
}

export default App;
