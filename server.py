import socket
import threading
import os
import time
import email.utils
import urllib.parse
from datetime import datetime

SERVER_ROOT = "www"          # Directory where web content is stored

# Global lock for thread-safe logging
log_lock = threading.Lock()

def get_mime_type(file_path):
    ext = os.path.splitext(file_path)[1].lower()
    mime_types = {
        '.html': 'text/html',
        '.txt': 'text/plain',
        '.css': 'text/css',
        '.js': 'application/javascript',
        '.jpg': 'image/jpeg',
        '.jpeg': 'image/jpeg',
        '.png': 'image/png',
        '.gif': 'image/gif',
        '.ico': 'image/x-icon',
        '.pdf': 'application/pdf',
    }
    return mime_types.get(ext, 'application/octet-stream')

def format_http_date(timestamp):
    return email.utils.formatdate(timestamp, usegmt=True)

def log_request(client_ip, request_file, status_code, access_time=None):
    """Format: client_ip | access_time | requested_file | response_status"""
    if access_time is None:
        access_time = datetime.now().isoformat(' ', timespec='seconds')
    log_line = f"{client_ip} | {access_time} | {request_file} | {status_code}\n"
    
    with log_lock:
        try:
            with open("server.log", 'a', encoding='utf-8') as log_f:
                log_f.write(log_line)
        except Exception as e:
            print(f"[LOG ERROR] {e}")

def read_http_request(client_sock):
    request_data = b''
    try:
        while b'\r\n\r\n' not in request_data:
            chunk = client_sock.recv(65536)
            if not chunk:
                return None
            request_data += chunk
        return request_data
    except socket.timeout:
        return None

def parse_request(request_bytes):
    try:
        request_str = request_bytes.decode('utf-8', errors='replace')
        lines = request_str.split('\r\n')
        parts = lines[0].strip().split()
        method, path, version = parts
        # Only accept GET and HEAD
        if method not in ['GET', 'HEAD']:
            return None, None, None, None, 400
        
        # Validate HTTP version (1.0 or 1.1)
        if version not in ['HTTP/1.0', 'HTTP/1.1']:
            return None, None, None, None, 400
        
        # Parse headers
        headers = {}
        for line in lines[1:]:
            if ':' in line:
                key, value = line.split(':', 1)
                headers[key.strip().lower()] = value.strip()
        
        return method, path, version, headers, None
    except Exception:
        return None, None, None, None, 400

def is_keep_alive(http_version, headers):
    conn_header = headers.get('connection', '').lower()
    if conn_header == 'close':
        return False
    if conn_header == 'keep-alive':
        return True
    # Default: HTTP/1.1 keep-alive, HTTP/1.0 close
    return http_version == 'HTTP/1.1'

def build_response_headers(status_code, content_type=None, content_length=None, last_modified=None, connection='close', extra_headers=None):
    status_text = {
        200: 'OK',
        304: 'Not Modified',
        400: 'Bad Request',
        403: 'Forbidden',
        404: 'Not Found'
    }.get(status_code, 'Internal Server Error')
    
    status_line = f"HTTP/1.1 {status_code} {status_text}\r\n"
    headers = {
        'Date': format_http_date(time.time()),
        'Server': 'COMP2322WebServer/1.0',
        'Connection': connection
    }
    if content_type:
        headers['Content-Type'] = content_type
    if content_length is not None:
        headers['Content-Length'] = str(content_length)
    if last_modified:
        headers['Last-Modified'] = last_modified
        
    if extra_headers:
        headers.update(extra_headers)
    
    header_str = status_line + ''.join(f'{k}: {v}\r\n' for k, v in headers.items()) + '\r\n'
    return header_str.encode('utf-8')

def send_error_response(client_sock, status_code, connection, client_ip, request_file):
    error_messages = {
        400: 'Bad Request',
        403: 'Forbidden',
        404: 'Not Found'
    }
    error_body = f"""
    <!DOCTYPE html>
    <html>
    <head><title>{status_code} {error_messages[status_code]}</title></head>
    <body>
    <h1>{status_code} {error_messages[status_code]}</h1>
    </body>
    </html>
    """.encode('utf-8')
    
    headers = build_response_headers(
        status_code=status_code,
        content_type='text/html',
        content_length=len(error_body),
        connection=connection
    )
    try:
        client_sock.sendall(headers)
        client_sock.sendall(error_body)
    except Exception:
        pass
    
    # Log the error
    log_request(client_ip, request_file, status_code)

def send_304_response(client_sock, connection, client_ip, request_file):
    headers = build_response_headers(
        status_code=304,
        connection=connection
    )
    try:
        client_sock.sendall(headers)
    except Exception:
        pass
    log_request(client_ip, request_file, 304)

def send_file_response(client_sock, file_path, method, headers_req, connection, client_ip, request_path):
    # Get file stats
    try:
        stat = os.stat(file_path)
        file_size = stat.st_size
        last_modified_timestamp = stat.st_mtime
        last_modified_str = format_http_date(last_modified_timestamp)
    except OSError:
        send_error_response(client_sock, 404, connection, client_ip, request_path)
        return False
    
    # Check If-Modified-Since header
    if_modified_since = headers_req.get('if-modified-since')
    if if_modified_since:
        try:
            # Parse the header value to a timestamp
            since_time = email.utils.parsedate_to_datetime(if_modified_since).timestamp()
            # If file not modified since that time, return 304
            if last_modified_timestamp <= since_time + 1:  # +1 second tolerance for rounding
                send_304_response(client_sock, connection, client_ip, request_path)
                return False
        except (TypeError, ValueError, OverflowError):
            # Invalid date, ignore header
            pass
    
    # Prepare headers
    content_type = get_mime_type(file_path)
    headers = build_response_headers(
        status_code=200,
        content_type=content_type,
        content_length=file_size,
        last_modified=last_modified_str,
        connection=connection
    )
    
    try:
        client_sock.sendall(headers)
        # For HEAD method, stop here (no body)
        if method == 'HEAD':
            log_request(client_ip, request_path, 200)
            return True
        
        # Send file content in chunks for GET
        with open(file_path, 'rb') as f:
            while True:
                chunk = f.read(65536)
                if not chunk:
                    break
                client_sock.sendall(chunk)
        log_request(client_ip, request_path, 200)
        return True
    except Exception as e:
        print(f"[ERROR] Failed to send file {file_path}: {e}")
        return False

def handle_client(client_sock, client_addr):
    client_ip = client_addr[0]
    print(f"[CONNECTION] New client from {client_ip}")
    
    try:
        # Set socket timeout for idle keep-alive connections
        client_sock.settimeout(5)
        
        while True:
            # Read the HTTP request
            request_bytes = read_http_request(client_sock)
            if request_bytes is None:
                print(f"[CLOSE] Client {client_ip} closed connection or timeout")
                break
            
            # Parse request
            method, path, version, headers, parse_error = parse_request(request_bytes)
            if parse_error:
                send_error_response(client_sock, 400, 'close', client_ip, 'unknown')
                break
            
            # Decode URL
            try:
                decoded_path = urllib.parse.unquote(path)
            except Exception:
                send_error_response(client_sock, 400, 'close', client_ip, path)
                break
            
            # Determine keep-alive behavior for this request
            keep_alive = is_keep_alive(version, headers)
            connection_header = 'keep-alive' if keep_alive else 'close'
            
            # default to index.txt if root requested
            if decoded_path.startswith('/'):
                decoded_path = decoded_path[1:]
            if not decoded_path:
                decoded_path = 'index.txt'
            
            # Check if file exists and is a regular file (not a directory)
            full_path = os.path.normpath(os.path.join(SERVER_ROOT, decoded_path))
            if not os.path.exists(full_path):
                send_error_response(client_sock, 404, connection_header, client_ip, decoded_path)
            elif not os.path.isfile(full_path):
                send_error_response(client_sock, 403, connection_header, client_ip, decoded_path)
            else:
                send_file_response(client_sock, full_path, method, headers, connection_header, client_ip, decoded_path)
            
            # If connection is non-persistent, break out of the loop
            if not keep_alive:
                print(f"[CLOSE] Non-persistent connection from {client_ip} closed")
                break
            
    except socket.error as e:
        print(f"[SOCKET ERROR] {client_ip}: {e}")
    finally:
        try:
            client_sock.close()
        except Exception:
            pass
        print(f"[DISCONNECT] {client_ip}")

def start_server(port):    
    # Create server socket
    server_sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server_sock.bind(('0.0.0.0', port))
    server_sock.listen(10)
    print(f"[SERVER] Listening on 0.0.0.0:{port}")
    print("[SERVER] Multi-threaded web server running")
    
    try:
        while True:
            client_sock, client_addr = server_sock.accept()
            # Create a new thread for each client connection
            client_thread = threading.Thread(target=handle_client, args=(client_sock, client_addr), daemon=True)
            client_thread.start()
    except KeyboardInterrupt:
        print("\n[SERVER] Shutting down...")
    finally:
        server_sock.close()

if __name__ == '__main__':
    start_server(80)