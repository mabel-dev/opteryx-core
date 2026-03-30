# libcurl Setup for HTTP Client Extension

The libcurl HTTP client extension requires libcurl development headers to compile.

## Installation Instructions

### macOS
```bash
# Using Homebrew (pre-installed on most systems)
brew install curl

# Or with MacPorts
sudo port install curl
```

### Ubuntu/Debian Linux
```bash
sudo apt-get update
sudo apt-get install libcurl4-openssl-dev
```

### CentOS/RHEL Linux
```bash
sudo yum install libcurl-devel
```

### Alpine Linux
```bash
apk add curl-dev
```

### Fedora
```bash
sudo dnf install libcurl-devel
```

## Verification

After installation, verify libcurl is available:

```bash
# Check if curl headers are installed
find /usr -name "curl.h" 2>/dev/null

# Or use curl-config
curl-config --version
curl-config --cflags
curl-config --libs
```

## Building the Extension

Once libcurl is installed, rebuild the extension:

```bash
python setup.py build_ext --inplace
```

## Troubleshooting

### "curl.h: No such file or directory"
- Ensure libcurl development package is installed (not just the curl binary)
- On Ubuntu: use `libcurl4-openssl-dev` not just `curl`

### Linker errors: "undefined reference to `curl_*`"
- Ensure libcurl development library is installed
- Check that `-lcurl` is being passed to linker in setup.py

### Mixed OpenSSL/LibreSSL versions
- On macOS with MacPorts, ensure consistent OpenSSL versions:
  ```bash
  port install curl +ssl
  port select openssl openssl3
  ```

## Notes

- The HTTP client uses CURLM (multi-handle) for connection pooling
- Supports both HTTP and HTTPS (curl handles this automatically)
- No additional Python dependencies needed
- Works with system libcurl on Linux/macOS
