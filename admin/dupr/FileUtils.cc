#include "FileUtils.h"

#include <sys/mman.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <errno.h>
#include <stdio.h>
#include <stdlib.h>
#include <fcntl.h>
#include <unistd.h>

#include <stdexcept>
#include <limits>

using std::string;


namespace dupr {

InputFile::InputFile(string const & path) : _path(path), _fd(-1), _sz(-1) {
    struct stat st;
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        throw std::runtime_error("open() failed");
    }
    if (fstat(fd, &st) != 0) {
        close(fd);
        throw std::runtime_error("fstat() failed");
    }
    _fd = fd;
    _sz = st.st_size;
}

InputFile::~InputFile() {
    if (_fd != -1 && close(_fd) != 0) {
        perror("close() failed");
        exit(EXIT_FAILURE);
    }
}

void * InputFile::read(void * buf, off_t off, size_t sz) const {
    char * b = static_cast<char *>(buf);
    if (!buf) {
        b = static_cast<char *>(malloc(sz));
        if (!b) {
            throw std::runtime_error("malloc() failed");
        }
    }
    char * cur = b;
    while (sz > 0) {
        ssize_t n = pread(_fd, cur, sz, off);
        if (n == 0) {
            if (!buf) { free(b); }
            throw std::runtime_error("reached end of file before consuming input block");
        } else if (n < 0 && errno != EINTR) {
            if (!buf) { free(b); }
            throw std::runtime_error("pread() failed");
        } else if (n > 0) {
            sz -= static_cast<size_t>(n);
            off += n;
            cur += n;
        }
    }
    return b;
}

OutputFile::OutputFile(string const & path) : _path(path), _fd(-1) {
    int fd = open(path.c_str(),
                  O_TRUNC | O_CREAT | O_WRONLY,
                  S_IROTH | S_IRGRP | S_IRUSR | S_IWUSR);
    if (fd == -1) {
        throw std::runtime_error("open() failed");
    }
    _fd = fd;
}

OutputFile::~OutputFile() {
    // flush to disk
    if (fdatasync(_fd) != 0) {
        perror("fdatasync() failed");
        exit(EXIT_FAILURE);
    }
    if (close(_fd) != 0) {
        perror("close() failed");
        exit(EXIT_FAILURE);
    }
}

void OutputFile::append(void const * buf, size_t sz) {
    if (!buf || sz == 0) {
        return;
    }
    char const * b = static_cast<char const *>(buf);
    while (sz > 0) {
        ssize_t n = write(_fd, b, sz);
        if (n < 0) {
            if (errno != EINTR) {
                perror("write() failed");
                exit(EXIT_FAILURE);
            }
        } else {
            sz -= static_cast<size_t>(n);
            b += n;
        }
    }
}


namespace {
    size_t roundUp(size_t s, size_t n) {
        if (s % n != 0) {
            s += n - s % n;
        }
        return s;
    }
}

MappedInputFile::MappedInputFile(string const & path) :
    _path(path),
    _fd(-1),
    _sz(-1),
    _pageSz(static_cast<size_t>(sysconf(_SC_PAGESIZE))),
    _data(MAP_FAILED)
{
    int fd = open(path.c_str(), O_RDONLY);
    if (fd == -1) {
        throw std::runtime_error("open() failed");
    }
    struct stat st;
    if (fstat(fd, &st) != 0) {
        close(fd);
        throw std::runtime_error("fstat() failed");
    }
    if (static_cast<uintmax_t>(st.st_size) > std::numeric_limits<size_t>::max()) {
        close(fd);
        throw std::runtime_error("input file too large to mmap");
    }
    size_t sz = roundUp(static_cast<size_t>(st.st_size), _pageSz);
    _data = mmap(0, sz, PROT_READ, MAP_SHARED | MAP_NORESERVE, fd, 0);
    if (_data == MAP_FAILED) {
        close(fd);
        throw std::runtime_error("mmap() failed");
    }
    madvise(_data, sz, MADV_DONTNEED);
    _fd = fd;
    _sz = st.st_size;
}

MappedInputFile::~MappedInputFile() {
    if (_data != MAP_FAILED) {
        munmap(_data, roundUp(static_cast<size_t>(_sz), _pageSz));
        _data = MAP_FAILED;
    }
    if (_fd != -1 && close(_fd) != 0) {
        perror("close() failed");
        exit(EXIT_FAILURE);
    }
}

} // namespace dupr

