#ifndef FILEUTILS_H
#define FILEUTILS_H

#include <sys/types.h>
#include <stdint.h>
#include <string>


namespace dupr {

/// An input file. Safe for use from multiple threads.
class InputFile {
public:
    InputFile(std::string const & path);
    ~InputFile();

    /// Return the size of the input file.
    off_t size() const { return _sz; }

    /// Return the path of the input file.
    std::string const & path() const { return _path; }

    /// Read a range of bytes into buf and return buf. If buf
    /// is null, a buffer of sz bytes is malloc-ed and returned.
    void * read(void * buf, off_t off, size_t sz) const;

private:
    // Disable copy construction and assignment.
    InputFile(InputFile const &);
    InputFile & operator=(InputFile const &);

    std::string _path;
    int _fd;
    off_t _sz;
};


/** An output file. Must be used by a single thread at a time. IO errors
    are treated as unrecoverable and will cause a process exit rather than
    raising an exception.
  */
class OutputFile {
public:
    OutputFile(std::string const & path);
    ~OutputFile();

    /// Return the path of the output file.
    std::string const & path() const { return _path; }

    /// Append sz bytes from buf to the file.
    void append(void const * buf, size_t sz);

private:
    // Disable copy construction and assignment.
    OutputFile(OutputFile const &);
    OutputFile & operator=(OutputFile const &);

    std::string _path;
    int _fd;
};


/// A memory mapped input file.
class MappedInputFile {
public:
    MappedInputFile(std::string const & path);
    ~MappedInputFile();

    /// Return the size of the input file.
    size_t size() const { return _sz; }

    /// Return the path of the input file.
    std::string const & path() const { return _path; }

    /// Return a pointer to the file data.
    char const * data() const { return static_cast<char *>(_data); }

private:
    // Disable copy construction and assignment.
    MappedInputFile(MappedInputFile const &);
    MappedInputFile & operator=(MappedInputFile const &);

    std::string _path;
    int _fd;
    size_t _sz;
    size_t const _pageSz;
    void * _data;
};

} // namespace dupr

#endif // FILEUTILS_H
