#pragma once

#include <exception>

namespace yudb {

class Error : public std::exception {
public:
    using std::exception::exception;
};

class IoError : public Error {
public:
    using Error::Error;
    const char* what() const {
        return Error::what();
    }
};

class InvalidArgumentError : public Error {
public:
    using Error::Error;
    const char* what() const {
        return Error::what();
    }
};

class RecoverError : public Error {
public:
    using Error::Error;
    const char* what() const {
        return Error::what();
    }
};

class CheckpointError : public Error {
public:
    using Error::Error;
    const char* what() const {
        return Error::what();
    }
};

class LogError : public Error {
public:
    using Error::Error;
    const char* what() const {
        return Error::what();
    }
};

class CacheManagerError : public Error {
public:
    using Error::Error;
    const char* what() const {
        return Error::what();
    }
};

class TxManagerError : public Error {
public:
    using Error::Error;
    const char* what() const {
        return Error::what();
    }
};

class PagerError : public Error {
public:
    using Error::Error;
    const char* what() const {
        return Error::what();
    }
};

} // namespace yudb