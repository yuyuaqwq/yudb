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
};

class InvalidArgumentError : public Error {
public:
    using Error::Error;
};

class LoggerError : public Error {
public:
    using Error::Error;
};

class CheckpointError : public Error {
public:
    using Error::Error;
};

class LogError : public Error {
public:
    using Error::Error;
};

class CacheManagerError : public Error {
public:
    using Error::Error;
};

class TxManagerError : public Error {
public:
    using Error::Error;
};

class PagerError : public Error {
public:
    using Error::Error;
};

} // namespace yudb