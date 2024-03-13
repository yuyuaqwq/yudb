#pragma once

#include <exception>

namespace yudb {

class Error : public std::exception {
public:
    using std::exception::exception;
};

class InvalidArgumentError : public Error {
public:
    using Error::Error;
};

class IoError : public Error {
public:
    using Error::Error;
};

class MetaError : public Error {
public:
    using Error::Error;
};

class LoggerError : public Error {
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