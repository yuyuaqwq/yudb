#pragma once

#include <exception>

namespace yudb {

class Error : public std::exception {
public:
    Error(const char* message) : msg(message) {}
    virtual const char* what() const noexcept override {
        return msg.c_str();
    }
private:
    std::string msg;
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
