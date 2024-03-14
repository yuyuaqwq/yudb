#include <gtest/gtest.h>

#include "yudb/log_reader.h"
#include "yudb/log_writer.h"

namespace yudb {

TEST(LogTest, ReadWrite) {
    {
        yudb::log::Writer writer;
        writer.Open("Z:/log_test.ydb-wal");
        writer.AppendRecordToBuffer("");
        writer.AppendRecordToBuffer("abc");
        writer.AppendRecordToBuffer("0000000000");
        writer.AppendRecordToBuffer("hello world");
        writer.AppendRecordToBuffer("123213123321");
        writer.AppendRecordToBuffer(std::string(100000, '*'));
        writer.FlushBuffer();
    }

    {
        yudb::log::Reader reader;
        reader.Open("Z:/log_test.ydb-wal");
        auto res = reader.ReadRecord();
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(*res, "");

        res = reader.ReadRecord();
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(*res, "abc");

        res = reader.ReadRecord();
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(*res, "0000000000");

        res = reader.ReadRecord();
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(*res, "hello world");

        res = reader.ReadRecord();
        ASSERT_TRUE(res.has_value());
        ASSERT_EQ(*res, "123213123321");

        res = reader.ReadRecord();
        ASSERT_EQ(res->size(), 100000);
        ASSERT_EQ(std::string(100000, '*'), *res);
    }
}

} // namespace yudb