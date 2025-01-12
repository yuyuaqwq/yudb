//The MIT License(MIT)
//Copyright ? 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#include <gtest/gtest.h>

#include <wal/log_reader.h>
#include <wal/log_writer.h>

namespace yudb {

TEST(LogTest, ReadWrite) {
    {
        wal::Writer writer;
        writer.Open("Z:/log_test.ydb-wal", tinyio::access_mode::write);
        writer.AppendRecordToBuffer("");
        writer.AppendRecordToBuffer("abc");
        writer.AppendRecordToBuffer("0000000000");
        writer.AppendRecordToBuffer("hello world");
        writer.AppendRecordToBuffer("123213123321");
        writer.AppendRecordToBuffer(std::string(100000, '*'));
        writer.FlushBuffer();
    }

    {
        wal::Reader reader;
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