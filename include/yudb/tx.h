//The MIT License(MIT)
//Copyright © 2024 https://github.com/yuyuaqwq
//
//Permission is hereby granted, free of charge, to any person obtaining a copy of this software and associated documentation files(the “Software”), to deal in the Software without restriction, including without limitation the rights to use, copy, modify, merge, publish, distribute, sublicense, and /or sell copies of the Software, and to permit persons to whom the Software is furnished to do so, subject to the following conditions :
//
//The above copyright notice and this permission notice shall be included in all copies or substantial portions of the Software.
//
//THE SOFTWARE IS PROVIDED “AS IS”, WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT.IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

#pragma once

#include <vector>
#include <shared_mutex>

#include "yudb/tx_impl.h"
#include "yudb/bucket.h"

namespace yudb {

class ViewTx : noncopyable {
public:
    ViewTx(TxManager* tx_manager, const MetaStruct& meta, std::shared_mutex* mmap_mutex);
    ~ViewTx();

    ViewBucket UserBucket();

private:
    friend class TxManager;

    std::shared_lock<std::shared_mutex> mmap_lock_;
    TxImpl tx_;
};

class UpdateTx : noncopyable {
public:
    explicit UpdateTx(TxImpl* tx);
    ~UpdateTx();

    UpdateTx(UpdateTx&& right) noexcept;

    UpdateBucket UserBucket();
    void RollBack();
    void Commit();

private:
    TxImpl* tx_;
};

} // namespace yudb