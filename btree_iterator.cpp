#include "btree_iterator.h"

#include "btree.h"

namespace yudb {

BTreeIterator::reference BTreeIterator::operator*() const noexcept {
    return *this;
}

const BTreeIterator* BTreeIterator::operator->() const noexcept {
    return this;
}

BTreeIterator& BTreeIterator::operator++() noexcept {
    Next();
    return *this;
}

BTreeIterator BTreeIterator::operator++(int) noexcept {
    BTreeIterator tmp = *this;
    tmp.Next();
    return tmp;
}

BTreeIterator& BTreeIterator::operator--() noexcept {
    Prev();
    return *this;
}

BTreeIterator BTreeIterator::operator--(int) noexcept {
    BTreeIterator tmp = *this;
    tmp.Prev();
    return tmp;
}

bool BTreeIterator::operator==(const BTreeIterator& right) const noexcept {
    if (Empty() || right.Empty()) {
        return Empty() && right.Empty();
    }
    auto& [pgid, index] = Cur();
    auto& [pgid2, index2] = right.Cur();
    return btree_ == right.btree_ && pgid == pgid2 && index == index2;
}



std::string BTreeIterator::key() const {
    auto [buf, size, ref] = KeySpan();
    std::string val{ reinterpret_cast<const char*>(buf), size };
    return val;
}

std::string BTreeIterator::value() const {
    auto [buf, size, ref] = ValueSpan();
    std::string val{ reinterpret_cast<const char*>(buf), size };
    return val;
}


void BTreeIterator::First(PageId pgid) {
    do {
        Noder noder{ btree_, pgid };
        auto node = noder.node();
        if (noder.IsLeaf()) {
            break;
        }
        stack_.push_back({ pgid, 0 });
        assert(node->element_count > 0);
        pgid = node->body.branch[0].left_child;
    } while (true);
    Noder noder{ btree_, pgid };
    auto node = noder.node();
    stack_.push_back({ pgid, 0 });
}

void BTreeIterator::Last(PageId pgid) {
    do {
        Noder noder{ btree_, pgid };
        auto node = noder.node();
        if (noder.IsLeaf()) {
            break;
        }
        stack_.push_back({ pgid, node->element_count - 1 });
        assert(node->element_count > 0);
        pgid = node->body.branch[node->element_count - 1].left_child;
    } while (true);
    Noder noder{ btree_, pgid };
    auto node = noder.node();
    stack_.push_back({ pgid, node->element_count - 1 });
}

void BTreeIterator::Next() {
    do {
        auto& [pgid, index] = Cur();
        Noder noder{ btree_, pgid };
        auto node = noder.node();

        if (++index < node->element_count) {
            if (noder.IsLeaf()) {
                return;
            }
            First(node->body.branch[index].left_child);
            return;
        }
        if (noder.IsBranch() && index == node->element_count) {
            First(node->body.tail_child);
            return;
        }
        Pop();
    } while (!Empty());
}

void BTreeIterator::Prev() {
    do {
        auto& [pgid, index] = Cur();
        Noder noder{ btree_, pgid };
        auto node = noder.node();

        if (index > 0) {
            --index;
            if (noder.IsLeaf()) {
                return;
            }
            Last(node->body.branch[index].left_child);
            return;
        }
        Pop();
    } while (!Empty());
}


std::tuple<const uint8_t*, size_t, std::optional<std::variant<PageReferencer, std::vector<uint8_t>>>>
BTreeIterator::KeySpan() const {
    assert(*this != btree_->end());
    auto& [pgid, index] = Cur();
    Noder noder{ btree_, pgid };
    if (!noder.IsLeaf()) {
        throw std::runtime_error("not pointing to valid data");
    }
    auto res = noder.SpanLoad(noder.node()->body.leaf[index].key);
    return res;
}

std::tuple<const uint8_t*, size_t, std::optional<std::variant<PageReferencer, std::vector<uint8_t>>>>
BTreeIterator::ValueSpan() const {
    assert(*this != btree_->end());
    auto& [pgid, index] = Cur();
    Noder noder{ btree_, pgid };
    if (!noder.IsLeaf()) {
        throw std::runtime_error("not pointing to valid data");
    }
    auto res = noder.SpanLoad(noder.node()->body.leaf[index].value);
    return res;
}


BTreeIterator::Status BTreeIterator::Top(std::span<const uint8_t> key) {
    return Down(key);
}

BTreeIterator::Status BTreeIterator::Down(std::span<const uint8_t> key) {
    PageId pgid;
    if (stack_.empty()) {
        pgid = btree_->root_pgid_;
        if (pgid == kPageInvalidId) {
            return Status::kEnd;
        }
    }
    else {
        auto [parent_pgid, pos] = stack_.front();
        Noder parent{ btree_, parent_pgid };
        auto parent_node = parent.node();
        if (parent.IsLeaf()) {
            return Status::kEnd;
        }
        pgid = parent.BranchGetLeftChild(pos);
    }
    Noder noder{ btree_, pgid };
    auto node = noder.node();

    // 在节点中进行二分查找
    uint16_t index;
    comp_result_ = CompResult::kInvalid;
    if (node->element_count > 0) {
        CompResult temp_comp_result = CompResult::kInvalid;
        auto iter = std::lower_bound(noder.begin(), noder.end(), key, [&](const Span& span, std::span<const uint8_t> search_key) -> bool {
            auto [buf, size, ref] = noder.SpanLoad(span);
            auto res = memcmp(buf, search_key.data(), std::min(size, search_key.size()));
            if (res == 0 && size != search_key.size()) {
                temp_comp_result = size < search_key.size() ? CompResult::kLt : CompResult::kGt;
                return temp_comp_result == CompResult::kLt;
            }
            if (res != 0) {
                temp_comp_result = res < 0 ? CompResult::kLt : CompResult::kGt;
                return temp_comp_result == CompResult::kLt;
            }
            else {
                comp_result_ = CompResult::kEq;
                return false;
            }
            });
        if (comp_result_ == CompResult::kInvalid) {
            comp_result_ = temp_comp_result;
        }
        index = iter.index();
        if (comp_result_ == CompResult::kEq && noder.IsBranch()) {
            ++index;
        }
    }
    else {
        index = 0;
    }
    stack_.push_back(std::pair{ pgid, index });
    return Status::kDown;
}


std::pair<PageId, uint16_t>& BTreeIterator::Cur() {
    return stack_.front();
}

const std::pair<PageId, uint16_t>& BTreeIterator::Cur() const {
    return stack_.front();
}


void BTreeIterator::Pop() {
    stack_.pop_back();
}

bool BTreeIterator::Empty() const {
    return stack_.empty();
}

} // namespace yudb