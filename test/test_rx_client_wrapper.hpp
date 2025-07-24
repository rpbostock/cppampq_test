#pragma once
#include <algorithm>
#include <mutex>
#include <string>

#include "rx_client_wrapper.hpp"

using namespace rmq;

class TestRxClientWrapper : public rmq::RxClientWrapper {
public:
    explicit TestRxClientWrapper(const RxClientWrapper &wrapper) : RxClientWrapper(wrapper) {}
    
    // Add move constructor
    TestRxClientWrapper(TestRxClientWrapper&& other) noexcept : RxClientWrapper(std::move(other)) {
        std::lock_guard<std::mutex> lock(other.mutex_);
        rx_messages_ = std::move(other.rx_messages_);
    }
    
    // Add move assignment operator
    TestRxClientWrapper& operator=(TestRxClientWrapper&& other) noexcept {
        if (this != &other) {
            RxClientWrapper::operator=(std::move(other));
            std::lock_guard<std::mutex> lock(other.mutex_);
            rx_messages_ = std::move(other.rx_messages_);
        }
        return *this;
    }

    void emplace_back(const std::string &msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        rx_messages_.emplace_back(msg);
    }

    void emplace_back(std::string &&msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        rx_messages_.emplace_back(std::move(msg));
    }

    size_t rxMessagesSize() const {
        std::lock_guard<std::mutex> lock(mutex_);
        return rx_messages_.size();
    }
private:
    mutable std::mutex mutex_;
    std::vector<std::string> rx_messages_;
};