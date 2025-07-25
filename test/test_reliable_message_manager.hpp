#pragma once

#include <atomic>
#include <ranges>
#include <set>

#include "channel_listener.hpp"

using namespace rmq;

/**
 * The aim of this is to provide a pseudo ReliableManager whereby if there's a reconnection that
 * we can then handle this and re-transmit the messages that were previously lost.
 */
class TestReliableMessageManager : public rmq::IChannelListener
{
public:
    using TestMessage = std::vector<char>;
    using TestMessagePtr = std::shared_ptr<TestMessage>;

    TestReliableMessageManager(const uint64_t num_messages) : num_messages_(num_messages)
    {
        constexpr uint64_t min_tag = 0;
        auto vals = std::ranges::iota_view(min_tag, num_messages);
        to_tx_numbers_.insert(std::begin(vals), std::end(vals));
    }

    bool isEmpty() const { std::lock_guard lock(mutex_); return to_tx_numbers_.empty(); }
    bool isComplete() const { std::lock_guard lock(mutex_); return to_tx_numbers_.empty() && sent_messages_.empty(); }
    size_t numUnacknowledged() const { std::lock_guard lock(mutex_); return sent_messages_.size(); }
    size_t numUnsent() const { std::lock_guard lock(mutex_); return to_tx_numbers_.size(); }

    TestMessagePtr getNextMessage()
    {
        std::lock_guard lock(mutex_);
        if (to_tx_numbers_.empty()) { return nullptr; }
        auto it = to_tx_numbers_.begin();
        to_tx_numbers_.erase(it);
        sent_messages_.insert(*it);
        std::string message = "test message " + std::to_string(*it);
        auto message_vec = std::make_shared<std::vector<char> >(message.begin(), message.end());
        return message_vec;
    }

    void onConnect(const std::string& channel_name) override {
        std::lock_guard lock(mutex_);
        // Got to assume that we've lost all the transmitted data that wasn't acked
        to_tx_numbers_.insert(sent_messages_.begin(), sent_messages_.end());
        sent_messages_.clear();
        LOG_DEBUG("Num messages, " << num_messages_ << ", num unsent, " << to_tx_numbers_.size()  << ", offset calculated, " << offset_due_to_reconnect_);
        offset_due_to_reconnect_ = num_messages_ - to_tx_numbers_.size();
    }

    /**
     *
     * @param channel_name - name of channel that this is operating on
     * @param delivery_tag - starting from 1, so there will always be an offset of 1
     * @param multiple - if true, then ack all delivery tags up to and including this one
     */
    void onAcknowledgement(const std::string& channel_name, uint64_t delivery_tag, bool multiple) override {
        std::lock_guard lock(mutex_);
        auto local_val = delivery_tag + offset_due_to_reconnect_ - 1;
        if (!multiple) {
            const auto size_before = sent_messages_.size();
            sent_messages_.erase(local_val);
            num_acknowledged_ += size_before - sent_messages_.size();
        } else {
            const auto size_before = sent_messages_.size();
            sent_messages_.erase(sent_messages_.begin(),
                                sent_messages_.upper_bound(local_val));
            num_acknowledged_ += size_before - sent_messages_.size();
        }
        LOG_DEBUG("Connection: Num acknowledged, " << num_acknowledged_
            << ", num unacked, " << sent_messages_.size()
            << ", num unsent, " << to_tx_numbers_.size()
            << ", delivery_tag, " << static_cast<int>(delivery_tag)
            );
    }



    void onNumberOfTransmittedMessages(const std::string& channel_name, size_t num_transmitted) override {
        if (num_transmitted%100000 == 0) {
            LOG_INFO(channel_name << ": Number of transmitted messages: " << num_transmitted);
        }
        num_transmitted_.store(num_transmitted);
    }

    void onNumberOfReceivedMessages(const std::string& channel_name, size_t num_received) override {
        if (num_received%100000 == 0) {
            LOG_INFO(channel_name << ": Number of received messages: " << num_received);
        }
        num_received_.store(num_received);
    }

    void onNumberOfAcknowledgedMessages(const std::string& channel_name, size_t num_acknowledged) override {
        // We are using our own internal count of the number of acknowledged messages, although this should tally
    }

    void onChannelStateChange(const std::string& channel_name, ChannelState state) override {
        LOG_INFO(channel_name << ": Channel state changed to " << static_cast<int>(state));
        state_.store(state);
    }

    void onRemoteQueueSize(const std::string& channel_name, uint32_t queue_size) override {
        remote_queue_size_.store(queue_size);
    }

    size_t getNumberOfTransmittedMessages() const override {
        return num_transmitted_.load();
    }

    size_t getNumberOfReceivedMessages() const override {
        return num_received_.load();
    }

    size_t getNumberOfAcknowledgedMessages() const override {
        return num_acknowledged_.load();
    }

    size_t getRemoteQueueSize() const override {
        return remote_queue_size_.load();
    }

    bool isActive() const override {
        return state_.load() == ChannelState::active;
    }

private:
    std::atomic<size_t> remote_queue_size_{0};
    std::atomic<size_t> num_transmitted_{0};
    std::atomic<size_t> num_received_{0};
    std::atomic<size_t> num_acknowledged_{0};
    std::atomic<ChannelState> state_{ChannelState::none};

    // The aim here is that we maintain ack numbers for the current connection - too complicated otherwise
    mutable std::mutex mutex_;
    std::set<uint64_t> to_tx_numbers_; // Not been sent yet
    std::set<uint64_t> sent_messages_; // In transit, but not acked
    uint64_t offset_due_to_reconnect_{0};
    uint64_t num_messages_;
};
