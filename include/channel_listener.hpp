#pragma once
#include <string>
#include <logging.hpp>
#include "channel_state.hpp"

namespace rmq
{

class IChannelListener;
using ChannelListenerPtr = std::shared_ptr<IChannelListener>;

class IChannelListener {
public:
    virtual ~IChannelListener() = default;

    virtual void onNumberOfTransmittedMessages(const std::string& channel_name, size_t num_transmitted) = 0;
    virtual void onNumberOfReceivedMessages(const std::string& channel_name, size_t num_received) = 0;
    virtual void onNumberOfAcknowledgedMessages(const std::string& channel_name, size_t num_acknowledged) = 0;
    virtual void onAcknowledgement(const std::string& channel_name, uint64_t delivery_tag, bool multiple) = 0;
    virtual void onConnect(const std::string& channel_name) = 0;
    virtual void onChannelStateChange(const std::string& channel_name, rmq::ChannelState state) = 0;
    virtual void onRemoteQueueSize(const std::string& channel_name, uint32_t queue_size) = 0;

    virtual size_t getNumberOfTransmittedMessages() const = 0;
    virtual size_t getNumberOfReceivedMessages() const = 0;
    virtual size_t getNumberOfAcknowledgedMessages() const = 0;
    virtual size_t getRemoteQueueSize() const = 0;
    virtual bool isActive() const = 0;
};

class SimpleChannelListener : public IChannelListener {
public:
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
        if (num_acknowledged%100000 == 0) {
            LOG_INFO(channel_name << ": Number of acknowledged messages: " << num_acknowledged);
        }
        num_acknowledged_.store(num_acknowledged);
    }

    void onAcknowledgement(const std::string& channel_name, uint64_t delivery_tag, bool multiple) override
    {
    }

    void onConnect(const std::string& channel_name) override
    {
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
};

}