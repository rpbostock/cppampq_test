#pragma once

#include <string>

namespace rmq
{
class IMessageAck
{
public:
    IMessageAck() = default;
    virtual ~IMessageAck() = default;

    void setDeliveryTag(uint64_t tag) { delivery_tag_ = tag; }
    void setConsumerTag(const std::string &tag) { consumer_tag_ = tag; }
    void setRedelivered(bool redelivered) { redelivered_ = redelivered; }
    void setRoutingKey(const std::string &key) { routing_key_ = key; }
    void setExchange(const std::string &exchange) { exchange_ = exchange; }

    uint64_t getDeliveryTag() const { return delivery_tag_; }
    const std::string &getConsumerTag() const { return consumer_tag_; }
    bool isRedelivered() const { return redelivered_; }
    const std::string &getRoutingKey() const { return routing_key_; }
    const std::string &getExchange() const { return exchange_; }

private:
    uint64_t delivery_tag_ = 0;
    std::string consumer_tag_;
    bool redelivered_ = false;
    std::string routing_key_;
    std::string exchange_;
};
} // namespace rmq

