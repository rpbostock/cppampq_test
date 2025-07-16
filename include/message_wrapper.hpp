#pragma once

#include "i_message_ack.hpp"

namespace rmq
{
/**
 * @brief Wrapper class that combines a message with its acknowledgment information.
 * @tparam MessageType The type of message being wrapped
 */
template<typename MessageType> class MessageWrapper
{
public:
	MessageWrapper() = default;
	MessageWrapper(MessageType msg, IMessageAck ack)
		: message_(std::move(msg)), ack_(std::move(ack))
	{
	}

	const MessageType &getMessage() const { return message_; }
	MessageType &getMessage() { return message_; }
	const IMessageAck &getAck() const { return ack_; }
	IMessageAck &getAck() { return ack_; }

	void setMessage(MessageType msg) { message_ = std::move(msg); }
	void setAck(IMessageAck ack) { ack_ = std::move(ack); }

private:
	MessageType message_;
	IMessageAck ack_;
};


}